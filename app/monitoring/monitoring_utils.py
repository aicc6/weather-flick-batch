"""
모니터링 유틸리티

모니터링 시스템을 쉽게 설정하고 사용할 수 있는 유틸리티 함수들을 제공합니다.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional

from .monitoring_system import MonitoringSystem, MonitoringConfig, get_monitoring_system
from .notification_channels import create_notification_manager_from_config
from .batch_job_monitor import BatchJobMonitor


def load_monitoring_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """모니터링 설정 파일 로드"""
    
    if config_path is None:
        # 기본 설정 파일 경로들 시도
        project_root = Path(__file__).parent.parent.parent
        possible_paths = [
            project_root / "config" / "monitoring.json",
            project_root / "monitoring.json",
            Path("config/monitoring.json"),
            Path("monitoring.json")
        ]
        
        for path in possible_paths:
            if path.exists():
                config_path = str(path)
                break
    
    if not config_path or not Path(config_path).exists():
        logging.warning("모니터링 설정 파일을 찾을 수 없습니다. 기본 설정을 사용합니다.")
        return get_default_config()
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logging.info(f"모니터링 설정 로드 완료: {config_path}")
        return config
    except Exception as e:
        logging.error(f"모니터링 설정 로드 실패: {e}")
        return get_default_config()


def get_default_config() -> Dict[str, Any]:
    """기본 모니터링 설정 반환"""
    return {
        "monitoring": {
            "check_interval": 30,
            "api_key_usage_threshold": 0.8,
            "api_key_critical_threshold": 0.95,
            "db_connection_timeout": 10,
            "db_pool_usage_threshold": 0.8,
            "memory_warning_mb": 500,
            "memory_critical_mb": 1000,
            "cpu_warning_threshold": 70.0,
            "cpu_critical_threshold": 90.0,
            "disk_warning_threshold": 80.0,
            "job_timeout_minutes": 60,
            "job_failure_threshold": 3,
            "enable_email_alerts": False,
            "enable_slack_alerts": False,
            "enable_log_alerts": True,
            "alert_cooldown_minutes": 5,
            "max_alerts_per_hour": 20
        },
        "notifications": {
            "email": {"enabled": False},
            "slack": {"enabled": False},
            "webhook": {"enabled": False}
        },
        "batch_jobs": {
            "job_history_limit": 1000,
            "timeout_minutes": 60,
            "failure_threshold": 3,
            "success_rate_threshold": 0.9
        }
    }


def setup_monitoring_system(config_path: Optional[str] = None) -> MonitoringSystem:
    """모니터링 시스템 설정 및 초기화"""
    
    # 설정 로드
    config = load_monitoring_config(config_path)
    
    # 모니터링 설정 생성
    monitoring_config = MonitoringConfig(**config.get('monitoring', {}))
    
    # 모니터링 시스템 생성
    monitoring_system = get_monitoring_system(monitoring_config)
    
    # 알림 매니저 설정
    if 'notifications' in config:
        notification_manager = create_notification_manager_from_config(config['notifications'])
        
        # 기존 AlertManager의 알림 채널들을 업데이트
        monitoring_system.alert_manager._send_alert = lambda alert: notification_manager.send_alert(alert)
    
    logging.info("모니터링 시스템 설정 완료")
    return monitoring_system


def setup_batch_job_monitor(
    monitoring_system: MonitoringSystem, 
    config: Optional[Dict[str, Any]] = None
) -> BatchJobMonitor:
    """배치 작업 모니터 설정"""
    
    if config is None:
        config = load_monitoring_config().get('batch_jobs', {})
    
    batch_monitor = BatchJobMonitor(monitoring_system.alert_manager, config)
    
    # 모니터링 시스템에 배치 모니터 등록
    monitoring_system.batch_monitor = batch_monitor
    
    logging.info("배치 작업 모니터 설정 완료")
    return batch_monitor


def start_monitoring_system(config_path: Optional[str] = None) -> MonitoringSystem:
    """모니터링 시스템 시작 (원스톱 설정)"""
    
    # 로깅 설정
    setup_monitoring_logging()
    
    # 모니터링 시스템 설정
    monitoring_system = setup_monitoring_system(config_path)
    
    # 배치 작업 모니터 설정
    batch_monitor = setup_batch_job_monitor(monitoring_system)
    
    # 모니터링 시작
    monitoring_system.start_monitoring()
    batch_monitor.start_monitoring()
    
    logging.info("🚨 실시간 모니터링 시스템이 시작되었습니다.")
    return monitoring_system


def stop_monitoring_system():
    """모니터링 시스템 중지"""
    
    monitoring_system = get_monitoring_system()
    
    if hasattr(monitoring_system, 'batch_monitor'):
        monitoring_system.batch_monitor.stop_monitoring()
    
    monitoring_system.stop_monitoring()
    
    logging.info("모니터링 시스템이 중지되었습니다.")


def setup_monitoring_logging():
    """모니터링용 로깅 설정"""
    
    # 로그 디렉토리 생성
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # 모니터링 전용 로거 설정
    monitoring_logger = logging.getLogger("monitoring")
    monitoring_logger.setLevel(logging.INFO)
    
    # 파일 핸들러 (모니터링 로그)
    monitor_handler = logging.FileHandler(log_dir / "monitoring.log")
    monitor_handler.setLevel(logging.INFO)
    
    # 알림 전용 핸들러
    alert_handler = logging.FileHandler(log_dir / "monitoring_alerts.log")
    alert_handler.setLevel(logging.WARNING)
    
    # 포맷터 설정
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    monitor_handler.setFormatter(formatter)
    alert_handler.setFormatter(formatter)
    
    # 핸들러 추가
    monitoring_logger.addHandler(monitor_handler)
    monitoring_logger.addHandler(alert_handler)
    
    # 알림 전용 로거
    alert_logger = logging.getLogger("monitoring.alerts")
    alert_logger.setLevel(logging.WARNING)
    alert_logger.addHandler(alert_handler)


def get_system_status_summary() -> Dict[str, Any]:
    """시스템 상태 요약 조회"""
    
    try:
        monitoring_system = get_monitoring_system()
        status = monitoring_system.get_system_status()
        
        # 요약 정보 생성
        summary = {
            'monitoring_active': status['monitoring_active'],
            'last_update': status['last_update'],
            'overall_status': 'healthy',
            'component_summary': {},
            'alert_summary': status['alert_summary']
        }
        
        # 구성 요소별 상태 요약
        for component_name, component_status in status['components'].items():
            if component_status:
                summary['component_summary'][component_name] = {
                    'status': component_status['status'],
                    'last_check': component_status['last_check'],
                    'alert_count': len(component_status['alerts'])
                }
                
                # 전체 상태 결정
                if component_status['status'] in ['critical', 'error']:
                    summary['overall_status'] = 'critical'
                elif component_status['status'] == 'warning' and summary['overall_status'] == 'healthy':
                    summary['overall_status'] = 'warning'
        
        # 배치 작업 상태 추가
        if hasattr(monitoring_system, 'batch_monitor'):
            batch_stats = monitoring_system.batch_monitor.get_job_stats()
            summary['batch_jobs'] = {
                'running_jobs': batch_stats.running_jobs,
                'success_rate': batch_stats.overall_success_rate,
                'avg_duration': batch_stats.avg_duration
            }
        
        return summary
        
    except Exception as e:
        logging.error(f"시스템 상태 요약 조회 실패: {e}")
        return {
            'monitoring_active': False,
            'overall_status': 'error',
            'error': str(e)
        }


def create_monitoring_dashboard_data() -> Dict[str, Any]:
    """모니터링 대시보드용 데이터 생성"""
    
    try:
        monitoring_system = get_monitoring_system()
        
        # 기본 시스템 상태
        system_status = monitoring_system.get_system_status()
        
        # 대시보드 데이터 구성
        dashboard_data = {
            'timestamp': system_status['last_update'],
            'overall_status': 'healthy',
            'components': {},
            'alerts': {
                'active': system_status['active_alerts'],
                'summary': system_status['alert_summary']
            },
            'metrics': {}
        }
        
        # 구성 요소별 데이터 처리
        for component_name, component_status in system_status['components'].items():
            if component_status:
                dashboard_data['components'][component_name] = {
                    'status': component_status['status'],
                    'last_check': component_status['last_check'],
                    'metrics': component_status['metrics'],
                    'alert_count': len(component_status['alerts'])
                }
                
                # 전체 상태 업데이트
                if component_status['status'] in ['critical', 'error']:
                    dashboard_data['overall_status'] = 'critical'
                elif component_status['status'] == 'warning' and dashboard_data['overall_status'] == 'healthy':
                    dashboard_data['overall_status'] = 'warning'
        
        # 배치 작업 데이터 추가
        if hasattr(monitoring_system, 'batch_monitor'):
            batch_summary = monitoring_system.batch_monitor.get_job_summary()
            dashboard_data['batch_jobs'] = batch_summary
        
        return dashboard_data
        
    except Exception as e:
        logging.error(f"대시보드 데이터 생성 실패: {e}")
        return {
            'timestamp': None,
            'overall_status': 'error',
            'error': str(e)
        }


def export_monitoring_config(output_path: str):
    """현재 모니터링 설정을 파일로 내보내기"""
    
    try:
        monitoring_system = get_monitoring_system()
        
        config_data = {
            'monitoring': monitoring_system.config.__dict__,
            'timestamp': monitoring_system.get_system_status()['last_update'],
            'version': '1.0'
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config_data, f, indent=2, default=str)
        
        logging.info(f"모니터링 설정 내보내기 완료: {output_path}")
        
    except Exception as e:
        logging.error(f"모니터링 설정 내보내기 실패: {e}")


def validate_monitoring_config(config_path: str) -> Dict[str, Any]:
    """모니터링 설정 파일 검증"""
    
    result = {
        'valid': True,
        'errors': [],
        'warnings': []
    }
    
    try:
        config = load_monitoring_config(config_path)
        
        # 필수 섹션 확인
        required_sections = ['monitoring', 'notifications']
        for section in required_sections:
            if section not in config:
                result['errors'].append(f"필수 섹션 누락: {section}")
                result['valid'] = False
        
        # 모니터링 설정 검증
        if 'monitoring' in config:
            monitoring_config = config['monitoring']
            
            # 임계값 범위 확인
            if monitoring_config.get('api_key_usage_threshold', 0.8) >= monitoring_config.get('api_key_critical_threshold', 0.95):
                result['warnings'].append("API 키 사용량 임계값이 위험 임계값보다 높거나 같습니다.")
            
            # 타임아웃 값 확인
            if monitoring_config.get('check_interval', 30) < 10:
                result['warnings'].append("체크 간격이 너무 짧습니다 (10초 미만).")
        
        # 알림 설정 검증
        if 'notifications' in config:
            notifications = config['notifications']
            
            # 활성화된 알림 채널 확인
            enabled_channels = [
                channel for channel, settings in notifications.items()
                if isinstance(settings, dict) and settings.get('enabled', False)
            ]
            
            if not enabled_channels:
                result['warnings'].append("활성화된 알림 채널이 없습니다.")
        
    except Exception as e:
        result['valid'] = False
        result['errors'].append(f"설정 파일 로드 실패: {e}")
    
    return result