"""
실시간 모니터링 시스템

Weather Flick 배치 시스템의 모든 구성 요소를 실시간으로 모니터링하고
문제 발생 시 즉시 알림을 보내는 중앙집중식 모니터링 시스템입니다.
"""

import logging
import threading
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from enum import Enum
import psutil

from app.core.multi_api_key_manager import get_api_key_manager
from app.core.database_connection_pool import get_connection_pool


class AlertLevel(Enum):
    """알림 수준"""
    INFO = "info"
    WARNING = "warning"  
    ERROR = "error"
    CRITICAL = "critical"


# AlertSeverity는 AlertLevel의 별칭
AlertSeverity = AlertLevel


class ComponentType(Enum):
    """모니터링 대상 구성 요소"""
    API_KEYS = "api_keys"
    DATABASE = "database"
    BATCH_JOBS = "batch_jobs"
    MEMORY = "memory"
    SYSTEM = "system"
    NETWORK = "network"


@dataclass
class Alert:
    """알림 정보"""
    id: str
    timestamp: datetime
    level: AlertLevel
    component: ComponentType
    title: str
    message: str
    details: Dict[str, Any]
    resolved: bool = False
    resolved_at: Optional[datetime] = None


@dataclass
class ComponentStatus:
    """구성 요소 상태"""
    component: ComponentType
    status: str  # healthy, warning, error, critical
    last_check: datetime
    metrics: Dict[str, Any]
    alerts: List[Alert]


@dataclass
class MonitoringConfig:
    """모니터링 설정"""
    
    # 체크 간격 (초)
    check_interval: int = 30
    
    # API 키 모니터링
    api_key_usage_threshold: float = 0.8  # 80% 사용량에서 경고
    api_key_critical_threshold: float = 0.95  # 95% 사용량에서 위험
    
    # 데이터베이스 모니터링
    db_connection_timeout: int = 10  # 연결 타임아웃 (초)
    db_pool_usage_threshold: float = 0.8  # 풀 사용량 임계값
    
    # 메모리 모니터링
    memory_warning_mb: int = 500
    memory_critical_mb: int = 1000
    
    # 시스템 모니터링
    cpu_warning_threshold: float = 70.0  # CPU 사용률 경고 임계값
    cpu_critical_threshold: float = 90.0  # CPU 사용률 위험 임계값
    disk_warning_threshold: float = 80.0  # 디스크 사용률 경고 임계값
    
    # 배치 작업 모니터링
    job_timeout_minutes: int = 60  # 작업 타임아웃 (분)
    job_failure_threshold: int = 3  # 연속 실패 횟수 임계값
    
    # 알림 설정
    enable_email_alerts: bool = False
    enable_slack_alerts: bool = False
    enable_log_alerts: bool = True
    
    # 알림 제한 (스팸 방지)
    alert_cooldown_minutes: int = 5
    max_alerts_per_hour: int = 20


class AlertManager:
    """알림 관리자"""
    
    def __init__(self, config: MonitoringConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: List[Alert] = []
        self.alert_cooldowns: Dict[str, datetime] = {}
        self.alert_count_per_hour: Dict[str, int] = {}
        
    def create_alert(
        self,
        component: ComponentType,
        level: AlertLevel,
        title: str,
        message: str,
        details: Dict[str, Any] = None
    ) -> Alert:
        """알림 생성"""
        
        alert_id = f"{component.value}_{level.value}_{int(time.time())}"
        
        alert = Alert(
            id=alert_id,
            timestamp=datetime.utcnow(),
            level=level,
            component=component,
            title=title,
            message=message,
            details=details or {},
        )
        
        # 쿨다운 체크
        cooldown_key = f"{component.value}_{title}"
        if self._is_in_cooldown(cooldown_key):
            self.logger.debug(f"알림 쿨다운 중: {title}")
            return alert
        
        # 시간당 알림 제한 체크
        if self._exceeds_hourly_limit():
            self.logger.warning("시간당 알림 제한 초과")
            return alert
        
        # 알림 발송
        self._send_alert(alert)
        
        # 알림 저장
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # 쿨다운 설정
        self.alert_cooldowns[cooldown_key] = datetime.utcnow()
        
        # 시간당 카운터 증가
        hour_key = datetime.utcnow().strftime("%Y%m%d%H")
        self.alert_count_per_hour[hour_key] = self.alert_count_per_hour.get(hour_key, 0) + 1
        
        return alert
    
    def resolve_alert(self, alert_id: str):
        """알림 해결"""
        if alert_id in self.active_alerts:
            alert = self.active_alerts[alert_id]
            alert.resolved = True
            alert.resolved_at = datetime.utcnow()
            del self.active_alerts[alert_id]
            
            self.logger.info(f"알림 해결됨: {alert.title}")
    
    def _is_in_cooldown(self, cooldown_key: str) -> bool:
        """쿨다운 중인지 확인"""
        if cooldown_key not in self.alert_cooldowns:
            return False
        
        last_alert = self.alert_cooldowns[cooldown_key]
        cooldown_end = last_alert + timedelta(minutes=self.config.alert_cooldown_minutes)
        return datetime.utcnow() < cooldown_end
    
    def _exceeds_hourly_limit(self) -> bool:
        """시간당 알림 제한 초과 확인"""
        hour_key = datetime.utcnow().strftime("%Y%m%d%H")
        current_count = self.alert_count_per_hour.get(hour_key, 0)
        return current_count >= self.config.max_alerts_per_hour
    
    def _send_alert(self, alert: Alert):
        """알림 발송"""
        
        # 로그 알림
        if self.config.enable_log_alerts:
            self._send_log_alert(alert)
        
        # 이메일 알림 (구현 예정)
        if self.config.enable_email_alerts:
            self._send_email_alert(alert)
        
        # 슬랙 알림 (구현 예정)
        if self.config.enable_slack_alerts:
            self._send_slack_alert(alert)
    
    def _send_log_alert(self, alert: Alert):
        """로그 알림 발송"""
        log_message = f"[{alert.level.value.upper()}] {alert.component.value}: {alert.title} - {alert.message}"
        
        if alert.level == AlertLevel.CRITICAL:
            self.logger.critical(log_message)
        elif alert.level == AlertLevel.ERROR:
            self.logger.error(log_message)
        elif alert.level == AlertLevel.WARNING:
            self.logger.warning(log_message)
        else:
            self.logger.info(log_message)
        
        # 상세 정보 로깅
        if alert.details:
            self.logger.debug(f"알림 상세 정보: {json.dumps(alert.details, default=str, indent=2)}")
    
    def _send_email_alert(self, alert: Alert):
        """이메일 알림 발송 (구현 예정)"""
        # TODO: 이메일 발송 로직 구현
        pass
    
    def _send_slack_alert(self, alert: Alert):
        """슬랙 알림 발송 (구현 예정)"""
        # TODO: 슬랙 발송 로직 구현
        pass
    
    def get_active_alerts(self) -> List[Alert]:
        """활성 알림 목록 조회"""
        return list(self.active_alerts.values())
    
    def get_alert_history(self, hours: int = 24) -> List[Alert]:
        """알림 히스토리 조회"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        return [alert for alert in self.alert_history if alert.timestamp > cutoff_time]
    
    def cleanup_old_alerts(self):
        """오래된 알림 정리"""
        # 24시간 이전 히스토리 제거
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        self.alert_history = [alert for alert in self.alert_history if alert.timestamp > cutoff_time]
        
        # 오래된 쿨다운 제거
        cutoff_cooldown = datetime.utcnow() - timedelta(hours=1)
        self.alert_cooldowns = {
            key: timestamp for key, timestamp in self.alert_cooldowns.items()
            if timestamp > cutoff_cooldown
        }
        
        # 오래된 시간당 카운터 제거
        current_hour = datetime.utcnow().strftime("%Y%m%d%H")
        keys_to_remove = [key for key in self.alert_count_per_hour.keys() if key != current_hour]
        for key in keys_to_remove:
            del self.alert_count_per_hour[key]


class SystemMonitor:
    """시스템 모니터링"""
    
    def __init__(self, config: MonitoringConfig, alert_manager: AlertManager):
        self.config = config
        self.alert_manager = alert_manager
        self.logger = logging.getLogger(__name__)
    
    def check_system_health(self) -> ComponentStatus:
        """시스템 상태 체크"""
        try:
            # CPU 사용률
            cpu_percent = psutil.cpu_percent(interval=1)
            
            # 메모리 사용률
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            memory_mb = memory.used / 1024 / 1024
            
            # 디스크 사용률
            disk = psutil.disk_usage('/')
            disk_percent = disk.percent
            
            # 네트워크 연결 수
            connections = len(psutil.net_connections())
            
            metrics = {
                'cpu_percent': cpu_percent,
                'memory_percent': memory_percent,
                'memory_mb': memory_mb,
                'disk_percent': disk_percent,
                'network_connections': connections
            }
            
            # 알림 체크
            alerts = []
            status = "healthy"
            
            # CPU 체크
            if cpu_percent >= self.config.cpu_critical_threshold:
                status = "critical"
                alerts.append(self.alert_manager.create_alert(
                    ComponentType.SYSTEM,
                    AlertLevel.CRITICAL,
                    "CPU 사용률 위험",
                    f"CPU 사용률이 {cpu_percent:.1f}%에 도달했습니다.",
                    {'cpu_percent': cpu_percent}
                ))
            elif cpu_percent >= self.config.cpu_warning_threshold:
                status = "warning" if status == "healthy" else status
                alerts.append(self.alert_manager.create_alert(
                    ComponentType.SYSTEM,
                    AlertLevel.WARNING,
                    "CPU 사용률 경고",
                    f"CPU 사용률이 {cpu_percent:.1f}%입니다.",
                    {'cpu_percent': cpu_percent}
                ))
            
            # 메모리 체크
            if memory_mb >= self.config.memory_critical_mb:
                status = "critical"
                alerts.append(self.alert_manager.create_alert(
                    ComponentType.SYSTEM,
                    AlertLevel.CRITICAL,
                    "메모리 사용량 위험",
                    f"메모리 사용량이 {memory_mb:.1f}MB에 도달했습니다.",
                    {'memory_mb': memory_mb, 'memory_percent': memory_percent}
                ))
            elif memory_mb >= self.config.memory_warning_mb:
                status = "warning" if status == "healthy" else status
                alerts.append(self.alert_manager.create_alert(
                    ComponentType.SYSTEM,
                    AlertLevel.WARNING,
                    "메모리 사용량 경고",
                    f"메모리 사용량이 {memory_mb:.1f}MB입니다.",
                    {'memory_mb': memory_mb, 'memory_percent': memory_percent}
                ))
            
            # 디스크 체크
            if disk_percent >= self.config.disk_warning_threshold:
                status = "warning" if status == "healthy" else status
                alerts.append(self.alert_manager.create_alert(
                    ComponentType.SYSTEM,
                    AlertLevel.WARNING,
                    "디스크 사용량 경고",
                    f"디스크 사용량이 {disk_percent:.1f}%입니다.",
                    {'disk_percent': disk_percent}
                ))
            
            return ComponentStatus(
                component=ComponentType.SYSTEM,
                status=status,
                last_check=datetime.utcnow(),
                metrics=metrics,
                alerts=alerts
            )
            
        except Exception as e:
            self.logger.error(f"시스템 모니터링 오류: {e}")
            return ComponentStatus(
                component=ComponentType.SYSTEM,
                status="error",
                last_check=datetime.utcnow(),
                metrics={},
                alerts=[self.alert_manager.create_alert(
                    ComponentType.SYSTEM,
                    AlertLevel.ERROR,
                    "시스템 모니터링 오류",
                    f"시스템 상태를 확인할 수 없습니다: {e}",
                    {'error': str(e)}
                )]
            )


class APIKeyMonitor:
    """API 키 모니터링"""
    
    def __init__(self, config: MonitoringConfig, alert_manager: AlertManager):
        self.config = config
        self.alert_manager = alert_manager
        self.logger = logging.getLogger(__name__)
    
    def check_api_keys(self) -> ComponentStatus:
        """API 키 상태 체크"""
        try:
            # KTO API 키 매니저 가져오기
            kto_manager = get_api_key_manager()
            
            # API 키 사용량 정보
            kto_stats = kto_manager.get_usage_stats()
            
            # KTO 키 정보 추출
            kto_provider_stats = kto_stats.get('providers', {}).get('KTO', {})
            
            metrics = {
                'kto_total_keys': kto_provider_stats.get('total_keys', 0),
                'kto_active_keys': kto_provider_stats.get('active_keys', 0),
                'kto_usage_stats': kto_provider_stats
            }
            
            alerts = []
            status = "healthy"
            
            # KTO API 키 체크
            if 'keys' in kto_provider_stats:
                for key_info in kto_provider_stats['keys']:
                    usage_rate = key_info.get('usage_percent', 0) / 100.0
                    key_preview = key_info.get('key_preview', 'unknown')
                    
                    if usage_rate >= self.config.api_key_critical_threshold:
                        status = "critical"
                        alerts.append(self.alert_manager.create_alert(
                            ComponentType.API_KEYS,
                            AlertLevel.CRITICAL,
                            "KTO API 키 한도 임계",
                            f"API 키 {key_preview}의 사용량이 {usage_rate:.1%}에 도달했습니다.",
                            {'key_preview': key_preview, 'usage_rate': usage_rate, 'usage_percent': key_info.get('usage_percent', 0)}
                        ))
                    elif usage_rate >= self.config.api_key_usage_threshold:
                        status = "warning" if status == "healthy" else status
                        alerts.append(self.alert_manager.create_alert(
                            ComponentType.API_KEYS,
                            AlertLevel.WARNING,
                            "KTO API 키 사용량 경고",
                            f"API 키 {key_preview}의 사용량이 {usage_rate:.1%}입니다.",
                            {'key_preview': key_preview, 'usage_rate': usage_rate, 'usage_percent': key_info.get('usage_percent', 0)}
                        ))
            
            # 활성 키가 없는 경우
            if metrics['kto_active_keys'] == 0:
                status = "critical"
                alerts.append(self.alert_manager.create_alert(
                    ComponentType.API_KEYS,
                    AlertLevel.CRITICAL,
                    "모든 KTO API 키 비활성화",
                    "사용 가능한 KTO API 키가 없습니다.",
                    {'total_keys': metrics['kto_total_keys']}
                ))
            
            return ComponentStatus(
                component=ComponentType.API_KEYS,
                status=status,
                last_check=datetime.utcnow(),
                metrics=metrics,
                alerts=alerts
            )
            
        except Exception as e:
            self.logger.error(f"API 키 모니터링 오류: {e}")
            return ComponentStatus(
                component=ComponentType.API_KEYS,
                status="error",
                last_check=datetime.utcnow(),
                metrics={},
                alerts=[self.alert_manager.create_alert(
                    ComponentType.API_KEYS,
                    AlertLevel.ERROR,
                    "API 키 모니터링 오류",
                    f"API 키 상태를 확인할 수 없습니다: {e}",
                    {'error': str(e)}
                )]
            )


class DatabaseMonitor:
    """데이터베이스 모니터링"""
    
    def __init__(self, config: MonitoringConfig, alert_manager: AlertManager):
        self.config = config
        self.alert_manager = alert_manager
        self.logger = logging.getLogger(__name__)
    
    def check_database(self) -> ComponentStatus:
        """데이터베이스 상태 체크"""
        try:
            connection_pool = get_connection_pool()
            
            # 연결 테스트
            connection_test_start = time.time()
            try:
                with connection_pool.get_sync_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                connection_time = time.time() - connection_test_start
                connection_ok = True
            except Exception as e:
                connection_time = time.time() - connection_test_start
                connection_ok = False
                connection_error = str(e)
            
            # 커넥션 풀 상태
            pool_stats = connection_pool.get_pool_stats()
            
            metrics = {
                'connection_ok': connection_ok,
                'connection_time': connection_time,
                'pool_stats': pool_stats
            }
            
            alerts = []
            status = "healthy"
            
            # 연결 실패 체크
            if not connection_ok:
                status = "critical"
                alerts.append(self.alert_manager.create_alert(
                    ComponentType.DATABASE,
                    AlertLevel.CRITICAL,
                    "데이터베이스 연결 실패",
                    f"데이터베이스에 연결할 수 없습니다: {connection_error}",
                    {'connection_time': connection_time, 'error': connection_error}
                ))
            else:
                # 연결 시간 체크
                if connection_time > self.config.db_connection_timeout:
                    status = "warning"
                    alerts.append(self.alert_manager.create_alert(
                        ComponentType.DATABASE,
                        AlertLevel.WARNING,
                        "데이터베이스 연결 지연",
                        f"데이터베이스 연결에 {connection_time:.2f}초가 소요되었습니다.",
                        {'connection_time': connection_time}
                    ))
                
                # 커넥션 풀 사용량 체크
                if 'sync_pool' in pool_stats:
                    sync_stats = pool_stats['sync_pool']
                    if 'pool_usage' in sync_stats:
                        pool_usage = sync_stats['pool_usage']
                        if pool_usage >= self.config.db_pool_usage_threshold:
                            status = "warning" if status == "healthy" else status
                            alerts.append(self.alert_manager.create_alert(
                                ComponentType.DATABASE,
                                AlertLevel.WARNING,
                                "커넥션 풀 사용량 높음",
                                f"커넥션 풀 사용량이 {pool_usage:.1%}입니다.",
                                {'pool_usage': pool_usage, 'pool_stats': sync_stats}
                            ))
            
            return ComponentStatus(
                component=ComponentType.DATABASE,
                status=status,
                last_check=datetime.utcnow(),
                metrics=metrics,
                alerts=alerts
            )
            
        except Exception as e:
            self.logger.error(f"데이터베이스 모니터링 오류: {e}")
            return ComponentStatus(
                component=ComponentType.DATABASE,
                status="error",
                last_check=datetime.utcnow(),
                metrics={},
                alerts=[self.alert_manager.create_alert(
                    ComponentType.DATABASE,
                    AlertLevel.ERROR,
                    "데이터베이스 모니터링 오류",
                    f"데이터베이스 상태를 확인할 수 없습니다: {e}",
                    {'error': str(e)}
                )]
            )


class MonitoringSystem:
    """중앙 모니터링 시스템"""
    
    def __init__(self, config: MonitoringConfig = None):
        self.config = config or MonitoringConfig()
        self.alert_manager = AlertManager(self.config)
        self.logger = logging.getLogger(__name__)
        
        # 개별 모니터 초기화
        self.system_monitor = SystemMonitor(self.config, self.alert_manager)
        self.api_key_monitor = APIKeyMonitor(self.config, self.alert_manager)
        self.database_monitor = DatabaseMonitor(self.config, self.alert_manager)
        
        # 모니터링 상태
        self._monitoring = False
        self._monitor_thread = None
        self._shutdown_event = threading.Event()
        
        # 구성 요소 상태 저장
        self.component_statuses: Dict[ComponentType, ComponentStatus] = {}
    
    def start_monitoring(self):
        """모니터링 시작"""
        if self._monitoring:
            self.logger.warning("모니터링이 이미 실행 중입니다.")
            return
        
        self._monitoring = True
        self._shutdown_event.clear()
        
        def monitor_worker():
            """모니터링 작업"""
            self.logger.info("실시간 모니터링 시작")
            
            while not self._shutdown_event.wait(self.config.check_interval):
                try:
                    # 모든 구성 요소 체크
                    self._check_all_components()
                    
                    # 오래된 알림 정리
                    self.alert_manager.cleanup_old_alerts()
                    
                except Exception as e:
                    self.logger.error(f"모니터링 루프 오류: {e}")
        
        self._monitor_thread = threading.Thread(
            target=monitor_worker,
            daemon=True,
            name="monitoring-system"
        )
        self._monitor_thread.start()
        
        self.logger.info("모니터링 시스템이 시작되었습니다.")
    
    def stop_monitoring(self):
        """모니터링 중지"""
        if not self._monitoring:
            return
        
        self._monitoring = False
        self._shutdown_event.set()
        
        if self._monitor_thread:
            self._monitor_thread.join(timeout=10)
        
        self.logger.info("모니터링 시스템이 중지되었습니다.")
    
    def _check_all_components(self):
        """모든 구성 요소 체크"""
        
        # 시스템 상태 체크
        self.component_statuses[ComponentType.SYSTEM] = self.system_monitor.check_system_health()
        
        # API 키 상태 체크
        self.component_statuses[ComponentType.API_KEYS] = self.api_key_monitor.check_api_keys()
        
        # 데이터베이스 상태 체크
        self.component_statuses[ComponentType.DATABASE] = self.database_monitor.check_database()
        
        # 전체 상태 로깅
        self._log_system_status()
    
    def _log_system_status(self):
        """시스템 상태 로깅"""
        status_summary = {}
        for component_type, status in self.component_statuses.items():
            status_summary[component_type.value] = {
                'status': status.status,
                'last_check': status.last_check.isoformat(),
                'alert_count': len(status.alerts)
            }
        
        self.logger.debug(f"시스템 상태 요약: {json.dumps(status_summary, indent=2)}")
    
    def get_system_status(self) -> Dict[str, Any]:
        """전체 시스템 상태 조회"""
        return {
            'monitoring_active': self._monitoring,
            'last_update': datetime.utcnow().isoformat(),
            'components': {
                component_type.value: asdict(status) if status else None
                for component_type, status in self.component_statuses.items()
            },
            'active_alerts': [asdict(alert) for alert in self.alert_manager.get_active_alerts()],
            'alert_summary': {
                'total_active': len(self.alert_manager.get_active_alerts()),
                'by_level': self._get_alerts_by_level(),
                'by_component': self._get_alerts_by_component()
            }
        }
    
    def _get_alerts_by_level(self) -> Dict[str, int]:
        """수준별 알림 통계"""
        level_counts = {}
        for alert in self.alert_manager.get_active_alerts():
            level = alert.level.value
            level_counts[level] = level_counts.get(level, 0) + 1
        return level_counts
    
    def _get_alerts_by_component(self) -> Dict[str, int]:
        """구성 요소별 알림 통계"""
        component_counts = {}
        for alert in self.alert_manager.get_active_alerts():
            component = alert.component.value
            component_counts[component] = component_counts.get(component, 0) + 1
        return component_counts
    
    def force_check(self):
        """즉시 체크 실행"""
        self._check_all_components()
    
    def get_component_status(self, component_type: ComponentType) -> Optional[ComponentStatus]:
        """특정 구성 요소 상태 조회"""
        return self.component_statuses.get(component_type)
    
    def resolve_alert(self, alert_id: str):
        """알림 해결"""
        self.alert_manager.resolve_alert(alert_id)
    
    def get_alert_history(self, hours: int = 24) -> List[Alert]:
        """알림 히스토리 조회"""
        return self.alert_manager.get_alert_history(hours)


# 싱글톤 인스턴스
_monitoring_system = None


def get_monitoring_system(config: MonitoringConfig = None) -> MonitoringSystem:
    """모니터링 시스템 인스턴스 반환"""
    global _monitoring_system
    if _monitoring_system is None:
        _monitoring_system = MonitoringSystem(config)
    return _monitoring_system


def reset_monitoring_system():
    """모니터링 시스템 리셋"""
    global _monitoring_system
    if _monitoring_system:
        _monitoring_system.stop_monitoring()
        _monitoring_system = None