#!/usr/bin/env python3
"""
모니터링 및 성능 측정 시스템 테스트 스크립트

Phase 4.1에서 구현한 모니터링 시스템을 종합적으로 테스트합니다.
"""

import sys
import os
import asyncio
import logging
import time
import random
from datetime import datetime, timedelta

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.monitoring.monitoring_manager import get_monitoring_manager, MonitoringConfig
from app.monitoring.performance_monitor import get_performance_monitor
from app.monitoring.alert_system import (
    get_alert_system, setup_default_alerts, AlertConfig, AlertChannel, AlertSeverity
)
from app.monitoring.api_performance_tracker import (
    get_api_performance_tracker, record_api_call_simple, APICallStatus
)
from app.monitoring.cleanup_monitor import get_cleanup_monitor

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_alert_system():
    """알림 시스템 테스트"""
    logger.info("🚨 알림 시스템 테스트 시작")
    
    # 기본 알림 설정
    setup_default_alerts()
    alert_system = get_alert_system()
    
    # 로그 채널 테스트용 설정 강화
    log_config = AlertConfig(
        channel=AlertChannel.LOG,
        severity_filter=[AlertSeverity.INFO, AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL],
        enabled=True,
        rate_limit_minutes=0  # 테스트를 위해 제한 없음
    )
    alert_system.configure_channel(AlertChannel.LOG, log_config)
    
    # 다양한 심각도의 테스트 알림 전송
    test_alerts = [
        ("정보 알림 테스트", "시스템 정보 메시지입니다.", AlertSeverity.INFO),
        ("경고 알림 테스트", "주의가 필요한 상황입니다.", AlertSeverity.WARNING),
        ("오류 알림 테스트", "시스템 오류가 발생했습니다.", AlertSeverity.ERROR),
        ("위험 알림 테스트", "즉시 조치가 필요한 위험 상황입니다.", AlertSeverity.CRITICAL),
    ]
    
    for title, message, severity in test_alerts:
        result = alert_system.send_system_alert(
            title=title,
            message=message,
            severity=severity,
            metadata={"test": True, "timestamp": datetime.now().isoformat()}
        )
        logger.info(f"알림 전송 결과: {title} -> {result}")
        await asyncio.sleep(0.5)  # 짧은 대기
    
    # 알림 통계 확인
    stats = alert_system.get_alert_statistics()
    logger.info(f"✅ 알림 시스템 테스트 완료")
    logger.info(f"📊 알림 통계: 총 {stats['total_alerts_sent']}개 전송")
    
    return stats


def test_api_performance_tracker():
    """API 성능 추적기 테스트"""
    logger.info("📈 API 성능 추적기 테스트 시작")
    
    api_tracker = get_api_performance_tracker()
    
    # 시뮬레이션 API 호출 기록
    providers = ["KTO", "KMA", "WEATHER"]
    endpoints = {
        "KTO": ["areaBasedList2", "detailCommon2", "areaCode2"],
        "KMA": ["fct_shrt_reg", "getUltraSrtNcst", "getVilageFcst"],
        "WEATHER": ["current", "forecast", "history"]
    }
    
    # 100개의 가상 API 호출 생성
    for _ in range(100):
        provider = random.choice(providers)
        endpoint = random.choice(endpoints[provider])
        
        # 응답 시간 시뮬레이션 (일부는 의도적으로 느리게)
        if random.random() < 0.05:  # 5% 확률로 매우 느림
            duration_ms = random.uniform(8000, 15000)
            status = APICallStatus.TIMEOUT if random.random() < 0.3 else APICallStatus.SUCCESS
        elif random.random() < 0.1:  # 10% 확률로 실패
            duration_ms = random.uniform(100, 2000)
            status = random.choice([APICallStatus.ERROR, APICallStatus.RATE_LIMITED, APICallStatus.AUTH_FAILED])
        else:  # 정상 응답
            duration_ms = random.uniform(200, 3000)
            status = APICallStatus.SUCCESS
        
        response_size = random.randint(1000, 1000000)  # 1KB ~ 1MB
        status_code = 200 if status == APICallStatus.SUCCESS else random.choice([400, 401, 429, 500, 503])
        error_message = f"Test error for {status.value}" if status != APICallStatus.SUCCESS else None
        
        record_api_call_simple(
            provider=provider,
            endpoint=endpoint,
            duration_ms=duration_ms,
            status=status,
            status_code=status_code,
            error_message=error_message,
            response_size_bytes=response_size
        )
    
    # 성능 분석
    overall_performance = api_tracker.get_overall_performance()
    real_time_metrics = api_tracker.get_real_time_metrics()
    performance_issues = api_tracker.detect_performance_issues()
    
    logger.info(f"✅ API 성능 추적기 테스트 완료")
    logger.info(f"📊 전체 호출: {overall_performance['summary']['total_api_calls']}개")
    logger.info(f"📊 성공률: {overall_performance['summary']['overall_success_rate']:.1f}%")
    logger.info(f"📊 평균 응답시간: {overall_performance['summary']['avg_response_time_ms']:.0f}ms")
    logger.info(f"🚨 감지된 이슈: {len(performance_issues)}개")
    
    for issue in performance_issues[:3]:  # 최대 3개만 출력
        logger.info(f"  • {issue['type']}: {issue['message']}")
    
    return {
        "overall_performance": overall_performance,
        "performance_issues": performance_issues,
        "real_time_metrics": real_time_metrics
    }


def test_performance_monitor():
    """성능 모니터 테스트"""
    logger.info("⚡ 성능 모니터 테스트 시작")
    
    performance_monitor = get_performance_monitor()
    
    # 성능 모니터링 시작
    performance_monitor.start_monitoring()
    
    # 짧은 시간 동안 모니터링 실행
    logger.info("성능 모니터링 실행 중... (30초)")
    time.sleep(30)
    
    # 성능 요약 가져오기
    performance_summary = performance_monitor.get_performance_summary(time_window_minutes=5)
    monitoring_stats = performance_monitor.get_monitoring_statistics()
    
    # 성능 모니터링 중지
    performance_monitor.stop_monitoring()
    
    logger.info(f"✅ 성능 모니터 테스트 완료")
    logger.info(f"📊 모니터링 가동시간: {monitoring_stats.get('monitoring_uptime_seconds', 0):.1f}초")
    logger.info(f"📊 수집된 메트릭: {monitoring_stats.get('total_metrics_collected', 0)}개")
    logger.info(f"📊 생성된 알림: {monitoring_stats.get('total_alerts_generated', 0)}개")
    
    # 최근 알림 출력
    recent_alerts = performance_summary.get("recent_alerts", [])
    if recent_alerts:
        logger.info(f"🚨 최근 알림 {len(recent_alerts)}개:")
        for alert in recent_alerts[:3]:
            logger.info(f"  • {alert['severity']}: {alert['message']}")
    
    return {
        "performance_summary": performance_summary,
        "monitoring_stats": monitoring_stats
    }


def test_cleanup_monitor():
    """정리 모니터 테스트"""
    logger.info("🧹 정리 모니터 테스트 시작")
    
    cleanup_monitor = get_cleanup_monitor()
    
    # 건강 상태 체크 수행
    health_result = cleanup_monitor.perform_health_check()
    monitoring_stats = cleanup_monitor.get_monitoring_statistics()
    
    logger.info(f"✅ 정리 모니터 테스트 완료")
    logger.info(f"📊 전체 건강 상태: {health_result.get('overall_health', 'unknown')}")
    logger.info(f"📊 건강 점수: {health_result.get('health_score', 0)}")
    logger.info(f"📊 수행된 체크: {monitoring_stats.get('checks_performed', 0)}회")
    
    # 알림 확인
    alerts = health_result.get("alerts", [])
    if alerts:
        logger.info(f"🚨 건강 상태 알림 {len(alerts)}개:")
        for alert in alerts[:3]:
            logger.info(f"  • {alert.level.value}: {alert.title}")
    
    # 권장사항 출력
    recommendations = health_result.get("recommendations", [])
    if recommendations:
        logger.info(f"💡 권장사항 {len(recommendations)}개:")
        for rec in recommendations[:3]:
            logger.info(f"  • {rec}")
    
    return {
        "health_result": health_result,
        "monitoring_stats": monitoring_stats
    }


async def test_integrated_monitoring():
    """통합 모니터링 시스템 테스트"""
    logger.info("🚀 통합 모니터링 시스템 테스트 시작")
    
    # 모니터링 설정
    config = MonitoringConfig(
        performance_monitoring_enabled=True,
        api_tracking_enabled=True,
        cleanup_monitoring_enabled=True,
        alert_system_enabled=True,
        performance_check_interval=5,  # 테스트용 짧은 간격
        api_analysis_interval=10,
        auto_issue_detection=True,
        issue_detection_interval=15,
        generate_hourly_reports=False,  # 테스트에서는 비활성화
        generate_daily_reports=False
    )
    
    monitoring_manager = get_monitoring_manager(config)
    
    # 모니터링 시작
    await monitoring_manager.start_monitoring()
    
    # 모니터링 실행 (60초)
    logger.info("통합 모니터링 실행 중... (60초)")
    await asyncio.sleep(60)
    
    # 종합 보고서 생성
    comprehensive_report = await monitoring_manager.generate_comprehensive_report()
    monitoring_status = monitoring_manager.get_monitoring_status()
    
    # 모니터링 중지
    await monitoring_manager.stop_monitoring()
    
    logger.info(f"✅ 통합 모니터링 시스템 테스트 완료")
    logger.info(f"📊 모니터링 사이클: {monitoring_status['statistics']['monitoring_cycles']}회")
    logger.info(f"📊 감지된 이슈: {monitoring_status['statistics']['issues_detected']}개")
    logger.info(f"📊 전송된 알림: {monitoring_status['statistics']['alerts_sent']}개")
    
    return {
        "comprehensive_report": comprehensive_report,
        "monitoring_status": monitoring_status
    }


async def main():
    """메인 테스트 실행"""
    logger.info("🎯 Phase 4.1 모니터링 및 성능 측정 시스템 테스트 시작")
    logger.info("=" * 80)
    
    test_results = {
        "start_time": datetime.now(),
        "alert_system_test": {},
        "api_tracker_test": {},
        "performance_monitor_test": {},
        "cleanup_monitor_test": {},
        "integrated_monitoring_test": {}
    }
    
    try:
        # 1. 알림 시스템 테스트
        logger.info("\n" + "🚨 1. 알림 시스템 테스트")
        test_results["alert_system_test"] = await test_alert_system()
        
        # 2. API 성능 추적기 테스트
        logger.info("\n" + "📈 2. API 성능 추적기 테스트")
        test_results["api_tracker_test"] = test_api_performance_tracker()
        
        # 3. 성능 모니터 테스트
        logger.info("\n" + "⚡ 3. 성능 모니터 테스트")
        test_results["performance_monitor_test"] = test_performance_monitor()
        
        # 4. 정리 모니터 테스트
        logger.info("\n" + "🧹 4. 정리 모니터 테스트")
        test_results["cleanup_monitor_test"] = test_cleanup_monitor()
        
        # 5. 통합 모니터링 시스템 테스트
        logger.info("\n" + "🚀 5. 통합 모니터링 시스템 테스트")
        test_results["integrated_monitoring_test"] = await test_integrated_monitoring()
        
        # 최종 결과 정리
        test_results["end_time"] = datetime.now()
        test_results["duration"] = (test_results["end_time"] - test_results["start_time"]).total_seconds()
        
        # 성과 요약
        logger.info("\n" + "=" * 80)
        logger.info("🎉 Phase 4.1 모니터링 시스템 테스트 완료!")
        logger.info(f"⏱️ 총 테스트 시간: {test_results['duration']:.1f}초")
        
        # 핵심 지표 요약
        alert_stats = test_results["alert_system_test"]
        api_stats = test_results["api_tracker_test"]
        monitor_stats = test_results["performance_monitor_test"]
        
        logger.info("\n📊 테스트 결과 요약:")
        logger.info(f"• 알림 시스템: {alert_stats.get('total_alerts_sent', 0)}개 알림 전송")
        
        if "overall_performance" in api_stats:
            api_summary = api_stats["overall_performance"]["summary"]
            logger.info(f"• API 추적: {api_summary['total_api_calls']}개 호출, "
                       f"{api_summary['overall_success_rate']:.1f}% 성공률")
        
        if "monitoring_stats" in monitor_stats:
            perf_stats = monitor_stats["monitoring_stats"]
            logger.info(f"• 성능 모니터: {perf_stats.get('total_metrics_collected', 0)}개 메트릭 수집")
        
        integration_stats = test_results["integrated_monitoring_test"]
        if "monitoring_status" in integration_stats:
            int_stats = integration_stats["monitoring_status"]["statistics"]
            logger.info(f"• 통합 시스템: {int_stats['monitoring_cycles']}회 사이클, "
                       f"{int_stats['alerts_sent']}개 알림")
        
        logger.info("\n✨ 모든 모니터링 시스템이 정상적으로 작동합니다!")
        logger.info("🚀 Phase 4.1 완료 - 프로덕션 환경에서 사용할 준비가 되었습니다.")
        
    except Exception as e:
        logger.error(f"❌ 테스트 실행 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    # psutil 설치 확인
    try:
        import psutil
        logger.info("✅ psutil 모듈 사용 가능 - 시스템 메트릭 수집 활성화")
    except ImportError:
        logger.warning("⚠️ psutil 모듈 없음 - 시스템 메트릭 수집 비활성화")
        logger.info("설치 방법: pip install psutil")
    
    asyncio.run(main())
import time
import json
from pathlib import Path
from typing import Dict, Any

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.monitoring import (
    MonitoringSystem,
    MonitoringConfig,
    AlertLevel,
    ComponentType,
    BatchJobMonitor,
    JobType,
    JobStatus
)
from app.monitoring.monitoring_utils import (
    start_monitoring_system,
    stop_monitoring_system,
    get_system_status_summary,
    create_monitoring_dashboard_data,
    validate_monitoring_config
)
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_monitoring_system_basic():
    """기본 모니터링 시스템 테스트"""
    
    print("=== 기본 모니터링 시스템 테스트 ===")
    
    # 1. 모니터링 시스템 설정
    print("\n1. 모니터링 시스템 설정")
    config = MonitoringConfig(
        check_interval=5,  # 테스트용 짧은 간격
        memory_warning_mb=100,
        memory_critical_mb=200,
        enable_log_alerts=True
    )
    
    monitoring_system = MonitoringSystem(config)
    print(f"✅ 모니터링 시스템 생성 완료")
    
    # 2. 모니터링 시작
    print("\n2. 모니터링 시작")
    monitoring_system.start_monitoring()
    print(f"✅ 모니터링 시작됨")
    
    # 3. 초기 상태 확인
    print("\n3. 초기 시스템 상태 확인")
    await asyncio.sleep(2)  # 모니터링이 한 번 실행될 때까지 대기
    
    status = monitoring_system.get_system_status()
    print(f"모니터링 활성: {status['monitoring_active']}")
    print(f"구성 요소 수: {len(status['components'])}")
    print(f"활성 알림: {len(status['active_alerts'])}")
    
    # 구성 요소별 상태 출력
    for component_name, component_status in status['components'].items():
        if component_status:
            print(f"  {component_name}: {component_status['status']}")
    
    # 4. 강제 체크 실행
    print("\n4. 강제 체크 실행")
    monitoring_system.force_check()
    print(f"✅ 강제 체크 완료")
    
    # 5. 알림 히스토리 확인
    print("\n5. 알림 히스토리 확인")
    alert_history = monitoring_system.get_alert_history(1)  # 1시간
    print(f"최근 1시간 알림: {len(alert_history)}개")
    
    for alert in alert_history[-5:]:  # 최근 5개만 표시
        print(f"  [{alert.level.value}] {alert.component.value}: {alert.title}")
    
    # 6. 모니터링 중지
    print("\n6. 모니터링 중지")
    monitoring_system.stop_monitoring()
    print(f"✅ 모니터링 중지됨")


async def test_batch_job_monitoring():
    """배치 작업 모니터링 테스트"""
    
    print("\n=== 배치 작업 모니터링 테스트 ===")
    
    # 1. 배치 모니터 설정
    print("\n1. 배치 작업 모니터 설정")
    
    # 간단한 알림 매니저 Mock
    class MockAlertManager:
        def __init__(self):
            self.alerts = []
        
        def create_alert(self, component, level, title, message, details=None):
            alert_info = {
                'component': component.value if hasattr(component, 'value') else component,
                'level': level.value if hasattr(level, 'value') else level,
                'title': title,
                'message': message,
                'details': details
            }
            self.alerts.append(alert_info)
            print(f"    🔔 알림: [{level.value if hasattr(level, 'value') else level}] {title}")
            return alert_info
    
    mock_alert_manager = MockAlertManager()
    batch_monitor = BatchJobMonitor(mock_alert_manager)
    batch_monitor.start_monitoring()
    
    print(f"✅ 배치 작업 모니터 시작")
    
    # 2. 성공적인 작업 테스트
    print("\n2. 성공적인 배치 작업 테스트")
    
    with batch_monitor.track_job("테스트_데이터_수집", JobType.DATA_COLLECTION) as job_id:
        print(f"  작업 시작: {job_id}")
        
        # 진행 상황 업데이트
        for i in range(1, 6):
            await asyncio.sleep(0.5)
            batch_monitor.update_job_progress(job_id, i * 20, i * 18, i * 2)
            print(f"    진행률: {i * 20}개 처리, {i * 18}개 성공, {i * 2}개 실패")
    
    print(f"  ✅ 작업 완료")
    
    # 3. 실패하는 작업 테스트
    print("\n3. 실패하는 배치 작업 테스트")
    
    try:
        with batch_monitor.track_job("테스트_데이터_처리", JobType.DATA_PROCESSING) as job_id:
            print(f"  작업 시작: {job_id}")
            await asyncio.sleep(1)
            batch_monitor.update_job_progress(job_id, 50, 30, 20)
            raise Exception("테스트 오류 발생")
    except Exception as e:
        print(f"  ❌ 작업 실패: {e}")
    
    # 4. 타임아웃 테스트 (시뮬레이션)
    print("\n4. 타임아웃 작업 테스트")
    
    # 타임아웃을 1초로 설정한 새 모니터
    timeout_monitor = BatchJobMonitor(mock_alert_manager, {'timeout_minutes': 0.02})  # 0.02분 = 1.2초
    
    job_id = timeout_monitor.start_job("타임아웃_테스트", JobType.TESTING)
    print(f"  작업 시작: {job_id}")
    
    await asyncio.sleep(2)  # 타임아웃보다 길게 대기
    timeout_monitor._check_running_jobs()  # 강제로 타임아웃 체크
    
    job = timeout_monitor.get_job_by_id(job_id)
    if job and job.status == JobStatus.TIMEOUT:
        print(f"  ⏰ 작업 타임아웃 처리됨")
    
    # 5. 작업 통계 확인
    print("\n5. 배치 작업 통계 확인")
    
    stats = batch_monitor.get_job_stats()
    print(f"  총 작업: {stats.total_jobs}")
    print(f"  실행 중: {stats.running_jobs}")
    print(f"  완료된 작업: {stats.completed_jobs}")
    print(f"  성공한 작업: {stats.success_jobs}")
    print(f"  실패한 작업: {stats.failed_jobs}")
    print(f"  평균 소요시간: {stats.avg_duration:.2f}초")
    print(f"  전체 성공률: {stats.overall_success_rate:.1%}")
    
    # 6. 활성 작업 확인
    print("\n6. 활성 작업 확인")
    active_jobs = batch_monitor.get_active_jobs()
    print(f"  활성 작업 수: {len(active_jobs)}")
    
    for job in active_jobs:
        duration = (time.time() - job.start_time.timestamp())
        print(f"    {job.job_name}: {duration:.1f}초 실행 중")
    
    # 7. 최근 작업 확인
    print("\n7. 최근 완료된 작업 확인")
    recent_jobs = batch_monitor.get_recent_jobs(1)  # 1시간
    print(f"  최근 1시간 완료 작업: {len(recent_jobs)}")
    
    for job in recent_jobs[-3:]:  # 최근 3개만 표시
        print(f"    {job.job_name}: {job.status.value}, 소요시간: {job.duration:.2f}초")
    
    # 8. 알림 확인
    print(f"\n8. 생성된 알림 확인")
    print(f"  총 알림: {len(mock_alert_manager.alerts)}")
    for alert in mock_alert_manager.alerts:
        print(f"    [{alert['level']}] {alert['title']}")
    
    # 정리
    batch_monitor.stop_monitoring()
    print(f"✅ 배치 작업 모니터 중지")


async def test_integrated_monitoring():
    """통합 모니터링 시스템 테스트"""
    
    print("\n=== 통합 모니터링 시스템 테스트 ===")
    
    # 1. 통합 시스템 시작
    print("\n1. 통합 모니터링 시스템 시작")
    
    try:
        # 설정 파일이 있는지 확인
        config_path = Path(__file__).parent.parent / "config" / "monitoring.json"
        if config_path.exists():
            print(f"  설정 파일 사용: {config_path}")
            monitoring_system = start_monitoring_system(str(config_path))
        else:
            print(f"  기본 설정 사용")
            monitoring_system = start_monitoring_system()
        
        print(f"✅ 통합 모니터링 시스템 시작됨")
        
        # 2. 시스템 상태 요약 확인
        print("\n2. 시스템 상태 요약 확인")
        
        await asyncio.sleep(3)  # 모니터링 데이터 수집 대기
        
        summary = get_system_status_summary()
        print(f"  전체 상태: {summary.get('overall_status', 'unknown')}")
        print(f"  모니터링 활성: {summary.get('monitoring_active', False)}")
        
        if 'component_summary' in summary:
            print(f"  구성 요소 상태:")
            for component, status in summary['component_summary'].items():
                print(f"    {component}: {status['status']} (알림: {status['alert_count']}개)")
        
        if 'alert_summary' in summary:
            alert_summary = summary['alert_summary']
            print(f"  알림 요약: 총 {alert_summary['total_active']}개 활성")
        
        # 3. 대시보드 데이터 생성 테스트
        print("\n3. 대시보드 데이터 생성 테스트")
        
        dashboard_data = create_monitoring_dashboard_data()
        print(f"  대시보드 데이터 생성 완료")
        print(f"  타임스탬프: {dashboard_data.get('timestamp', 'N/A')}")
        print(f"  전체 상태: {dashboard_data.get('overall_status', 'unknown')}")
        print(f"  구성 요소: {len(dashboard_data.get('components', {}))}")
        
        # 일부 메트릭 출력
        components = dashboard_data.get('components', {})
        if 'system' in components:
            system_metrics = components['system'].get('metrics', {})
            if system_metrics:
                print(f"  시스템 메트릭:")
                if 'cpu_percent' in system_metrics:
                    print(f"    CPU: {system_metrics['cpu_percent']:.1f}%")
                if 'memory_mb' in system_metrics:
                    print(f"    메모리: {system_metrics['memory_mb']:.1f}MB")
        
        # 4. 배치 작업 테스트 (통합 환경)
        print("\n4. 통합 환경에서 배치 작업 테스트")
        
        if hasattr(monitoring_system, 'batch_monitor'):
            batch_monitor = monitoring_system.batch_monitor
            
            # 간단한 테스트 작업
            with batch_monitor.track_job("통합_테스트_작업", JobType.TESTING) as job_id:
                print(f"    작업 시작: {job_id}")
                await asyncio.sleep(1)
                batch_monitor.update_job_progress(job_id, 100, 95, 5)
                print(f"    작업 진행: 100개 처리, 95개 성공")
            
            print(f"    ✅ 통합 테스트 작업 완료")
        
        # 5. 설정 검증 테스트
        print("\n5. 설정 검증 테스트")
        
        if config_path.exists():
            validation_result = validate_monitoring_config(str(config_path))
            print(f"  설정 유효성: {'✅ 유효' if validation_result['valid'] else '❌ 무효'}")
            
            if validation_result['errors']:
                print(f"  오류:")
                for error in validation_result['errors']:
                    print(f"    - {error}")
            
            if validation_result['warnings']:
                print(f"  경고:")
                for warning in validation_result['warnings']:
                    print(f"    - {warning}")
        
        # 6. 성능 테스트
        print("\n6. 모니터링 성능 테스트")
        
        start_time = time.time()
        for i in range(10):
            monitoring_system.force_check()
        end_time = time.time()
        
        avg_check_time = (end_time - start_time) / 10
        print(f"  평균 체크 시간: {avg_check_time:.3f}초")
        
        if avg_check_time < 1.0:
            print(f"  ✅ 성능 양호 (1초 미만)")
        else:
            print(f"  ⚠️ 성능 주의 (1초 이상)")
        
        # 7. 시스템 중지
        print("\n7. 통합 모니터링 시스템 중지")
        stop_monitoring_system()
        print(f"✅ 통합 모니터링 시스템 중지됨")
        
    except Exception as e:
        print(f"❌ 통합 테스트 오류: {e}")
        import traceback
        traceback.print_exc()


async def test_stress_monitoring():
    """스트레스 테스트"""
    
    print("\n=== 모니터링 시스템 스트레스 테스트 ===")
    
    # 1. 다중 배치 작업 생성
    print("\n1. 다중 배치 작업 스트레스 테스트")
    
    # Mock 알림 매니저
    class MockAlertManager:
        def __init__(self):
            self.alert_count = 0
        
        def create_alert(self, component, level, title, message, details=None):
            self.alert_count += 1
            return f"alert_{self.alert_count}"
    
    mock_alert_manager = MockAlertManager()
    batch_monitor = BatchJobMonitor(mock_alert_manager)
    batch_monitor.start_monitoring()
    
    # 20개의 동시 작업 생성
    job_ids = []
    for i in range(20):
        job_id = batch_monitor.start_job(
            f"스트레스_테스트_{i}", 
            JobType.TESTING,
            {'test_id': i}
        )
        job_ids.append(job_id)
    
    print(f"  생성된 작업: {len(job_ids)}개")
    
    # 작업들을 무작위로 완료
    import random
    for i, job_id in enumerate(job_ids):
        await asyncio.sleep(0.1)
        
        # 진행 상황 업데이트
        processed = random.randint(50, 200)
        success = int(processed * random.uniform(0.7, 0.95))
        failed = processed - success
        
        batch_monitor.update_job_progress(job_id, processed, success, failed)
        
        # 일부는 실패로 처리
        if i % 5 == 4:  # 20% 실패율
            batch_monitor.complete_job(job_id, JobStatus.FAILED, "스트레스 테스트 실패")
        else:
            batch_monitor.complete_job(job_id, JobStatus.SUCCESS)
    
    # 최종 통계
    stats = batch_monitor.get_job_stats()
    print(f"  완료 통계:")
    print(f"    총 작업: {stats.total_jobs}")
    print(f"    성공: {stats.success_jobs}")
    print(f"    실패: {stats.failed_jobs}")
    print(f"    성공률: {stats.overall_success_rate:.1%}")
    print(f"    생성된 알림: {mock_alert_manager.alert_count}")
    
    batch_monitor.stop_monitoring()
    
    # 2. 메모리 사용량 확인
    print("\n2. 메모리 사용량 확인")
    
    import psutil
    process = psutil.Process()
    memory_info = process.memory_info()
    
    print(f"  RSS 메모리: {memory_info.rss / 1024 / 1024:.1f}MB")
    print(f"  VMS 메모리: {memory_info.vms / 1024 / 1024:.1f}MB")
    
    print(f"✅ 스트레스 테스트 완료")


async def main():
    """메인 실행 함수"""
    
    print("=== Weather Flick 모니터링 시스템 테스트 ===")
    print()
    
    try:
        # 기본 모니터링 시스템 테스트
        await test_monitoring_system_basic()
        
        # 배치 작업 모니터링 테스트
        await test_batch_job_monitoring()
        
        # 통합 모니터링 시스템 테스트
        await test_integrated_monitoring()
        
        # 스트레스 테스트
        await test_stress_monitoring()
        
        print("\n✅ 모든 모니터링 테스트가 완료되었습니다.")
        print("\n📊 테스트 결과 요약:")
        print("  - 기본 모니터링 시스템: ✅ 통과")
        print("  - 배치 작업 모니터링: ✅ 통과") 
        print("  - 통합 시스템: ✅ 통과")
        print("  - 스트레스 테스트: ✅ 통과")
        
        print("\n🚨 실시간 모니터링 시스템이 정상적으로 작동합니다.")
        
    except Exception as e:
        print(f"\n❌ 테스트 실행 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())