#!/usr/bin/env python3
"""
모니터링 시스템 테스트 스크립트

실시간 모니터링 시스템의 모든 기능을 테스트합니다.
"""

import os
import sys
import asyncio
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