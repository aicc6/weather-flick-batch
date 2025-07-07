"""
배치 작업 모니터링

배치 작업의 실행 상태, 성능, 성공/실패를 추적하고 모니터링합니다.
"""

import time
import logging
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from contextlib import contextmanager
import uuid

from .monitoring_system import Alert, AlertLevel, ComponentType, AlertManager


class JobStatus(Enum):
    """배치 작업 상태"""
    PENDING = "pending"      # 대기 중
    RUNNING = "running"      # 실행 중
    SUCCESS = "success"      # 성공
    FAILED = "failed"        # 실패
    TIMEOUT = "timeout"      # 타임아웃
    CANCELLED = "cancelled"  # 취소됨


class JobType(Enum):
    """배치 작업 유형"""
    DATA_COLLECTION = "data_collection"          # 데이터 수집
    DATA_PROCESSING = "data_processing"          # 데이터 처리
    DATA_VALIDATION = "data_validation"          # 데이터 검증
    PERFORMANCE_OPTIMIZATION = "performance_optimization"  # 성능 최적화
    MAINTENANCE = "maintenance"                  # 유지보수
    TESTING = "testing"                         # 테스트


@dataclass
class JobExecution:
    """배치 작업 실행 정보"""
    job_id: str
    job_name: str
    job_type: JobType
    status: JobStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    duration: Optional[float] = None  # 초
    processed_records: int = 0
    success_records: int = 0
    failed_records: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def is_running(self) -> bool:
        """실행 중인지 확인"""
        return self.status == JobStatus.RUNNING
    
    @property
    def is_completed(self) -> bool:
        """완료되었는지 확인"""
        return self.status in [JobStatus.SUCCESS, JobStatus.FAILED, JobStatus.TIMEOUT, JobStatus.CANCELLED]
    
    @property
    def success_rate(self) -> float:
        """성공률 계산"""
        if self.processed_records == 0:
            return 0.0
        return self.success_records / self.processed_records
    
    def update_progress(self, processed: int, success: int, failed: int):
        """진행 상황 업데이트"""
        self.processed_records = processed
        self.success_records = success
        self.failed_records = failed
    
    def complete(self, status: JobStatus, error_message: Optional[str] = None):
        """작업 완료"""
        self.status = status
        self.end_time = datetime.utcnow()
        self.duration = (self.end_time - self.start_time).total_seconds()
        if error_message:
            self.error_message = error_message


@dataclass
class JobStats:
    """작업 통계"""
    total_jobs: int = 0
    running_jobs: int = 0
    completed_jobs: int = 0
    success_jobs: int = 0
    failed_jobs: int = 0
    avg_duration: float = 0.0
    total_processed_records: int = 0
    overall_success_rate: float = 0.0


class BatchJobMonitor:
    """배치 작업 모니터"""
    
    def __init__(self, alert_manager: AlertManager, config: Dict[str, Any] = None):
        self.alert_manager = alert_manager
        self.config = config or {}
        self.logger = logging.getLogger(__name__)
        
        # 작업 저장소
        self.active_jobs: Dict[str, JobExecution] = {}
        self.completed_jobs: List[JobExecution] = []
        self.job_history_limit = self.config.get('job_history_limit', 1000)
        
        # 성능 임계값
        self.timeout_minutes = self.config.get('timeout_minutes', 60)
        self.failure_threshold = self.config.get('failure_threshold', 3)
        self.success_rate_threshold = self.config.get('success_rate_threshold', 0.9)
        
        # 연속 실패 추적
        self.consecutive_failures: Dict[str, int] = {}
        
        # 모니터링 스레드
        self._monitoring = False
        self._monitor_thread = None
        self._shutdown_event = threading.Event()
        
        # 콜백
        self._job_start_callbacks: List[Callable[[JobExecution], None]] = []
        self._job_complete_callbacks: List[Callable[[JobExecution], None]] = []
    
    def start_monitoring(self):
        """모니터링 시작"""
        if self._monitoring:
            return
        
        self._monitoring = True
        self._shutdown_event.clear()
        
        def monitor_worker():
            """모니터링 작업"""
            self.logger.info("배치 작업 모니터링 시작")
            
            while not self._shutdown_event.wait(30):  # 30초마다 체크
                try:
                    self._check_running_jobs()
                    self._cleanup_old_jobs()
                except Exception as e:
                    self.logger.error(f"배치 작업 모니터링 오류: {e}")
        
        self._monitor_thread = threading.Thread(
            target=monitor_worker,
            daemon=True,
            name="batch-job-monitor"
        )
        self._monitor_thread.start()
        
        self.logger.info("배치 작업 모니터링이 시작되었습니다.")
    
    def stop_monitoring(self):
        """모니터링 중지"""
        if not self._monitoring:
            return
        
        self._monitoring = False
        self._shutdown_event.set()
        
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        
        self.logger.info("배치 작업 모니터링이 중지되었습니다.")
    
    def start_job(
        self, 
        job_name: str, 
        job_type: JobType, 
        metadata: Dict[str, Any] = None
    ) -> str:
        """배치 작업 시작"""
        job_id = str(uuid.uuid4())
        
        job_execution = JobExecution(
            job_id=job_id,
            job_name=job_name,
            job_type=job_type,
            status=JobStatus.RUNNING,
            start_time=datetime.utcnow(),
            metadata=metadata or {}
        )
        
        self.active_jobs[job_id] = job_execution
        
        # 연속 실패 카운터 리셋 (새 작업 시작 시)
        if job_name in self.consecutive_failures:
            del self.consecutive_failures[job_name]
        
        # 콜백 실행
        for callback in self._job_start_callbacks:
            try:
                callback(job_execution)
            except Exception as e:
                self.logger.error(f"작업 시작 콜백 오류: {e}")
        
        self.logger.info(f"배치 작업 시작: {job_name} (ID: {job_id})")
        return job_id
    
    def update_job_progress(
        self, 
        job_id: str, 
        processed: int, 
        success: int, 
        failed: int,
        metadata: Dict[str, Any] = None
    ):
        """작업 진행 상황 업데이트"""
        if job_id not in self.active_jobs:
            self.logger.warning(f"존재하지 않는 작업 ID: {job_id}")
            return
        
        job = self.active_jobs[job_id]
        job.update_progress(processed, success, failed)
        
        if metadata:
            job.metadata.update(metadata)
        
        # 성공률 체크
        if processed > 100 and job.success_rate < self.success_rate_threshold:
            self.alert_manager.create_alert(
                ComponentType.BATCH_JOBS,
                AlertLevel.WARNING,
                "배치 작업 성공률 낮음",
                f"작업 '{job.job_name}'의 성공률이 {job.success_rate:.1%}입니다.",
                {
                    'job_id': job_id,
                    'job_name': job.job_name,
                    'success_rate': job.success_rate,
                    'processed': processed,
                    'success': success,
                    'failed': failed
                }
            )
    
    def complete_job(
        self, 
        job_id: str, 
        status: JobStatus, 
        error_message: Optional[str] = None,
        final_metadata: Dict[str, Any] = None
    ):
        """배치 작업 완료"""
        if job_id not in self.active_jobs:
            self.logger.warning(f"존재하지 않는 작업 ID: {job_id}")
            return
        
        job = self.active_jobs[job_id]
        job.complete(status, error_message)
        
        if final_metadata:
            job.metadata.update(final_metadata)
        
        # 활성 작업에서 제거하고 완료 목록에 추가
        del self.active_jobs[job_id]
        self.completed_jobs.append(job)
        
        # 완료된 작업 수 제한
        if len(self.completed_jobs) > self.job_history_limit:
            self.completed_jobs = self.completed_jobs[-self.job_history_limit:]
        
        # 실패 처리
        if status == JobStatus.FAILED:
            self._handle_job_failure(job)
        elif status == JobStatus.SUCCESS:
            self._handle_job_success(job)
        
        # 콜백 실행
        for callback in self._job_complete_callbacks:
            try:
                callback(job)
            except Exception as e:
                self.logger.error(f"작업 완료 콜백 오류: {e}")
        
        self.logger.info(
            f"배치 작업 완료: {job.job_name} (ID: {job_id}, 상태: {status.value}, "
            f"소요시간: {job.duration:.1f}초)"
        )
    
    def _handle_job_failure(self, job: JobExecution):
        """작업 실패 처리"""
        job_name = job.job_name
        
        # 연속 실패 카운트 증가
        self.consecutive_failures[job_name] = self.consecutive_failures.get(job_name, 0) + 1
        failure_count = self.consecutive_failures[job_name]
        
        # 실패 알림
        alert_level = AlertLevel.ERROR if failure_count < self.failure_threshold else AlertLevel.CRITICAL
        alert_title = "배치 작업 실패" if failure_count < self.failure_threshold else "배치 작업 연속 실패"
        
        self.alert_manager.create_alert(
            ComponentType.BATCH_JOBS,
            alert_level,
            alert_title,
            f"작업 '{job_name}'이 실패했습니다. (연속 실패: {failure_count}회)",
            {
                'job_id': job.job_id,
                'job_name': job_name,
                'consecutive_failures': failure_count,
                'error_message': job.error_message,
                'duration': job.duration,
                'processed_records': job.processed_records
            }
        )
    
    def _handle_job_success(self, job: JobExecution):
        """작업 성공 처리"""
        # 연속 실패 카운터 리셋
        if job.job_name in self.consecutive_failures:
            del self.consecutive_failures[job.job_name]
    
    def _check_running_jobs(self):
        """실행 중인 작업들 체크 (타임아웃 등)"""
        current_time = datetime.utcnow()
        timeout_threshold = timedelta(minutes=self.timeout_minutes)
        
        jobs_to_timeout = []
        
        for job_id, job in self.active_jobs.items():
            # 타임아웃 체크
            if current_time - job.start_time > timeout_threshold:
                jobs_to_timeout.append(job_id)
        
        # 타임아웃된 작업들 처리
        for job_id in jobs_to_timeout:
            job = self.active_jobs[job_id]
            self.complete_job(
                job_id, 
                JobStatus.TIMEOUT, 
                f"작업이 {self.timeout_minutes}분 내에 완료되지 않았습니다."
            )
    
    def _cleanup_old_jobs(self):
        """오래된 작업 기록 정리"""
        # 24시간 이전의 완료된 작업들 제거
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        self.completed_jobs = [
            job for job in self.completed_jobs 
            if job.end_time and job.end_time > cutoff_time
        ]
    
    @contextmanager
    def track_job(self, job_name: str, job_type: JobType, metadata: Dict[str, Any] = None):
        """작업 추적 컨텍스트 매니저"""
        job_id = self.start_job(job_name, job_type, metadata)
        
        try:
            yield job_id
            # 성공적으로 완료
            self.complete_job(job_id, JobStatus.SUCCESS)
        except Exception as e:
            # 예외 발생 시 실패로 처리
            self.complete_job(job_id, JobStatus.FAILED, str(e))
            raise
    
    def get_job_stats(self) -> JobStats:
        """작업 통계 조회"""
        all_jobs = list(self.active_jobs.values()) + self.completed_jobs
        
        if not all_jobs:
            return JobStats()
        
        running_jobs = len(self.active_jobs)
        completed_jobs = len(self.completed_jobs)
        success_jobs = len([job for job in self.completed_jobs if job.status == JobStatus.SUCCESS])
        failed_jobs = len([job for job in self.completed_jobs if job.status == JobStatus.FAILED])
        
        # 평균 소요 시간 계산 (완료된 작업만)
        completed_durations = [job.duration for job in self.completed_jobs if job.duration]
        avg_duration = sum(completed_durations) / len(completed_durations) if completed_durations else 0.0
        
        # 전체 처리된 레코드 수
        total_processed = sum(job.processed_records for job in all_jobs)
        total_success = sum(job.success_records for job in all_jobs)
        overall_success_rate = total_success / total_processed if total_processed > 0 else 0.0
        
        return JobStats(
            total_jobs=len(all_jobs),
            running_jobs=running_jobs,
            completed_jobs=completed_jobs,
            success_jobs=success_jobs,
            failed_jobs=failed_jobs,
            avg_duration=avg_duration,
            total_processed_records=total_processed,
            overall_success_rate=overall_success_rate
        )
    
    def get_active_jobs(self) -> List[JobExecution]:
        """실행 중인 작업 목록"""
        return list(self.active_jobs.values())
    
    def get_recent_jobs(self, hours: int = 24) -> List[JobExecution]:
        """최근 완료된 작업 목록"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        return [
            job for job in self.completed_jobs
            if job.end_time and job.end_time > cutoff_time
        ]
    
    def get_job_by_id(self, job_id: str) -> Optional[JobExecution]:
        """ID로 작업 조회"""
        if job_id in self.active_jobs:
            return self.active_jobs[job_id]
        
        for job in self.completed_jobs:
            if job.job_id == job_id:
                return job
        
        return None
    
    def cancel_job(self, job_id: str, reason: str = "사용자 요청"):
        """작업 취소"""
        if job_id not in self.active_jobs:
            self.logger.warning(f"취소할 수 없는 작업 ID: {job_id}")
            return False
        
        self.complete_job(job_id, JobStatus.CANCELLED, reason)
        return True
    
    def add_job_start_callback(self, callback: Callable[[JobExecution], None]):
        """작업 시작 콜백 추가"""
        self._job_start_callbacks.append(callback)
    
    def add_job_complete_callback(self, callback: Callable[[JobExecution], None]):
        """작업 완료 콜백 추가"""
        self._job_complete_callbacks.append(callback)
    
    def get_job_summary(self) -> Dict[str, Any]:
        """작업 요약 정보"""
        stats = self.get_job_stats()
        
        return {
            'stats': asdict(stats),
            'active_jobs': [
                {
                    'job_id': job.job_id,
                    'job_name': job.job_name,
                    'job_type': job.job_type.value,
                    'start_time': job.start_time.isoformat(),
                    'duration': (datetime.utcnow() - job.start_time).total_seconds(),
                    'processed_records': job.processed_records,
                    'success_rate': job.success_rate
                }
                for job in self.get_active_jobs()
            ],
            'recent_failures': [
                {
                    'job_name': job.job_name,
                    'end_time': job.end_time.isoformat() if job.end_time else None,
                    'error_message': job.error_message,
                    'duration': job.duration
                }
                for job in self.get_recent_jobs(6)  # 최근 6시간
                if job.status == JobStatus.FAILED
            ],
            'consecutive_failures': dict(self.consecutive_failures)
        }
    
    def register_job(self, job_name: str, description: str = "") -> str:
        """작업 등록 (간단한 인터페이스)"""
        return self.start_job(job_name, JobType.TESTING, {"description": description})
    
    def start_job_by_id(self, job_id: str):
        """작업 시작 (기존 ID 사용)"""
        if job_id in self.active_jobs:
            self.active_jobs[job_id].status = JobStatus.RUNNING
            self.logger.info(f"작업 시작: {job_id}")
    
    def complete_job_simple(self, job_id: str, metadata: Dict[str, Any] = None):
        """작업 완료 (간단한 인터페이스)"""
        self.complete_job(job_id, JobStatus.SUCCESS, None, metadata)
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """작업 상태 조회"""
        job = self.get_job_by_id(job_id)
        if job:
            return {
                "job_id": job.job_id,
                "job_name": job.job_name,
                "status": job.status.value,
                "start_time": job.start_time.isoformat(),
                "end_time": job.end_time.isoformat() if job.end_time else None,
                "duration": job.duration,
                "processed_records": job.processed_records,
                "success_rate": job.success_rate
            }
        return None
    
    def get_job_statistics(self) -> Dict[str, Any]:
        """작업 통계"""
        stats = self.get_job_stats()
        return asdict(stats)