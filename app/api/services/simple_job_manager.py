"""
간단한 배치 작업 관리자 (테스트용)
"""

import logging
from datetime import datetime
from typing import Optional, List, Dict, Any
from app.api.schemas import (
    JobType, JobStatus, JobInfo, JobListResponse,
    JobLogsResponse, JobStatistics, SystemStatus
)

logger = logging.getLogger(__name__)

class SimpleJobManager:
    """간단한 배치 작업 관리자"""
    
    def __init__(self):
        self.jobs_store = {}  # type: Dict[str, JobInfo]
        self.jobs_logs = {}   # type: Dict[str, List[JobLog]]
        logger.info("SimpleJobManager 초기화 완료")
    
    async def get_jobs(
        self,
        job_type: Optional[JobType] = None,
        status: Optional[JobStatus] = None,
        page: int = 1,
        size: int = 20
    ) -> JobListResponse:
        """작업 목록 조회"""
        # 더미 데이터 반환
        dummy_jobs = [
            JobInfo(
                id="test-job-1",
                job_type=JobType.KTO_DATA_COLLECTION,
                status=JobStatus.COMPLETED,
                created_at=datetime.now(),
                started_at=datetime.now(),
                completed_at=datetime.now(),
                progress=100,
                message="테스트 작업 완료"
            ),
            JobInfo(
                id="test-job-2",
                job_type=JobType.WEATHER_DATA_COLLECTION,
                status=JobStatus.RUNNING,
                created_at=datetime.now(),
                started_at=datetime.now(),
                progress=50,
                message="날씨 데이터 수집 중..."
            )
        ]
        
        # 필터링
        filtered_jobs = []
        for job in dummy_jobs:
            if job_type and job.job_type != job_type:
                continue
            if status and job.status != status:
                continue
            filtered_jobs.append(job)
        
        # 페이지네이션 적용
        start_idx = (page - 1) * size
        end_idx = start_idx + size
        paginated_jobs = filtered_jobs[start_idx:end_idx]
        
        # 전체 페이지 수 계산
        total_pages = (len(filtered_jobs) + size - 1) // size if len(filtered_jobs) > 0 else 1
        
        return JobListResponse(
            jobs=paginated_jobs,
            total=len(filtered_jobs),
            page=page,
            size=size,
            total_pages=total_pages
        )
    
    async def is_job_running(self, job_type: JobType) -> bool:
        """특정 타입의 작업이 실행 중인지 확인"""
        return False
    
    async def execute_job(
        self,
        job_id: str,
        job_type: JobType,
        parameters: Dict[str, Any],
        requested_by: Optional[str] = None
    ):
        """작업 실행 (더미)"""
        logger.info(f"작업 실행: {job_type.value} (ID: {job_id})")
        
    async def get_job_info(self, job_id: str) -> Optional[JobInfo]:
        """작업 정보 조회"""
        return self.jobs_store.get(job_id)
    
    async def stop_job(self, job_id: str, reason: str, force: bool = False) -> bool:
        """작업 중단"""
        return True
    
    async def get_job_logs(
        self,
        job_id: str,
        level: Optional[str] = None,
        page: int = 1,
        size: int = 100
    ) -> JobLogsResponse:
        """작업 로그 조회"""
        return JobLogsResponse(
            logs=[],
            total=0,
            page=page,
            size=size
        )
    
    async def get_statistics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[JobStatistics]:
        """작업 통계 조회"""
        return [
            JobStatistics(
                job_type=JobType.KTO_DATA_COLLECTION,
                total_count=10,
                success_count=8,
                failed_count=2,
                average_duration_seconds=300
            )
        ]
    
    async def get_system_status(self) -> SystemStatus:
        """시스템 상태 조회"""
        return SystemStatus(
            is_healthy=True,
            active_jobs=0,
            queued_jobs=0,
            cpu_usage=10.5,
            memory_usage=25.3,
            disk_usage=45.0,
            uptime_seconds=3600
        )