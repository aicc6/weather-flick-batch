"""
배치 작업 관리 API 라우터
"""

from fastapi import APIRouter, HTTPException, Query, BackgroundTasks, Header
from typing import Optional, List
from datetime import datetime
import uuid
import logging

from app.api.schemas import (
    JobType,
    JobStatus,
    JobExecuteRequest,
    JobExecuteResponse,
    JobInfo,
    JobListResponse,
    JobLogsResponse,
    JobStatistics,
    SystemStatus,
    JobStopRequest,
)
from app.api.config import settings

logger = logging.getLogger(__name__)
router = APIRouter()

# 작업 관리자 인스턴스
# TODO: 실제 JobManager로 변경 필요 (현재는 DB 연결 문제로 SimpleJobManager 사용)
job_manager = None


def get_job_manager():
    global job_manager
    if job_manager is None:
        from app.api.services.job_manager_db import JobManagerDB

        job_manager = JobManagerDB()
        logger.info("JobManagerDB 초기화 성공")
    return job_manager


@router.get("/jobs", response_model=JobListResponse)
async def list_jobs(
    job_type: Optional[JobType] = None,
    status: Optional[JobStatus] = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100, alias="limit"),
    api_key: str = Header(None, alias="X-API-Key"),
):
    """배치 작업 목록 조회"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    manager = get_job_manager()
    jobs = await manager.get_jobs(
        job_type=job_type, status=status, page=page, size=size
    )

    return jobs


@router.post("/jobs/{job_type}/execute", response_model=JobExecuteResponse)
async def execute_job(
    job_type: JobType,
    background_tasks: BackgroundTasks,
    request: JobExecuteRequest = JobExecuteRequest(),
    api_key: str = Header(None, alias="X-API-Key"),
):
    """배치 작업 실행"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # 이미 실행 중인 작업 확인
    manager = get_job_manager()
    if await manager.is_job_running(job_type):
        raise HTTPException(
            status_code=400, detail=f"{job_type.value} 작업이 이미 실행 중입니다."
        )

    # 작업 생성 및 실행
    job_id = str(uuid.uuid4())

    # 백그라운드에서 작업 실행
    background_tasks.add_task(
        manager.execute_job,
        job_id=job_id,
        job_type=job_type,
        parameters=request.parameters,
        requested_by=request.requested_by,
    )

    return JobExecuteResponse(
        job_id=job_id,
        job_type=job_type,
        status=JobStatus.PENDING,
        message=f"{job_type.value} 작업이 시작되었습니다.",
    )


@router.get("/jobs/{job_id}", response_model=JobInfo)
async def get_job_info(job_id: str, api_key: str = Header(None, alias="X-API-Key")):
    """특정 작업 정보 조회"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    manager = get_job_manager()
    job = await manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다.")

    return job


@router.get("/jobs/{job_id}/logs", response_model=JobLogsResponse)
async def get_job_logs(
    job_id: str,
    level: Optional[str] = None,
    page: int = Query(1, ge=1),
    size: int = Query(100, ge=1, le=1000),
    api_key: str = Header(None, alias="X-API-Key"),
):
    """작업 로그 조회"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    manager = get_job_manager()
    logs = await manager.get_job_logs(job_id=job_id, level=level, page=page, size=size)

    if not logs:
        raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다.")

    return logs


@router.post("/jobs/{job_id}/stop")
async def stop_job(
    job_id: str,
    request: JobStopRequest = JobStopRequest(),
    api_key: str = Header(None, alias="X-API-Key"),
):
    """작업 중단"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    manager = get_job_manager()
    success = await manager.stop_job(
        job_id=job_id, reason=request.reason, force=request.force
    )

    if not success:
        raise HTTPException(status_code=400, detail="작업을 중단할 수 없습니다.")

    return {"message": "작업 중단 요청이 전송되었습니다."}


@router.get("/statistics", response_model=List[JobStatistics])
async def get_statistics(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    api_key: str = Header(None, alias="X-API-Key"),
):
    """작업 통계 조회"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    manager = get_job_manager()
    stats = await manager.get_statistics(start_date=start_date, end_date=end_date)

    return stats


@router.get("/system/status", response_model=SystemStatus)
async def get_system_status(api_key: str = Header(None, alias="X-API-Key")):
    """시스템 상태 조회"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    manager = get_job_manager()
    status = await manager.get_system_status()
    return status


@router.post("/system/cleanup")
async def cleanup_old_data(
    days: int = Query(30, ge=1, le=365), api_key: str = Header(None, alias="X-API-Key")
):
    """오래된 데이터 정리"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

    manager = get_job_manager()
    result = await manager.cleanup_old_data(days=days)
    return {
        "message": f"{result.get('deleted_jobs', 0)}개의 작업과 {result.get('deleted_logs', 0)}개의 로그가 삭제되었습니다."
    }
