"""
배치 작업 스케줄 관리 API 라우터
"""
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, Query, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.config import settings
from app.api.schemas_schedule import (
    ScheduleCreate, ScheduleUpdate, ScheduleResponse,
    ScheduleListResponse, ScheduleExecutionResponse,
    UpcomingSchedule, ScheduleStatus
)
from app.api.services.schedule_manager import ScheduleManager
from app.core.async_database import get_async_db_manager
from app.core.logger import get_logger

router = APIRouter(prefix="/schedules", tags=["schedules"])
logger = get_logger(__name__)

# 스케줄 매니저 인스턴스 (main.py에서 초기화됨)
schedule_manager: Optional[ScheduleManager] = None


async def get_db():
    """데이터베이스 세션 의존성"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as session:
        yield session


@router.post("/", response_model=ScheduleResponse)
async def create_schedule(
    schedule_data: ScheduleCreate,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """새 스케줄 생성"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        if not schedule_manager:
            raise HTTPException(status_code=500, detail="스케줄 관리자가 초기화되지 않았습니다")
        
        # 일회성 스케줄과 반복 스케줄 중 하나는 필수
        if not schedule_data.scheduled_time and not schedule_data.cron_expression:
            raise HTTPException(
                status_code=400,
                detail="scheduled_time 또는 cron_expression 중 하나는 필수입니다"
            )
        
        schedule = await schedule_manager.create_schedule(schedule_data, db)
        
        return ScheduleResponse(
            schedule_id=schedule.schedule_id,
            job_type=schedule.job_type,
            scheduled_time=schedule.scheduled_time,
            cron_expression=schedule.cron_expression,
            priority=schedule.priority,
            is_active=schedule.is_active,
            status=schedule.status,
            config=schedule.config,
            description=schedule.description,
            next_run=schedule_manager._get_next_run_time(schedule),
            last_run=schedule.last_run,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"스케줄 생성 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=ScheduleListResponse)
async def list_schedules(
    job_type: Optional[str] = Query(None, description="작업 유형 필터"),
    status: Optional[ScheduleStatus] = Query(None, description="상태 필터"),
    is_active: Optional[bool] = Query(None, description="활성화 여부 필터"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    page_size: int = Query(20, ge=1, le=100, description="페이지 크기"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """스케줄 목록 조회"""
    try:
        if not schedule_manager:
            raise HTTPException(status_code=500, detail="스케줄 관리자가 초기화되지 않았습니다")
        
        result = await schedule_manager.list_schedules(
            db=db,
            job_type=job_type,
            status=status,
            is_active=is_active,
            page=page,
            page_size=page_size
        )
        
        schedules = []
        for schedule in result["schedules"]:
            schedules.append(ScheduleResponse(
                schedule_id=schedule.schedule_id,
                job_type=schedule.job_type,
                scheduled_time=schedule.scheduled_time,
                cron_expression=schedule.cron_expression,
                priority=schedule.priority,
                is_active=schedule.is_active,
                status=schedule.status,
                config=schedule.config,
                description=schedule.description,
                next_run=getattr(schedule, 'next_run', None),
                last_run=schedule.last_run,
                created_at=schedule.created_at,
                updated_at=schedule.updated_at
            ))
        
        return ScheduleListResponse(
            schedules=schedules,
            total=result["total"],
            page=result["page"],
            page_size=result["page_size"]
        )
        
    except Exception as e:
        logger.error(f"스케줄 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/upcoming", response_model=List[UpcomingSchedule])
async def get_upcoming_schedules(
    hours: int = Query(24, ge=1, le=168, description="조회할 시간 범위"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """예정된 스케줄 조회"""
    try:
        if not schedule_manager:
            raise HTTPException(status_code=500, detail="스케줄 관리자가 초기화되지 않았습니다")
        
        schedules = await schedule_manager.get_upcoming_schedules(db, hours)
        
        upcoming = []
        for schedule in schedules:
            upcoming.append(UpcomingSchedule(
                schedule_id=schedule.schedule_id,
                job_type=schedule.job_type,
                scheduled_time=getattr(schedule, 'next_run', None) or schedule.scheduled_time,
                priority=schedule.priority,
                description=schedule.description
            ))
        
        return upcoming
        
    except Exception as e:
        logger.error(f"예정 스케줄 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{schedule_id}", response_model=ScheduleResponse)
async def get_schedule(
    schedule_id: int,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """특정 스케줄 조회"""
    try:
        if not schedule_manager:
            raise HTTPException(status_code=500, detail="스케줄 관리자가 초기화되지 않았습니다")
        
        schedule = await schedule_manager.get_schedule(schedule_id, db)
        
        if not schedule:
            raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
        
        return ScheduleResponse(
            schedule_id=schedule.schedule_id,
            job_type=schedule.job_type,
            scheduled_time=schedule.scheduled_time,
            cron_expression=schedule.cron_expression,
            priority=schedule.priority,
            is_active=schedule.is_active,
            status=schedule.status,
            config=schedule.config,
            description=schedule.description,
            next_run=schedule_manager._get_next_run_time(schedule),
            last_run=schedule.last_run,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"스케줄 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{schedule_id}", response_model=ScheduleResponse)
async def update_schedule(
    schedule_id: int,
    update_data: ScheduleUpdate,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """스케줄 수정"""
    try:
        if not schedule_manager:
            raise HTTPException(status_code=500, detail="스케줄 관리자가 초기화되지 않았습니다")
        
        schedule = await schedule_manager.update_schedule(schedule_id, update_data, db)
        
        return ScheduleResponse(
            schedule_id=schedule.schedule_id,
            job_type=schedule.job_type,
            scheduled_time=schedule.scheduled_time,
            cron_expression=schedule.cron_expression,
            priority=schedule.priority,
            is_active=schedule.is_active,
            status=schedule.status,
            config=schedule.config,
            description=schedule.description,
            next_run=schedule_manager._get_next_run_time(schedule),
            last_run=schedule.last_run,
            created_at=schedule.created_at,
            updated_at=schedule.updated_at
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"스케줄 수정 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{schedule_id}")
async def delete_schedule(
    schedule_id: int,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """스케줄 삭제"""
    try:
        if not schedule_manager:
            raise HTTPException(status_code=500, detail="스케줄 관리자가 초기화되지 않았습니다")
        
        deleted = await schedule_manager.delete_schedule(schedule_id, db)
        
        if not deleted:
            raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
        
        return {"message": "스케줄이 삭제되었습니다"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"스케줄 삭제 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{schedule_id}/execute", response_model=ScheduleExecutionResponse)
async def execute_schedule(
    schedule_id: int,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """스케줄 즉시 실행"""
    try:
        if not schedule_manager:
            raise HTTPException(status_code=500, detail="스케줄 관리자가 초기화되지 않았습니다")
        
        # 스케줄 조회
        schedule = await schedule_manager.get_schedule(schedule_id, db)
        if not schedule:
            raise HTTPException(status_code=404, detail="스케줄을 찾을 수 없습니다")
        
        # 실행
        execution_id = await schedule_manager.execute_schedule(schedule_id)
        
        # 업데이트된 스케줄 정보 조회
        schedule = await schedule_manager.get_schedule(schedule_id, db)
        
        return ScheduleExecutionResponse(
            schedule_id=schedule.schedule_id,
            execution_id=execution_id,
            job_type=schedule.job_type,
            status=schedule.status,
            started_at=schedule.started_at,
            completed_at=schedule.completed_at,
            result_summary=schedule.result_summary,
            error_message=schedule.error_message
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"스케줄 실행 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))