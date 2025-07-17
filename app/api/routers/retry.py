"""
배치 작업 재시도 관리 API 라우터
"""
from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.config import settings
from app.api.schemas_retry import (
    RetryPolicyCreate, RetryPolicyUpdate, RetryPolicyResponse,
    RetryHistoryResponse, RetryQueueResponse, RetryQueueItem,
    RetryMetrics, RetryStatus
)
from app.api.services.retry_manager import RetryManager
from app.core.async_database import get_async_db_manager
from app.core.logger import get_logger

router = APIRouter(prefix="/retries", tags=["retries"])
logger = get_logger(__name__)

# 재시도 매니저 인스턴스 (main.py에서 초기화됨)
retry_manager: Optional[RetryManager] = None


async def get_db():
    """데이터베이스 세션 의존성"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as session:
        yield session


@router.post("/policies", response_model=RetryPolicyResponse)
async def create_retry_policy(
    policy_data: RetryPolicyCreate,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """재시도 정책 생성"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        policy = await retry_manager.create_retry_policy(policy_data, db)
        
        return RetryPolicyResponse(
            policy_id=policy.policy_id,
            job_type=policy.job_type,
            max_attempts=policy.max_attempts,
            retry_strategy=policy.retry_strategy,
            initial_delay_seconds=policy.initial_delay_seconds,
            max_delay_seconds=policy.max_delay_seconds,
            backoff_multiplier=policy.backoff_multiplier,
            retry_on_errors=policy.retry_on_errors,
            enabled=policy.enabled,
            created_at=policy.created_at,
            updated_at=policy.updated_at
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"재시도 정책 생성 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/policies", response_model=List[RetryPolicyResponse])
async def list_retry_policies(
    enabled: Optional[bool] = Query(None, description="활성화 여부 필터"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """재시도 정책 목록 조회"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        # 모든 정책 조회
        from sqlalchemy import select
        from app.models import BatchJobRetryPolicy
        
        query = select(BatchJobRetryPolicy)
        if enabled is not None:
            query = query.where(BatchJobRetryPolicy.enabled == enabled)
        
        result = await db.execute(query)
        policies = result.scalars().all()
        
        return [
            RetryPolicyResponse(
                policy_id=policy.policy_id,
                job_type=policy.job_type,
                max_attempts=policy.max_attempts,
                retry_strategy=policy.retry_strategy,
                initial_delay_seconds=policy.initial_delay_seconds,
                max_delay_seconds=policy.max_delay_seconds,
                backoff_multiplier=policy.backoff_multiplier,
                retry_on_errors=policy.retry_on_errors,
                enabled=policy.enabled,
                created_at=policy.created_at,
                updated_at=policy.updated_at
            )
            for policy in policies
        ]
        
    except Exception as e:
        logger.error(f"재시도 정책 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/policies/{job_type}", response_model=RetryPolicyResponse)
async def get_retry_policy(
    job_type: str,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """특정 작업의 재시도 정책 조회"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        policy = await retry_manager.get_retry_policy(job_type, db)
        
        if not policy:
            raise HTTPException(status_code=404, detail="재시도 정책을 찾을 수 없습니다")
        
        return RetryPolicyResponse(
            policy_id=policy.policy_id,
            job_type=policy.job_type,
            max_attempts=policy.max_attempts,
            retry_strategy=policy.retry_strategy,
            initial_delay_seconds=policy.initial_delay_seconds,
            max_delay_seconds=policy.max_delay_seconds,
            backoff_multiplier=policy.backoff_multiplier,
            retry_on_errors=policy.retry_on_errors,
            enabled=policy.enabled,
            created_at=policy.created_at,
            updated_at=policy.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"재시도 정책 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/policies/{job_type}", response_model=RetryPolicyResponse)
async def update_retry_policy(
    job_type: str,
    update_data: RetryPolicyUpdate,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """재시도 정책 수정"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        policy = await retry_manager.update_retry_policy(job_type, update_data, db)
        
        return RetryPolicyResponse(
            policy_id=policy.policy_id,
            job_type=policy.job_type,
            max_attempts=policy.max_attempts,
            retry_strategy=policy.retry_strategy,
            initial_delay_seconds=policy.initial_delay_seconds,
            max_delay_seconds=policy.max_delay_seconds,
            backoff_multiplier=policy.backoff_multiplier,
            retry_on_errors=policy.retry_on_errors,
            enabled=policy.enabled,
            created_at=policy.created_at,
            updated_at=policy.updated_at
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"재시도 정책 수정 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/policies/{job_type}")
async def delete_retry_policy(
    job_type: str,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """재시도 정책 삭제"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        # 정책 조회
        policy = await retry_manager.get_retry_policy(job_type, db)
        if not policy:
            raise HTTPException(status_code=404, detail="재시도 정책을 찾을 수 없습니다")
        
        # 삭제
        await db.delete(policy)
        await db.commit()
        
        return {"message": "재시도 정책이 삭제되었습니다"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"재시도 정책 삭제 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/{job_id}/retry")
async def retry_failed_job(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """실패한 작업 수동 재시도"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        # 작업 상태 확인
        from sqlalchemy import select
        from app.models import BatchJobExecution
        
        result = await db.execute(
            select(BatchJobExecution).where(BatchJobExecution.id == job_id)
        )
        job = result.scalar_one_or_none()
        
        if not job:
            raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다")
        
        if job.status not in ["failed", "cancelled"]:
            raise HTTPException(
                status_code=400,
                detail=f"재시도할 수 없는 상태입니다: {job.status}"
            )
        
        # 재시도 스케줄링
        attempt_id = await retry_manager.check_and_retry_job(
            job_id=job_id,
            error_message="Manual retry requested",
            error_type="ManualRetry",
            db=db
        )
        
        if not attempt_id:
            raise HTTPException(
                status_code=400,
                detail="재시도 정책이 없거나 최대 재시도 횟수를 초과했습니다"
            )
        
        return {
            "message": "재시도가 예약되었습니다",
            "attempt_id": attempt_id
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"수동 재시도 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}/history", response_model=RetryHistoryResponse)
async def get_retry_history(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """작업의 재시도 이력 조회"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        history = await retry_manager.get_retry_history(job_id, db)
        
        if not history:
            raise HTTPException(status_code=404, detail="작업을 찾을 수 없습니다")
        
        return history
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"재시도 이력 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/queue", response_model=RetryQueueResponse)
async def get_retry_queue(
    status: Optional[RetryStatus] = Query(None, description="상태 필터"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """재시도 대기열 조회"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        queue_items = await retry_manager.get_retry_queue(db, status)
        
        # 상태별 카운트
        pending_count = sum(1 for item in queue_items if item["status"] == RetryStatus.PENDING)
        in_progress_count = sum(1 for item in queue_items if item["status"] == RetryStatus.IN_PROGRESS)
        
        return RetryQueueResponse(
            queue_items=[
                RetryQueueItem(
                    job_id=item["job_id"],
                    job_type=item["job_type"],
                    attempt_number=item["attempt_number"],
                    scheduled_at=item["scheduled_at"]
                )
                for item in queue_items
            ],
            total=len(queue_items),
            pending_count=pending_count,
            in_progress_count=in_progress_count
        )
        
    except Exception as e:
        logger.error(f"재시도 대기열 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", response_model=List[RetryMetrics])
async def get_retry_metrics(
    job_type: Optional[str] = Query(None, description="작업 유형 필터"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """재시도 메트릭 조회"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        metrics = await retry_manager.get_retry_metrics(
            db=db,
            job_type=job_type,
            start_date=start_date,
            end_date=end_date
        )
        
        # 가장 흔한 에러 분석 (간단한 구현)
        from sqlalchemy import select, func
        from app.models import BatchJobRetryAttempt
        
        for metric in metrics:
            # 해당 job_type의 실패한 재시도의 에러 타입 집계
            error_query = (
                select(
                    BatchJobRetryAttempt.error_type,
                    func.count(BatchJobRetryAttempt.attempt_id).label("count")
                )
                .where(
                    BatchJobRetryAttempt.job_type == metric["job_type"],
                    BatchJobRetryAttempt.status == RetryStatus.FAILED,
                    BatchJobRetryAttempt.error_type.isnot(None)
                )
                .group_by(BatchJobRetryAttempt.error_type)
                .order_by(func.count(BatchJobRetryAttempt.attempt_id).desc())
                .limit(5)
            )
            
            if start_date:
                error_query = error_query.where(BatchJobRetryAttempt.started_at >= start_date)
            if end_date:
                error_query = error_query.where(BatchJobRetryAttempt.started_at <= end_date)
            
            error_result = await db.execute(error_query)
            common_errors = [
                {"error_type": row.error_type, "count": row.count}
                for row in error_result
            ]
            
            metric["most_common_errors"] = common_errors
        
        return [
            RetryMetrics(
                job_type=m["job_type"],
                total_attempts=m["total_attempts"],
                successful_retries=m["successful_retries"],
                failed_retries=m["failed_retries"],
                average_attempts_to_success=m["average_attempts"],
                max_attempts_used=m["max_attempts_used"],
                retry_success_rate=m["retry_success_rate"],
                most_common_errors=m.get("most_common_errors", [])
            )
            for m in metrics
        ]
        
    except Exception as e:
        logger.error(f"재시도 메트릭 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/jobs/{job_id}/cancel")
async def cancel_retry(
    job_id: str,
    api_key: str = Header(None, alias="X-API-Key")
):
    """재시도 취소"""
    try:
        if not retry_manager:
            raise HTTPException(status_code=500, detail="재시도 관리자가 초기화되지 않았습니다")
        
        retry_manager.cancel_retry(job_id)
        
        return {"message": "재시도가 취소되었습니다"}
        
    except Exception as e:
        logger.error(f"재시도 취소 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))