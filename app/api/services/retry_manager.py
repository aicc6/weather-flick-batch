"""
배치 작업 재시도 관리 서비스
"""
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from sqlalchemy import select, update, and_, or_, func, Integer
from sqlalchemy.ext.asyncio import AsyncSession
import json

from app.api.schemas_retry import (
    RetryPolicyCreate, RetryPolicyUpdate, RetryStrategy,
    RetryStatus, RetryAttemptCreate
)
from app.models import BatchJobRetryPolicy, BatchJobRetryAttempt
from app.models_batch import BatchJobExecution
from app.core.logger import get_logger
from app.api.services.job_manager_db import JobManagerDB


class RetryManager:
    """재시도 관리자"""
    
    def __init__(self, job_manager: JobManagerDB):
        self.logger = get_logger(__name__)
        self.job_manager = job_manager
        self._retry_tasks = {}  # job_id -> asyncio.Task
        
    async def create_retry_policy(
        self,
        policy_data: RetryPolicyCreate,
        db: AsyncSession
    ) -> BatchJobRetryPolicy:
        """재시도 정책 생성"""
        try:
            # 기존 정책 확인
            existing = await db.execute(
                select(BatchJobRetryPolicy).where(
                    BatchJobRetryPolicy.job_type == policy_data.job_type
                )
            )
            if existing.scalar_one_or_none():
                raise ValueError(f"재시도 정책이 이미 존재합니다: {policy_data.job_type}")
            
            # 새 정책 생성
            policy = BatchJobRetryPolicy(
                job_type=policy_data.job_type,
                max_attempts=policy_data.max_attempts,
                retry_strategy=policy_data.retry_strategy,
                initial_delay_seconds=policy_data.initial_delay_seconds,
                max_delay_seconds=policy_data.max_delay_seconds,
                backoff_multiplier=policy_data.backoff_multiplier,
                retry_on_errors=policy_data.retry_on_errors,
                enabled=policy_data.enabled
            )
            
            db.add(policy)
            await db.commit()
            await db.refresh(policy)
            
            self.logger.info(f"재시도 정책 생성 완료: {policy_data.job_type}")
            return policy
            
        except Exception as e:
            await db.rollback()
            self.logger.error(f"재시도 정책 생성 실패: {e}")
            raise
    
    async def update_retry_policy(
        self,
        job_type: str,
        update_data: RetryPolicyUpdate,
        db: AsyncSession
    ) -> BatchJobRetryPolicy:
        """재시도 정책 수정"""
        try:
            # 기존 정책 조회
            result = await db.execute(
                select(BatchJobRetryPolicy).where(
                    BatchJobRetryPolicy.job_type == job_type
                )
            )
            policy = result.scalar_one_or_none()
            
            if not policy:
                raise ValueError(f"재시도 정책을 찾을 수 없습니다: {job_type}")
            
            # 업데이트
            update_dict = update_data.model_dump(exclude_unset=True)
            for key, value in update_dict.items():
                setattr(policy, key, value)
            
            policy.updated_at = datetime.now()
            await db.commit()
            await db.refresh(policy)
            
            self.logger.info(f"재시도 정책 수정 완료: {job_type}")
            return policy
            
        except Exception as e:
            await db.rollback()
            self.logger.error(f"재시도 정책 수정 실패: {e}")
            raise
    
    async def get_retry_policy(
        self,
        job_type: str,
        db: AsyncSession
    ) -> Optional[BatchJobRetryPolicy]:
        """재시도 정책 조회"""
        result = await db.execute(
            select(BatchJobRetryPolicy).where(
                BatchJobRetryPolicy.job_type == job_type
            )
        )
        return result.scalar_one_or_none()
    
    async def check_and_retry_job(
        self,
        job_id: str,
        error_message: str,
        error_type: Optional[str] = None,
        db: AsyncSession = None
    ) -> Optional[str]:
        """작업 실패 시 재시도 필요 여부 확인 및 재시도 스케줄링"""
        try:
            # 자체 DB 세션 사용
            if db is None:
                from app.core.async_database import get_async_db_manager
                async_db_manager = get_async_db_manager()
                async with async_db_manager.get_session() as db:
                    return await self._check_and_retry_job_internal(
                        job_id, error_message, error_type, db
                    )
            else:
                return await self._check_and_retry_job_internal(
                    job_id, error_message, error_type, db
                )
                
        except Exception as e:
            self.logger.error(f"재시도 확인 중 오류: {e}")
            return None
    
    async def _check_and_retry_job_internal(
        self,
        job_id: str,
        error_message: str,
        error_type: Optional[str],
        db: AsyncSession
    ) -> Optional[str]:
        """실제 재시도 확인 및 스케줄링 로직"""
        # 작업 정보 조회
        job_result = await db.execute(
            select(BatchJobExecution).where(
                BatchJobExecution.id == job_id
            )
        )
        job = job_result.scalar_one_or_none()
        
        if not job:
            self.logger.error(f"작업을 찾을 수 없습니다: {job_id}")
            return None
        
        # 재시도 정책 조회
        policy = await self.get_retry_policy(job.job_type, db)
        if not policy or not policy.enabled:
            self.logger.info(f"재시도 정책이 없거나 비활성화됨: {job.job_type}")
            return None
        
        # 에러 타입 확인
        if policy.retry_on_errors and error_type:
            if error_type not in policy.retry_on_errors:
                self.logger.info(f"재시도 대상 에러가 아님: {error_type}")
                return None
        
        # 현재까지의 재시도 횟수 확인
        attempts_result = await db.execute(
            select(func.count(BatchJobRetryAttempt.attempt_id)).where(
                BatchJobRetryAttempt.job_id == job_id
            )
        )
        attempt_count = attempts_result.scalar() or 0
        
        if attempt_count >= policy.max_attempts:
            self.logger.warning(f"최대 재시도 횟수 도달: {job_id}, {attempt_count}/{policy.max_attempts}")
            # 최종 실패 상태로 업데이트
            await self._update_job_retry_status(job_id, RetryStatus.MAX_ATTEMPTS_REACHED, db)
            return None
        
        # 다음 시도까지의 지연 시간 계산
        delay_seconds = self._calculate_retry_delay(
            attempt_count + 1,
            policy.retry_strategy,
            policy.initial_delay_seconds,
            policy.max_delay_seconds,
            policy.backoff_multiplier
        )
        
        # 재시도 시도 기록
        retry_attempt = BatchJobRetryAttempt(
            job_id=job_id,
            job_type=job.job_type,
            attempt_number=attempt_count + 1,
            status=RetryStatus.PENDING,
            error_message=error_message,
            error_type=error_type,
            delay_seconds=delay_seconds,
            next_retry_at=datetime.now() + timedelta(seconds=delay_seconds)
        )
        
        db.add(retry_attempt)
        await db.commit()
        await db.refresh(retry_attempt)
        
        # 재시도 스케줄링
        retry_task = asyncio.create_task(
            self._schedule_retry(job_id, retry_attempt.attempt_id, delay_seconds)
        )
        self._retry_tasks[job_id] = retry_task
        
        self.logger.info(
            f"재시도 스케줄링: job_id={job_id}, "
            f"attempt={attempt_count + 1}, delay={delay_seconds}초"
        )
        
        return retry_attempt.attempt_id
    
    def _calculate_retry_delay(
        self,
        attempt_number: int,
        strategy: RetryStrategy,
        initial_delay: int,
        max_delay: int,
        backoff_multiplier: float
    ) -> int:
        """재시도 지연 시간 계산"""
        if strategy == RetryStrategy.IMMEDIATE:
            return 0
        elif strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = initial_delay * attempt_number
        elif strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = initial_delay * (backoff_multiplier ** (attempt_number - 1))
        else:  # CUSTOM
            delay = initial_delay
        
        return min(int(delay), max_delay)
    
    async def _schedule_retry(self, job_id: str, attempt_id: str, delay_seconds: int):
        """재시도 스케줄링"""
        try:
            # 지연 시간 대기
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
            
            # 재시도 실행
            await self._execute_retry(job_id, attempt_id)
            
        except asyncio.CancelledError:
            self.logger.info(f"재시도 스케줄 취소됨: {job_id}")
        except Exception as e:
            self.logger.error(f"재시도 스케줄링 오류: {e}")
    
    async def _execute_retry(self, job_id: str, attempt_id: str):
        """재시도 실행"""
        from app.core.async_database import get_async_db_manager
        async_db_manager = get_async_db_manager()
        async with async_db_manager.get_session() as db:
            try:
                # 재시도 시도 상태 업데이트
                await db.execute(
                    update(BatchJobRetryAttempt)
                    .where(BatchJobRetryAttempt.attempt_id == attempt_id)
                    .values(
                        status=RetryStatus.IN_PROGRESS,
                        started_at=datetime.now()
                    )
                )
                await db.commit()
                
                # 원본 작업 정보 조회
                job_result = await db.execute(
                    select(BatchJobExecution).where(
                        BatchJobExecution.id == job_id
                    )
                )
                original_job = job_result.scalar_one_or_none()
                
                if not original_job:
                    raise ValueError(f"원본 작업을 찾을 수 없습니다: {job_id}")
                
                # 작업 재실행
                self.logger.info(f"작업 재시도 시작: {job_id} (시도 {attempt_id})")
                
                new_job_id = await self.job_manager.execute_job(
                    job_type=original_job.job_type,
                    config=original_job.config or {},
                    requested_by=f"retry_{attempt_id}"
                )
                
                # 재시도 성공 처리
                await db.execute(
                    update(BatchJobRetryAttempt)
                    .where(BatchJobRetryAttempt.attempt_id == attempt_id)
                    .values(
                        status=RetryStatus.SUCCESS,
                        completed_at=datetime.now(),
                        retry_job_id=new_job_id
                    )
                )
                await db.commit()
                
                self.logger.info(f"재시도 성공: {job_id} -> {new_job_id}")
                
            except Exception as e:
                self.logger.error(f"재시도 실행 실패: {e}")
                
                # 재시도 실패 처리
                await db.execute(
                    update(BatchJobRetryAttempt)
                    .where(BatchJobRetryAttempt.attempt_id == attempt_id)
                    .values(
                        status=RetryStatus.FAILED,
                        completed_at=datetime.now(),
                        error_message=str(e)
                    )
                )
                await db.commit()
                
                # 추가 재시도 필요 여부 확인
                await self.check_and_retry_job(job_id, str(e), type(e).__name__, db)
    
    async def _update_job_retry_status(
        self,
        job_id: str,
        status: RetryStatus,
        db: AsyncSession
    ):
        """작업의 재시도 상태 업데이트"""
        # 현재 재시도 횟수 조회
        attempts_result = await db.execute(
            select(func.count(BatchJobRetryAttempt.attempt_id)).where(
                BatchJobRetryAttempt.job_id == job_id
            )
        )
        retry_count = attempts_result.scalar() or 0
        
        await db.execute(
            update(BatchJobExecution)
            .where(BatchJobExecution.id == job_id)
            .values(
                retry_status=status.value,
                retry_count=retry_count,
                updated_at=datetime.now()
            )
        )
        await db.commit()
    
    async def get_retry_history(
        self,
        job_id: str,
        db: AsyncSession
    ) -> Dict[str, Any]:
        """작업의 재시도 이력 조회"""
        # 원본 작업 정보
        job_result = await db.execute(
            select(BatchJobExecution).where(
                BatchJobExecution.id == job_id
            )
        )
        job = job_result.scalar_one_or_none()
        
        if not job:
            return None
        
        # 재시도 시도 목록
        attempts_result = await db.execute(
            select(BatchJobRetryAttempt)
            .where(BatchJobRetryAttempt.job_id == job_id)
            .order_by(BatchJobRetryAttempt.attempt_number)
        )
        attempts = attempts_result.scalars().all()
        
        return {
            "job_id": job_id,
            "job_type": job.job_type,
            "original_started_at": job.started_at,
            "total_attempts": len(attempts),
            "successful": any(a.status == RetryStatus.SUCCESS for a in attempts),
            "attempts": attempts
        }
    
    async def get_retry_queue(
        self,
        db: AsyncSession,
        status: Optional[RetryStatus] = None
    ) -> List[Dict[str, Any]]:
        """재시도 대기열 조회"""
        query = select(BatchJobRetryAttempt).join(
            BatchJobExecution,
            BatchJobRetryAttempt.job_id == BatchJobExecution.id
        )
        
        if status:
            query = query.where(BatchJobRetryAttempt.status == status)
        else:
            query = query.where(
                BatchJobRetryAttempt.status.in_([
                    RetryStatus.PENDING,
                    RetryStatus.IN_PROGRESS
                ])
            )
        
        query = query.order_by(BatchJobRetryAttempt.next_retry_at)
        
        result = await db.execute(query)
        attempts = result.scalars().all()
        
        queue_items = []
        for attempt in attempts:
            queue_items.append({
                "job_id": attempt.job_id,
                "job_type": attempt.job_type,
                "attempt_number": attempt.attempt_number,
                "scheduled_at": attempt.next_retry_at,
                "status": attempt.status
            })
        
        return queue_items
    
    async def get_retry_metrics(
        self,
        db: AsyncSession,
        job_type: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[Dict[str, Any]]:
        """재시도 메트릭 조회"""
        # 기본 쿼리
        query = select(
            BatchJobRetryAttempt.job_type,
            func.count(BatchJobRetryAttempt.attempt_id).label('total_attempts'),
            func.sum(
                func.cast(
                    BatchJobRetryAttempt.status == RetryStatus.SUCCESS,
                    Integer
                )
            ).label('successful_retries'),
            func.avg(BatchJobRetryAttempt.attempt_number).label('avg_attempts'),
            func.max(BatchJobRetryAttempt.attempt_number).label('max_attempts')
        ).group_by(BatchJobRetryAttempt.job_type)
        
        # 필터 적용
        filters = []
        if job_type:
            filters.append(BatchJobRetryAttempt.job_type == job_type)
        if start_date:
            filters.append(BatchJobRetryAttempt.started_at >= start_date)
        if end_date:
            filters.append(BatchJobRetryAttempt.started_at <= end_date)
        
        if filters:
            query = query.where(and_(*filters))
        
        result = await db.execute(query)
        metrics = []
        
        for row in result:
            metrics.append({
                "job_type": row.job_type,
                "total_attempts": row.total_attempts,
                "successful_retries": row.successful_retries or 0,
                "failed_retries": row.total_attempts - (row.successful_retries or 0),
                "average_attempts": float(row.avg_attempts) if row.avg_attempts else 0,
                "max_attempts_used": row.max_attempts or 0,
                "retry_success_rate": (
                    (row.successful_retries or 0) / row.total_attempts * 100
                    if row.total_attempts > 0 else 0
                )
            })
        
        return metrics
    
    def cancel_retry(self, job_id: str):
        """재시도 취소"""
        if job_id in self._retry_tasks:
            task = self._retry_tasks[job_id]
            if not task.done():
                task.cancel()
            del self._retry_tasks[job_id]
            self.logger.info(f"재시도 취소됨: {job_id}")