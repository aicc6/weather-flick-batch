"""
배치 작업 스케줄 관리 서비스
"""
import asyncio
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from sqlalchemy import select, update, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
import pytz

from app.models import BatchJobSchedule, BatchJob
from app.api.schemas_schedule import (
    ScheduleCreate, ScheduleUpdate, ScheduleStatus,
    CronExpression
)
from app.core.logger import get_logger
from app.api.services.job_manager_db import JobManagerDB


class ScheduleManager:
    """스케줄 관리자"""
    
    def __init__(self, job_manager: JobManagerDB):
        self.logger = get_logger(__name__)
        self.job_manager = job_manager
        self.scheduler = AsyncIOScheduler(timezone=pytz.timezone('Asia/Seoul'))
        self._schedule_jobs = {}  # schedule_id -> APScheduler job
        
    async def initialize(self, db: AsyncSession):
        """스케줄러 초기화 및 기존 스케줄 로드"""
        try:
            # 스케줄러 시작
            if not self.scheduler.running:
                self.scheduler.start()
                self.logger.info("스케줄러가 시작되었습니다.")
            
            # 기존 활성 스케줄 로드
            await self._load_active_schedules(db)
            
        except Exception as e:
            self.logger.error(f"스케줄러 초기화 실패: {e}")
            raise
    
    async def _load_active_schedules(self, db: AsyncSession):
        """활성 스케줄 로드 및 등록"""
        try:
            # 활성 상태이고 예정된 스케줄 조회
            query = select(BatchJobSchedule).where(
                and_(
                    BatchJobSchedule.is_active == True,
                    or_(
                        BatchJobSchedule.status == ScheduleStatus.PENDING,
                        BatchJobSchedule.status == ScheduleStatus.SCHEDULED
                    )
                )
            )
            result = await db.execute(query)
            schedules = result.scalars().all()
            
            for schedule in schedules:
                await self._register_schedule_job(schedule, db)
                
            self.logger.info(f"{len(schedules)}개의 스케줄이 로드되었습니다.")
            
        except Exception as e:
            self.logger.error(f"스케줄 로드 실패: {e}")
            raise
    
    async def create_schedule(
        self, 
        schedule_data: ScheduleCreate, 
        db: AsyncSession
    ) -> BatchJobSchedule:
        """새 스케줄 생성"""
        try:
            # 배치 작업 유형 확인
            job_query = select(BatchJob).where(BatchJob.job_type == schedule_data.job_type)
            job_result = await db.execute(job_query)
            job = job_result.scalar_one_or_none()
            
            if not job:
                raise ValueError(f"존재하지 않는 작업 유형: {schedule_data.job_type}")
            
            # 스케줄 생성
            schedule = BatchJobSchedule(
                job_id=job.job_id,
                job_type=schedule_data.job_type,
                scheduled_time=schedule_data.scheduled_time,
                cron_expression=self._cron_to_dict(schedule_data.cron_expression) if schedule_data.cron_expression else None,
                priority=schedule_data.priority,
                is_active=schedule_data.is_active,
                status=ScheduleStatus.SCHEDULED if schedule_data.is_active else ScheduleStatus.PENDING,
                config=schedule_data.config,
                description=schedule_data.description
            )
            
            db.add(schedule)
            await db.commit()
            await db.refresh(schedule)
            
            # 활성 스케줄이면 스케줄러에 등록
            if schedule.is_active:
                await self._register_schedule_job(schedule, db)
            
            self.logger.info(f"스케줄 생성 완료: {schedule.schedule_id}")
            return schedule
            
        except Exception as e:
            await db.rollback()
            self.logger.error(f"스케줄 생성 실패: {e}")
            raise
    
    async def update_schedule(
        self,
        schedule_id: int,
        update_data: ScheduleUpdate,
        db: AsyncSession
    ) -> BatchJobSchedule:
        """스케줄 수정"""
        try:
            # 기존 스케줄 조회
            query = select(BatchJobSchedule).where(BatchJobSchedule.schedule_id == schedule_id)
            result = await db.execute(query)
            schedule = result.scalar_one_or_none()
            
            if not schedule:
                raise ValueError(f"스케줄을 찾을 수 없습니다: {schedule_id}")
            
            # 업데이트 적용
            update_dict = update_data.model_dump(exclude_unset=True)
            if 'cron_expression' in update_dict and update_dict['cron_expression']:
                update_dict['cron_expression'] = self._cron_to_dict(update_dict['cron_expression'])
            
            for key, value in update_dict.items():
                setattr(schedule, key, value)
            
            schedule.updated_at = datetime.now()
            await db.commit()
            await db.refresh(schedule)
            
            # 스케줄러 재등록
            await self._unregister_schedule_job(schedule_id)
            if schedule.is_active:
                await self._register_schedule_job(schedule, db)
            
            self.logger.info(f"스케줄 수정 완료: {schedule_id}")
            return schedule
            
        except Exception as e:
            await db.rollback()
            self.logger.error(f"스케줄 수정 실패: {e}")
            raise
    
    async def delete_schedule(self, schedule_id: int, db: AsyncSession) -> bool:
        """스케줄 삭제"""
        try:
            # 스케줄 조회
            query = select(BatchJobSchedule).where(BatchJobSchedule.schedule_id == schedule_id)
            result = await db.execute(query)
            schedule = result.scalar_one_or_none()
            
            if not schedule:
                return False
            
            # 스케줄러에서 제거
            await self._unregister_schedule_job(schedule_id)
            
            # DB에서 삭제
            await db.delete(schedule)
            await db.commit()
            
            self.logger.info(f"스케줄 삭제 완료: {schedule_id}")
            return True
            
        except Exception as e:
            await db.rollback()
            self.logger.error(f"스케줄 삭제 실패: {e}")
            raise
    
    async def get_schedule(self, schedule_id: int, db: AsyncSession) -> Optional[BatchJobSchedule]:
        """스케줄 조회"""
        query = select(BatchJobSchedule).where(BatchJobSchedule.schedule_id == schedule_id)
        result = await db.execute(query)
        return result.scalar_one_or_none()
    
    async def list_schedules(
        self,
        db: AsyncSession,
        job_type: Optional[str] = None,
        status: Optional[ScheduleStatus] = None,
        is_active: Optional[bool] = None,
        page: int = 1,
        page_size: int = 20
    ) -> Dict[str, Any]:
        """스케줄 목록 조회"""
        try:
            # 기본 쿼리
            query = select(BatchJobSchedule)
            
            # 필터 적용
            filters = []
            if job_type:
                filters.append(BatchJobSchedule.job_type == job_type)
            if status:
                filters.append(BatchJobSchedule.status == status)
            if is_active is not None:
                filters.append(BatchJobSchedule.is_active == is_active)
            
            if filters:
                query = query.where(and_(*filters))
            
            # 전체 개수
            count_result = await db.execute(query)
            total = len(count_result.scalars().all())
            
            # 페이징
            query = query.offset((page - 1) * page_size).limit(page_size)
            query = query.order_by(BatchJobSchedule.created_at.desc())
            
            result = await db.execute(query)
            schedules = result.scalars().all()
            
            # 다음 실행 시간 계산
            for schedule in schedules:
                schedule.next_run = self._get_next_run_time(schedule)
            
            return {
                "schedules": schedules,
                "total": total,
                "page": page,
                "page_size": page_size
            }
            
        except Exception as e:
            self.logger.error(f"스케줄 목록 조회 실패: {e}")
            raise
    
    async def get_upcoming_schedules(
        self,
        db: AsyncSession,
        hours: int = 24
    ) -> List[BatchJobSchedule]:
        """예정된 스케줄 조회"""
        try:
            now = datetime.now()
            end_time = now + timedelta(hours=hours)
            
            # 일회성 스케줄
            query = select(BatchJobSchedule).where(
                and_(
                    BatchJobSchedule.is_active == True,
                    BatchJobSchedule.scheduled_time.between(now, end_time),
                    BatchJobSchedule.status.in_([ScheduleStatus.SCHEDULED, ScheduleStatus.PENDING])
                )
            )
            
            result = await db.execute(query)
            schedules = list(result.scalars().all())
            
            # 반복 스케줄 확인
            cron_query = select(BatchJobSchedule).where(
                and_(
                    BatchJobSchedule.is_active == True,
                    BatchJobSchedule.cron_expression.isnot(None),
                    BatchJobSchedule.status.in_([ScheduleStatus.SCHEDULED, ScheduleStatus.PENDING])
                )
            )
            
            cron_result = await db.execute(cron_query)
            cron_schedules = cron_result.scalars().all()
            
            # 반복 스케줄의 다음 실행 시간 확인
            for schedule in cron_schedules:
                next_run = self._get_next_run_time(schedule)
                if next_run and next_run <= end_time:
                    schedule.next_run = next_run
                    schedules.append(schedule)
            
            # 시간순 정렬
            schedules.sort(key=lambda s: s.next_run or s.scheduled_time or datetime.max)
            
            return schedules
            
        except Exception as e:
            self.logger.error(f"예정 스케줄 조회 실패: {e}")
            raise
    
    async def execute_schedule(self, schedule_id: int) -> str:
        """스케줄 즉시 실행"""
        try:
            async with self.job_manager.db_manager.get_session() as db:
                # 스케줄 조회
                schedule = await self.get_schedule(schedule_id, db)
                if not schedule:
                    raise ValueError(f"스케줄을 찾을 수 없습니다: {schedule_id}")
                
                # 실행 중 상태로 변경
                schedule.status = ScheduleStatus.RUNNING
                schedule.started_at = datetime.now()
                await db.commit()
                
                # 작업 실행
                execution_id = await self.job_manager.execute_job(
                    job_type=schedule.job_type,
                    config=schedule.config or {}
                )
                
                # 실행 정보 업데이트
                schedule.last_execution_id = execution_id
                schedule.last_run = datetime.now()
                await db.commit()
                
                self.logger.info(f"스케줄 실행 시작: {schedule_id}, 실행 ID: {execution_id}")
                return execution_id
                
        except Exception as e:
            self.logger.error(f"스케줄 실행 실패: {e}")
            # 실패 상태로 변경
            async with self.job_manager.db_manager.get_session() as db:
                schedule = await self.get_schedule(schedule_id, db)
                if schedule:
                    schedule.status = ScheduleStatus.FAILED
                    schedule.error_message = str(e)
                    await db.commit()
            raise
    
    async def _register_schedule_job(self, schedule: BatchJobSchedule, db: AsyncSession):
        """스케줄을 APScheduler에 등록"""
        try:
            job_id = f"schedule_{schedule.schedule_id}"
            
            # 기존 작업 제거
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)
            
            # 트리거 생성
            trigger = None
            if schedule.cron_expression:
                # 크론 표현식 기반 반복 스케줄
                cron = schedule.cron_expression
                trigger = CronTrigger(
                    minute=cron.get('minute', '*'),
                    hour=cron.get('hour', '*'),
                    day=cron.get('day', '*'),
                    month=cron.get('month', '*'),
                    day_of_week=cron.get('day_of_week', '*'),
                    timezone='Asia/Seoul'
                )
            elif schedule.scheduled_time and schedule.scheduled_time > datetime.now():
                # 일회성 스케줄
                trigger = DateTrigger(
                    run_date=schedule.scheduled_time,
                    timezone='Asia/Seoul'
                )
            
            if trigger:
                # 작업 등록
                job = self.scheduler.add_job(
                    func=self._execute_scheduled_job,
                    trigger=trigger,
                    args=[schedule.schedule_id],
                    id=job_id,
                    name=f"{schedule.job_type}_{schedule.schedule_id}",
                    misfire_grace_time=300  # 5분
                )
                
                self._schedule_jobs[schedule.schedule_id] = job
                self.logger.info(f"스케줄 등록: {schedule.schedule_id}")
            
        except Exception as e:
            self.logger.error(f"스케줄 등록 실패: {e}")
            raise
    
    async def _unregister_schedule_job(self, schedule_id: int):
        """스케줄을 APScheduler에서 제거"""
        try:
            job_id = f"schedule_{schedule_id}"
            
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)
                
            if schedule_id in self._schedule_jobs:
                del self._schedule_jobs[schedule_id]
                
            self.logger.info(f"스케줄 제거: {schedule_id}")
            
        except Exception as e:
            self.logger.error(f"스케줄 제거 실패: {e}")
    
    async def _execute_scheduled_job(self, schedule_id: int):
        """스케줄된 작업 실행"""
        try:
            await self.execute_schedule(schedule_id)
            
            # 일회성 스케줄인 경우 완료 처리
            async with self.job_manager.db_manager.get_session() as db:
                schedule = await self.get_schedule(schedule_id, db)
                if schedule and not schedule.cron_expression:
                    schedule.status = ScheduleStatus.COMPLETED
                    schedule.completed_at = datetime.now()
                    await db.commit()
                    
        except Exception as e:
            self.logger.error(f"스케줄 작업 실행 실패: {e}")
    
    def _cron_to_dict(self, cron: CronExpression) -> Dict[str, str]:
        """CronExpression을 딕셔너리로 변환"""
        if not cron:
            return None
        
        return {
            k: v for k, v in cron.model_dump().items() if v is not None
        }
    
    def _get_next_run_time(self, schedule: BatchJobSchedule) -> Optional[datetime]:
        """다음 실행 시간 계산"""
        try:
            job_id = f"schedule_{schedule.schedule_id}"
            job = self.scheduler.get_job(job_id)
            
            if job:
                return job.next_run_time
            
            # 일회성 스케줄
            if schedule.scheduled_time and not schedule.cron_expression:
                if schedule.scheduled_time > datetime.now() and schedule.status in [ScheduleStatus.SCHEDULED, ScheduleStatus.PENDING]:
                    return schedule.scheduled_time
            
            return None
            
        except Exception as e:
            self.logger.error(f"다음 실행 시간 계산 실패: {e}")
            return None
    
    def shutdown(self):
        """스케줄러 종료"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            self.logger.info("스케줄러가 종료되었습니다.")