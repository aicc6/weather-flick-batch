"""
배치 작업 관리자 - 데이터베이스 연동 버전
"""

import asyncio
import logging
import psutil
import json
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import threading
from dataclasses import asdict, is_dataclass
from sqlalchemy import and_, desc, create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError

from app.api.schemas import (
    JobType, JobStatus, JobInfo, JobListResponse,
    JobLogsResponse, JobLog, JobStatistics, SystemStatus, LogLevel
)
from app.api.config import settings
from app.collectors.unified_kto_client import UnifiedKTOClient
from app.collectors.weather_collector import WeatherDataCollector
from jobs.quality.data_quality_job import DataQualityJob
from app.monitoring.monitoring_system import MonitoringSystem
from app.models_batch import BatchJobExecution, BatchJobDetail, Base

logger = logging.getLogger(__name__)

# WebSocket 로그 전송을 위한 import (circular import 방지를 위해 동적 import)
websocket_module = None

def to_dict_safe(obj):
    from pydantic import BaseModel
    if is_dataclass(obj):
        return asdict(obj)
    elif isinstance(obj, dict):
        return obj
    elif isinstance(obj, BaseModel) or hasattr(obj, 'dict'):
        try:
            return obj.dict()
        except Exception:
            pass
    try:
        return json.loads(json.dumps(obj, default=lambda o: o.__dict__))
    except Exception:
        return {"value": str(obj)}

class JobManagerDB:
    """배치 작업 관리자 - 데이터베이스 연동"""

    def __init__(self):
        self.running_jobs: Dict[str, Dict[str, Any]] = {}
        self.job_locks: Dict[str, threading.Lock] = {}
        self.executor = ThreadPoolExecutor(max_workers=settings.MAX_CONCURRENT_JOBS)
        self.monitoring = MonitoringSystem()

        # SQLAlchemy 엔진 및 세션 설정
        self.engine = create_engine(settings.DATABASE_URL)
        Base.metadata.create_all(bind=self.engine)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        # 작업 타입별 실행 함수 매핑
        self.job_executors = {
            JobType.KTO_DATA_COLLECTION: self._execute_kto_collection,
            JobType.WEATHER_DATA_COLLECTION: self._execute_weather_collection,
            JobType.RECOMMENDATION_CALCULATION: self._execute_recommendation,
            JobType.DATA_QUALITY_CHECK: self._execute_quality_check,
            JobType.ARCHIVE_BACKUP: self._execute_archive,
            JobType.SYSTEM_HEALTH_CHECK: self._execute_health_check,
            JobType.WEATHER_CHANGE_NOTIFICATION: self._execute_weather_change_notification
        }
        
        # 재시도 매니저 참조 (나중에 설정됨)
        self.retry_manager = None
        
        # 알림 매니저 참조 (나중에 설정됨)
        self.notification_manager = None

    def get_db(self) -> Session:
        """데이터베이스 세션 생성"""
        return self.SessionLocal()

    async def get_jobs(
        self,
        job_type: Optional[JobType] = None,
        status: Optional[JobStatus] = None,
        page: int = 1,
        size: int = 20
    ) -> JobListResponse:
        """작업 목록 조회 - DB에서"""
        db = self.get_db()
        try:
            query = db.query(BatchJobExecution)

            # 필터링
            if job_type:
                query = query.filter(BatchJobExecution.job_type == job_type.value)
            if status:
                query = query.filter(BatchJobExecution.status == status.value)

            # 총 개수
            total = query.count()

            # 페이징
            offset = (page - 1) * size
            jobs = query.order_by(desc(BatchJobExecution.created_at)).offset(offset).limit(size).all()

            # JobInfo 변환
            job_infos = []
            for job in jobs:
                job_info = JobInfo(
                    id=job.id,
                    job_type=JobType(job.job_type),
                    status=JobStatus(job.status),
                    progress=job.progress or 0.0,
                    created_at=job.created_at,
                    started_at=job.started_at,
                    completed_at=job.completed_at,
                    parameters=job.parameters or {},
                    result_summary=job.result_summary,
                    error_message=job.error_message,
                    current_step=job.current_step,
                    total_steps=job.total_steps
                )
                job_infos.append(job_info)

            return JobListResponse(
                jobs=job_infos,
                total=total,
                page=page,
                size=size
            )
        finally:
            db.close()

    async def is_job_running(self, job_type: JobType) -> bool:
        """특정 타입의 작업이 실행 중인지 확인"""
        db = self.get_db()
        try:
            count = db.query(BatchJobExecution).filter(
                BatchJobExecution.job_type == job_type.value,
                BatchJobExecution.status == JobStatus.RUNNING.value
            ).count()
            return count > 0
        finally:
            db.close()

    async def execute_job(
        self,
        job_id: str,
        job_type: JobType,
        parameters: Dict[str, Any],
        requested_by: Optional[str] = None
    ):
        """작업 실행"""
        # DB에 작업 생성
        db = self.get_db()
        try:
            job = BatchJobExecution(
                id=job_id,
                job_type=job_type.value,
                status=JobStatus.PENDING.value,
                parameters=parameters,
                created_by=requested_by or "system",
                created_at=datetime.utcnow(),
                progress=0.0
            )
            db.add(job)
            db.commit()
        finally:
            db.close()

        self.running_jobs[job_id] = {"should_stop": False}

        # 로그 기록
        await self._add_log(job_id, LogLevel.INFO, f"{job_type.value} 작업 시작 요청", {
            "requested_by": requested_by,
            "parameters": parameters
        })

        # 비동기로 작업 실행
        self.executor.submit(self._run_job, job_id, job_type, parameters)

    def _run_job(self, job_id: str, job_type: JobType, parameters: Dict[str, Any]):
        """실제 작업 실행 (동기 함수)"""
        try:
            # 상태 업데이트
            db = self.get_db()
            try:
                job = db.query(BatchJobExecution).filter(
                    BatchJobExecution.id == job_id
                ).first()
                if job:
                    job.status = JobStatus.RUNNING.value
                    job.started_at = datetime.utcnow()
                    db.commit()
            finally:
                db.close()

            # 작업 타입별 실행
            executor = self.job_executors.get(job_type)
            if not executor:
                raise ValueError(f"지원하지 않는 작업 타입: {job_type}")

            # 작업 시작 알림 발송
            self._send_notification_sync(job_id, "job_started")
            
            # 작업 실행
            asyncio.run(executor(job_id, parameters))

            # 성공 처리
            if not self.running_jobs[job_id]["should_stop"]:
                db = self.get_db()
                try:
                    job = db.query(BatchJobExecution).filter(
                        BatchJobExecution.id == job_id
                    ).first()
                    if job:
                        job.status = JobStatus.COMPLETED.value
                        job.progress = 100.0
                        job.completed_at = datetime.utcnow()
                        db.commit()
                finally:
                    db.close()

                asyncio.run(self._add_log(job_id, LogLevel.INFO, "작업 완료"))
                
                # 작업 완료 알림 발송
                self._send_notification_sync(job_id, "job_completed")

        except Exception as e:
            # 실패 처리
            db = self.get_db()
            try:
                job = db.query(BatchJobExecution).filter(
                    BatchJobExecution.id == job_id
                ).first()
                if job:
                    job.status = JobStatus.FAILED.value
                    job.error_message = str(e)
                    job.completed_at = datetime.utcnow()
                    db.commit()
            finally:
                db.close()

            asyncio.run(self._add_log(job_id, LogLevel.ERROR, f"작업 실패: {str(e)}"))
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            
            # 작업 실패 알림 발송
            self._send_notification_sync(job_id, "job_failed", {"error_message": str(e)})
            
            # 재시도 매니저가 설정되어 있으면 재시도 확인
            if self.retry_manager:
                try:
                    # 비동기 재시도 확인을 동기 컨텍스트에서 실행
                    asyncio.run(self.retry_manager.check_and_retry_job(
                        job_id=job_id,
                        error_message=str(e),
                        error_type=type(e).__name__
                    ))
                except Exception as retry_error:
                    logger.error(f"재시도 처리 중 오류: {retry_error}")

        finally:
            # 종료 처리
            del self.running_jobs[job_id]

    async def _execute_kto_collection(self, job_id: str, parameters: Dict[str, Any]):
        """KTO 데이터 수집 실행"""
        try:
            await self._add_log(job_id, LogLevel.INFO, "KTO 데이터 수집 시작")
            await self._update_job_progress(job_id, 10.0, "KTO 클라이언트 초기화 중")

            # 테스트용 강제 실패
            if parameters.get("force_fail"):
                await self._add_log(job_id, LogLevel.WARNING, "테스트용 강제 실패 요청됨")
                raise ConnectionError("테스트를 위한 의도적인 연결 오류")

            # KTO 수집기 생성
            collector = UnifiedKTOClient()

            # 진행률 업데이트
            await self._update_job_progress(job_id, 25.0, "KTO 데이터 수집 중")

            # 실제 수집 로직 호출
            result = await asyncio.to_thread(
                collector.collect_all_data
            )

            # 결과 저장
            await self._update_job_result(job_id, {
                "status": "completed",
                "total_items": result.get("total_items", 0) if isinstance(result, dict) else 0,
                "new_items": result.get("new_items", 0) if isinstance(result, dict) else 0,
                "updated_items": result.get("updated_items", 0) if isinstance(result, dict) else 0,
                "errors": result.get("errors", 0) if isinstance(result, dict) else 0
            })

            await self._add_log(job_id, LogLevel.INFO,
                "KTO 데이터 수집 완료",
                {"result": result}
            )

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"KTO 수집 오류: {str(e)}")
            raise

    async def _execute_weather_collection(self, job_id: str, parameters: Dict[str, Any]):
        """날씨 데이터 수집 실행"""
        try:
            await self._add_log(job_id, LogLevel.INFO, "날씨 데이터 수집 시작")
            await self._update_job_progress(job_id, 10.0, "날씨 수집기 초기화 중")

            # 테스트용 강제 실패
            if parameters.get("force_fail"):
                await self._add_log(job_id, LogLevel.WARNING, "테스트용 강제 실패 요청됨")
                raise ConnectionError("테스트를 위한 의도적인 연결 오류")

            # 날씨 수집기 생성
            collector = WeatherDataCollector()

            # 수집 실행
            await self._update_job_progress(job_id, 20.0, "날씨 데이터 수집 중")

            # 실제 수집 로직
            from config.constants import WEATHER_COORDINATES

            collected_data = []
            total_regions = len(WEATHER_COORDINATES)
            failed_regions = 0

            for idx, region_name in enumerate(WEATHER_COORDINATES.keys()):
                try:
                    # 현재 날씨 수집
                    current_weather = await asyncio.to_thread(
                        collector.get_current_weather, region_name
                    )
                    if current_weather:
                        collected_data.append(current_weather)

                    # 진행률 업데이트
                    progress = 20 + (60 * (idx + 1) / total_regions)
                    await self._update_job_progress(
                        job_id, 
                        progress, 
                        f"{region_name} 날씨 수집 중 ({idx + 1}/{total_regions})"
                    )

                    # API 호출 제한 방지
                    await asyncio.sleep(0.1)

                except Exception as e:
                    failed_regions += 1
                    await self._add_log(
                        job_id,
                        LogLevel.WARNING,
                        f"{region_name} 날씨 수집 실패: {str(e)}"
                    )

            # 결과 저장
            result_summary = {
                "collected_cities": len(collected_data),
                "total_regions": total_regions,
                "failed_regions": failed_regions,
                "success_rate": ((total_regions - failed_regions) / total_regions * 100) if total_regions > 0 else 0
            }

            await self._update_job_result(job_id, result_summary)
            await self._add_log(job_id, LogLevel.INFO, "날씨 데이터 수집 완료", result_summary)

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"날씨 수집 오류: {str(e)}")
            raise

    async def _execute_recommendation(self, job_id: str, parameters: Dict[str, Any]):
        """추천 점수 계산 실행"""
        try:
            await self._add_log(job_id, LogLevel.INFO, "추천 점수 계산 시작")
            await self._update_job_progress(job_id, 10.0, "추천 시스템 초기화 중")

            # TODO: 실제 추천 로직 구현
            await self._update_job_progress(job_id, 50.0, "추천 점수 계산 중")

            # 시뮬레이션
            await asyncio.sleep(5)

            result = {
                "destinations_processed": 1000,
                "recommendations_generated": 500,
                "average_score": 75.5,
                "processing_time": 5.0
            }

            await self._update_job_result(job_id, result)
            await self._add_log(job_id, LogLevel.INFO, "추천 점수 계산 완료", result)

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"추천 계산 오류: {str(e)}")
            raise

    async def _execute_quality_check(self, job_id: str, parameters: Dict[str, Any]):
        """데이터 품질 검사 실행"""
        try:
            await self._add_log(job_id, LogLevel.INFO, "데이터 품질 검사 시작")
            await self._update_job_progress(job_id, 10.0, "품질 검사 초기화 중")

            # 품질 검사 작업 생성
            from app.core.base_job import JobConfig
            from config.constants import JobType as ConstJobType
            
            config = JobConfig(
                job_name="data_quality_check",
                job_type=ConstJobType.DATA_QUALITY_CHECK,
                schedule_expression="manual",
                retry_count=3,
                timeout_minutes=60,
                enabled=True,
            )
            quality_job = DataQualityJob(config)

            await self._update_job_progress(job_id, 50.0, "데이터 품질 검사 중")

            # 실행
            result = await asyncio.to_thread(quality_job.run)

            await self._update_job_result(job_id, to_dict_safe(result))
            await self._add_log(job_id, LogLevel.INFO, "데이터 품질 검사 완료", to_dict_safe(result))

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"품질 검사 오류: {str(e)}")
            raise

    async def _execute_archive(self, job_id: str, parameters: Dict[str, Any]):
        """아카이빙 및 백업 실행"""
        try:
            await self._add_log(job_id, LogLevel.INFO, "아카이빙 시작")
            await self._update_job_progress(job_id, 10.0, "아카이브 작업 초기화 중")

            # TODO: 실제 아카이브 로직 구현
            await self._update_job_progress(job_id, 50.0, "데이터 아카이빙 중")

            # 시뮬레이션
            await asyncio.sleep(3)

            result = {
                "archived_records": 5000,
                "compressed_size_mb": 150.5,
                "backup_location": "/backups/weather_flick_20240116.tar.gz"
            }

            await self._update_job_result(job_id, result)
            await self._add_log(job_id, LogLevel.INFO, "아카이빙 완료", result)

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"아카이빙 오류: {str(e)}")
            raise

    async def _execute_health_check(self, job_id: str, parameters: Dict[str, Any]):
        """시스템 헬스체크 실행"""
        try:
            await self._add_log(job_id, LogLevel.INFO, "시스템 헬스체크 시작")
            await self._update_job_progress(job_id, 50.0, "시스템 상태 확인 중")

            # 헬스체크 수행
            health_status = await self.get_system_status()

            result = {
                "cpu_usage": health_status.cpu_usage,
                "memory_usage": health_status.memory_usage,
                "disk_usage": health_status.disk_usage,
                "running_jobs": health_status.running_jobs,
                "status": "healthy" if health_status.cpu_usage < 80 else "warning",
                "timestamp": datetime.utcnow().isoformat()
            }

            await self._update_job_result(job_id, result)
            await self._add_log(job_id, LogLevel.INFO, "시스템 헬스체크 완료", result)

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"헬스체크 오류: {str(e)}")
            raise

    async def _execute_weather_change_notification(self, job_id: str, parameters: Dict[str, Any]):
        """날씨 변경 알림 실행"""
        try:
            await self._add_log(job_id, LogLevel.INFO, "날씨 변경 알림 작업 시작")
            await self._update_job_progress(job_id, 10.0, "날씨 변경 알림 시스템 초기화 중")

            # WeatherChangeNotificationJob 실행
            from jobs.notification.weather_change_notification_job import WeatherChangeNotificationJob
            from app.core.base_job import JobConfig
            from config.constants import JobType as ConstJobType
            
            config = JobConfig(
                job_name="weather_change_notification",
                job_type=ConstJobType.WEATHER_CHANGE_NOTIFICATION,
                schedule_expression="manual",
                retry_count=3,
                timeout_minutes=30,
                enabled=True,
                **parameters
            )
            
            notification_job = WeatherChangeNotificationJob(config)

            await self._update_job_progress(job_id, 50.0, "날씨 변경 감지 및 알림 처리 중")

            # 실행
            result = await asyncio.to_thread(notification_job.run)

            await self._update_job_result(job_id, to_dict_safe(result))
            await self._add_log(job_id, LogLevel.INFO, "날씨 변경 알림 작업 완료", to_dict_safe(result))

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"날씨 변경 알림 오류: {str(e)}")
            raise

    async def get_job(self, job_id: str) -> Optional[JobInfo]:
        """작업 정보 조회"""
        db = self.get_db()
        try:
            job = db.query(BatchJobExecution).filter(
                BatchJobExecution.id == job_id
            ).first()
            
            if not job:
                return None

            return JobInfo(
                id=job.id,
                job_type=JobType(job.job_type),
                status=JobStatus(job.status),
                progress=job.progress or 0.0,
                created_at=job.created_at,
                started_at=job.started_at,
                completed_at=job.completed_at,
                parameters=job.parameters or {},
                result_summary=job.result_summary,
                error_message=job.error_message,
                current_step=job.current_step,
                total_steps=job.total_steps
            )
        finally:
            db.close()

    async def get_job_logs(
        self,
        job_id: str,
        level: Optional[str] = None,
        page: int = 1,
        size: int = 100
    ) -> Optional[JobLogsResponse]:
        """작업 로그 조회"""
        db = self.get_db()
        try:
            query = db.query(BatchJobDetail).filter(
                BatchJobDetail.job_id == job_id
            )

            # 레벨 필터링
            if level:
                query = query.filter(BatchJobDetail.log_level == level)

            # 총 개수
            total = query.count()

            # 페이징
            offset = (page - 1) * size
            details = query.order_by(BatchJobDetail.created_at).offset(offset).limit(size).all()

            # JobLog 변환
            logs = []
            for detail in details:
                log = JobLog(
                    timestamp=detail.created_at,
                    level=LogLevel(detail.log_level) if detail.log_level else LogLevel.INFO,
                    message=detail.message or "",
                    details=detail.details
                )
                logs.append(log)

            return JobLogsResponse(
                job_id=job_id,
                logs=logs,
                total=total,
                page=page,
                size=size
            )
        finally:
            db.close()

    async def stop_job(self, job_id: str, reason: Optional[str] = None, force: bool = False) -> bool:
        """작업 중단"""
        if job_id not in self.running_jobs:
            return False

        db = self.get_db()
        try:
            job = db.query(BatchJobExecution).filter(
                BatchJobExecution.id == job_id
            ).first()
            
            if not job or job.status != JobStatus.RUNNING.value:
                return False

            # 중단 플래그 설정
            self.running_jobs[job_id]["should_stop"] = True

            # DB 상태 업데이트
            job.status = JobStatus.STOPPED.value
            job.completed_at = datetime.utcnow()
            job.error_message = f"작업이 중단됨: {reason}" if reason else "사용자에 의해 중단됨"
            db.commit()
        finally:
            db.close()

        await self._add_log(job_id, LogLevel.WARNING, "작업 중단 요청", {
            "reason": reason,
            "force": force
        })

        return True

    async def get_statistics(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[JobStatistics]:
        """작업 통계 조회"""
        db = self.get_db()
        try:
            query = db.query(BatchJobExecution)

            # 날짜 필터링
            if start_date:
                query = query.filter(BatchJobExecution.created_at >= start_date)
            if end_date:
                query = query.filter(BatchJobExecution.created_at <= end_date)

            jobs = query.all()

            # 통계 집계
            stats = {}
            for job in jobs:
                job_type = JobType(job.job_type)
                if job_type not in stats:
                    stats[job_type] = {
                        "total": 0,
                        "completed": 0,
                        "failed": 0,
                        "running": 0,
                        "stopped": 0,
                        "durations": []
                    }

                stats[job_type]["total"] += 1

                if job.status == JobStatus.COMPLETED.value:
                    stats[job_type]["completed"] += 1
                    if job.started_at and job.completed_at:
                        duration = (job.completed_at - job.started_at).total_seconds()
                        stats[job_type]["durations"].append(duration)
                elif job.status == JobStatus.FAILED.value:
                    stats[job_type]["failed"] += 1
                elif job.status == JobStatus.RUNNING.value:
                    stats[job_type]["running"] += 1
                elif job.status == JobStatus.STOPPED.value:
                    stats[job_type]["stopped"] += 1

            # 통계 객체 생성
            result = []
            for job_type, stat in stats.items():
                avg_duration = None
                if stat["durations"]:
                    avg_duration = sum(stat["durations"]) / len(stat["durations"])

                success_rate = 0.0
                if stat["total"] > 0:
                    success_rate = (stat["completed"] / stat["total"]) * 100

                result.append(JobStatistics(
                    job_type=job_type,
                    total_count=stat["total"],
                    completed_count=stat["completed"],
                    failed_count=stat["failed"],
                    running_count=stat["running"],
                    average_duration_seconds=avg_duration,
                    success_rate=success_rate
                ))

            return result
        finally:
            db.close()

    async def get_system_status(self) -> SystemStatus:
        """시스템 상태 조회"""
        # CPU, 메모리, 디스크 사용률
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')

        # API 키 상태 (예시)
        api_keys_status = {
            "KTO": {"available": 3, "used_today": 150, "limit": 1000},
            "KMA": {"available": 2, "used_today": 80, "limit": 1000}
        }

        return SystemStatus(
            running_jobs=len(self.running_jobs),
            max_concurrent_jobs=settings.MAX_CONCURRENT_JOBS,
            cpu_usage=cpu_percent,
            memory_usage=memory.percent,
            disk_usage=disk.percent,
            api_keys_status=api_keys_status
        )

    async def cleanup_old_data(self, days: int) -> Dict[str, Any]:
        """오래된 데이터 정리"""
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        db = self.get_db()
        try:
            # 완료된 작업 중 오래된 것 삭제
            deleted_jobs = db.query(BatchJobExecution).filter(
                BatchJobExecution.created_at < cutoff_date,
                BatchJobExecution.status.in_([
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.STOPPED.value
                ])
            ).delete()

            # 관련 로그도 삭제
            deleted_logs = db.query(BatchJobDetail).filter(
                BatchJobDetail.created_at < cutoff_date
            ).delete()

            db.commit()
        finally:
            db.close()

        logger.info(f"Cleaned up {deleted_jobs} old jobs and {deleted_logs} logs older than {days} days")

        return {
            "deleted_jobs": deleted_jobs,
            "deleted_logs": deleted_logs
        }

    async def _add_log(
        self,
        job_id: str,
        level: LogLevel,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """로그 추가"""
        db = self.get_db()
        try:
            log = BatchJobDetail(
                job_id=job_id,
                log_level=level.value,
                message=message,
                details=to_dict_safe(details) if details else None,
                created_at=datetime.utcnow()
            )
            db.add(log)
            db.commit()
        finally:
            db.close()

        # 실제 로거에도 기록
        log_method = getattr(logger, level.value.lower(), logger.info)
        log_method(f"[Job {job_id}] {message}")
        
        # WebSocket으로 실시간 로그 전송
        try:
            global websocket_module
            if websocket_module is None:
                from app.api.routers import websocket as ws_module
                websocket_module = ws_module
            
            # 비동기 태스크로 실행 (현재 함수의 실행을 차단하지 않음)
            asyncio.create_task(
                websocket_module.send_realtime_log(job_id, level.value, message, details)
            )
        except Exception as e:
            logger.warning(f"실시간 로그 전송 실패: {e}")

    async def _update_job_progress(self, job_id: str, progress: float, current_step: Optional[str] = None):
        """작업 진행률 업데이트"""
        db = self.get_db()
        try:
            job = db.query(BatchJobExecution).filter(
                BatchJobExecution.id == job_id
            ).first()
            if job:
                job.progress = progress
                if current_step:
                    job.current_step = current_step
                db.commit()
        finally:
            db.close()
        
        # WebSocket으로 진행률 업데이트 전송
        try:
            global websocket_module
            if websocket_module is None:
                from app.api.routers import websocket as ws_module
                websocket_module = ws_module
            
            asyncio.create_task(
                websocket_module.send_job_progress_update(job_id, progress, current_step)
            )
        except Exception as e:
            logger.warning(f"실시간 진행률 업데이트 전송 실패: {e}")

    async def _update_job_result(self, job_id: str, result_summary: Dict[str, Any]):
        """작업 결과 업데이트"""
        db = self.get_db()
        try:
            job = db.query(BatchJobExecution).filter(
                BatchJobExecution.id == job_id
            ).first()
            if job:
                job.result_summary = result_summary
                db.commit()
        finally:
            db.close()
    
    def _send_notification_sync(self, job_id: str, event: str, additional_data: Optional[Dict[str, Any]] = None):
        """동기 컨텍스트에서 알림 발송"""
        if not self.notification_manager:
            return
            
        try:
            from app.api.schemas_notification import NotificationEvent, NotificationLevel, SendNotificationRequest
            
            # 이벤트 레벨 결정
            level = NotificationLevel.INFO
            if event == "job_failed":
                level = NotificationLevel.ERROR
            elif event == "job_retry_max_attempts":
                level = NotificationLevel.CRITICAL
                
            # 알림 요청 생성
            request = SendNotificationRequest(
                job_id=job_id,
                event=NotificationEvent(event),
                level=level,
                additional_data=additional_data
            )
            
            # 비동기 알림 발송을 동기 컨텍스트에서 실행
            asyncio.run(self._send_notification_async(request))
            
        except Exception as e:
            logger.error(f"알림 발송 중 오류: {e}")
    
    async def _send_notification_async(self, request):
        """비동기 알림 발송"""
        from app.core.async_database import get_async_db_manager
        
        async_db_manager = get_async_db_manager()
        async with async_db_manager.get_session() as db:
            await self.notification_manager.send_notification(request, db)