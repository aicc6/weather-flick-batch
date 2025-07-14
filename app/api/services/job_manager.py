"""
배치 작업 관리자
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

from app.api.schemas import (
    JobType, JobStatus, JobInfo, JobListResponse,
    JobLogsResponse, JobLog, JobStatistics, SystemStatus, LogLevel
)
from app.api.config import settings
from app.core.database_manager import DatabaseManager
from app.collectors.unified_kto_client import UnifiedKTOClient
from app.collectors.weather_collector import WeatherDataCollector
from jobs.quality.data_quality_job import DataQualityJob
from app.monitoring.monitoring_system import MonitoringSystem

logger = logging.getLogger(__name__)

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

class JobManager:
    """배치 작업 관리자"""

    def __init__(self):
        self.running_jobs: Dict[str, Dict[str, Any]] = {}
        self.job_locks: Dict[str, threading.Lock] = {}
        self.executor = ThreadPoolExecutor(max_workers=settings.MAX_CONCURRENT_JOBS)
        self.db_manager = DatabaseManager()
        self.monitoring = MonitoringSystem()

        # 작업 타입별 실행 함수 매핑
        self.job_executors = {
            JobType.KTO_DATA_COLLECTION: self._execute_kto_collection,
            JobType.WEATHER_DATA_COLLECTION: self._execute_weather_collection,
            JobType.RECOMMENDATION_CALCULATION: self._execute_recommendation,
            JobType.DATA_QUALITY_CHECK: self._execute_quality_check,
            JobType.ARCHIVE_BACKUP: self._execute_archive,
            JobType.SYSTEM_HEALTH_CHECK: self._execute_health_check
        }

        # 메모리에 작업 정보 저장 (실제로는 DB 사용 권장)
        self.jobs_store: Dict[str, JobInfo] = {}
        self.jobs_logs: Dict[str, List[JobLog]] = {}

    async def get_jobs(
        self,
        job_type: Optional[JobType] = None,
        status: Optional[JobStatus] = None,
        page: int = 1,
        size: int = 20
    ) -> JobListResponse:
        """작업 목록 조회"""
        # 필터링
        filtered_jobs = []
        for job in self.jobs_store.values():
            if job_type and job.job_type != job_type:
                continue
            if status and job.status != status:
                continue
            filtered_jobs.append(job)

        # 정렬 (최신순)
        filtered_jobs.sort(key=lambda x: x.created_at, reverse=True)

        # 페이징
        start = (page - 1) * size
        end = start + size
        paged_jobs = filtered_jobs[start:end]

        return JobListResponse(
            jobs=paged_jobs,
            total=len(filtered_jobs),
            page=page,
            size=size
        )

    async def is_job_running(self, job_type: JobType) -> bool:
        """특정 타입의 작업이 실행 중인지 확인"""
        for job in self.jobs_store.values():
            if job.job_type == job_type and job.status == JobStatus.RUNNING:
                return True
        return False

    async def execute_job(
        self,
        job_id: str,
        job_type: JobType,
        parameters: Dict[str, Any],
        requested_by: Optional[str] = None
    ):
        """작업 실행"""
        # 작업 정보 생성
        job_info = JobInfo(
            id=job_id,
            job_type=job_type,
            status=JobStatus.PENDING,
            progress=0.0,
            created_at=datetime.now(),
            parameters=parameters
        )

        self.jobs_store[job_id] = job_info
        self.jobs_logs[job_id] = []
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
        job = self.jobs_store[job_id]

        try:
            # 상태 업데이트
            job.status = JobStatus.RUNNING
            job.started_at = datetime.now()

            # 작업 타입별 실행
            executor = self.job_executors.get(job_type)
            if not executor:
                raise ValueError(f"지원하지 않는 작업 타입: {job_type}")

            # 작업 실행
            asyncio.run(executor(job_id, parameters))

            # 성공 처리
            if not self.running_jobs[job_id]["should_stop"]:
                job.status = JobStatus.COMPLETED
                job.progress = 100.0
                asyncio.run(self._add_log(job_id, LogLevel.INFO, "작업 완료"))

        except Exception as e:
            # 실패 처리
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            asyncio.run(self._add_log(job_id, LogLevel.ERROR, f"작업 실패: {str(e)}"))
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)

        finally:
            # 종료 처리
            job.completed_at = datetime.now()
            del self.running_jobs[job_id]

    async def _execute_kto_collection(self, job_id: str, parameters: Dict[str, Any]):
        """KTO 데이터 수집 실행"""
        job = self.jobs_store[job_id]

        try:
            await self._add_log(job_id, LogLevel.INFO, "KTO 데이터 수집 시작")

            # KTO 수집기 생성
            collector = UnifiedKTOClient()

            # 지역 코드 목록
            region_codes = parameters.get("region_codes", ["1", "2", "3", "4", "5", "6", "7", "8"])
            total_regions = len(region_codes)

            # 전체 데이터 수집 실행
            job.current_step = "KTO 데이터 수집 중"
            job.progress = 25.0

            try:
                # 실제 수집 로직 호출
                result = await asyncio.to_thread(
                    collector.collect_all_data
                )

                await self._add_log(job_id, LogLevel.INFO,
                    "KTO 데이터 수집 완료",
                    {"result": result}
                )

            except Exception as e:
                await self._add_log(job_id, LogLevel.ERROR,
                    f"KTO 데이터 수집 실패: {str(e)}"
                )
                raise

            # 결과 요약
            job.result_summary = {
                "status": "completed",
                "result": result if 'result' in locals() else None
            }

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"KTO 수집 오류: {str(e)}")
            raise

    async def _execute_weather_collection(self, job_id: str, parameters: Dict[str, Any]):
        """날씨 데이터 수집 실행"""
        job = self.jobs_store[job_id]

        try:
            await self._add_log(job_id, LogLevel.INFO, "날씨 데이터 수집 시작")

            # 날씨 수집기 생성
            collector = WeatherDataCollector()

            # 수집 실행
            job.current_step = "날씨 데이터 수집 중"
            job.progress = 50.0

            # 실제 수집 로직
            # 모든 지역에 대해 날씨 데이터 수집
            from config.constants import WEATHER_COORDINATES

            collected_data = []
            total_regions = len(WEATHER_COORDINATES)

            for idx, region_name in enumerate(WEATHER_COORDINATES.keys()):
                try:
                    # 현재 날씨 수집
                    current_weather = await asyncio.to_thread(
                        collector.get_current_weather, region_name
                    )
                    if current_weather:
                        collected_data.append(current_weather)

                    # 진행률 업데이트
                    job.progress = 20 + (60 * (idx + 1) / total_regions)

                    # API 호출 제한 방지
                    await asyncio.sleep(0.1)

                except Exception as e:
                    await self._add_log(
                        job_id,
                        LogLevel.WARNING,
                        f"{region_name} 날씨 수집 실패: {str(e)}"
                    )

            job.result_summary = {
                "collected_cities": len(collected_data),
                "total_records": len(collected_data),
                "total_regions": total_regions
            }

            await self._add_log(job_id, LogLevel.INFO, "날씨 데이터 수집 완료", job.result_summary)

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"날씨 수집 오류: {str(e)}")
            raise

    async def _execute_recommendation(self, job_id: str, parameters: Dict[str, Any]):
        """추천 점수 계산 실행"""
        job = self.jobs_store[job_id]

        try:
            await self._add_log(job_id, LogLevel.INFO, "추천 점수 계산 시작")

            # TODO: 실제 추천 로직 구현
            job.current_step = "추천 점수 계산 중"
            job.progress = 50.0

            # 시뮬레이션
            await asyncio.sleep(5)

            result = {
                "destinations_processed": 1000,
                "recommendations_generated": 500
            }

            job.result_summary = to_dict_safe(result)
            await self._add_log(job_id, LogLevel.INFO, "추천 점수 계산 완료", to_dict_safe(result))

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"추천 계산 오류: {str(e)}")
            raise

    async def _execute_quality_check(self, job_id: str, parameters: Dict[str, Any]):
        """데이터 품질 검사 실행"""
        job = self.jobs_store[job_id]
        from app.core.base_job import JobConfig
        from config.constants import JobType
        try:
            await self._add_log(job_id, LogLevel.INFO, "데이터 품질 검사 시작")

            # 품질 검사 작업 생성
            config = JobConfig(
                job_name="data_quality_check",
                job_type=JobType.DATA_QUALITY_CHECK,
                schedule_expression="manual",  # 수동 실행 표시
                retry_count=3,
                timeout_minutes=60,
                enabled=True,
            )
            quality_job = DataQualityJob(config)

            job.current_step = "데이터 품질 검사 중"
            job.progress = 50.0

            # 실행
            result = await asyncio.to_thread(quality_job.run)

            job.result_summary = to_dict_safe(result)
            await self._add_log(job_id, LogLevel.INFO, "데이터 품질 검사 완료", to_dict_safe(result))

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"품질 검사 오류: {str(e)}")
            raise

    async def _execute_archive(self, job_id: str, parameters: Dict[str, Any]):
        """아카이빙 및 백업 실행"""
        job = self.jobs_store[job_id]

        try:
            await self._add_log(job_id, LogLevel.INFO, "아카이빙 시작")

            # 아카이브 작업 생성
            archive_job = ArchiveJob()

            job.current_step = "데이터 아카이빙 중"
            job.progress = 50.0

            # 실행
            result = await asyncio.to_thread(archive_job.run)

            job.result_summary = to_dict_safe(result)
            await self._add_log(job_id, LogLevel.INFO, "아카이빙 완료", to_dict_safe(result))

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"아카이빙 오류: {str(e)}")
            raise

    async def _execute_health_check(self, job_id: str, parameters: Dict[str, Any]):
        """시스템 헬스체크 실행"""
        job = self.jobs_store[job_id]

        try:
            await self._add_log(job_id, LogLevel.INFO, "시스템 헬스체크 시작")

            job.current_step = "시스템 상태 확인 중"
            job.progress = 50.0

            # 헬스체크 수행
            health_status = await self.get_system_status()

            job.result_summary = {
                "cpu_usage": health_status.cpu_usage,
                "memory_usage": health_status.memory_usage,
                "disk_usage": health_status.disk_usage,
                "running_jobs": health_status.running_jobs,
                "status": "healthy" if health_status.cpu_usage < 80 else "warning"
            }

            await self._add_log(job_id, LogLevel.INFO, "시스템 헬스체크 완료", job.result_summary)

        except Exception as e:
            await self._add_log(job_id, LogLevel.ERROR, f"헬스체크 오류: {str(e)}")
            raise

    async def get_job(self, job_id: str) -> Optional[JobInfo]:
        """작업 정보 조회"""
        return self.jobs_store.get(job_id)

    async def get_job_logs(
        self,
        job_id: str,
        level: Optional[str] = None,
        page: int = 1,
        size: int = 100
    ) -> Optional[JobLogsResponse]:
        """작업 로그 조회"""
        if job_id not in self.jobs_logs:
            return None

        logs = self.jobs_logs[job_id]

        # 레벨 필터링
        if level:
            logs = [log for log in logs if log.level == level]

        # 페이징
        start = (page - 1) * size
        end = start + size
        paged_logs = logs[start:end]

        return JobLogsResponse(
            job_id=job_id,
            logs=paged_logs,
            total=len(logs),
            page=page,
            size=size
        )

    async def stop_job(self, job_id: str, reason: Optional[str] = None, force: bool = False) -> bool:
        """작업 중단"""
        if job_id not in self.running_jobs:
            return False

        job = self.jobs_store.get(job_id)
        if not job or job.status != JobStatus.RUNNING:
            return False

        # 중단 플래그 설정
        self.running_jobs[job_id]["should_stop"] = True

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
        stats = {}

        for job in self.jobs_store.values():
            # 날짜 필터링
            if start_date and job.created_at < start_date:
                continue
            if end_date and job.created_at > end_date:
                continue

            # 통계 집계
            job_type = job.job_type
            if job_type not in stats:
                stats[job_type] = {
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                    "running": 0,
                    "durations": []
                }

            stats[job_type]["total"] += 1

            if job.status == JobStatus.COMPLETED:
                stats[job_type]["completed"] += 1
                if job.started_at and job.completed_at:
                    duration = (job.completed_at - job.started_at).total_seconds()
                    stats[job_type]["durations"].append(duration)
            elif job.status == JobStatus.FAILED:
                stats[job_type]["failed"] += 1
            elif job.status == JobStatus.RUNNING:
                stats[job_type]["running"] += 1

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
        cutoff_date = datetime.now() - timedelta(days=days)
        deleted = 0

        # 완료된 작업 중 오래된 것 삭제
        job_ids_to_delete = []
        for job_id, job in self.jobs_store.items():
            if job.status in [JobStatus.COMPLETED, JobStatus.FAILED]:
                if job.created_at < cutoff_date:
                    job_ids_to_delete.append(job_id)

        # 삭제 실행
        for job_id in job_ids_to_delete:
            del self.jobs_store[job_id]
            if job_id in self.jobs_logs:
                del self.jobs_logs[job_id]
            deleted += 1

        logger.info(f"Cleaned up {deleted} old jobs older than {days} days")

        return {"deleted": deleted}

    async def _add_log(
        self,
        job_id: str,
        level: LogLevel,
        message: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """로그 추가"""
        if job_id not in self.jobs_logs:
            self.jobs_logs[job_id] = []

        log = JobLog(
            timestamp=datetime.now(),
            level=level,
            message=message,
            details=to_dict_safe(details) if details is not None else None
        )

        self.jobs_logs[job_id].append(log)

        # 실제 로거에도 기록
        log_method = getattr(logger, level.value.lower(), logger.info)
        log_method(f"[Job {job_id}] {message}")
