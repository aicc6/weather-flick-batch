"""
고급 배치 작업 스케줄러 (APScheduler 기반)

WeatherFlick 프로젝트의 포괄적인 배치 및 스케줄링 시스템
"""

from datetime import datetime
from typing import Dict, List, Callable, Any
from dataclasses import dataclass
from enum import Enum

try:
    from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
    from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED

    APSCHEDULER_AVAILABLE = True
except ImportError:
    # APScheduler가 없으면 기본 스케줄러 사용
    APSCHEDULER_AVAILABLE = False
    print("⚠️ APScheduler를 찾을 수 없습니다. 기본 스케줄러를 사용합니다.")
    print("설치하려면: pip install apscheduler redis")

from app.core.logger import get_logger
from config.batch_settings import get_batch_settings


# JobResult와 JobStatus는 여기서 정의 (의존성 문제 해결)
@dataclass
class JobResult:
    """배치 작업 실행 결과"""

    job_name: str
    job_type: str
    start_time: datetime
    end_time: datetime
    status: "JobStatus" = None
    processed_records: int = 0
    error_message: str = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class JobStatus(Enum):
    """작업 상태"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class BatchJobType(Enum):
    """배치 작업 유형"""

    # 데이터 관리
    WEATHER_UPDATE = "weather_update"
    DESTINATION_SYNC = "destination_sync"
    USER_STATS = "user_stats"

    # 관광정보 데이터
    COMPREHENSIVE_TOURISM_SYNC = "comprehensive_tourism_sync"
    INCREMENTAL_TOURISM_SYNC = "incremental_tourism_sync"
    CULTURAL_DATA = "cultural_data"
    FESTIVAL_DATA = "festival_data"
    ACCOMMODATION_DATA = "accommodation_data"
    RESTAURANT_DATA = "restaurant_data"

    # 시스템 유지보수
    LOG_CLEANUP = "log_cleanup"
    DATABASE_BACKUP = "database_backup"
    CACHE_MAINTENANCE = "cache_maintenance"

    # 비즈니스 로직
    RECOMMENDATION_UPDATE = "recommendation_update"
    POPULARITY_UPDATE = "popularity_update"
    BEHAVIOR_ANALYSIS = "behavior_analysis"

    # 모니터링
    HEALTH_CHECK = "health_check"
    METRICS_COLLECTION = "metrics_collection"
    NOTIFICATION = "notification"
    WEATHER_CHANGE_NOTIFICATION = "weather_change_notification"
    
    # 데이터 품질
    DATA_QUALITY_CHECK = "data_quality_check"


class JobPriority(Enum):
    """작업 우선순위"""

    CRITICAL = 1
    HIGH = 2
    MEDIUM = 3
    LOW = 4


@dataclass
class BatchJobConfig:
    """배치 작업 설정"""

    job_id: str
    job_type: BatchJobType
    name: str
    description: str
    priority: JobPriority
    max_instances: int = 1
    coalesce: bool = True
    misfire_grace_time: int = 60  # seconds
    dependencies: List[str] = None
    timeout: int = 3600  # seconds
    retry_attempts: int = 3
    retry_delay: int = 60  # seconds

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []

    @property
    def job_name(self) -> str:
        """job_name 속성 호환성을 위한 프로퍼티"""
        return self.name


class BatchJobManager:
    """배치 작업 관리자"""

    def __init__(self):
        self.settings = get_batch_settings()
        self.logger = get_logger(__name__)
        self.scheduler = None
        self.job_configs: Dict[str, BatchJobConfig] = {}
        self.job_functions: Dict[str, Callable] = {}
        self.job_results: Dict[str, JobResult] = {}
        self.is_running = False

        if APSCHEDULER_AVAILABLE:
            self._setup_scheduler()
        else:
            self._setup_fallback_scheduler()

    def _setup_scheduler(self):
        """APScheduler 기반 스케줄러 초기 설정"""
        try:
            # Job Store 설정 (직렬화 문제 해결을 위해 메모리 사용)
            jobstores = {}
            from apscheduler.jobstores.memory import MemoryJobStore

            jobstores["default"] = MemoryJobStore()
            self.logger.info("메모리 기반 Job Store 사용 (직렬화 문제 해결)")

            # Executor 설정
            executors = {
                "default": ThreadPoolExecutor(max_workers=20),
                "processpool": ProcessPoolExecutor(max_workers=5),
            }

            # Job 기본 설정
            job_defaults = {
                "coalesce": True,
                "max_instances": 3,
                "misfire_grace_time": 60,
            }

            # 스케줄러 생성 (BackgroundScheduler 사용으로 변경)
            from apscheduler.schedulers.background import BackgroundScheduler

            self.scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone="Asia/Seoul",
            )

            # 이벤트 리스너 등록
            self.scheduler.add_listener(
                self._job_executed_listener,
                EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED,
            )

            self.logger.info("APScheduler 스케줄러 초기화 완료")

        except Exception as e:
            self.logger.error(f"APScheduler 초기화 실패: {e}")
            self._setup_fallback_scheduler()

    def _setup_fallback_scheduler(self):
        """기본 스케줄러 설정 (APScheduler 없을 때)"""
        self.logger.warning("기본 스케줄러 모드로 실행됩니다.")
        self.scheduler = None  # 기본 스케줄러는 별도 구현

    def register_job(
        self, config: BatchJobConfig, job_function: Callable, **schedule_kwargs
    ) -> str:
        """작업 등록"""
        try:
            # 의존성 검증
            self._validate_dependencies(config.dependencies)

            # 설정 저장
            self.job_configs[config.job_id] = config
            self.job_functions[config.job_id] = job_function

            if APSCHEDULER_AVAILABLE and self.scheduler:
                # APScheduler 사용
                job = self.scheduler.add_job(
                    func=self._execute_job_with_wrapper,
                    args=[config.job_id],
                    id=config.job_id,
                    name=config.name,
                    max_instances=config.max_instances,
                    coalesce=config.coalesce,
                    misfire_grace_time=config.misfire_grace_time,
                    **{k: v for k, v in schedule_kwargs.items() if k != "id"},
                )
                job_id = job.id
            else:
                # 기본 스케줄러 사용
                job_id = config.job_id
                self.logger.warning(f"기본 스케줄러 모드로 작업 등록: {config.job_id}")

            self.logger.info(f"배치 작업 등록 완료: {config.job_id} - {config.name}")
            return job_id

        except Exception as e:
            self.logger.error(f"작업 등록 실패: {config.job_id}, 오류: {e}")
            raise

    def _execute_job_with_wrapper(self, job_id: str):
        """작업 실행 래퍼 (의존성, 타임아웃, 재시도 처리)"""
        config = self.job_configs[job_id]
        job_function = self.job_functions[job_id]

        start_time = datetime.now()

        try:
            # 의존성 체크 (동기 버전으로 변경)
            if not self._check_dependencies_sync(config.dependencies):
                raise Exception(f"의존성 체크 실패: {config.dependencies}")

            # 작업 실행 (간소화)
            result = self._execute_job_with_retry_sync(job_id, job_function)

            # 성공 결과 저장
            job_result = JobResult(
                job_name=job_id,
                job_type=config.job_type.value,
                start_time=start_time,
                end_time=datetime.now(),
                status=JobStatus.SUCCESS,
                processed_records=(
                    result.get("processed_records", 0)
                    if isinstance(result, dict)
                    else 0
                ),
                metadata=result if isinstance(result, dict) else {},
            )

        except Exception as e:
            # 타임아웃 처리 제거 (간소화)
            if "timeout" in str(e).lower():
                job_result = JobResult(
                    job_name=job_id,
                    job_type=config.job_type.value,
                    start_time=start_time,
                    end_time=datetime.now(),
                    status=JobStatus.FAILED,
                    error_message=f"작업 타임아웃 ({config.timeout}초)",
                )
            else:
                job_result = JobResult(
                    job_name=job_id,
                    job_type=config.job_type.value,
                    start_time=start_time,
                    end_time=datetime.now(),
                    status=JobStatus.FAILED,
                    error_message=str(e),
                )

        # 결과 저장 및 로깅
        self.job_results[job_id] = job_result
        self._log_job_result_sync(job_result)

        return job_result

    def _execute_job_with_retry_sync(self, job_id: str, job_function: Callable):
        """재시도 로직이 포함된 작업 실행 (동기 버전)"""
        config = self.job_configs[job_id]
        last_error = None

        for attempt in range(config.retry_attempts + 1):
            try:
                self.logger.info(
                    f"작업 실행 시도 {attempt + 1}/{config.retry_attempts + 1}: {job_id}"
                )

                # 작업 실행 (동기 버전)
                result = job_function()

                if attempt > 0:
                    self.logger.info(f"작업 재시도 성공: {job_id}")

                return result

            except Exception as e:
                last_error = e

                if attempt < config.retry_attempts:
                    self.logger.warning(
                        f"작업 실패, 재시도 {attempt + 1}/{config.retry_attempts}: {job_id}, 오류: {e}"
                    )
                    import time

                    time.sleep(config.retry_delay * (2**attempt))  # 지수 백오프
                else:
                    self.logger.error(f"작업 최종 실패: {job_id}, 오류: {e}")

        raise last_error

    def _check_dependencies_sync(self, dependencies: List[str]) -> bool:
        """작업 의존성 체크 (동기 버전)"""
        if not dependencies:
            return True

        for dep_job_id in dependencies:
            # 최근 24시간 내 성공한 작업이 있는지 확인
            if dep_job_id not in self.job_results:
                self.logger.warning(f"의존성 작업 결과 없음: {dep_job_id}")
                return False

            dep_result = self.job_results[dep_job_id]

            # 24시간 이내 성공 여부 확인
            if (datetime.now() - dep_result.end_time).total_seconds() > 86400:
                self.logger.warning(f"의존성 작업이 오래됨: {dep_job_id}")
                return False

            if dep_result.status != JobStatus.SUCCESS:
                self.logger.warning(f"의존성 작업 실패: {dep_job_id}")
                return False

        return True

    def _validate_dependencies(self, dependencies: List[str]):
        """의존성 순환 참조 검증"""
        # TODO: 순환 참조 검출 로직 구현
        pass

    def _log_job_result_sync(self, result: JobResult):
        """작업 결과 로깅 (동기 버전)"""
        try:
            # 데이터베이스에 결과 저장 (DatabaseManager 사용)
            try:
                from app.core.database_manager import DatabaseManager

                db_manager = DatabaseManager()

                # job_type 파라미터 포함하여 호출
                db_manager.log_job_result(
                    job_name=result.job_name,
                    job_type=result.job_type,  # 필수 파라미터 추가
                    status=result.status.value,
                    start_time=result.start_time,
                    end_time=result.end_time,
                    processed_records=result.processed_records,
                    error_message=result.error_message,
                )
            except Exception as db_error:
                self.logger.warning(f"데이터베이스 로깅 실패: {db_error}")

            # 로그 출력
            duration = (result.end_time - result.start_time).total_seconds()

            if result.status == JobStatus.SUCCESS:
                self.logger.info(
                    f"작업 완료: {result.job_name}, "
                    f"소요시간: {duration:.2f}초, "
                    f"처리건수: {result.processed_records}"
                )
            else:
                self.logger.error(
                    f"작업 실패: {result.job_name}, 오류: {result.error_message}"
                )

        except Exception as e:
            self.logger.error(f"작업 결과 로깅 실패: {e}")

    def _job_executed_listener(self, event):
        """작업 실행 이벤트 리스너"""
        job_id = event.job_id

        if event.exception:
            self.logger.error(f"작업 예외 발생: {job_id}, 예외: {event.exception}")
        else:
            self.logger.debug(f"작업 실행 완료: {job_id}")

    def get_job_status(self, job_id: str = None) -> Dict[str, Any]:
        """작업 상태 조회"""
        if job_id:
            # 특정 작업 상태
            if APSCHEDULER_AVAILABLE and self.scheduler:
                job = self.scheduler.get_job(job_id)
                if not job:
                    return {"error": f"작업을 찾을 수 없음: {job_id}"}

                return {
                    "job_id": job.id,
                    "name": job.name,
                    "next_run": (
                        job.next_run_time.isoformat() if job.next_run_time else None
                    ),
                    "last_result": self.job_results.get(job_id),
                }
            else:
                # 기본 스케줄러 모드
                config = self.job_configs.get(job_id)
                if not config:
                    return {"error": f"작업을 찾을 수 없음: {job_id}"}

                return {
                    "job_id": job_id,
                    "name": config.name,
                    "next_run": None,  # 기본 스케줄러에서는 다음 실행 시간 계산 안 함
                    "last_result": self.job_results.get(job_id),
                }
        else:
            # 전체 작업 상태
            jobs_status = {}

            if APSCHEDULER_AVAILABLE and self.scheduler:
                for job in self.scheduler.get_jobs():
                    jobs_status[job.id] = {
                        "name": job.name,
                        "next_run": (
                            job.next_run_time.isoformat() if job.next_run_time else None
                        ),
                        "last_result": self.job_results.get(job.id),
                    }
            else:
                # 기본 스케줄러 모드
                for job_id, config in self.job_configs.items():
                    jobs_status[job_id] = {
                        "name": config.name,
                        "next_run": None,
                        "last_result": self.job_results.get(job_id),
                    }

            return jobs_status

    def run_job_now(self, job_id: str) -> JobResult:
        """작업 즉시 실행"""
        if job_id not in self.job_configs:
            raise ValueError(f"등록되지 않은 작업: {job_id}")

        self.logger.info(f"수동 작업 실행: {job_id}")
        return self._execute_job_with_wrapper(job_id)

    def pause_job(self, job_id: str):
        """작업 일시 정지"""
        if APSCHEDULER_AVAILABLE and self.scheduler:
            self.scheduler.pause_job(job_id)
            self.logger.info(f"작업 일시 정지: {job_id}")
        else:
            self.logger.warning(
                f"기본 스케줄러에서는 일시 정지 기능을 지원하지 않습니다: {job_id}"
            )

    def resume_job(self, job_id: str):
        """작업 재개"""
        if APSCHEDULER_AVAILABLE and self.scheduler:
            self.scheduler.resume_job(job_id)
            self.logger.info(f"작업 재개: {job_id}")
        else:
            self.logger.warning(
                f"기본 스케줄러에서는 재개 기능을 지원하지 않습니다: {job_id}"
            )

    def modify_job_schedule(self, job_id: str, **schedule_kwargs):
        """작업 스케줄 변경"""
        if APSCHEDULER_AVAILABLE and self.scheduler:
            self.scheduler.modify_job(job_id, **schedule_kwargs)
            self.logger.info(f"작업 스케줄 변경: {job_id}")
        else:
            self.logger.warning(
                f"기본 스케줄러에서는 스케줄 변경 기능을 지원하지 않습니다: {job_id}"
            )

    def start(self):
        """스케줄러 시작"""
        self.logger.info("고급 배치 스케줄러 시작")
        self.is_running = True

        if APSCHEDULER_AVAILABLE and self.scheduler:
            self.scheduler.start()
        else:
            self.logger.info("기본 스케줄러 모드로 시작")

    def shutdown(self, wait: bool = True):
        """스케줄러 종료"""
        self.logger.info("고급 배치 스케줄러 종료")
        self.is_running = False

        if APSCHEDULER_AVAILABLE and self.scheduler:
            # APScheduler 2.x에서는 timeout 파라미터 제거
            try:
                self.scheduler.shutdown(wait=wait)
            except TypeError:
                # timeout 파라미터가 지원되지 않는 경우
                self.scheduler.shutdown()

        self.logger.info("배치 스케줄러 종료 완료")


# 전역 인스턴스
_batch_manager = None


def get_batch_manager() -> BatchJobManager:
    """배치 작업 관리자 인스턴스 반환"""
    global _batch_manager
    if _batch_manager is None:
        _batch_manager = BatchJobManager()
    return _batch_manager


# 편의 함수들
def register_batch_job(
    config: BatchJobConfig, job_function: Callable, **schedule_kwargs
) -> str:
    """배치 작업 등록 편의 함수"""
    manager = get_batch_manager()
    return manager.register_job(config, job_function, **schedule_kwargs)


def run_job_immediately(job_id: str) -> JobResult:
    """작업 즉시 실행 편의 함수"""
    manager = get_batch_manager()
    return manager.run_job_now(job_id)


def get_all_job_status() -> Dict[str, Any]:
    """전체 작업 상태 조회 편의 함수"""
    manager = get_batch_manager()
    return manager.get_job_status()
