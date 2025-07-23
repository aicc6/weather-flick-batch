"""
WeatherFlick 고급 배치 시스템 메인 실행 파일

배치 문서 기반으로 구현된 포괄적인 배치 및 스케줄링 시스템
"""

import asyncio
import signal
import sys
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from app.schedulers.advanced_scheduler import (
    get_batch_manager,
    BatchJobConfig,
    BatchJobType,
    JobPriority,
)
from app.core.logger import get_logger
from config.settings import get_app_settings

# 타임존 처리 유틸리티 추가
from utils.timezone_batch_utils import (
    BatchTimezoneUtils,
    ExternalApiTimezoneHelper,
    log_batch_execution,
    get_batch_job_schedule_config
)

# 배치 작업 임포트
from jobs.data_management.weather_update_job import weather_update_task
from jobs.data_management.destination_sync_job import destination_sync_task
from jobs.system_maintenance.log_cleanup_job import log_cleanup_task
from jobs.monitoring.health_check_job import health_check_task
from jobs.recommendation.recommendation_job import RecommendationJob
from jobs.quality.data_quality_job import DataQualityJob

# from jobs.tourism.tourism_sync_job import TourismSyncJob  # 존재하지 않는 모듈
from jobs.tourism.comprehensive_tourism_job import (
    ComprehensiveTourismJob,
    IncrementalTourismJob,
)
from jobs.system_maintenance.database_backup_job import DatabaseBackupJob
from jobs.notification.weather_change_notification_job import WeatherChangeNotificationJob


class WeatherFlickBatchSystem:
    """WeatherFlick 배치 시스템"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.settings = get_app_settings()
        self.batch_manager = get_batch_manager()
        self.shutdown_requested = False
        
        # 타임존 유틸리티 초기화
        self.timezone_utils = BatchTimezoneUtils()
        self.api_helper = ExternalApiTimezoneHelper()
        
        # 배치 스케줄 설정 가져오기
        self.schedule_config = get_batch_job_schedule_config()

        # 시그널 핸들러 등록
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """시스템 시그널 핸들러"""
        self.logger.info(f"종료 시그널 수신: {signum}")
        self.shutdown_requested = True
    
    def _create_batch_wrapper(self, job_name: str, job_func):
        """배치 작업을 타임존 처리와 함께 래핑"""
        @log_batch_execution(job_name)
        def wrapped_job():
            start_time = self.timezone_utils.get_collection_timestamp()
            self.logger.info(f"[{job_name}] 작업 시작 - UTC: {start_time.isoformat()}")
            
            try:
                result = job_func()
                
                end_time = self.timezone_utils.get_collection_timestamp()
                duration = self.timezone_utils.format_duration(start_time, end_time)
                
                self.logger.info(f"[{job_name}] 작업 완료 - 소요시간: {duration}")
                return result
                
            except Exception as e:
                end_time = self.timezone_utils.get_collection_timestamp()
                duration = self.timezone_utils.format_duration(start_time, end_time)
                
                self.logger.error(f"[{job_name}] 작업 실패 - 소요시간: {duration}, 오류: {str(e)}")
                raise
        
        return wrapped_job
    
    def _create_async_job_wrapper(self, async_job_func):
        """비동기 작업을 동기로 변환하고 데이터베이스 연결을 정리하는 래퍼"""
        def sync_wrapper():
            from app.core.async_database import get_async_db_manager, reset_async_db_manager
            
            async def run_with_cleanup():
                try:
                    result = await async_job_func()
                    return result
                finally:
                    # 작업 완료 후 데이터베이스 연결 정리
                    try:
                        db_manager = get_async_db_manager()
                        await db_manager.close()
                        reset_async_db_manager()  # 매니저 인스턴스 초기화
                    except Exception as e:
                        self.logger.warning(f"데이터베이스 연결 정리 중 경고: {e}")
            
            return asyncio.run(run_with_cleanup())
        
        return sync_wrapper

    def setup_data_management_jobs(self):
        """데이터 관리 배치 작업 설정"""

        # 날씨 데이터 업데이트 (1시간마다)
        weather_config = BatchJobConfig(
            job_id="weather_update",
            job_type=BatchJobType.WEATHER_UPDATE,
            name="날씨 데이터 업데이트",
            description="외부 날씨 API로부터 최신 날씨 정보 수집 및 업데이트",
            priority=JobPriority.HIGH,
            max_instances=1,
            timeout=1800,  # 30분
            retry_attempts=3,
        )

        # 비동기 작업을 동기 래퍼로 감싸기 (타임존 처리 포함)
        weather_update_sync = self._create_async_job_wrapper(weather_update_task)
        weather_job_wrapped = self._create_batch_wrapper("날씨 데이터 업데이트", weather_update_sync)
        
        self.batch_manager.register_job(
            weather_config, weather_job_wrapped, trigger="interval", hours=1
        )

        # 여행지 정보 동기화 (매일 새벽 3시)
        destination_config = BatchJobConfig(
            job_id="destination_sync",
            job_type=BatchJobType.DESTINATION_SYNC,
            name="여행지 정보 동기화",
            description="외부 관광 API와 여행지 정보 동기화",
            priority=JobPriority.HIGH,
            max_instances=1,
            timeout=3600,  # 1시간
            retry_attempts=2,
            dependencies=[],
        )

        # 비동기 작업을 동기 래퍼로 감싸기 (타임존 처리 포함)
        destination_sync_sync = self._create_async_job_wrapper(destination_sync_task)
        destination_job_wrapped = self._create_batch_wrapper("여행지 정보 동기화", destination_sync_sync)
        
        self.batch_manager.register_job(
            destination_config, destination_job_wrapped, trigger="cron", hour=3, minute=0
        )

        # 종합 관광정보 수집 작업 (매주 일요일 새벽 2시)
        comprehensive_tourism_config = BatchJobConfig(
            job_id="comprehensive_tourism_sync",
            job_type=BatchJobType.COMPREHENSIVE_TOURISM_SYNC,
            name="종합 관광정보 수집",
            description="한국관광공사 API 모든 엔드포인트를 통한 종합적인 관광정보 수집",
            priority=JobPriority.HIGH,
            max_instances=1,
            timeout=14400,  # 4시간
            retry_attempts=2,
        )

        def comprehensive_tourism_task():
            job = ComprehensiveTourismJob()
            async def execute_job():
                return await job.execute()
            return self._create_async_job_wrapper(execute_job)()

        comprehensive_job_wrapped = self._create_batch_wrapper("종합 관광정보 수집", comprehensive_tourism_task)

        self.batch_manager.register_job(
            comprehensive_tourism_config,
            comprehensive_job_wrapped,
            trigger="cron",
            day_of_week="sun",
            hour=2,
            minute=0,
        )

        # 증분 관광정보 수집 작업 (매일 새벽 3시)
        incremental_tourism_config = BatchJobConfig(
            job_id="incremental_tourism_sync",
            job_type=BatchJobType.INCREMENTAL_TOURISM_SYNC,
            name="증분 관광정보 수집",
            description="주요 관광정보 일일 증분 업데이트",
            priority=JobPriority.MEDIUM,
            max_instances=1,
            timeout=3600,  # 1시간
            retry_attempts=3,
        )

        def incremental_tourism_task():
            job = IncrementalTourismJob()
            async def execute_job():
                return await job.execute()
            return self._create_async_job_wrapper(execute_job)()

        self.batch_manager.register_job(
            incremental_tourism_config,
            incremental_tourism_task,
            trigger="cron",
            hour=3,
            minute=0,
        )

        # 기존 관광지 데이터 동기화 작업 (매주 일요일 새벽 4시) - 호환성 유지
        tourism_config = BatchJobConfig(
            job_id="tourism_sync",
            job_type=BatchJobType.DESTINATION_SYNC,
            name="관광지 데이터 동기화",
            description="한국관광공사 API를 통한 관광지 정보 수집 및 동기화",
            priority=JobPriority.HIGH,
            max_instances=1,
            timeout=7200,  # 2시간
            retry_attempts=2,
        )

        # 관광지 동기화 작업 함수 생성 - 증분 업데이트 재사용
        def tourism_sync_task():
            job = IncrementalTourismJob()
            async def execute_job():
                return await job.execute()
            return self._create_async_job_wrapper(execute_job)()

        self.batch_manager.register_job(
            tourism_config,
            tourism_sync_task,
            trigger="cron",
            day_of_week="sun",
            hour=4,
            minute=0,
        )


        self.logger.info("데이터 관리 배치 작업 설정 완료")

    def setup_system_maintenance_jobs(self):
        """시스템 유지보수 배치 작업 설정"""

        # 로그 정리 및 아카이빙 (매일 새벽 1시)
        log_cleanup_config = BatchJobConfig(
            job_id="log_cleanup",
            job_type=BatchJobType.LOG_CLEANUP,
            name="로그 정리 및 아카이빙",
            description="시스템 로그 파일 정리 및 장기 보관을 위한 아카이빙",
            priority=JobPriority.MEDIUM,
            max_instances=1,
            timeout=3600,  # 1시간
            retry_attempts=2,
        )

        # 비동기 작업을 동기 래퍼로 감싸기
        log_cleanup_sync = self._create_async_job_wrapper(log_cleanup_task)

        self.batch_manager.register_job(
            log_cleanup_config, log_cleanup_sync, trigger="cron", hour=1, minute=0
        )

        # 데이터베이스 백업 작업 (매일 새벽 2시)
        backup_config = BatchJobConfig(
            job_id="database_backup",
            job_type=BatchJobType.DATABASE_BACKUP,
            name="데이터베이스 백업",
            description="PostgreSQL 데이터베이스 정기 백업 및 압축 저장",
            priority=JobPriority.HIGH,
            max_instances=1,
            timeout=7200,  # 2시간
            retry_attempts=1,
        )

        # 백업 작업 함수 생성
        def database_backup_task():
            job = DatabaseBackupJob(backup_config)
            # DatabaseBackupJob.run()이 비동기인지 확인 필요
            if asyncio.iscoroutinefunction(job.run):
                return asyncio.run(job.run())
            return job.run()

        self.batch_manager.register_job(
            backup_config, database_backup_task, trigger="cron", hour=2, minute=0
        )

        # TODO: 캐시 정리 작업 추가

        self.logger.info("시스템 유지보수 배치 작업 설정 완료")

    def setup_monitoring_jobs(self):
        """모니터링 배치 작업 설정"""

        # 시스템 헬스체크 (5분마다)
        health_check_config = BatchJobConfig(
            job_id="health_check",
            job_type=BatchJobType.HEALTH_CHECK,
            name="시스템 헬스체크",
            description="시스템 전반의 상태 모니터링 및 이상 감지",
            priority=JobPriority.CRITICAL,
            max_instances=1,
            timeout=300,  # 5분
            retry_attempts=1,
        )

        # 비동기 작업을 동기 래퍼로 감싸기
        health_check_sync = self._create_async_job_wrapper(health_check_task)

        self.batch_manager.register_job(
            health_check_config, health_check_sync, trigger="interval", minutes=5
        )

        # 여행 플랜 날씨 변화 알림 작업 (하루 3번: 오전 9시, 오후 3시, 오후 9시)
        def weather_notification_task():
            job = WeatherChangeNotificationJob()
            # WeatherChangeNotificationJob.execute()는 동기 함수이므로 직접 반환
            return job.execute()

        # 하루 3번 실행 (9시, 15시, 21시)
        for hour in [9, 15, 21]:
            # 각 시간대별로 고유한 ID를 가진 설정 생성
            hour_config = BatchJobConfig(
                job_id=f"weather_change_notification_{hour}h",
                job_type=BatchJobType.NOTIFICATION,
                name=f"여행 플랜 날씨 변화 알림 ({hour}시)",
                description="여행 플랜의 날씨 변화를 감지하고 사용자에게 알림 전송",
                priority=JobPriority.HIGH,
                max_instances=1,
                timeout=1800,  # 30분
                retry_attempts=2,
            )
            self.batch_manager.register_job(
                hour_config,
                weather_notification_task,
                trigger="cron",
                hour=hour,
                minute=0,
            )

        # TODO: 성능 메트릭 수집 작업 추가 (1분마다)
        # TODO: 기타 알림 발송 작업 추가

        self.logger.info("모니터링 배치 작업 설정 완료")

    def setup_business_logic_jobs(self):
        """비즈니스 로직 배치 작업 설정"""

        # 추천 알고리즘 재계산 작업 (매일 새벽 5시)
        recommendation_config = BatchJobConfig(
            job_id="recommendation_update",
            job_type=BatchJobType.RECOMMENDATION_UPDATE,
            name="추천 점수 계산",
            description="과거 날씨 데이터를 기반으로 지역별 여행 추천 점수 계산",
            priority=JobPriority.MEDIUM,
            max_instances=1,
            timeout=1800,  # 30분
            retry_attempts=2,
        )

        # 추천 작업 함수 생성
        def recommendation_task():
            job = RecommendationJob(recommendation_config)
            # RecommendationJob.run()이 비동기인지 확인 필요
            if asyncio.iscoroutinefunction(job.run):
                return asyncio.run(job.run())
            return job.run()

        self.batch_manager.register_job(
            recommendation_config, recommendation_task, trigger="cron", hour=5, minute=0
        )

        # TODO: 인기도 점수 업데이트 작업 (매일 오전 6시)
        # TODO: 사용자 행동 패턴 분석 작업 (매주 월요일)

        self.logger.info("비즈니스 로직 배치 작업 설정 완료")

    def setup_quality_jobs(self):
        """데이터 품질 검사 배치 작업 설정"""

        # 데이터 품질 검사 작업 (매일 새벽 6시)
        quality_config = BatchJobConfig(
            job_id="data_quality_check",
            job_type=BatchJobType.HEALTH_CHECK,  # 데이터 품질 검사용
            name="데이터 품질 검사",
            description="시스템 전체 데이터 품질 검증 및 이상 탐지",
            priority=JobPriority.MEDIUM,
            max_instances=1,
            timeout=1200,  # 20분
            retry_attempts=2,
        )

        # 품질 검사 작업 함수 생성
        def quality_check_task():
            job = DataQualityJob(quality_config)
            # DataQualityJob.run()이 비동기인지 확인 필요
            if asyncio.iscoroutinefunction(job.run):
                return asyncio.run(job.run())
            return job.run()

        self.batch_manager.register_job(
            quality_config, quality_check_task, trigger="cron", hour=6, minute=0
        )

        self.logger.info("데이터 품질 검사 배치 작업 설정 완료")

    def setup_all_jobs(self):
        """모든 배치 작업 설정"""
        self.logger.info("WeatherFlick 배치 작업 설정 시작")

        try:
            self.setup_data_management_jobs()
            self.setup_system_maintenance_jobs()
            self.setup_monitoring_jobs()
            self.setup_business_logic_jobs()
            self.setup_quality_jobs()

            self.logger.info("모든 배치 작업 설정 완료")

        except Exception as e:
            self.logger.error(f"배치 작업 설정 실패: {e}")
            raise

    def start(self):
        """배치 시스템 시작"""
        self.logger.info("WeatherFlick 고급 배치 시스템 시작")

        try:
            # 배치 작업 설정
            self.setup_all_jobs()

            # 스케줄러 시작
            self.batch_manager.start()

            self.logger.info("배치 시스템이 성공적으로 시작되었습니다")
            self.logger.info("종료하려면 Ctrl+C를 누르세요")

            # 메인 루프
            while not self.shutdown_requested:
                try:
                    # 1분마다 상태 체크
                    import time

                    time.sleep(60)

                    # 작업 상태 로깅 (10분마다)
                    if int(time.time()) % 600 == 0:
                        self._log_job_status()

                except KeyboardInterrupt:
                    self.logger.info("키보드 인터럽트 수신")
                    break

        except Exception as e:
            self.logger.error(f"배치 시스템 실행 오류: {e}")
            raise
        finally:
            self.shutdown()

    def _log_job_status(self):
        """작업 상태 로깅 (타임존 정보 포함)"""
        try:
            job_status = self.batch_manager.get_job_status()
            current_time = self.timezone_utils.get_collection_timestamp()
            current_kst = current_time.astimezone(self.timezone_utils.KST)

            if job_status:
                self.logger.info(f"배치 작업 상태 (현재 시간: UTC {current_time.strftime('%H:%M')}, KST {current_kst.strftime('%H:%M')}):")
                for job_id, status in job_status.items():
                    next_run = status.get("next_run", "N/A")
                    
                    # 다음 실행 시간을 KST로 변환
                    if next_run != "N/A" and hasattr(next_run, 'astimezone'):
                        try:
                            next_run_kst = next_run.astimezone(self.timezone_utils.KST)
                            next_run_str = f"UTC {next_run.strftime('%m/%d %H:%M')}, KST {next_run_kst.strftime('%m/%d %H:%M')}"
                        except:
                            next_run_str = str(next_run)
                    else:
                        next_run_str = str(next_run)
                    
                    self.logger.info(f"  - {job_id}: 다음 실행 {next_run_str}")

        except Exception as e:
            self.logger.error(f"작업 상태 로깅 실패: {e}")

    def shutdown(self):
        """배치 시스템 종료"""
        self.logger.info("WeatherFlick 배치 시스템 종료 시작")

        try:
            self.batch_manager.shutdown(wait=True)
            
            # 데이터베이스 연결 정리
            import asyncio
            from app.core.async_database import get_async_db_manager
            
            async def cleanup_db():
                try:
                    db_manager = get_async_db_manager()
                    await db_manager.close()
                    self.logger.info("데이터베이스 연결이 정상적으로 종료되었습니다")
                except Exception as e:
                    self.logger.error(f"데이터베이스 연결 종료 중 오류: {e}")
            
            # 새로운 이벤트 루프를 생성하여 정리 작업 수행
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(cleanup_db())
            finally:
                loop.close()
            
            self.logger.info("배치 시스템이 정상적으로 종료되었습니다")

        except Exception as e:
            self.logger.error(f"배치 시스템 종료 오류: {e}")


def main():
    """메인 실행 함수"""
    # 로깅 초기화
    logger = get_logger(__name__)
    
    # 시작 시간 정보 (타임존 포함)
    timezone_utils = BatchTimezoneUtils()
    start_time = timezone_utils.get_collection_timestamp()
    start_kst = start_time.astimezone(timezone_utils.KST)

    logger.info("=" * 60)
    logger.info("WeatherFlick 고급 배치 시스템")
    logger.info("배치 문서 기반 포괄적 스케줄링 시스템")
    logger.info(f"시작 시간: UTC {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"시작 시간: KST {start_kst.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"스케줄러 타임존: Asia/Seoul (KST)")
    logger.info("=" * 60)

    try:
        # 배치 시스템 생성 및 시작
        batch_system = WeatherFlickBatchSystem()
        batch_system.start()

    except KeyboardInterrupt:
        logger.info("사용자 요청으로 시스템 종료")
    except Exception as e:
        logger.error(f"시스템 오류: {e}")
        sys.exit(1)
    finally:
        logger.info("WeatherFlick 배치 시스템 종료")


if __name__ == "__main__":
    main()
