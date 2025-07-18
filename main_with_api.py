"""
Weather Flick Batch System with API Server

배치 시스템과 API 서버를 함께 실행
"""

import asyncio
import threading
import signal
import sys
import os
import logging
from datetime import datetime

# .env 파일 로드
from dotenv import load_dotenv

load_dotenv()

# API 서버 임포트
import uvicorn
from app.api.main import app
from app.api.config import settings

# 배치 시스템 임포트
from app.monitoring.monitoring_system import MonitoringSystem
from app.schedulers.advanced_scheduler import (
    get_batch_manager,
    BatchJobConfig,
    BatchJobType,
    JobPriority,
)
from app.core.logger import get_logger

# 배치 작업 임포트
from jobs.data_management.weather_update_job import weather_update_task
from jobs.monitoring.health_check_job import health_check_task
from jobs.quality.data_quality_job import DataQualityJob

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(
            f'logs/batch_system_{datetime.now().strftime("%Y%m%d")}.log'
        ),
    ],
)
logger = logging.getLogger(__name__)


class BatchSystemWithAPI:
    """API 서버를 포함한 배치 시스템"""

    def __init__(self):
        self.batch_manager = None
        self.monitoring_system = None
        self.api_server_thread = None
        self.running = False
        self.batch_logger = get_logger("batch_system")

    def start_api_server(self):
        """API 서버 시작 (별도 스레드)"""

        def run_api():
            logger.info(f"Starting API server on {settings.HOST}:{settings.PORT}")
            uvicorn.run(
                app,
                host=settings.HOST,
                port=settings.PORT,
                log_level=settings.LOG_LEVEL.lower(),
            )

        self.api_server_thread = threading.Thread(target=run_api, daemon=True)
        self.api_server_thread.start()
        logger.info("API server thread started")

    def _register_batch_jobs(self):
        """배치 작업 등록"""
        try:
            # 1. 날씨 데이터 업데이트 (30분마다)
            weather_config = BatchJobConfig(
                job_id="weather_update",
                job_type=BatchJobType.WEATHER_UPDATE,
                name="날씨 데이터 업데이트",
                description="외부 날씨 API로부터 최신 날씨 정보 수집",
                priority=JobPriority.HIGH,
                max_instances=1,
                timeout=1800,
                retry_attempts=3,
            )

            def weather_update_sync():
                return asyncio.run(weather_update_task())

            self.batch_manager.register_job(
                weather_config, weather_update_sync, trigger="interval", minutes=30
            )

            # 2. 헬스체크 (5분마다)
            health_config = BatchJobConfig(
                job_id="health_check",
                job_type=BatchJobType.HEALTH_CHECK,
                name="시스템 헬스체크",
                description="시스템 리소스 및 상태 모니터링",
                priority=JobPriority.CRITICAL,
                max_instances=1,
                timeout=300,
                retry_attempts=1,
            )

            def health_check_sync():
                return asyncio.run(health_check_task())

            self.batch_manager.register_job(
                health_config, health_check_sync, trigger="interval", minutes=5
            )

            # 3. 데이터 품질 검사 (매일 새벽 2시)
            quality_config = BatchJobConfig(
                job_id="data_quality",
                job_type=BatchJobType.HEALTH_CHECK,
                name="데이터 품질 검사",
                description="시스템 데이터 품질 검증",
                priority=JobPriority.MEDIUM,
                max_instances=1,
                timeout=1200,
                retry_attempts=2,
            )

            def quality_check_task():
                job = DataQualityJob(quality_config)
                if asyncio.iscoroutinefunction(job.run):
                    return asyncio.run(job.run())
                return job.run()

            self.batch_manager.register_job(
                quality_config, quality_check_task, trigger="cron", hour=2, minute=0
            )

            # 4. PENDING 작업 처리 워커 (30초마다)
            pending_worker_config = BatchJobConfig(
                job_id="pending_job_worker",
                job_type=BatchJobType.NOTIFICATION,
                name="대기 중인 작업 처리",
                description="데이터베이스에서 PENDING 상태의 작업을 찾아서 실행",
                priority=JobPriority.HIGH,
                max_instances=1,
                timeout=300,
                retry_attempts=1,
            )

            def pending_worker_task():
                return asyncio.run(self._process_pending_jobs())

            self.batch_manager.register_job(
                pending_worker_config,
                pending_worker_task,
                trigger="interval",
                seconds=30,
            )

            self.batch_logger.info("배치 작업 등록 완료")

        except Exception as e:
            self.batch_logger.error(f"배치 작업 등록 실패: {e}")
            raise

    async def _process_pending_jobs(self):
        """데이터베이스에서 PENDING 상태의 작업을 처리"""
        try:
            from app.core.database_manager import DatabaseManager
            from app.api.services.job_manager_db import JobManagerDB
            import uuid

            db_manager = DatabaseManager()
            job_manager = JobManagerDB()

            # PENDING 상태의 작업 조회
            pending_jobs = db_manager.get_pending_batch_jobs()

            if not pending_jobs:
                return

            self.batch_logger.info(f"PENDING 작업 {len(pending_jobs)}개 발견")

            for job_data in pending_jobs:
                try:
                    job_id = job_data.get("id")
                    job_type = job_data.get("job_type")
                    parameters = job_data.get("parameters", {})

                    self.batch_logger.info(
                        f"PENDING 작업 실행 시작: {job_id} ({job_type})"
                    )

                    # JobManager를 통해 작업 실행
                    from app.api.schemas import JobType

                    # 문자열을 JobType enum으로 변환
                    try:
                        job_type_enum = JobType(job_type)
                    except ValueError:
                        self.batch_logger.error(f"지원하지 않는 작업 타입: {job_type}")
                        continue

                    # 작업 실행 (비동기)
                    await job_manager.execute_job(
                        job_id=job_id,
                        job_type=job_type_enum,
                        parameters=parameters,
                        requested_by="batch_system",
                    )

                    self.batch_logger.info(f"PENDING 작업 실행 요청 완료: {job_id}")

                except Exception as e:
                    self.batch_logger.error(
                        f"PENDING 작업 처리 중 오류: {job_id if 'job_id' in locals() else 'unknown'} - {e}"
                    )

        except Exception as e:
            self.batch_logger.error(f"PENDING 작업 조회 중 오류: {e}")

    def start_batch_scheduler(self):
        """배치 스케줄러 시작"""
        logger.info("Starting batch scheduler...")

        # 모니터링 시스템 초기화
        self.monitoring_system = MonitoringSystem()

        # 배치 매니저 초기화
        self.batch_manager = get_batch_manager()

        # 배치 작업 등록
        self._register_batch_jobs()

        # 스케줄러 시작
        self.batch_manager.start()
        logger.info("Batch scheduler started")

    def run(self):
        """전체 시스템 실행"""
        self.running = True

        # 시그널 핸들러 설정
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        logger.info("=" * 50)
        logger.info("Weather Flick Batch System with API Starting...")
        logger.info("=" * 50)

        # API 서버 시작
        self.start_api_server()

        # 잠시 대기 (API 서버 시작 대기)
        import time

        time.sleep(2)

        # 배치 스케줄러 시작
        self.start_batch_scheduler()

        logger.info("System fully started!")
        logger.info(f"API Server: http://{settings.HOST}:{settings.PORT}")
        logger.info(f"API Docs: http://{settings.HOST}:{settings.PORT}/docs")

        # 메인 루프
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")

        self.shutdown()

    def handle_shutdown(self, signum, frame):
        """시그널 핸들러"""
        logger.info(f"Received signal {signum}")
        self.running = False

    def shutdown(self):
        """시스템 종료"""
        logger.info("Shutting down system...")

        # 스케줄러 종료
        if self.batch_manager:
            self.batch_manager.shutdown()
            logger.info("Batch scheduler stopped")

        # 모니터링 시스템 종료
        if self.monitoring_system:
            # 모니터링 시스템 정리
            logger.info("Monitoring system stopped")

        logger.info("System shutdown complete")
        sys.exit(0)


def main():
    """메인 진입점"""
    # 로그 디렉토리 생성
    os.makedirs("logs", exist_ok=True)

    # 시스템 시작
    system = BatchSystemWithAPI()
    system.run()


if __name__ == "__main__":
    main()
