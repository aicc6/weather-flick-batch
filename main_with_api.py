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
from app.core.monitoring_config import get_monitoring_config
from app.monitoring.monitoring_system import MonitoringSystem
from jobs.scheduler_config import create_scheduler
from config import Config

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/batch_system_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

class BatchSystemWithAPI:
    """API 서버를 포함한 배치 시스템"""
    
    def __init__(self):
        self.scheduler = None
        self.monitoring_system = None
        self.api_server_thread = None
        self.running = False
        
    def start_api_server(self):
        """API 서버 시작 (별도 스레드)"""
        def run_api():
            logger.info(f"Starting API server on {settings.HOST}:{settings.PORT}")
            uvicorn.run(
                app,
                host=settings.HOST,
                port=settings.PORT,
                log_level=settings.LOG_LEVEL.lower()
            )
        
        self.api_server_thread = threading.Thread(target=run_api, daemon=True)
        self.api_server_thread.start()
        logger.info("API server thread started")
        
    def start_batch_scheduler(self):
        """배치 스케줄러 시작"""
        logger.info("Starting batch scheduler...")
        
        # 모니터링 시스템 초기화
        monitoring_config = get_monitoring_config()
        self.monitoring_system = MonitoringSystem(monitoring_config)
        
        # 스케줄러 생성
        self.scheduler = create_scheduler()
        
        # 스케줄러 시작
        self.scheduler.start()
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
        if self.scheduler:
            self.scheduler.shutdown(wait=True)
            logger.info("Scheduler stopped")
        
        # 모니터링 시스템 종료
        if self.monitoring_system:
            # 모니터링 시스템 정리
            logger.info("Monitoring system stopped")
        
        logger.info("System shutdown complete")
        sys.exit(0)

def main():
    """메인 진입점"""
    # 로그 디렉토리 생성
    os.makedirs('logs', exist_ok=True)
    
    # 시스템 시작
    system = BatchSystemWithAPI()
    system.run()

if __name__ == "__main__":
    main()