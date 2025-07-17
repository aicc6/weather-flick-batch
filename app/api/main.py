"""
Weather Flick Batch API Server

배치 작업을 관리하고 실행하기 위한 REST API 서버
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from app.api.routers import batch, websocket, schedule, retry, notification, performance
from app.api.config import settings
from app.api.services.job_manager_db import JobManagerDB
from app.api.services.schedule_manager import ScheduleManager
from app.api.services.retry_manager import RetryManager
from app.api.services.notification_manager import NotificationManager
from app.core.async_database import get_async_db_manager
import uvicorn
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 전역 매니저 인스턴스
job_manager: JobManagerDB = None
schedule_manager_instance: ScheduleManager = None
retry_manager_instance: RetryManager = None
notification_manager_instance: NotificationManager = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """애플리케이션 생명주기 관리"""
    global job_manager, schedule_manager_instance, retry_manager_instance, notification_manager_instance
    
    # 시작 시
    logger.info(f"🚀 Weather Flick Batch API 시작 - Port: {settings.PORT}")
    logger.info(f"환경: {settings.ENVIRONMENT}")
    
    # Job Manager 초기화
    job_manager = JobManagerDB()
    batch.job_manager = job_manager
    
    # Schedule Manager 초기화
    schedule_manager_instance = ScheduleManager(job_manager)
    schedule.schedule_manager = schedule_manager_instance
    
    # Retry Manager 초기화
    retry_manager_instance = RetryManager(job_manager)
    retry.retry_manager = retry_manager_instance
    job_manager.retry_manager = retry_manager_instance  # 상호 참조 설정
    
    # Notification Manager 초기화
    notification_manager_instance = NotificationManager()
    notification.notification_manager = notification_manager_instance
    job_manager.notification_manager = notification_manager_instance  # 상호 참조 설정
    
    # 데이터베이스 연결로 스케줄러 초기화 (일단 건너뛰기)
    # TODO: 데이터베이스 테이블 생성 후 활성화
    # async_db_manager = get_async_db_manager()
    # async with async_db_manager.get_session() as db:
    #     await schedule_manager_instance.initialize(db)
    
    yield
    
    # 종료 시
    logger.info("Weather Flick Batch API 종료")
    if schedule_manager_instance:
        schedule_manager_instance.shutdown()

# FastAPI 앱 생성
app = FastAPI(
    title="Weather Flick Batch API",
    description="배치 작업 실행 및 모니터링 API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS 설정 - 모든 오리진 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=3600,
)

# 라우터 등록
app.include_router(batch.router, prefix="/api/batch", tags=["batch"])
app.include_router(websocket.router, prefix="/api/ws", tags=["websocket"])
app.include_router(schedule.router, prefix="/api/batch", tags=["schedules"])
app.include_router(retry.router, prefix="/api/batch", tags=["retries"])
app.include_router(notification.router, prefix="/api/batch", tags=["notifications"])
app.include_router(performance.router, prefix="/api/batch", tags=["performance"])

@app.get("/")
async def root():
    return {
        "message": "Weather Flick Batch API",
        "version": "1.0.0",
        "status": "running"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "weather-flick-batch",
        "version": settings.VERSION
    }

@app.on_event("startup")
async def startup_event():
    """애플리케이션 시작 이벤트"""
    logger.info(f"🚀 Weather Flick Batch API 시작 - Port: {settings.PORT}")
    logger.info(f"환경: {settings.ENVIRONMENT}")

@app.on_event("shutdown")
async def shutdown_event():
    """애플리케이션 종료 이벤트"""
    logger.info("Weather Flick Batch API 종료")

if __name__ == "__main__":
    uvicorn.run(
        "app.api.main:app",
        host=settings.HOST,
        port=settings.PORT,
        reload=settings.DEBUG
    )
