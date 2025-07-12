"""
Weather Flick Batch API Server

배치 작업을 관리하고 실행하기 위한 REST API 서버
"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routers import batch
from app.api.config import settings
import uvicorn
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# FastAPI 앱 생성
app = FastAPI(
    title="Weather Flick Batch API",
    description="배치 작업 실행 및 모니터링 API",
    version="1.0.0"
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
