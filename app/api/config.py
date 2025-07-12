"""
Batch API 설정
"""

import os
from pydantic_settings import BaseSettings
from typing import List

class Settings(BaseSettings):
    """API 설정"""
    
    # 기본 설정
    SERVICE_NAME: str = "weather-flick-batch"
    VERSION: str = "1.0.0"
    ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development")
    DEBUG: bool = os.getenv("DEBUG", "true").lower() == "true"
    
    # 서버 설정
    HOST: str = os.getenv("BATCH_API_HOST", "0.0.0.0")
    PORT: int = int(os.getenv("BATCH_API_PORT", "9090"))
    
    # 보안 설정
    API_KEY: str = os.getenv("BATCH_API_KEY", "batch-api-secret-key")
    ALLOWED_HOSTS: List[str] = ["*"]
    
    # 데이터베이스 설정 (기존 배치 시스템과 공유)
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/weather_flick")
    
    # 배치 작업 설정
    MAX_CONCURRENT_JOBS: int = int(os.getenv("MAX_CONCURRENT_JOBS", "3"))
    JOB_TIMEOUT_SECONDS: int = int(os.getenv("JOB_TIMEOUT_SECONDS", "3600"))  # 1시간
    
    # 로그 설정
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FILE: str = os.getenv("LOG_FILE", "logs/batch_api.log")
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"  # 추가 환경 변수 무시

settings = Settings()