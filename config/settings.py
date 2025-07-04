"""
애플리케이션 설정 관리 모듈

환경 변수와 설정 값들을 중앙에서 관리합니다.
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()


@dataclass
class DatabaseConfig:
    """데이터베이스 설정"""

    host: str
    user: str
    password: str
    database: str
    port: int = 5432


@dataclass
class APIConfig:
    """API 설정"""

    kto_api_key: str
    kto_base_url: str
    kma_api_key: str
    timeout: int = 30
    retry_count: int = 3


@dataclass
class WeatherAPIConfig:
    """날씨 API 설정"""

    weather_api_key: str
    weather_api_base_url: str
    timeout: int = 30
    retry_count: int = 3


@dataclass
class TourismAPIConfig:
    """관광 API 설정"""

    tourism_api_key: str
    tourism_api_base_url: str
    timeout: int = 30
    retry_count: int = 3


@dataclass
class AWSConfig:
    """AWS 설정"""

    access_key_id: str
    secret_access_key: str
    region: str
    s3_bucket: str


@dataclass
class MonitoringConfig:
    """모니터링 설정"""

    health_check_interval: int = 300  # 5분
    performance_check_interval: int = 600  # 10분
    disk_usage_threshold: float = 80.0  # 80%
    memory_usage_threshold: float = 85.0  # 85%
    alert_enabled: bool = True
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_password: str = ""
    redis_db: int = 0
    weather_api_base_url: str = "http://api.openweathermap.org/data/2.5"
    tourism_api_base_url: str = "http://apis.data.go.kr/B551011/KorService1"
    app_base_url: str = "http://localhost:8000"


@dataclass
class ScheduleConfig:
    """스케줄 설정"""

    weather_data_time: str = "02:00"
    tourist_data_time: str = "03:00"
    score_calculation_time: str = "04:00"
    data_quality_time: str = "05:00"
    tourist_data_day: str = "sunday"


@dataclass
class LoggingConfig:
    """로깅 설정"""

    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_prefix: str = "weather_flick_batch"
    max_bytes: int = 10485760  # 10MB
    backup_count: int = 5


@dataclass
class AppSettings:
    """전체 애플리케이션 설정"""

    debug: bool
    environment: str
    database: DatabaseConfig
    api: APIConfig
    schedule: ScheduleConfig
    logging: LoggingConfig


def get_database_config() -> DatabaseConfig:
    """데이터베이스 설정 조회"""
    return DatabaseConfig(
        host=os.getenv("DB_HOST", "localhost"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", ""),
        database=os.getenv("DB_NAME", "weather_flick"),
        port=int(os.getenv("DB_PORT", "5432")),
    )


def get_api_config() -> APIConfig:
    """API 설정 조회"""
    return APIConfig(
        kto_api_key=os.getenv("KTO_API_KEY", ""),
        kto_base_url=os.getenv(
            "KTO_API_BASE_URL", "http://apis.data.go.kr/B551011/KorService1"
        ),
        kma_api_key=os.getenv("KMA_API_KEY", ""),
        timeout=int(os.getenv("API_TIMEOUT", "30")),
        retry_count=int(os.getenv("API_RETRY_COUNT", "3")),
    )


def get_schedule_config() -> ScheduleConfig:
    """스케줄 설정 조회"""
    return ScheduleConfig(
        weather_data_time=os.getenv("WEATHER_DATA_TIME", "02:00"),
        tourist_data_time=os.getenv("TOURIST_DATA_TIME", "03:00"),
        score_calculation_time=os.getenv("SCORE_CALCULATION_TIME", "04:00"),
        data_quality_time=os.getenv("DATA_QUALITY_TIME", "05:00"),
        tourist_data_day=os.getenv("TOURIST_DATA_DAY", "sunday"),
    )


def get_logging_config() -> LoggingConfig:
    """로깅 설정 조회"""
    return LoggingConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format=os.getenv(
            "LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
        file_prefix=os.getenv("LOG_FILE_PREFIX", "weather_flick_batch"),
        max_bytes=int(os.getenv("LOG_MAX_BYTES", "10485760")),
        backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5")),
    )


def get_app_settings() -> AppSettings:
    """전체 애플리케이션 설정 조회"""
    return AppSettings(
        debug=os.getenv("DEBUG", "False").lower() == "true",
        environment=os.getenv("ENVIRONMENT", "development"),
        database=get_database_config(),
        api=get_api_config(),
        schedule=get_schedule_config(),
        logging=get_logging_config(),
    )


def get_weather_api_settings() -> WeatherAPIConfig:
    """날씨 API 설정 조회"""
    return WeatherAPIConfig(
        weather_api_key=os.getenv("WEATHER_API_KEY", ""),
        weather_api_base_url=os.getenv(
            "WEATHER_API_BASE_URL", "http://api.openweathermap.org/data/2.5"
        ),
        timeout=int(os.getenv("WEATHER_API_TIMEOUT", "30")),
        retry_count=int(os.getenv("WEATHER_API_RETRY_COUNT", "3")),
    )


def get_tourism_api_settings() -> TourismAPIConfig:
    """관광 API 설정 조회"""
    return TourismAPIConfig(
        tourism_api_key=os.getenv("KTO_API_KEY", ""),
        tourism_api_base_url=os.getenv(
            "KTO_API_BASE_URL", "http://apis.data.go.kr/B551011/KorService1"
        ),
        timeout=int(os.getenv("TOURISM_API_TIMEOUT", "30")),
        retry_count=int(os.getenv("TOURISM_API_RETRY_COUNT", "3")),
    )


def get_log_settings() -> LoggingConfig:
    """로그 설정 조회 (별칭)"""
    return get_logging_config()


def get_aws_settings() -> AWSConfig:
    """AWS 설정 조회"""
    return AWSConfig(
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        region=os.getenv("AWS_REGION", "ap-northeast-2"),
        s3_bucket=os.getenv("AWS_S3_BUCKET", ""),
    )


def get_monitoring_settings() -> MonitoringConfig:
    """모니터링 설정 조회"""
    return MonitoringConfig(
        health_check_interval=int(os.getenv("HEALTH_CHECK_INTERVAL", "300")),
        performance_check_interval=int(os.getenv("PERFORMANCE_CHECK_INTERVAL", "600")),
        disk_usage_threshold=float(os.getenv("DISK_USAGE_THRESHOLD", "80.0")),
        memory_usage_threshold=float(os.getenv("MEMORY_USAGE_THRESHOLD", "85.0")),
        alert_enabled=os.getenv("ALERT_ENABLED", "true").lower() == "true",
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        redis_password=os.getenv("REDIS_PASSWORD", ""),
        redis_db=int(os.getenv("REDIS_DB", "0")),
        weather_api_base_url=os.getenv(
            "WEATHER_API_BASE_URL", "http://api.openweathermap.org/data/2.5"
        ),
        tourism_api_base_url=os.getenv(
            "TOURISM_API_BASE_URL", "http://apis.data.go.kr/B551011/KorService1"
        ),
        app_base_url=os.getenv("APP_BASE_URL", "http://localhost:8000"),
    )


# 전역 설정 인스턴스
settings = get_app_settings()
