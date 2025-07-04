"""
배치 시스템 설정

배치 작업 실행에 필요한 모든 설정을 관리합니다.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class BatchSettings:
    """배치 시스템 기본 설정"""

    # Redis 설정 (스케줄러 Job Store)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 1
    redis_password: Optional[str] = None

    # 스케줄러 설정
    max_workers: int = 20
    max_process_workers: int = 5
    timezone: str = "Asia/Seoul"

    # 작업 기본 설정
    default_timeout: int = 3600  # 1시간
    default_retry_attempts: int = 3
    default_retry_delay: int = 60  # 1분

    # 로깅 설정
    log_level: str = "INFO"
    log_format: str = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"


@dataclass
class WeatherAPISettings:
    """날씨 API 설정"""

    weather_api_base_url: str = "https://api.openweathermap.org/data/2.5"
    weather_api_key: str = ""
    request_timeout: int = 30
    max_retries: int = 3
    retry_delay: int = 1


@dataclass
class TourismAPISettings:
    """관광 API 설정"""

    tourism_api_base_url: str = "http://apis.data.go.kr/B551011/KorService1"
    tourism_api_key: str = ""
    request_timeout: int = 60
    max_retries: int = 2
    retry_delay: int = 2


@dataclass
class DatabaseSettings:
    """데이터베이스 설정"""

    host: str = "localhost"
    port: int = 5432
    database: str = "weather_flick"
    username: str = ""
    password: str = ""
    pool_size: int = 20
    max_overflow: int = 10
    pool_timeout: int = 30


@dataclass
class LogSettings:
    """로그 관리 설정"""

    log_directory: str = "/var/log/weather-flick-batch"
    max_log_age_days: int = 30
    compression_age_days: int = 7
    archive_age_days: int = 90
    cleanup_schedule: str = "0 1 * * *"  # 매일 새벽 1시


@dataclass
class AWSSettings:
    """AWS 서비스 설정"""

    access_key_id: str = ""
    secret_access_key: str = ""
    region: str = "ap-northeast-2"
    s3_log_bucket: str = "weather-flick-logs"
    s3_backup_bucket: str = "weather-flick-backups"


@dataclass
class MonitoringSettings:
    """모니터링 설정"""

    # Redis (헬스체크용)
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0

    # 외부 API
    weather_api_base_url: str = "https://api.openweathermap.org/data/2.5"
    tourism_api_base_url: str = "http://apis.data.go.kr/B551011/KorService1"

    # 애플리케이션
    app_base_url: str = "http://localhost:8000"

    # 임계값
    cpu_warning_threshold: float = 80.0
    cpu_critical_threshold: float = 90.0
    memory_warning_threshold: float = 80.0
    memory_critical_threshold: float = 90.0
    disk_warning_threshold: float = 80.0
    disk_critical_threshold: float = 90.0

    # 알림 설정
    slack_webhook_url: str = ""
    email_smtp_server: str = ""
    email_from: str = ""
    email_to: list = None

    def __post_init__(self):
        if self.email_to is None:
            self.email_to = []


@dataclass
class NotificationSettings:
    """알림 설정"""

    # Slack
    slack_enabled: bool = False
    slack_webhook_url: str = ""
    slack_channel: str = "#alerts"

    # 이메일
    email_enabled: bool = False
    smtp_server: str = ""
    smtp_port: int = 587
    smtp_username: str = ""
    smtp_password: str = ""
    from_email: str = ""
    admin_emails: list = None

    # SMS (선택사항)
    sms_enabled: bool = False
    sms_api_key: str = ""
    admin_phones: list = None

    def __post_init__(self):
        if self.admin_emails is None:
            self.admin_emails = []
        if self.admin_phones is None:
            self.admin_phones = []


def get_batch_settings() -> BatchSettings:
    """배치 시스템 설정 반환"""
    return BatchSettings(
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        redis_db=int(os.getenv("REDIS_DB", "1")),
        redis_password=os.getenv("REDIS_PASSWORD"),
        max_workers=int(os.getenv("BATCH_MAX_WORKERS", "20")),
        max_process_workers=int(os.getenv("BATCH_MAX_PROCESS_WORKERS", "5")),
        timezone=os.getenv("TIMEZONE", "Asia/Seoul"),
        default_timeout=int(os.getenv("BATCH_DEFAULT_TIMEOUT", "3600")),
        default_retry_attempts=int(os.getenv("BATCH_DEFAULT_RETRY_ATTEMPTS", "3")),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )


def get_weather_api_settings() -> WeatherAPISettings:
    """날씨 API 설정 반환"""
    return WeatherAPISettings(
        weather_api_base_url=os.getenv(
            "WEATHER_API_BASE_URL", "https://api.openweathermap.org/data/2.5"
        ),
        weather_api_key=os.getenv("WEATHER_API_KEY", ""),
        request_timeout=int(os.getenv("WEATHER_API_TIMEOUT", "30")),
        max_retries=int(os.getenv("WEATHER_API_MAX_RETRIES", "3")),
    )


def get_tourism_api_settings() -> TourismAPISettings:
    """관광 API 설정 반환"""
    return TourismAPISettings(
        tourism_api_base_url=os.getenv(
            "TOURISM_API_BASE_URL", "http://apis.data.go.kr/B551011/KorService1"
        ),
        tourism_api_key=os.getenv("TOURISM_API_KEY", ""),
        request_timeout=int(os.getenv("TOURISM_API_TIMEOUT", "60")),
        max_retries=int(os.getenv("TOURISM_API_MAX_RETRIES", "2")),
    )


def get_database_settings() -> DatabaseSettings:
    """데이터베이스 설정 반환"""
    return DatabaseSettings(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "weather_flick"),
        username=os.getenv("DB_USER", ""),
        password=os.getenv("DB_PASSWORD", ""),
        pool_size=int(os.getenv("DB_POOL_SIZE", "20")),
        max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
    )


def get_log_settings() -> LogSettings:
    """로그 설정 반환"""
    return LogSettings(
        log_directory=os.getenv("LOG_DIRECTORY", "/var/log/weather-flick-batch"),
        max_log_age_days=int(os.getenv("LOG_MAX_AGE_DAYS", "30")),
        compression_age_days=int(os.getenv("LOG_COMPRESSION_AGE_DAYS", "7")),
        archive_age_days=int(os.getenv("LOG_ARCHIVE_AGE_DAYS", "90")),
    )


def get_aws_settings() -> AWSSettings:
    """AWS 설정 반환"""
    return AWSSettings(
        access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
        secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
        region=os.getenv("AWS_REGION", "ap-northeast-2"),
        s3_log_bucket=os.getenv("AWS_S3_LOG_BUCKET", "weather-flick-logs"),
        s3_backup_bucket=os.getenv("AWS_S3_BACKUP_BUCKET", "weather-flick-backups"),
    )


def get_monitoring_settings() -> MonitoringSettings:
    """모니터링 설정 반환"""
    return MonitoringSettings(
        redis_host=os.getenv("REDIS_HOST", "localhost"),
        redis_port=int(os.getenv("REDIS_PORT", "6379")),
        redis_db=int(os.getenv("REDIS_DB", "0")),
        weather_api_base_url=os.getenv(
            "WEATHER_API_BASE_URL", "https://api.openweathermap.org/data/2.5"
        ),
        tourism_api_base_url=os.getenv(
            "TOURISM_API_BASE_URL", "http://apis.data.go.kr/B551011/KorService1"
        ),
        app_base_url=os.getenv("APP_BASE_URL", "http://localhost:8000"),
        cpu_warning_threshold=float(os.getenv("CPU_WARNING_THRESHOLD", "80.0")),
        cpu_critical_threshold=float(os.getenv("CPU_CRITICAL_THRESHOLD", "90.0")),
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
    )


def get_notification_settings() -> NotificationSettings:
    """알림 설정 반환"""
    admin_emails = (
        os.getenv("ADMIN_EMAILS", "").split(",") if os.getenv("ADMIN_EMAILS") else []
    )
    admin_phones = (
        os.getenv("ADMIN_PHONES", "").split(",") if os.getenv("ADMIN_PHONES") else []
    )

    return NotificationSettings(
        slack_enabled=os.getenv("SLACK_ENABLED", "false").lower() == "true",
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL", ""),
        slack_channel=os.getenv("SLACK_CHANNEL", "#alerts"),
        email_enabled=os.getenv("EMAIL_ENABLED", "false").lower() == "true",
        smtp_server=os.getenv("SMTP_SERVER", ""),
        smtp_port=int(os.getenv("SMTP_PORT", "587")),
        smtp_username=os.getenv("SMTP_USERNAME", ""),
        smtp_password=os.getenv("SMTP_PASSWORD", ""),
        from_email=os.getenv("FROM_EMAIL", ""),
        admin_emails=admin_emails,
        sms_enabled=os.getenv("SMS_ENABLED", "false").lower() == "true",
        sms_api_key=os.getenv("SMS_API_KEY", ""),
        admin_phones=admin_phones,
    )
