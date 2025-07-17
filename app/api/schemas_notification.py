"""
배치 작업 알림 관련 스키마
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict, EmailStr
from enum import Enum


class NotificationChannel(str, Enum):
    """알림 채널"""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"


class NotificationEvent(str, Enum):
    """알림 이벤트 유형"""
    JOB_STARTED = "job_started"
    JOB_COMPLETED = "job_completed"
    JOB_FAILED = "job_failed"
    JOB_CANCELLED = "job_cancelled"
    JOB_RETRY_STARTED = "job_retry_started"
    JOB_RETRY_FAILED = "job_retry_failed"
    JOB_RETRY_MAX_ATTEMPTS = "job_retry_max_attempts"
    LONG_RUNNING_JOB = "long_running_job"


class NotificationLevel(str, Enum):
    """알림 레벨"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class EmailConfig(BaseModel):
    """이메일 설정"""
    smtp_host: str = Field(..., description="SMTP 서버 호스트")
    smtp_port: int = Field(587, description="SMTP 포트")
    smtp_user: str = Field(..., description="SMTP 사용자")
    smtp_password: str = Field(..., description="SMTP 비밀번호")
    from_email: EmailStr = Field(..., description="발신자 이메일")
    from_name: str = Field("Weather Flick Batch", description="발신자 이름")
    use_tls: bool = Field(True, description="TLS 사용 여부")


class SlackConfig(BaseModel):
    """슬랙 설정"""
    webhook_url: str = Field(..., description="슬랙 웹훅 URL")
    channel: Optional[str] = Field(None, description="채널 이름")
    username: str = Field("Weather Flick Batch", description="봇 사용자명")
    icon_emoji: Optional[str] = Field(":robot_face:", description="봇 아이콘")


class WebhookConfig(BaseModel):
    """웹훅 설정"""
    url: str = Field(..., description="웹훅 URL")
    method: str = Field("POST", description="HTTP 메소드")
    headers: Optional[Dict[str, str]] = Field(None, description="추가 헤더")
    timeout: int = Field(30, description="타임아웃(초)")


class NotificationSubscriptionCreate(BaseModel):
    """알림 구독 생성"""
    job_type: Optional[str] = Field(None, description="작업 유형 (None이면 모든 작업)")
    channel: NotificationChannel = Field(..., description="알림 채널")
    events: List[NotificationEvent] = Field(..., description="구독할 이벤트 목록")
    recipient: str = Field(..., description="수신자 (이메일 주소 또는 슬랙 사용자)")
    config: Optional[Dict[str, Any]] = Field(None, description="채널별 추가 설정")
    filters: Optional[Dict[str, Any]] = Field(None, description="알림 필터 조건")
    enabled: bool = Field(True, description="활성화 여부")


class NotificationSubscriptionUpdate(BaseModel):
    """알림 구독 수정"""
    events: Optional[List[NotificationEvent]] = None
    recipient: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    filters: Optional[Dict[str, Any]] = None
    enabled: Optional[bool] = None


class NotificationSubscriptionResponse(BaseModel):
    """알림 구독 응답"""
    model_config = ConfigDict(from_attributes=True)
    
    subscription_id: int
    job_type: Optional[str]
    channel: NotificationChannel
    events: List[NotificationEvent]
    recipient: str
    config: Optional[Dict[str, Any]]
    filters: Optional[Dict[str, Any]]
    enabled: bool
    created_at: datetime
    updated_at: Optional[datetime]


class NotificationHistoryResponse(BaseModel):
    """알림 발송 이력"""
    model_config = ConfigDict(from_attributes=True)
    
    notification_id: int
    job_id: str
    job_type: str
    event: NotificationEvent
    channel: NotificationChannel
    recipient: str
    subject: Optional[str]
    message: str
    level: NotificationLevel
    sent_at: datetime
    success: bool
    error_message: Optional[str]
    retry_count: int


class NotificationTemplateCreate(BaseModel):
    """알림 템플릿 생성"""
    event: NotificationEvent = Field(..., description="이벤트 유형")
    channel: NotificationChannel = Field(..., description="알림 채널")
    subject_template: Optional[str] = Field(None, description="제목 템플릿 (이메일용)")
    message_template: str = Field(..., description="메시지 템플릿")
    level: NotificationLevel = Field(NotificationLevel.INFO, description="알림 레벨")
    variables: Optional[List[str]] = Field(None, description="사용 가능한 변수 목록")


class NotificationTemplateResponse(BaseModel):
    """알림 템플릿 응답"""
    model_config = ConfigDict(from_attributes=True)
    
    template_id: int
    event: NotificationEvent
    channel: NotificationChannel
    subject_template: Optional[str]
    message_template: str
    level: NotificationLevel
    variables: Optional[List[str]]
    created_at: datetime
    updated_at: Optional[datetime]


class SendNotificationRequest(BaseModel):
    """알림 발송 요청"""
    job_id: str = Field(..., description="작업 ID")
    event: NotificationEvent = Field(..., description="이벤트 유형")
    level: NotificationLevel = Field(NotificationLevel.INFO, description="알림 레벨")
    additional_data: Optional[Dict[str, Any]] = Field(None, description="추가 데이터")


class NotificationSettingsUpdate(BaseModel):
    """알림 전역 설정 업데이트"""
    email_config: Optional[EmailConfig] = None
    slack_config: Optional[SlackConfig] = None
    webhook_config: Optional[WebhookConfig] = None
    enabled: Optional[bool] = None
    rate_limit: Optional[int] = Field(None, ge=0, description="분당 최대 알림 수")


class NotificationMetrics(BaseModel):
    """알림 메트릭"""
    total_sent: int
    success_count: int
    failure_count: int
    by_channel: Dict[str, int]
    by_event: Dict[str, int]
    average_send_time: float
    last_24h_count: int