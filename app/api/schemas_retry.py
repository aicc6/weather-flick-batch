"""
배치 작업 재시도 관련 스키마
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


class RetryStrategy(str, Enum):
    """재시도 전략"""
    IMMEDIATE = "immediate"  # 즉시 재시도
    EXPONENTIAL_BACKOFF = "exponential_backoff"  # 지수 백오프
    LINEAR_BACKOFF = "linear_backoff"  # 선형 백오프
    CUSTOM = "custom"  # 사용자 정의


class RetryStatus(str, Enum):
    """재시도 상태"""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    MAX_ATTEMPTS_REACHED = "max_attempts_reached"


class RetryPolicyCreate(BaseModel):
    """재시도 정책 생성"""
    job_type: str = Field(..., description="작업 유형")
    max_attempts: int = Field(3, ge=1, le=10, description="최대 재시도 횟수")
    retry_strategy: RetryStrategy = Field(RetryStrategy.EXPONENTIAL_BACKOFF, description="재시도 전략")
    initial_delay_seconds: int = Field(60, ge=1, description="초기 지연 시간(초)")
    max_delay_seconds: int = Field(3600, ge=1, description="최대 지연 시간(초)")
    backoff_multiplier: float = Field(2.0, ge=1.0, description="백오프 배수")
    retry_on_errors: Optional[List[str]] = Field(None, description="재시도할 에러 타입 목록")
    enabled: bool = Field(True, description="정책 활성화 여부")


class RetryPolicyUpdate(BaseModel):
    """재시도 정책 수정"""
    max_attempts: Optional[int] = Field(None, ge=1, le=10)
    retry_strategy: Optional[RetryStrategy] = None
    initial_delay_seconds: Optional[int] = Field(None, ge=1)
    max_delay_seconds: Optional[int] = Field(None, ge=1)
    backoff_multiplier: Optional[float] = Field(None, ge=1.0)
    retry_on_errors: Optional[List[str]] = None
    enabled: Optional[bool] = None


class RetryPolicyResponse(BaseModel):
    """재시도 정책 응답"""
    model_config = ConfigDict(from_attributes=True)
    
    policy_id: int
    job_type: str
    max_attempts: int
    retry_strategy: RetryStrategy
    initial_delay_seconds: int
    max_delay_seconds: int
    backoff_multiplier: float
    retry_on_errors: Optional[List[str]]
    enabled: bool
    created_at: datetime
    updated_at: Optional[datetime]


class RetryAttemptCreate(BaseModel):
    """재시도 시도 생성"""
    job_id: str = Field(..., description="작업 ID")
    attempt_number: int = Field(..., ge=1, description="시도 번호")
    error_message: Optional[str] = Field(None, description="에러 메시지")
    error_type: Optional[str] = Field(None, description="에러 타입")
    delay_seconds: int = Field(0, ge=0, description="다음 시도까지 지연 시간")


class RetryAttemptResponse(BaseModel):
    """재시도 시도 응답"""
    model_config = ConfigDict(from_attributes=True)
    
    attempt_id: int
    job_id: str
    job_type: str
    attempt_number: int
    status: RetryStatus
    error_message: Optional[str]
    error_type: Optional[str]
    delay_seconds: int
    started_at: datetime
    completed_at: Optional[datetime]
    next_retry_at: Optional[datetime]


class RetryHistoryResponse(BaseModel):
    """재시도 이력 응답"""
    job_id: str
    job_type: str
    original_started_at: datetime
    total_attempts: int
    successful: bool
    attempts: List[RetryAttemptResponse]
    
    
class RetryQueueItem(BaseModel):
    """재시도 대기열 항목"""
    job_id: str
    job_type: str
    attempt_number: int
    scheduled_at: datetime
    priority: int = Field(5, ge=1, le=10)
    config: Optional[Dict[str, Any]] = None


class RetryQueueResponse(BaseModel):
    """재시도 대기열 응답"""
    queue_items: List[RetryQueueItem]
    total: int
    pending_count: int
    in_progress_count: int
    

class RetryMetrics(BaseModel):
    """재시도 메트릭"""
    job_type: str
    total_attempts: int
    successful_retries: int
    failed_retries: int
    average_attempts_to_success: float
    max_attempts_used: int
    retry_success_rate: float
    most_common_errors: List[Dict[str, Any]]