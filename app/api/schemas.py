"""
Batch API Pydantic 스키마
"""

from datetime import datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from enum import Enum
import uuid

class JobType(str, Enum):
    """배치 작업 유형"""
    KTO_DATA_COLLECTION = "KTO_DATA_COLLECTION"
    WEATHER_DATA_COLLECTION = "WEATHER_DATA_COLLECTION"
    RECOMMENDATION_CALCULATION = "RECOMMENDATION_CALCULATION"
    DATA_QUALITY_CHECK = "DATA_QUALITY_CHECK"
    ARCHIVE_BACKUP = "ARCHIVE_BACKUP"
    SYSTEM_HEALTH_CHECK = "SYSTEM_HEALTH_CHECK"

class JobStatus(str, Enum):
    """작업 상태"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    STOPPED = "STOPPED"

class LogLevel(str, Enum):
    """로그 레벨"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

# Request 스키마
class JobExecuteRequest(BaseModel):
    """작업 실행 요청"""
    parameters: Optional[Dict[str, Any]] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=1, le=10)
    requested_by: Optional[str] = None

class JobStopRequest(BaseModel):
    """작업 중단 요청"""
    reason: Optional[str] = None
    force: bool = False

# Response 스키마
class JobInfo(BaseModel):
    """작업 정보"""
    id: str
    job_type: JobType
    status: JobStatus
    progress: float = Field(ge=0, le=100)
    current_step: Optional[str] = None
    total_steps: Optional[int] = None
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    result_summary: Optional[Dict[str, Any]] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)

class JobListResponse(BaseModel):
    """작업 목록 응답"""
    jobs: List[JobInfo]
    total: int
    page: int
    size: int

class JobExecuteResponse(BaseModel):
    """작업 실행 응답"""
    job_id: str
    job_type: JobType
    status: JobStatus
    message: str

class JobLog(BaseModel):
    """작업 로그"""
    timestamp: datetime
    level: LogLevel
    message: str
    details: Optional[Dict[str, Any]] = None

class JobLogsResponse(BaseModel):
    """작업 로그 응답"""
    job_id: str
    logs: List[JobLog]
    total: int
    page: int
    size: int

class JobStatistics(BaseModel):
    """작업 통계"""
    job_type: JobType
    total_count: int
    completed_count: int
    failed_count: int
    running_count: int
    average_duration_seconds: Optional[float] = None
    success_rate: float

class SystemStatus(BaseModel):
    """시스템 상태"""
    running_jobs: int
    max_concurrent_jobs: int
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    api_keys_status: Dict[str, Dict[str, Any]]

class ErrorResponse(BaseModel):
    """에러 응답"""
    error: str
    message: str
    details: Optional[Dict[str, Any]] = None