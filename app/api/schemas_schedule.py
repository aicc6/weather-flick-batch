"""
배치 작업 스케줄 관련 스키마
"""
from datetime import datetime
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict
from enum import Enum


class ScheduleStatus(str, Enum):
    """스케줄 상태"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"


class CronExpression(BaseModel):
    """크론 표현식"""
    minute: Optional[str] = Field(None, description="분 (0-59)")
    hour: Optional[str] = Field(None, description="시 (0-23)")
    day: Optional[str] = Field(None, description="일 (1-31)")
    month: Optional[str] = Field(None, description="월 (1-12)")
    day_of_week: Optional[str] = Field(None, description="요일 (0-6, 0=일요일)")


class ScheduleCreate(BaseModel):
    """스케줄 생성 요청"""
    job_type: str = Field(..., description="배치 작업 유형")
    scheduled_time: Optional[datetime] = Field(None, description="일회성 실행 시간")
    cron_expression: Optional[CronExpression] = Field(None, description="반복 실행을 위한 크론 표현식")
    priority: int = Field(5, ge=1, le=10, description="우선순위 (1-10, 10이 가장 높음)")
    is_active: bool = Field(True, description="스케줄 활성화 여부")
    config: Optional[Dict[str, Any]] = Field(None, description="작업별 설정")
    description: Optional[str] = Field(None, description="스케줄 설명")


class ScheduleUpdate(BaseModel):
    """스케줄 수정 요청"""
    scheduled_time: Optional[datetime] = None
    cron_expression: Optional[CronExpression] = None
    priority: Optional[int] = Field(None, ge=1, le=10)
    is_active: Optional[bool] = None
    config: Optional[Dict[str, Any]] = None
    description: Optional[str] = None


class ScheduleResponse(BaseModel):
    """스케줄 응답"""
    model_config = ConfigDict(from_attributes=True)
    
    schedule_id: int
    job_type: str
    scheduled_time: Optional[datetime]
    cron_expression: Optional[Dict[str, str]]
    priority: int
    is_active: bool
    status: ScheduleStatus
    config: Optional[Dict[str, Any]]
    description: Optional[str]
    next_run: Optional[datetime]
    last_run: Optional[datetime]
    created_at: datetime
    updated_at: Optional[datetime]


class ScheduleExecutionResponse(BaseModel):
    """스케줄 실행 결과"""
    schedule_id: int
    execution_id: str
    job_type: str
    status: str
    started_at: datetime
    completed_at: Optional[datetime]
    result_summary: Optional[Dict[str, Any]]
    error_message: Optional[str]


class ScheduleListResponse(BaseModel):
    """스케줄 목록 응답"""
    schedules: List[ScheduleResponse]
    total: int
    page: int
    page_size: int


class UpcomingSchedule(BaseModel):
    """예정된 스케줄"""
    schedule_id: int
    job_type: str
    scheduled_time: datetime
    priority: int
    description: Optional[str]
    estimated_duration: Optional[int] = Field(None, description="예상 소요 시간(초)")