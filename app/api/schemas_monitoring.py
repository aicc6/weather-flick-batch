"""
배치 성능 모니터링 스키마
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from enum import Enum


class MetricType(str, Enum):
    """메트릭 타입"""
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"
    DISK_USAGE = "disk_usage"
    NETWORK_IO = "network_io"
    JOB_EXECUTION_TIME = "job_execution_time"
    JOB_SUCCESS_RATE = "job_success_rate"
    API_RESPONSE_TIME = "api_response_time"
    QUEUE_SIZE = "queue_size"
    ERROR_RATE = "error_rate"
    THROUGHPUT = "throughput"


class TimeRange(str, Enum):
    """시간 범위"""
    LAST_HOUR = "1h"
    LAST_6_HOURS = "6h"
    LAST_24_HOURS = "24h"
    LAST_7_DAYS = "7d"
    LAST_30_DAYS = "30d"


class AggregationType(str, Enum):
    """집계 타입"""
    AVG = "avg"
    MAX = "max"
    MIN = "min"
    SUM = "sum"
    COUNT = "count"


# 메트릭 데이터 포인트
class MetricDataPoint(BaseModel):
    """메트릭 데이터 포인트"""
    timestamp: datetime
    value: float
    metadata: Optional[Dict[str, Any]] = None


class MetricSeries(BaseModel):
    """메트릭 시계열 데이터"""
    metric_type: MetricType
    job_type: Optional[str] = None
    aggregation: AggregationType
    data_points: List[MetricDataPoint]
    total_count: int
    time_range: str


# 성능 메트릭 요청
class PerformanceMetricsRequest(BaseModel):
    """성능 메트릭 조회 요청"""
    metric_types: List[MetricType]
    time_range: TimeRange
    job_type: Optional[str] = None
    aggregation: AggregationType = AggregationType.AVG
    interval_minutes: Optional[int] = 5  # 데이터 포인트 간격 (분)


# 실시간 메트릭
class RealTimeMetrics(BaseModel):
    """실시간 메트릭"""
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    running_jobs: int
    queued_jobs: int
    completed_jobs_today: int
    failed_jobs_today: int
    avg_execution_time_minutes: Optional[float] = None
    success_rate_today: float
    api_response_time_ms: Optional[float] = None


# 작업 타입별 성능 통계
class JobTypePerformance(BaseModel):
    """작업 타입별 성능 통계"""
    job_type: str
    total_executions: int
    successful_executions: int
    failed_executions: int
    avg_execution_time_seconds: float
    min_execution_time_seconds: float
    max_execution_time_seconds: float
    success_rate: float
    last_execution: Optional[datetime] = None
    next_scheduled: Optional[datetime] = None


# 성능 대시보드 데이터
class PerformanceDashboard(BaseModel):
    """성능 대시보드 데이터"""
    real_time_metrics: RealTimeMetrics
    job_type_performance: List[JobTypePerformance]
    metric_series: List[MetricSeries]
    alerts: List[Dict[str, Any]]
    system_health_score: float  # 0-100


# 알럿 관련
class AlertLevel(str, Enum):
    """알럿 레벨"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertCondition(BaseModel):
    """알럿 조건"""
    metric_type: MetricType
    operator: str  # >, <, >=, <=, ==
    threshold: float
    duration_minutes: int  # 지속 시간


class Alert(BaseModel):
    """알럿"""
    alert_id: str
    level: AlertLevel
    metric_type: MetricType
    message: str
    current_value: float
    threshold: float
    triggered_at: datetime
    resolved_at: Optional[datetime] = None
    job_type: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class AlertRule(BaseModel):
    """알럿 규칙"""
    rule_id: str
    name: str
    description: str
    condition: AlertCondition
    level: AlertLevel
    enabled: bool = True
    notification_channels: List[str] = []  # email, slack, webhook
    created_at: datetime
    updated_at: datetime


# 성능 레포트
class PerformanceReport(BaseModel):
    """성능 레포트"""
    report_id: str
    title: str
    time_range: TimeRange
    generated_at: datetime
    summary: Dict[str, Any]
    job_type_analysis: List[JobTypePerformance]
    system_metrics: Dict[str, Any]
    recommendations: List[str]
    trends: Dict[str, Any]


# API 요청/응답 스키마
class CreateAlertRuleRequest(BaseModel):
    """알럿 규칙 생성 요청"""
    name: str
    description: str
    condition: AlertCondition
    level: AlertLevel
    notification_channels: List[str] = []


class AlertRuleResponse(BaseModel):
    """알럿 규칙 응답"""
    rule_id: str
    name: str
    description: str
    condition: AlertCondition
    level: AlertLevel
    enabled: bool
    notification_channels: List[str]
    created_at: datetime
    updated_at: datetime


class GenerateReportRequest(BaseModel):
    """레포트 생성 요청"""
    title: str
    time_range: TimeRange
    job_types: Optional[List[str]] = None
    include_recommendations: bool = True


class PerformanceMetricsResponse(BaseModel):
    """성능 메트릭 응답"""
    metrics: List[MetricSeries]
    time_range: str
    total_data_points: int
    generated_at: datetime


# 시스템 건강도 점수 계산을 위한 가중치
class HealthScoreWeights(BaseModel):
    """시스템 건강도 점수 가중치"""
    cpu_weight: float = 0.2
    memory_weight: float = 0.2
    disk_weight: float = 0.1
    success_rate_weight: float = 0.3
    response_time_weight: float = 0.1
    error_rate_weight: float = 0.1


# 트렌드 분석
class TrendAnalysis(BaseModel):
    """트렌드 분석"""
    metric_type: MetricType
    trend_direction: str  # "increasing", "decreasing", "stable"
    change_percentage: float
    confidence_score: float  # 0-1
    time_range: str
    analysis_date: datetime