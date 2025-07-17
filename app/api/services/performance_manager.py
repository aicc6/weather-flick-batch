"""
성능 모니터링 매니저
"""

import asyncio
import logging
import psutil
import socket
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from statistics import mean, stdev
from collections import defaultdict
import uuid

from sqlalchemy import func, and_, or_, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from app.api.schemas_monitoring import (
    MetricType, TimeRange, AggregationType, AlertLevel,
    MetricDataPoint, MetricSeries, RealTimeMetrics,
    JobTypePerformance, PerformanceDashboard, Alert, AlertRule,
    PerformanceReport, PerformanceMetricsRequest, PerformanceMetricsResponse,
    CreateAlertRuleRequest, AlertRuleResponse, GenerateReportRequest,
    TrendAnalysis, HealthScoreWeights
)
from app.models import (
    BatchJobPerformanceMetric, SystemPerformanceMetric, PerformanceAlert,
    AlertRule as AlertRuleModel, PerformanceReport as PerformanceReportModel,
    BatchJobExecution, MetricType as MetricTypeEnum, AlertLevelEnum
)

logger = logging.getLogger(__name__)


class PerformanceManager:
    """성능 모니터링 매니저"""
    
    def __init__(self):
        self.hostname = socket.gethostname()
        self.service_name = "weather-flick-batch"
        self.health_weights = HealthScoreWeights()
        
        # 알럿 확인을 위한 캐시
        self.alert_cache = {}
        self.last_alert_check = None
        
    async def collect_system_metrics(self, db: AsyncSession):
        """시스템 메트릭 수집"""
        try:
            current_time = datetime.utcnow()
            
            # CPU 사용률
            cpu_percent = psutil.cpu_percent(interval=1)
            await self._save_system_metric(
                db, MetricTypeEnum.CPU_USAGE, cpu_percent, "percent", current_time
            )
            
            # 메모리 사용률
            memory = psutil.virtual_memory()
            await self._save_system_metric(
                db, MetricTypeEnum.MEMORY_USAGE, memory.percent, "percent", current_time
            )
            
            # 디스크 사용률
            disk = psutil.disk_usage('/')
            await self._save_system_metric(
                db, MetricTypeEnum.DISK_USAGE, disk.percent, "percent", current_time
            )
            
            # 네트워크 I/O
            net_io = psutil.net_io_counters()
            if net_io:
                await self._save_system_metric(
                    db, MetricTypeEnum.NETWORK_IO, 
                    net_io.bytes_sent + net_io.bytes_recv, "bytes", current_time
                )
                
            logger.debug(f"시스템 메트릭 수집 완료: CPU={cpu_percent}%, Memory={memory.percent}%, Disk={disk.percent}%")
            
        except Exception as e:
            logger.error(f"시스템 메트릭 수집 오류: {e}")
    
    async def collect_job_metric(self, db: AsyncSession, job_id: str, metric_type: MetricType, 
                                value: float, unit: str = None, metadata: Dict[str, Any] = None):
        """작업 메트릭 수집"""
        try:
            current_time = datetime.utcnow()
            
            metric = BatchJobPerformanceMetric(
                metric_id=uuid.uuid4(),
                job_id=job_id,
                metric_type=MetricTypeEnum(metric_type.value),
                metric_value=value,
                metric_unit=unit,
                measured_at=current_time,
                metadata=metadata
            )
            
            db.add(metric)
            await db.commit()
            
            logger.debug(f"작업 메트릭 수집: {job_id} - {metric_type.value}={value}")
            
        except Exception as e:
            logger.error(f"작업 메트릭 수집 오류: {e}")
    
    async def get_performance_metrics(self, request: PerformanceMetricsRequest, 
                                    db: AsyncSession) -> PerformanceMetricsResponse:
        """성능 메트릭 조회"""
        try:
            # 시간 범위 계산
            end_time = datetime.utcnow()
            start_time = self._calculate_start_time(end_time, request.time_range)
            
            all_series = []
            total_points = 0
            
            for metric_type in request.metric_types:
                # 시스템 메트릭과 작업 메트릭을 분리해서 조회
                if metric_type in [MetricType.CPU_USAGE, MetricType.MEMORY_USAGE, 
                                 MetricType.DISK_USAGE, MetricType.NETWORK_IO]:
                    series = await self._get_system_metric_series(
                        db, metric_type, start_time, end_time, 
                        request.aggregation, request.interval_minutes
                    )
                else:
                    series = await self._get_job_metric_series(
                        db, metric_type, start_time, end_time,
                        request.job_type, request.aggregation, request.interval_minutes
                    )
                
                if series:
                    all_series.append(series)
                    total_points += len(series.data_points)
            
            return PerformanceMetricsResponse(
                metrics=all_series,
                time_range=f"{start_time.isoformat()} ~ {end_time.isoformat()}",
                total_data_points=total_points,
                generated_at=datetime.utcnow()
            )
            
        except Exception as e:
            logger.error(f"성능 메트릭 조회 오류: {e}")
            return PerformanceMetricsResponse(
                metrics=[],
                time_range="",
                total_data_points=0,
                generated_at=datetime.utcnow()
            )
    
    async def get_real_time_metrics(self, db: AsyncSession) -> RealTimeMetrics:
        """실시간 메트릭 조회"""
        try:
            current_time = datetime.utcnow()
            today_start = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            
            # 최신 시스템 메트릭 조회
            latest_metrics = await self._get_latest_system_metrics(db)
            
            # 오늘의 작업 통계
            job_stats = await self._get_today_job_stats(db, today_start, current_time)
            
            # 실행 중인 작업 수
            running_jobs = await self._get_running_jobs_count(db)
            
            # 대기 중인 작업 수 (스케줄된 작업)
            queued_jobs = await self._get_queued_jobs_count(db)
            
            return RealTimeMetrics(
                timestamp=current_time,
                cpu_usage=latest_metrics.get('cpu_usage', 0.0),
                memory_usage=latest_metrics.get('memory_usage', 0.0),
                disk_usage=latest_metrics.get('disk_usage', 0.0),
                running_jobs=running_jobs,
                queued_jobs=queued_jobs,
                completed_jobs_today=job_stats['completed'],
                failed_jobs_today=job_stats['failed'],
                avg_execution_time_minutes=job_stats['avg_execution_time'],
                success_rate_today=job_stats['success_rate'],
                api_response_time_ms=latest_metrics.get('api_response_time', None)
            )
            
        except Exception as e:
            logger.error(f"실시간 메트릭 조회 오류: {e}")
            return RealTimeMetrics(
                timestamp=datetime.utcnow(),
                cpu_usage=0.0,
                memory_usage=0.0,
                disk_usage=0.0,
                running_jobs=0,
                queued_jobs=0,
                completed_jobs_today=0,
                failed_jobs_today=0,
                success_rate_today=0.0
            )
    
    async def get_job_type_performance(self, db: AsyncSession, 
                                     time_range: TimeRange) -> List[JobTypePerformance]:
        """작업 타입별 성능 통계"""
        try:
            end_time = datetime.utcnow()
            start_time = self._calculate_start_time(end_time, time_range)
            
            # 작업 실행 이력 조회
            jobs_query = await db.execute(
                f"""
                SELECT 
                    job_type,
                    COUNT(*) as total_executions,
                    COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as successful_executions,
                    COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_executions,
                    AVG(CASE 
                        WHEN status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL 
                        THEN EXTRACT(EPOCH FROM (completed_at - started_at))
                    END) as avg_execution_time_seconds,
                    MIN(CASE 
                        WHEN status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL 
                        THEN EXTRACT(EPOCH FROM (completed_at - started_at))
                    END) as min_execution_time_seconds,
                    MAX(CASE 
                        WHEN status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL 
                        THEN EXTRACT(EPOCH FROM (completed_at - started_at))
                    END) as max_execution_time_seconds,
                    MAX(started_at) as last_execution
                FROM batch_job_executions
                WHERE created_at >= :start_time AND created_at <= :end_time
                GROUP BY job_type
                """,
                {'start_time': start_time, 'end_time': end_time}
            )
            
            results = []
            for row in jobs_query.fetchall():
                success_rate = 0.0
                if row.total_executions > 0:
                    success_rate = (row.successful_executions / row.total_executions) * 100
                
                results.append(JobTypePerformance(
                    job_type=row.job_type,
                    total_executions=row.total_executions or 0,
                    successful_executions=row.successful_executions or 0,
                    failed_executions=row.failed_executions or 0,
                    avg_execution_time_seconds=row.avg_execution_time_seconds or 0.0,
                    min_execution_time_seconds=row.min_execution_time_seconds or 0.0,
                    max_execution_time_seconds=row.max_execution_time_seconds or 0.0,
                    success_rate=success_rate,
                    last_execution=row.last_execution
                ))
            
            return results
            
        except Exception as e:
            logger.error(f"작업 타입별 성능 통계 조회 오류: {e}")
            return []
    
    async def get_performance_dashboard(self, db: AsyncSession, 
                                      time_range: TimeRange = TimeRange.LAST_24_HOURS) -> PerformanceDashboard:
        """성능 대시보드 데이터 조회"""
        try:
            # 실시간 메트릭
            real_time_metrics = await self.get_real_time_metrics(db)
            
            # 작업 타입별 성능
            job_performance = await self.get_job_type_performance(db, time_range)
            
            # 주요 메트릭 시계열 데이터
            metrics_request = PerformanceMetricsRequest(
                metric_types=[
                    MetricType.CPU_USAGE,
                    MetricType.MEMORY_USAGE,
                    MetricType.JOB_EXECUTION_TIME
                ],
                time_range=time_range,
                aggregation=AggregationType.AVG,
                interval_minutes=15
            )
            metrics_response = await self.get_performance_metrics(metrics_request, db)
            
            # 활성 알럿 조회
            alerts = await self.get_active_alerts(db)
            
            # 시스템 건강도 점수 계산
            health_score = await self._calculate_health_score(real_time_metrics, job_performance)
            
            return PerformanceDashboard(
                real_time_metrics=real_time_metrics,
                job_type_performance=job_performance,
                metric_series=metrics_response.metrics,
                alerts=[alert.dict() for alert in alerts],
                system_health_score=health_score
            )
            
        except Exception as e:
            logger.error(f"성능 대시보드 조회 오류: {e}")
            # 기본값 반환
            return PerformanceDashboard(
                real_time_metrics=await self.get_real_time_metrics(db),
                job_type_performance=[],
                metric_series=[],
                alerts=[],
                system_health_score=0.0
            )
    
    async def check_alerts(self, db: AsyncSession):
        """알럿 확인 및 발생"""
        try:
            # 활성화된 알럿 규칙 조회
            rules_query = await db.execute(
                "SELECT * FROM alert_rules WHERE enabled = true"
            )
            
            for rule_row in rules_query.fetchall():
                await self._check_alert_rule(db, rule_row)
                
        except Exception as e:
            logger.error(f"알럿 확인 오류: {e}")
    
    async def create_alert_rule(self, request: CreateAlertRuleRequest, 
                              db: AsyncSession) -> AlertRuleResponse:
        """알럿 규칙 생성"""
        try:
            rule = AlertRuleModel(
                rule_id=uuid.uuid4(),
                name=request.name,
                description=request.description,
                metric_type=MetricTypeEnum(request.condition.metric_type.value),
                operator=request.condition.operator,
                threshold_value=request.condition.threshold,
                duration_minutes=request.condition.duration_minutes,
                alert_level=AlertLevelEnum(request.level.value),
                notification_channels=request.notification_channels,
                enabled=True
            )
            
            db.add(rule)
            await db.commit()
            
            return AlertRuleResponse(
                rule_id=str(rule.rule_id),
                name=rule.name,
                description=rule.description,
                condition=request.condition,
                level=request.level,
                enabled=rule.enabled,
                notification_channels=rule.notification_channels,
                created_at=rule.created_at,
                updated_at=rule.updated_at
            )
            
        except Exception as e:
            logger.error(f"알럿 규칙 생성 오류: {e}")
            raise
    
    async def get_active_alerts(self, db: AsyncSession) -> List[Alert]:
        """활성 알럿 조회"""
        try:
            alerts_query = await db.execute(
                """
                SELECT a.*, r.name as rule_name
                FROM performance_alerts a
                JOIN alert_rules r ON a.rule_id = r.rule_id
                WHERE a.resolved_at IS NULL
                ORDER BY a.triggered_at DESC
                LIMIT 50
                """
            )
            
            alerts = []
            for row in alerts_query.fetchall():
                alert = Alert(
                    alert_id=str(row.alert_id),
                    level=AlertLevel(row.level),
                    metric_type=MetricType(row.metric_type),
                    message=row.message,
                    current_value=row.current_value,
                    threshold=row.threshold_value,
                    triggered_at=row.triggered_at,
                    resolved_at=row.resolved_at,
                    job_type=row.job_type,
                    metadata=row.metadata
                )
                alerts.append(alert)
            
            return alerts
            
        except Exception as e:
            logger.error(f"활성 알럿 조회 오류: {e}")
            return []
    
    # Private helper methods
    
    async def _save_system_metric(self, db: AsyncSession, metric_type: MetricTypeEnum, 
                                 value: float, unit: str, timestamp: datetime):
        """시스템 메트릭 저장"""
        metric = SystemPerformanceMetric(
            metric_id=uuid.uuid4(),
            metric_type=metric_type,
            metric_value=value,
            metric_unit=unit,
            measured_at=timestamp,
            hostname=self.hostname,
            service_name=self.service_name
        )
        
        db.add(metric)
        await db.commit()
    
    def _calculate_start_time(self, end_time: datetime, time_range: TimeRange) -> datetime:
        """시간 범위 계산"""
        if time_range == TimeRange.LAST_HOUR:
            return end_time - timedelta(hours=1)
        elif time_range == TimeRange.LAST_6_HOURS:
            return end_time - timedelta(hours=6)
        elif time_range == TimeRange.LAST_24_HOURS:
            return end_time - timedelta(hours=24)
        elif time_range == TimeRange.LAST_7_DAYS:
            return end_time - timedelta(days=7)
        elif time_range == TimeRange.LAST_30_DAYS:
            return end_time - timedelta(days=30)
        else:
            return end_time - timedelta(hours=24)
    
    async def _get_system_metric_series(self, db: AsyncSession, metric_type: MetricType,
                                       start_time: datetime, end_time: datetime,
                                       aggregation: AggregationType, interval_minutes: int) -> MetricSeries:
        """시스템 메트릭 시계열 데이터 조회"""
        # SQL 쿼리로 집계된 데이터 조회
        query = f"""
        SELECT 
            date_trunc('hour', measured_at) + 
            (EXTRACT(minute FROM measured_at)::int / {interval_minutes}) * interval '{interval_minutes} minutes' as time_bucket,
            {aggregation.value.upper()}(metric_value) as value
        FROM system_performance_metrics
        WHERE metric_type = :metric_type 
        AND measured_at >= :start_time 
        AND measured_at <= :end_time
        AND service_name = :service_name
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        
        result = await db.execute(query, {
            'metric_type': metric_type.value,
            'start_time': start_time,
            'end_time': end_time,
            'service_name': self.service_name
        })
        
        data_points = []
        for row in result.fetchall():
            data_points.append(MetricDataPoint(
                timestamp=row.time_bucket,
                value=float(row.value) if row.value else 0.0
            ))
        
        return MetricSeries(
            metric_type=metric_type,
            aggregation=aggregation,
            data_points=data_points,
            total_count=len(data_points),
            time_range=f"{start_time.isoformat()} ~ {end_time.isoformat()}"
        )
    
    async def _get_job_metric_series(self, db: AsyncSession, metric_type: MetricType,
                                    start_time: datetime, end_time: datetime,
                                    job_type: Optional[str], aggregation: AggregationType,
                                    interval_minutes: int) -> MetricSeries:
        """작업 메트릭 시계열 데이터 조회"""
        job_filter = ""
        params = {
            'metric_type': metric_type.value,
            'start_time': start_time,
            'end_time': end_time
        }
        
        if job_type:
            job_filter = "AND e.job_type = :job_type"
            params['job_type'] = job_type
        
        query = f"""
        SELECT 
            date_trunc('hour', m.measured_at) + 
            (EXTRACT(minute FROM m.measured_at)::int / {interval_minutes}) * interval '{interval_minutes} minutes' as time_bucket,
            {aggregation.value.upper()}(m.metric_value) as value
        FROM batch_job_performance_metrics m
        JOIN batch_job_executions e ON m.job_id = e.id
        WHERE m.metric_type = :metric_type 
        AND m.measured_at >= :start_time 
        AND m.measured_at <= :end_time
        {job_filter}
        GROUP BY time_bucket
        ORDER BY time_bucket
        """
        
        result = await db.execute(query, params)
        
        data_points = []
        for row in result.fetchall():
            data_points.append(MetricDataPoint(
                timestamp=row.time_bucket,
                value=float(row.value) if row.value else 0.0
            ))
        
        return MetricSeries(
            metric_type=metric_type,
            job_type=job_type,
            aggregation=aggregation,
            data_points=data_points,
            total_count=len(data_points),
            time_range=f"{start_time.isoformat()} ~ {end_time.isoformat()}"
        )
    
    async def _get_latest_system_metrics(self, db: AsyncSession) -> Dict[str, float]:
        """최신 시스템 메트릭 조회"""
        metrics = {}
        
        for metric_type in [MetricTypeEnum.CPU_USAGE, MetricTypeEnum.MEMORY_USAGE, 
                           MetricTypeEnum.DISK_USAGE, MetricTypeEnum.API_RESPONSE_TIME]:
            query = await db.execute(
                """
                SELECT metric_value 
                FROM system_performance_metrics 
                WHERE metric_type = :metric_type 
                AND service_name = :service_name
                ORDER BY measured_at DESC 
                LIMIT 1
                """,
                {'metric_type': metric_type.value, 'service_name': self.service_name}
            )
            
            row = query.fetchone()
            if row:
                metrics[metric_type.value] = row.metric_value
        
        return metrics
    
    async def _get_today_job_stats(self, db: AsyncSession, 
                                  today_start: datetime, current_time: datetime) -> Dict[str, Any]:
        """오늘의 작업 통계"""
        query = await db.execute(
            """
            SELECT 
                COUNT(*) as total_jobs,
                COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) as completed_jobs,
                COUNT(CASE WHEN status = 'FAILED' THEN 1 END) as failed_jobs,
                AVG(CASE 
                    WHEN status = 'COMPLETED' AND started_at IS NOT NULL AND completed_at IS NOT NULL 
                    THEN EXTRACT(EPOCH FROM (completed_at - started_at)) / 60.0
                END) as avg_execution_minutes
            FROM batch_job_executions
            WHERE created_at >= :today_start AND created_at <= :current_time
            """,
            {'today_start': today_start, 'current_time': current_time}
        )
        
        row = query.fetchone()
        if row and row.total_jobs > 0:
            success_rate = (row.completed_jobs / row.total_jobs) * 100
        else:
            success_rate = 0.0
        
        return {
            'completed': row.completed_jobs if row else 0,
            'failed': row.failed_jobs if row else 0,
            'avg_execution_time': row.avg_execution_minutes if row else None,
            'success_rate': success_rate
        }
    
    async def _get_running_jobs_count(self, db: AsyncSession) -> int:
        """실행 중인 작업 수"""
        query = await db.execute(
            "SELECT COUNT(*) FROM batch_job_executions WHERE status = 'RUNNING'"
        )
        row = query.fetchone()
        return row[0] if row else 0
    
    async def _get_queued_jobs_count(self, db: AsyncSession) -> int:
        """대기 중인 작업 수 (예약된 작업)"""
        query = await db.execute(
            "SELECT COUNT(*) FROM batch_job_executions WHERE status = 'PENDING'"
        )
        row = query.fetchone()
        return row[0] if row else 0
    
    async def _calculate_health_score(self, real_time_metrics: RealTimeMetrics, 
                                     job_performance: List[JobTypePerformance]) -> float:
        """시스템 건강도 점수 계산"""
        try:
            score = 100.0
            
            # CPU 사용률 점수 (80% 이상이면 감점)
            if real_time_metrics.cpu_usage > 80:
                score -= (real_time_metrics.cpu_usage - 80) * self.health_weights.cpu_weight * 5
            
            # 메모리 사용률 점수 (80% 이상이면 감점)
            if real_time_metrics.memory_usage > 80:
                score -= (real_time_metrics.memory_usage - 80) * self.health_weights.memory_weight * 5
            
            # 디스크 사용률 점수 (90% 이상이면 감점)
            if real_time_metrics.disk_usage > 90:
                score -= (real_time_metrics.disk_usage - 90) * self.health_weights.disk_weight * 10
            
            # 성공률 점수 (80% 미만이면 감점)
            if real_time_metrics.success_rate_today < 80:
                score -= (80 - real_time_metrics.success_rate_today) * self.health_weights.success_rate_weight
            
            # 응답 시간 점수 (1000ms 이상이면 감점)
            if real_time_metrics.api_response_time_ms and real_time_metrics.api_response_time_ms > 1000:
                score -= min(20, (real_time_metrics.api_response_time_ms - 1000) / 100) * self.health_weights.response_time_weight
            
            return max(0.0, min(100.0, score))
            
        except Exception as e:
            logger.error(f"건강도 점수 계산 오류: {e}")
            return 50.0  # 기본값
    
    async def _check_alert_rule(self, db: AsyncSession, rule_row):
        """개별 알럿 규칙 확인"""
        try:
            # 최근 duration_minutes 동안의 메트릭 조회
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(minutes=rule_row.duration_minutes)
            
            # 메트릭 값 조회 (시스템 또는 작업 메트릭)
            if rule_row.metric_type in ['cpu_usage', 'memory_usage', 'disk_usage', 'network_io']:
                current_value = await self._get_current_system_metric_value(
                    db, rule_row.metric_type, start_time, end_time
                )
            else:
                current_value = await self._get_current_job_metric_value(
                    db, rule_row.metric_type, start_time, end_time, rule_row.job_type_filter
                )
            
            if current_value is None:
                return
            
            # 조건 확인
            threshold_exceeded = self._check_threshold(
                current_value, rule_row.operator, rule_row.threshold_value
            )
            
            if threshold_exceeded:
                # 이미 발생한 알럿이 있는지 확인
                existing_alert = await self._get_existing_alert(db, rule_row.rule_id)
                
                if not existing_alert:
                    # 새 알럿 생성
                    await self._create_alert(db, rule_row, current_value)
            else:
                # 기존 알럿 해결
                await self._resolve_existing_alert(db, rule_row.rule_id)
                
        except Exception as e:
            logger.error(f"알럿 규칙 확인 오류: {e}")
    
    def _check_threshold(self, current_value: float, operator: str, threshold: float) -> bool:
        """임계값 확인"""
        if operator == '>':
            return current_value > threshold
        elif operator == '<':
            return current_value < threshold
        elif operator == '>=':
            return current_value >= threshold
        elif operator == '<=':
            return current_value <= threshold
        elif operator == '==':
            return abs(current_value - threshold) < 0.001
        return False
    
    async def _get_current_system_metric_value(self, db: AsyncSession, metric_type: str,
                                              start_time: datetime, end_time: datetime) -> Optional[float]:
        """현재 시스템 메트릭 값 조회"""
        query = await db.execute(
            """
            SELECT AVG(metric_value) as avg_value
            FROM system_performance_metrics
            WHERE metric_type = :metric_type
            AND measured_at >= :start_time
            AND measured_at <= :end_time
            AND service_name = :service_name
            """,
            {
                'metric_type': metric_type,
                'start_time': start_time,
                'end_time': end_time,
                'service_name': self.service_name
            }
        )
        
        row = query.fetchone()
        return row.avg_value if row and row.avg_value else None
    
    async def _get_current_job_metric_value(self, db: AsyncSession, metric_type: str,
                                           start_time: datetime, end_time: datetime,
                                           job_type_filter: Optional[str]) -> Optional[float]:
        """현재 작업 메트릭 값 조회"""
        job_filter = ""
        params = {
            'metric_type': metric_type,
            'start_time': start_time,
            'end_time': end_time
        }
        
        if job_type_filter:
            job_filter = "AND e.job_type = :job_type"
            params['job_type'] = job_type_filter
        
        query = await db.execute(
            f"""
            SELECT AVG(m.metric_value) as avg_value
            FROM batch_job_performance_metrics m
            JOIN batch_job_executions e ON m.job_id = e.id
            WHERE m.metric_type = :metric_type
            AND m.measured_at >= :start_time
            AND m.measured_at <= :end_time
            {job_filter}
            """,
            params
        )
        
        row = query.fetchone()
        return row.avg_value if row and row.avg_value else None
    
    async def _get_existing_alert(self, db: AsyncSession, rule_id: str):
        """기존 알럿 조회"""
        query = await db.execute(
            "SELECT * FROM performance_alerts WHERE rule_id = :rule_id AND resolved_at IS NULL",
            {'rule_id': rule_id}
        )
        return query.fetchone()
    
    async def _create_alert(self, db: AsyncSession, rule_row, current_value: float):
        """알럿 생성"""
        alert = PerformanceAlert(
            alert_id=uuid.uuid4(),
            rule_id=rule_row.rule_id,
            level=AlertLevelEnum(rule_row.alert_level),
            metric_type=MetricTypeEnum(rule_row.metric_type),
            message=f"{rule_row.name}: {rule_row.metric_type} 값이 임계값을 초과했습니다",
            current_value=current_value,
            threshold_value=rule_row.threshold_value,
            triggered_at=datetime.utcnow(),
            job_type=rule_row.job_type_filter,
            metadata={
                'rule_name': rule_row.name,
                'operator': rule_row.operator,
                'duration_minutes': rule_row.duration_minutes
            }
        )
        
        db.add(alert)
        await db.commit()
        
        logger.warning(f"알럿 발생: {rule_row.name} - {current_value} {rule_row.operator} {rule_row.threshold_value}")
    
    async def _resolve_existing_alert(self, db: AsyncSession, rule_id: str):
        """기존 알럿 해결"""
        await db.execute(
            "UPDATE performance_alerts SET resolved_at = :now WHERE rule_id = :rule_id AND resolved_at IS NULL",
            {'rule_id': rule_id, 'now': datetime.utcnow()}
        )
        await db.commit()