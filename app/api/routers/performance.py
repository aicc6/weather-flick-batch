"""
성능 모니터링 API 라우터
"""

from datetime import datetime, timedelta
from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.auth import verify_api_key
from app.core.async_database import get_async_db_manager
from app.api.services.performance_manager import PerformanceManager
from app.api.schemas_monitoring import (
    PerformanceMetricsRequest, PerformanceMetricsResponse,
    PerformanceDashboard, RealTimeMetrics, JobTypePerformance,
    Alert, AlertRule, PerformanceReport, TrendAnalysis,
    CreateAlertRuleRequest, AlertRuleResponse, GenerateReportRequest,
    TimeRange, MetricType, AggregationType
)

router = APIRouter(prefix="/performance", tags=["Performance Monitoring"])
performance_manager = PerformanceManager()


@router.get("/dashboard", response_model=PerformanceDashboard)
async def get_performance_dashboard(
    time_range: TimeRange = Query(TimeRange.LAST_24_HOURS, description="시간 범위"),
    api_key: str = Depends(verify_api_key)
):
    """성능 대시보드 조회"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        return await performance_manager.get_performance_dashboard(db, time_range)


@router.get("/metrics", response_model=PerformanceMetricsResponse)
async def get_performance_metrics(
    metric_types: List[MetricType] = Query(..., description="조회할 메트릭 타입들"),
    time_range: TimeRange = Query(TimeRange.LAST_24_HOURS, description="시간 범위"),
    job_type: Optional[str] = Query(None, description="작업 타입 필터"),
    aggregation: AggregationType = Query(AggregationType.AVG, description="집계 방식"),
    interval_minutes: int = Query(5, description="데이터 포인트 간격 (분)", ge=1, le=60),
    api_key: str = Depends(verify_api_key)
):
    """성능 메트릭 조회"""
    request = PerformanceMetricsRequest(
        metric_types=metric_types,
        time_range=time_range,
        job_type=job_type,
        aggregation=aggregation,
        interval_minutes=interval_minutes
    )
    
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        return await performance_manager.get_performance_metrics(request, db)


@router.get("/realtime", response_model=RealTimeMetrics)
async def get_realtime_metrics(
    api_key: str = Depends(verify_api_key)
):
    """실시간 메트릭 조회"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        return await performance_manager.get_real_time_metrics(db)


@router.get("/job-types", response_model=List[JobTypePerformance])
async def get_job_type_performance(
    time_range: TimeRange = Query(TimeRange.LAST_24_HOURS, description="시간 범위"),
    api_key: str = Depends(verify_api_key)
):
    """작업 타입별 성능 통계"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        return await performance_manager.get_job_type_performance(db, time_range)


@router.post("/collect-system-metrics")
async def collect_system_metrics(
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key)
):
    """시스템 메트릭 수집 (백그라운드 실행)"""
    async def collect_task():
        async_db_manager = get_async_db_manager()
        async with async_db_manager.get_session() as db:
            await performance_manager.collect_system_metrics(db)
    
    background_tasks.add_task(collect_task)
    return {"message": "시스템 메트릭 수집이 백그라운드에서 시작되었습니다"}


@router.post("/collect-job-metric")
async def collect_job_metric(
    job_id: str,
    metric_type: MetricType,
    value: float,
    unit: Optional[str] = None,
    metadata: Optional[dict] = None,
    api_key: str = Depends(verify_api_key)
):
    """작업 메트릭 수집"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        await performance_manager.collect_job_metric(
            db, job_id, metric_type, value, unit, metadata
        )
    
    return {"message": f"작업 메트릭이 수집되었습니다: {job_id} - {metric_type.value}={value}"}


# 알럿 관련 엔드포인트
@router.get("/alerts", response_model=List[Alert])
async def get_active_alerts(
    api_key: str = Depends(verify_api_key)
):
    """활성 알럿 조회"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        return await performance_manager.get_active_alerts(db)


@router.post("/alerts/rules", response_model=AlertRuleResponse)
async def create_alert_rule(
    request: CreateAlertRuleRequest,
    api_key: str = Depends(verify_api_key)
):
    """알럿 규칙 생성"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        return await performance_manager.create_alert_rule(request, db)


@router.post("/alerts/check")
async def check_alerts(
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key)
):
    """알럿 확인 실행 (백그라운드)"""
    async def check_task():
        async_db_manager = get_async_db_manager()
        async with async_db_manager.get_session() as db:
            await performance_manager.check_alerts(db)
    
    background_tasks.add_task(check_task)
    return {"message": "알럿 확인이 백그라운드에서 시작되었습니다"}


@router.get("/alerts/rules")
async def get_alert_rules(
    api_key: str = Depends(verify_api_key)
):
    """알럿 규칙 목록 조회"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        # 알럿 규칙 조회 쿼리
        rules_query = await db.execute(
            "SELECT * FROM alert_rules ORDER BY created_at DESC"
        )
        
        rules = []
        for row in rules_query.fetchall():
            rules.append({
                "rule_id": str(row.rule_id),
                "name": row.name,
                "description": row.description,
                "metric_type": row.metric_type,
                "operator": row.operator,
                "threshold_value": row.threshold_value,
                "duration_minutes": row.duration_minutes,
                "alert_level": row.alert_level,
                "enabled": row.enabled,
                "notification_channels": row.notification_channels,
                "created_at": row.created_at,
                "updated_at": row.updated_at
            })
        
        return rules


@router.put("/alerts/rules/{rule_id}/enable")
async def enable_alert_rule(
    rule_id: str,
    enabled: bool = Query(..., description="활성화 여부"),
    api_key: str = Depends(verify_api_key)
):
    """알럿 규칙 활성화/비활성화"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        result = await db.execute(
            "UPDATE alert_rules SET enabled = :enabled, updated_at = :now WHERE rule_id = :rule_id",
            {"rule_id": rule_id, "enabled": enabled, "now": datetime.utcnow()}
        )
        
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="알럿 규칙을 찾을 수 없습니다")
        
        await db.commit()
        
        return {"message": f"알럿 규칙이 {'활성화' if enabled else '비활성화'}되었습니다"}


@router.delete("/alerts/rules/{rule_id}")
async def delete_alert_rule(
    rule_id: str,
    api_key: str = Depends(verify_api_key)
):
    """알럿 규칙 삭제"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        # 관련된 알럿들도 함께 삭제
        await db.execute(
            "DELETE FROM performance_alerts WHERE rule_id = :rule_id",
            {"rule_id": rule_id}
        )
        
        result = await db.execute(
            "DELETE FROM alert_rules WHERE rule_id = :rule_id",
            {"rule_id": rule_id}
        )
        
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="알럿 규칙을 찾을 수 없습니다")
        
        await db.commit()
        
        return {"message": "알럿 규칙이 삭제되었습니다"}


# 리포트 관련 엔드포인트
@router.post("/reports/generate")
async def generate_performance_report(
    request: GenerateReportRequest,
    background_tasks: BackgroundTasks,
    api_key: str = Depends(verify_api_key)
):
    """성능 리포트 생성 (백그라운드)"""
    async def generate_task():
        # 리포트 생성 로직 구현 예정
        pass
    
    background_tasks.add_task(generate_task)
    return {"message": f"성능 리포트 '{request.title}' 생성이 백그라운드에서 시작되었습니다"}


@router.get("/reports")
async def get_performance_reports(
    limit: int = Query(10, description="조회할 리포트 수", ge=1, le=100),
    api_key: str = Depends(verify_api_key)
):
    """성능 리포트 목록 조회"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        reports_query = await db.execute(
            """
            SELECT report_id, title, report_type, start_date, end_date, 
                   generated_by, generated_at, file_path, file_size_bytes
            FROM performance_reports 
            ORDER BY generated_at DESC 
            LIMIT :limit
            """,
            {"limit": limit}
        )
        
        reports = []
        for row in reports_query.fetchall():
            reports.append({
                "report_id": str(row.report_id),
                "title": row.title,
                "report_type": row.report_type,
                "start_date": row.start_date,
                "end_date": row.end_date,
                "generated_by": row.generated_by,
                "generated_at": row.generated_at,
                "file_path": row.file_path,
                "file_size_bytes": row.file_size_bytes
            })
        
        return reports


@router.get("/reports/{report_id}")
async def get_performance_report(
    report_id: str,
    api_key: str = Depends(verify_api_key)
):
    """성능 리포트 상세 조회"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        report_query = await db.execute(
            "SELECT * FROM performance_reports WHERE report_id = :report_id",
            {"report_id": report_id}
        )
        
        row = report_query.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="리포트를 찾을 수 없습니다")
        
        return {
            "report_id": str(row.report_id),
            "title": row.title,
            "report_type": row.report_type,
            "start_date": row.start_date,
            "end_date": row.end_date,
            "summary": row.summary,
            "job_type_analysis": row.job_type_analysis,
            "system_metrics_analysis": row.system_metrics_analysis,
            "recommendations": row.recommendations,
            "trends_analysis": row.trends_analysis,
            "generated_by": row.generated_by,
            "generated_at": row.generated_at,
            "file_path": row.file_path,
            "file_size_bytes": row.file_size_bytes
        }


# 클린업 관련 엔드포인트
@router.post("/cleanup/metrics")
async def cleanup_old_metrics(
    days: int = Query(30, description="삭제할 메트릭 데이터 기간 (일)", ge=1, le=365),
    api_key: str = Depends(verify_api_key)
):
    """오래된 메트릭 데이터 정리"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # 시스템 메트릭 정리
        system_result = await db.execute(
            "DELETE FROM system_performance_metrics WHERE measured_at < :cutoff_date",
            {"cutoff_date": cutoff_date}
        )
        
        # 작업 메트릭 정리
        job_result = await db.execute(
            "DELETE FROM batch_job_performance_metrics WHERE measured_at < :cutoff_date",
            {"cutoff_date": cutoff_date}
        )
        
        await db.commit()
        
        return {
            "message": f"{days}일 이전 메트릭 데이터가 정리되었습니다",
            "deleted_system_metrics": system_result.rowcount,
            "deleted_job_metrics": job_result.rowcount
        }


@router.post("/cleanup/alerts")
async def cleanup_old_alerts(
    days: int = Query(90, description="삭제할 알럿 데이터 기간 (일)", ge=1, le=365),
    api_key: str = Depends(verify_api_key)
):
    """오래된 알럿 데이터 정리"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as db:
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        # 해결된 알럿 정리
        result = await db.execute(
            """
            DELETE FROM performance_alerts 
            WHERE resolved_at IS NOT NULL AND resolved_at < :cutoff_date
            """,
            {"cutoff_date": cutoff_date}
        )
        
        await db.commit()
        
        return {
            "message": f"{days}일 이전 해결된 알럿이 정리되었습니다",
            "deleted_alerts": result.rowcount
        }


# 헬스체크
@router.get("/health")
async def performance_health_check():
    """성능 모니터링 시스템 헬스체크"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "service": "performance-monitoring",
        "version": "1.0.0"
    }