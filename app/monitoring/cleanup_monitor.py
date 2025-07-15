"""
데이터 정리 작업 모니터링 시스템

TTL 기반 정리 작업의 성능과 상태를 모니터링하고 알림을 제공합니다.
"""

import logging
import time
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from app.core.ttl_policy_engine import get_ttl_engine
from jobs.maintenance.ttl_cleanup_job import TTLCleanupJob

logger = logging.getLogger(__name__)


class AlertLevel(Enum):
    """알림 레벨"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class CleanupAlert:
    """정리 작업 알림"""
    level: AlertLevel
    title: str
    message: str
    timestamp: datetime
    metrics: Dict[str, Any]
    recommendations: List[str]


class CleanupMonitor:
    """데이터 정리 작업 모니터링"""
    
    def __init__(self):
        """모니터링 시스템 초기화"""
        self.ttl_engine = get_ttl_engine(dry_run=True)  # 모니터링용은 dry-run
        self.cleanup_job = TTLCleanupJob(dry_run=True)
        
        # 모니터링 설정
        self.alert_thresholds = {
            "storage_size_gb": 2.0,         # 2GB 이상 시 경고
            "old_data_ratio": 0.3,          # 30% 이상이 오래된 데이터 시 경고
            "large_file_ratio": 0.1,        # 10% 이상이 대용량 파일 시 경고
            "cleanup_failure_rate": 0.05,   # 5% 이상 정리 실패 시 경고
            "last_cleanup_hours": 48,       # 48시간 이상 정리 안 됨 시 경고
        }
        
        # 모니터링 통계
        self.monitor_stats = {
            "checks_performed": 0,
            "alerts_generated": 0,
            "last_check": None,
            "system_health_score": 100,
        }
        
        logger.info("데이터 정리 모니터링 시스템 초기화 완료")
    
    def perform_health_check(self) -> Dict[str, Any]:
        """
        정리 시스템 전체 상태 검사
        
        Returns:
            건강 상태 검사 결과
        """
        logger.info("정리 시스템 건강 상태 검사 시작")
        
        self.monitor_stats["checks_performed"] += 1
        self.monitor_stats["last_check"] = datetime.now()
        
        health_results = {
            "overall_health": "healthy",
            "health_score": 100,
            "checks": {},
            "alerts": [],
            "recommendations": [],
            "timestamp": datetime.now()
        }
        
        try:
            # 1. 스토리지 상태 검사
            storage_check = self._check_storage_health()
            health_results["checks"]["storage"] = storage_check
            
            # 2. 정리 작업 상태 검사
            cleanup_check = self._check_cleanup_health()
            health_results["checks"]["cleanup"] = cleanup_check
            
            # 3. 데이터 품질 검사
            data_quality_check = self._check_data_quality()
            health_results["checks"]["data_quality"] = data_quality_check
            
            # 4. 성능 검사
            performance_check = self._check_performance()
            health_results["checks"]["performance"] = performance_check
            
            # 5. 전체 건강 점수 계산
            health_score = self._calculate_health_score(health_results["checks"])
            health_results["health_score"] = health_score
            self.monitor_stats["system_health_score"] = health_score
            
            # 6. 전체 상태 결정
            if health_score >= 90:
                health_results["overall_health"] = "healthy"
            elif health_score >= 70:
                health_results["overall_health"] = "warning"
            elif health_score >= 50:
                health_results["overall_health"] = "unhealthy"
            else:
                health_results["overall_health"] = "critical"
            
            # 7. 알림 생성
            alerts = self._generate_alerts(health_results["checks"])
            health_results["alerts"] = alerts
            self.monitor_stats["alerts_generated"] += len(alerts)
            
            # 8. 권장사항 생성
            recommendations = self._generate_recommendations(health_results["checks"])
            health_results["recommendations"] = recommendations
            
            logger.info(f"건강 상태 검사 완료: {health_results['overall_health']} "
                       f"(점수: {health_score})")
            
            return health_results
            
        except Exception as e:
            logger.error(f"건강 상태 검사 오류: {e}")
            return {
                "overall_health": "error",
                "health_score": 0,
                "error": str(e),
                "timestamp": datetime.now()
            }
    
    def _check_storage_health(self) -> Dict[str, Any]:
        """스토리지 상태 검사"""
        try:
            storage_stats = self.ttl_engine.get_storage_usage_stats()
            overall = storage_stats.get('overall', {})
            cleanup_potential = storage_stats.get('cleanup_potential', {})
            
            total_size_mb = overall.get('total_size_mb', 0)
            total_records = overall.get('total_records', 0)
            old_records_90d = cleanup_potential.get('old_records_90d', 0)
            large_records_10mb = cleanup_potential.get('large_records_10mb', 0)
            
            # 상태 평가
            issues = []
            score = 100
            
            # 전체 크기 검사
            if total_size_mb > self.alert_thresholds["storage_size_gb"] * 1024:
                issues.append(f"스토리지 크기가 {self.alert_thresholds['storage_size_gb']}GB 초과")
                score -= 20
            
            # 오래된 데이터 비율 검사
            if total_records > 0:
                old_ratio = old_records_90d / total_records
                if old_ratio > self.alert_thresholds["old_data_ratio"]:
                    issues.append(f"90일 이상된 데이터가 {old_ratio*100:.1f}% (임계값: {self.alert_thresholds['old_data_ratio']*100}%)")
                    score -= 15
            
            # 대용량 파일 비율 검사
            if total_records > 0:
                large_ratio = large_records_10mb / total_records
                if large_ratio > self.alert_thresholds["large_file_ratio"]:
                    issues.append(f"대용량 파일(10MB+)이 {large_ratio*100:.1f}% (임계값: {self.alert_thresholds['large_file_ratio']*100}%)")
                    score -= 10
            
            return {
                "status": "healthy" if score >= 80 else "warning" if score >= 60 else "critical",
                "score": max(0, score),
                "total_size_mb": total_size_mb,
                "total_records": total_records,
                "old_records_ratio": old_records_90d / max(total_records, 1),
                "large_files_ratio": large_records_10mb / max(total_records, 1),
                "issues": issues
            }
            
        except Exception as e:
            logger.error(f"스토리지 상태 검사 오류: {e}")
            return {
                "status": "error",
                "score": 0,
                "error": str(e)
            }
    
    def _check_cleanup_health(self) -> Dict[str, Any]:
        """정리 작업 상태 검사"""
        try:
            ttl_stats = self.ttl_engine.get_statistics()
            job_stats = self.cleanup_job.get_job_statistics()
            
            issues = []
            score = 100
            
            # 마지막 정리 작업 시간 검사
            last_cleanup = ttl_stats.get("last_cleanup")
            if last_cleanup:
                hours_since_cleanup = (datetime.now() - last_cleanup).total_seconds() / 3600
                if hours_since_cleanup > self.alert_thresholds["last_cleanup_hours"]:
                    issues.append(f"마지막 정리 작업이 {hours_since_cleanup:.1f}시간 전")
                    score -= 30
            else:
                issues.append("정리 작업 이력이 없음")
                score -= 50
            
            # 정리 작업 성공률 검사
            total_runs = ttl_stats.get("cleanup_runs", 0)
            if total_runs > 0:
                # 실패율 계산 (예시 - 실제로는 더 정교한 계산 필요)
                failure_rate = 0.0  # 현재는 실패 추적이 없어서 0으로 설정
                if failure_rate > self.alert_thresholds["cleanup_failure_rate"]:
                    issues.append(f"정리 작업 실패율이 {failure_rate*100:.1f}% (임계값: {self.alert_thresholds['cleanup_failure_rate']*100}%)")
                    score -= 25
            
            return {
                "status": "healthy" if score >= 80 else "warning" if score >= 60 else "critical",
                "score": max(0, score),
                "total_runs": total_runs,
                "total_deleted": ttl_stats.get("total_deleted", 0),
                "total_space_freed_mb": ttl_stats.get("total_space_freed_mb", 0),
                "hours_since_last_cleanup": hours_since_cleanup if last_cleanup else None,
                "issues": issues
            }
            
        except Exception as e:
            logger.error(f"정리 작업 상태 검사 오류: {e}")
            return {
                "status": "error",
                "score": 0,
                "error": str(e)
            }
    
    def _check_data_quality(self) -> Dict[str, Any]:
        """데이터 품질 검사"""
        try:
            # 데이터 무결성 및 품질 검사
            quality_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN raw_response IS NULL THEN 1 END) as null_responses,
                COUNT(CASE WHEN response_size IS NULL OR response_size = 0 THEN 1 END) as zero_size,
                COUNT(CASE WHEN created_at IS NULL THEN 1 END) as null_dates,
                COUNT(CASE WHEN api_provider IS NULL OR api_provider = '' THEN 1 END) as null_providers
            FROM api_raw_data
            """
            
            result = self.ttl_engine.db_manager.fetch_one(quality_query)
            
            issues = []
            score = 100
            
            if result:
                total = result['total_records']
                
                # NULL 응답 검사
                null_response_ratio = result['null_responses'] / max(total, 1)
                if null_response_ratio > 0.01:  # 1% 이상
                    issues.append(f"NULL 응답이 {null_response_ratio*100:.1f}%")
                    score -= 15
                
                # 크기 0 검사
                zero_size_ratio = result['zero_size'] / max(total, 1)
                if zero_size_ratio > 0.05:  # 5% 이상
                    issues.append(f"크기가 0인 응답이 {zero_size_ratio*100:.1f}%")
                    score -= 10
                
                # NULL 날짜 검사
                null_date_ratio = result['null_dates'] / max(total, 1)
                if null_date_ratio > 0:
                    issues.append(f"NULL 생성일이 {null_date_ratio*100:.1f}%")
                    score -= 20
                
                # NULL 제공자 검사
                null_provider_ratio = result['null_providers'] / max(total, 1)
                if null_provider_ratio > 0:
                    issues.append(f"NULL 제공자가 {null_provider_ratio*100:.1f}%")
                    score -= 25
            
            return {
                "status": "healthy" if score >= 90 else "warning" if score >= 70 else "critical",
                "score": max(0, score),
                "quality_metrics": dict(result) if result else {},
                "issues": issues
            }
            
        except Exception as e:
            logger.error(f"데이터 품질 검사 오류: {e}")
            return {
                "status": "error",
                "score": 0,
                "error": str(e)
            }
    
    def _check_performance(self) -> Dict[str, Any]:
        """성능 검사"""
        try:
            # 쿼리 성능 테스트
            performance_queries = [
                {
                    "name": "count_query",
                    "query": "SELECT COUNT(*) FROM api_raw_data",
                    "max_time_ms": 100
                },
                {
                    "name": "recent_data_query", 
                    "query": "SELECT COUNT(*) FROM api_raw_data WHERE created_at >= NOW() - INTERVAL '1 day'",
                    "max_time_ms": 200
                },
                {
                    "name": "provider_query",
                    "query": "SELECT api_provider, COUNT(*) FROM api_raw_data GROUP BY api_provider",
                    "max_time_ms": 300
                }
            ]
            
            performance_results = []
            issues = []
            score = 100
            
            for test in performance_queries:
                start_time = time.time()
                try:
                    self.ttl_engine.db_manager.fetch_all(test["query"])
                    execution_time_ms = (time.time() - start_time) * 1000
                    
                    performance_results.append({
                        "query": test["name"],
                        "execution_time_ms": execution_time_ms,
                        "passed": execution_time_ms <= test["max_time_ms"]
                    })
                    
                    if execution_time_ms > test["max_time_ms"]:
                        issues.append(f"{test['name']} 쿼리가 {execution_time_ms:.1f}ms 소요 (제한: {test['max_time_ms']}ms)")
                        score -= 15
                        
                except Exception as e:
                    performance_results.append({
                        "query": test["name"],
                        "execution_time_ms": -1,
                        "passed": False,
                        "error": str(e)
                    })
                    issues.append(f"{test['name']} 쿼리 실행 실패: {str(e)[:50]}")
                    score -= 30
            
            return {
                "status": "healthy" if score >= 80 else "warning" if score >= 60 else "critical",
                "score": max(0, score),
                "query_performance": performance_results,
                "issues": issues
            }
            
        except Exception as e:
            logger.error(f"성능 검사 오류: {e}")
            return {
                "status": "error",
                "score": 0,
                "error": str(e)
            }
    
    def _calculate_health_score(self, checks: Dict[str, Any]) -> int:
        """전체 건강 점수 계산"""
        weights = {
            "storage": 0.3,
            "cleanup": 0.3, 
            "data_quality": 0.2,
            "performance": 0.2
        }
        
        total_score = 0
        total_weight = 0
        
        for check_name, weight in weights.items():
            check_result = checks.get(check_name, {})
            if "score" in check_result:
                total_score += check_result["score"] * weight
                total_weight += weight
        
        return int(total_score / max(total_weight, 1))
    
    def _generate_alerts(self, checks: Dict[str, Any]) -> List[CleanupAlert]:
        """상태 검사 결과에 따른 알림 생성"""
        alerts = []
        
        for check_name, check_result in checks.items():
            status = check_result.get("status", "unknown")
            score = check_result.get("score", 0)
            issues = check_result.get("issues", [])
            
            if status == "critical" or score < 50:
                alert = CleanupAlert(
                    level=AlertLevel.CRITICAL,
                    title=f"{check_name.upper()} 시스템 위험",
                    message=f"{check_name} 검사에서 심각한 문제 발견 (점수: {score})",
                    timestamp=datetime.now(),
                    metrics=check_result,
                    recommendations=[f"{check_name} 시스템 즉시 점검 필요"] + issues[:3]
                )
                alerts.append(alert)
                
            elif status == "warning" or score < 80:
                alert = CleanupAlert(
                    level=AlertLevel.WARNING,
                    title=f"{check_name.upper()} 시스템 경고",
                    message=f"{check_name} 검사에서 주의 필요 (점수: {score})",
                    timestamp=datetime.now(),
                    metrics=check_result,
                    recommendations=[f"{check_name} 시스템 점검 권장"] + issues[:2]
                )
                alerts.append(alert)
        
        return alerts
    
    def _generate_recommendations(self, checks: Dict[str, Any]) -> List[str]:
        """전체 권장사항 생성"""
        recommendations = []
        
        # 스토리지 관련 권장사항
        storage_check = checks.get("storage", {})
        if storage_check.get("score", 100) < 80:
            recommendations.append("정기적인 데이터 정리 작업 실행")
            if storage_check.get("old_records_ratio", 0) > 0.2:
                recommendations.append("90일 이상된 데이터 우선 정리")
            if storage_check.get("large_files_ratio", 0) > 0.05:
                recommendations.append("대용량 파일 정리 검토")
        
        # 정리 작업 관련 권장사항  
        cleanup_check = checks.get("cleanup", {})
        if cleanup_check.get("score", 100) < 80:
            recommendations.append("정리 작업 스케줄 및 설정 점검")
            hours_since = cleanup_check.get("hours_since_last_cleanup")
            if hours_since and hours_since > 24:
                recommendations.append("수동 정리 작업 실행 고려")
        
        # 데이터 품질 관련 권장사항
        quality_check = checks.get("data_quality", {})
        if quality_check.get("score", 100) < 90:
            recommendations.append("데이터 품질 문제 조사 및 수정")
            recommendations.append("API 데이터 수집 프로세스 점검")
        
        # 성능 관련 권장사항
        performance_check = checks.get("performance", {})
        if performance_check.get("score", 100) < 80:
            recommendations.append("데이터베이스 인덱스 최적화")
            recommendations.append("쿼리 성능 튜닝 검토")
        
        return recommendations
    
    def get_monitoring_statistics(self) -> Dict[str, Any]:
        """모니터링 통계 반환"""
        return {
            **self.monitor_stats,
            "alert_thresholds": self.alert_thresholds
        }
    
    def update_alert_thresholds(self, new_thresholds: Dict[str, float]):
        """알림 임계값 업데이트"""
        self.alert_thresholds.update(new_thresholds)
        logger.info(f"알림 임계값 업데이트: {new_thresholds}")


# 전역 정리 모니터 인스턴스
_cleanup_monitor: Optional[CleanupMonitor] = None


def get_cleanup_monitor() -> CleanupMonitor:
    """전역 정리 모니터 인스턴스 반환 (싱글톤)"""
    global _cleanup_monitor
    
    if _cleanup_monitor is None:
        _cleanup_monitor = CleanupMonitor()
    
    return _cleanup_monitor