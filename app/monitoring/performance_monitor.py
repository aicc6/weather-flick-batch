"""
성능 모니터링 시스템

API 저장 시스템의 성능을 실시간으로 모니터링하고 측정하는 모듈입니다.
"""

import logging
import time
import threading
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import deque, defaultdict
from enum import Enum
import statistics

from app.core.selective_storage_manager import get_storage_manager
from app.core.ttl_policy_engine import get_ttl_engine
from app.monitoring.cleanup_monitor import get_cleanup_monitor

logger = logging.getLogger(__name__)


class PerformanceMetricType(Enum):
    """성능 메트릭 타입"""
    RESPONSE_TIME = "response_time"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"
    STORAGE_EFFICIENCY = "storage_efficiency"
    MEMORY_USAGE = "memory_usage"
    CPU_USAGE = "cpu_usage"


@dataclass
class PerformanceMetric:
    """성능 메트릭 데이터"""
    metric_type: PerformanceMetricType
    value: float
    timestamp: datetime
    source: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PerformanceAlert:
    """성능 알림"""
    metric_type: PerformanceMetricType
    severity: str  # critical, warning, info
    message: str
    value: float
    threshold: float
    timestamp: datetime
    source: str


class PerformanceMonitor:
    """성능 모니터링 시스템"""
    
    def __init__(self, max_history_size: int = 1000):
        """
        성능 모니터 초기화
        
        Args:
            max_history_size: 메트릭 히스토리 최대 크기
        """
        self.max_history_size = max_history_size
        
        # 의존성 주입
        self.storage_manager = get_storage_manager()
        self.ttl_engine = get_ttl_engine(dry_run=True)
        self.cleanup_monitor = get_cleanup_monitor()
        
        # 메트릭 저장소
        self.metrics_history: Dict[PerformanceMetricType, deque] = {
            metric_type: deque(maxlen=max_history_size)
            for metric_type in PerformanceMetricType
        }
        
        # 성능 임계값 설정
        self.thresholds = {
            PerformanceMetricType.RESPONSE_TIME: {
                "warning": 5000.0,    # 5초
                "critical": 10000.0   # 10초
            },
            PerformanceMetricType.THROUGHPUT: {
                "warning": 10.0,      # 10 requests/sec 이하
                "critical": 5.0       # 5 requests/sec 이하
            },
            PerformanceMetricType.ERROR_RATE: {
                "warning": 5.0,       # 5% 이상
                "critical": 10.0      # 10% 이상
            },
            PerformanceMetricType.STORAGE_EFFICIENCY: {
                "warning": 70.0,      # 70% 이하
                "critical": 50.0      # 50% 이하
            },
            PerformanceMetricType.MEMORY_USAGE: {
                "warning": 80.0,      # 80% 이상
                "critical": 90.0      # 90% 이상
            },
            PerformanceMetricType.CPU_USAGE: {
                "warning": 80.0,      # 80% 이상
                "critical": 90.0      # 90% 이상
            }
        }
        
        # 모니터링 상태
        self.monitoring_active = False
        self.monitoring_thread: Optional[threading.Thread] = None
        self.monitoring_interval = 30  # 30초마다 체크
        
        # 알림 히스토리
        self.alerts_history: deque = deque(maxlen=100)
        
        # 성능 통계
        self.performance_stats = {
            "monitoring_start_time": None,
            "total_metrics_collected": 0,
            "total_alerts_generated": 0,
            "last_health_check": None
        }
        
        logger.info("성능 모니터링 시스템 초기화 완료")
    
    def start_monitoring(self):
        """모니터링 시작"""
        if self.monitoring_active:
            logger.warning("모니터링이 이미 활성화되어 있습니다")
            return
        
        self.monitoring_active = True
        self.performance_stats["monitoring_start_time"] = datetime.now()
        
        self.monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True
        )
        self.monitoring_thread.start()
        
        logger.info("성능 모니터링 시작")
    
    def stop_monitoring(self):
        """모니터링 중지"""
        self.monitoring_active = False
        
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5)
        
        logger.info("성능 모니터링 중지")
    
    def _monitoring_loop(self):
        """모니터링 루프"""
        while self.monitoring_active:
            try:
                self._collect_performance_metrics()
                time.sleep(self.monitoring_interval)
            except Exception as e:
                logger.error(f"모니터링 루프 오류: {e}")
                time.sleep(self.monitoring_interval)
    
    def _collect_performance_metrics(self):
        """성능 메트릭 수집"""
        try:
            # 1. 저장 시스템 성능 메트릭
            self._collect_storage_metrics()
            
            # 2. TTL 엔진 성능 메트릭
            self._collect_ttl_metrics()
            
            # 3. 시스템 리소스 메트릭
            self._collect_system_metrics()
            
            # 4. 전체 건강 상태 체크
            self._perform_health_check()
            
            self.performance_stats["last_health_check"] = datetime.now()
            
        except Exception as e:
            logger.error(f"성능 메트릭 수집 실패: {e}")
    
    def _collect_storage_metrics(self):
        """저장 시스템 성능 메트릭 수집"""
        try:
            storage_stats = self.storage_manager.get_statistics()
            
            # 평균 응답 시간
            avg_decision_time = storage_stats.get("avg_decision_time_ms", 0)
            self._add_metric(
                PerformanceMetricType.RESPONSE_TIME,
                avg_decision_time,
                "storage_manager",
                {"metric": "decision_time"}
            )
            
            # 처리량 (requests per second)
            total_requests = storage_stats.get("total_requests", 0)
            if hasattr(self, '_last_storage_check'):
                time_diff = (datetime.now() - self._last_storage_check).total_seconds()
                if time_diff > 0:
                    requests_diff = total_requests - getattr(self, '_last_total_requests', 0)
                    throughput = requests_diff / time_diff
                    self._add_metric(
                        PerformanceMetricType.THROUGHPUT,
                        throughput,
                        "storage_manager",
                        {"metric": "requests_per_second"}
                    )
            
            # 저장 성공률
            success_rate = storage_stats.get("storage_success_rate", 100)
            error_rate = 100 - success_rate
            self._add_metric(
                PerformanceMetricType.ERROR_RATE,
                error_rate,
                "storage_manager",
                {"metric": "storage_error_rate"}
            )
            
            # 저장 효율성 (저장 결정 비율)
            decision_rate = storage_stats.get("decision_rate", 100)
            self._add_metric(
                PerformanceMetricType.STORAGE_EFFICIENCY,
                decision_rate,
                "storage_manager",
                {"metric": "decision_efficiency"}
            )
            
            # 마지막 체크 시간 및 요청 수 저장
            self._last_storage_check = datetime.now()
            self._last_total_requests = total_requests
            
        except Exception as e:
            logger.error(f"저장 시스템 메트릭 수집 실패: {e}")
    
    def _collect_ttl_metrics(self):
        """TTL 엔진 성능 메트릭 수집"""
        try:
            ttl_stats = self.ttl_engine.get_statistics()
            
            # TTL 엔진 응답 시간
            avg_execution_time = ttl_stats.get("avg_execution_time_sec", 0) * 1000  # ms로 변환
            self._add_metric(
                PerformanceMetricType.RESPONSE_TIME,
                avg_execution_time,
                "ttl_engine",
                {"metric": "cleanup_execution_time"}
            )
            
            # 정리 효율성 (삭제된 레코드 수)
            total_deleted = ttl_stats.get("total_deleted", 0)
            cleanup_runs = ttl_stats.get("cleanup_runs", 1)
            cleanup_efficiency = total_deleted / max(cleanup_runs, 1)
            self._add_metric(
                PerformanceMetricType.STORAGE_EFFICIENCY,
                cleanup_efficiency,
                "ttl_engine",
                {"metric": "cleanup_efficiency", "unit": "records_per_run"}
            )
            
        except Exception as e:
            logger.error(f"TTL 엔진 메트릭 수집 실패: {e}")
    
    def _collect_system_metrics(self):
        """시스템 리소스 메트릭 수집"""
        try:
            import psutil
            
            # CPU 사용률
            cpu_percent = psutil.cpu_percent(interval=1)
            self._add_metric(
                PerformanceMetricType.CPU_USAGE,
                cpu_percent,
                "system",
                {"metric": "cpu_percent"}
            )
            
            # 메모리 사용률
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            self._add_metric(
                PerformanceMetricType.MEMORY_USAGE,
                memory_percent,
                "system",
                {"metric": "memory_percent", "available_gb": memory.available / (1024**3)}
            )
            
        except ImportError:
            logger.warning("psutil이 설치되지 않아 시스템 메트릭을 수집할 수 없습니다")
        except Exception as e:
            logger.error(f"시스템 메트릭 수집 실패: {e}")
    
    def _perform_health_check(self):
        """전체 시스템 건강 상태 체크"""
        try:
            health_result = self.cleanup_monitor.perform_health_check()
            
            # 전체 건강 점수를 저장 효율성 메트릭으로 기록
            health_score = health_result.get("health_score", 100)
            self._add_metric(
                PerformanceMetricType.STORAGE_EFFICIENCY,
                health_score,
                "health_monitor",
                {"metric": "overall_health_score"}
            )
            
            # 알림 생성 확인
            alerts = health_result.get("alerts", [])
            for alert in alerts:
                self._generate_alert_from_health_check(alert)
                
        except Exception as e:
            logger.error(f"건강 상태 체크 실패: {e}")
    
    def _add_metric(self, metric_type: PerformanceMetricType, value: float, 
                   source: str, metadata: Dict[str, Any]):
        """메트릭 추가 및 임계값 체크"""
        metric = PerformanceMetric(
            metric_type=metric_type,
            value=value,
            timestamp=datetime.now(),
            source=source,
            metadata=metadata
        )
        
        self.metrics_history[metric_type].append(metric)
        self.performance_stats["total_metrics_collected"] += 1
        
        # 임계값 체크 및 알림 생성
        self._check_thresholds(metric)
    
    def _check_thresholds(self, metric: PerformanceMetric):
        """임계값 체크 및 알림 생성"""
        thresholds = self.thresholds.get(metric.metric_type, {})
        
        critical_threshold = thresholds.get("critical")
        warning_threshold = thresholds.get("warning")
        
        alert = None
        
        # 메트릭 타입에 따른 임계값 비교 로직
        if metric.metric_type in [PerformanceMetricType.ERROR_RATE, 
                                PerformanceMetricType.MEMORY_USAGE, 
                                PerformanceMetricType.CPU_USAGE,
                                PerformanceMetricType.RESPONSE_TIME]:
            # 높을수록 나쁨
            if critical_threshold and metric.value >= critical_threshold:
                alert = PerformanceAlert(
                    metric_type=metric.metric_type,
                    severity="critical",
                    message=f"{metric.metric_type.value} 위험 수준: {metric.value:.2f} (임계값: {critical_threshold})",
                    value=metric.value,
                    threshold=critical_threshold,
                    timestamp=metric.timestamp,
                    source=metric.source
                )
            elif warning_threshold and metric.value >= warning_threshold:
                alert = PerformanceAlert(
                    metric_type=metric.metric_type,
                    severity="warning",
                    message=f"{metric.metric_type.value} 경고 수준: {metric.value:.2f} (임계값: {warning_threshold})",
                    value=metric.value,
                    threshold=warning_threshold,
                    timestamp=metric.timestamp,
                    source=metric.source
                )
        
        elif metric.metric_type in [PerformanceMetricType.THROUGHPUT,
                                  PerformanceMetricType.STORAGE_EFFICIENCY]:
            # 낮을수록 나쁨
            if critical_threshold and metric.value <= critical_threshold:
                alert = PerformanceAlert(
                    metric_type=metric.metric_type,
                    severity="critical",
                    message=f"{metric.metric_type.value} 위험 수준: {metric.value:.2f} (임계값: {critical_threshold} 이하)",
                    value=metric.value,
                    threshold=critical_threshold,
                    timestamp=metric.timestamp,
                    source=metric.source
                )
            elif warning_threshold and metric.value <= warning_threshold:
                alert = PerformanceAlert(
                    metric_type=metric.metric_type,
                    severity="warning",
                    message=f"{metric.metric_type.value} 경고 수준: {metric.value:.2f} (임계값: {warning_threshold} 이하)",
                    value=metric.value,
                    threshold=warning_threshold,
                    timestamp=metric.timestamp,
                    source=metric.source
                )
        
        if alert:
            self._handle_alert(alert)
    
    def _generate_alert_from_health_check(self, health_alert):
        """건강 상태 체크에서 알림 생성"""
        try:
            severity_map = {
                "INFO": "info",
                "WARNING": "warning", 
                "ERROR": "warning",
                "CRITICAL": "critical"
            }
            
            alert = PerformanceAlert(
                metric_type=PerformanceMetricType.STORAGE_EFFICIENCY,
                severity=severity_map.get(health_alert.level.value, "info"),
                message=health_alert.message,
                value=0.0,
                threshold=0.0,
                timestamp=health_alert.timestamp,
                source="health_monitor"
            )
            
            self._handle_alert(alert)
            
        except Exception as e:
            logger.error(f"건강 상태 알림 생성 실패: {e}")
    
    def _handle_alert(self, alert: PerformanceAlert):
        """알림 처리"""
        self.alerts_history.append(alert)
        self.performance_stats["total_alerts_generated"] += 1
        
        # 로그 출력
        log_level = {
            "info": logging.INFO,
            "warning": logging.WARNING,
            "critical": logging.ERROR
        }.get(alert.severity, logging.INFO)
        
        logger.log(log_level, f"🚨 성능 알림 ({alert.severity.upper()}): {alert.message}")
    
    def get_performance_summary(self, time_window_minutes: int = 60) -> Dict[str, Any]:
        """성능 요약 정보 반환"""
        cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
        
        summary = {
            "time_window_minutes": time_window_minutes,
            "summary_generated_at": datetime.now(),
            "metrics_summary": {},
            "recent_alerts": [],
            "overall_health": "healthy"
        }
        
        # 각 메트릭 타입별 요약
        for metric_type in PerformanceMetricType:
            recent_metrics = [
                m for m in self.metrics_history[metric_type]
                if m.timestamp >= cutoff_time
            ]
            
            if recent_metrics:
                values = [m.value for m in recent_metrics]
                summary["metrics_summary"][metric_type.value] = {
                    "count": len(values),
                    "avg": statistics.mean(values),
                    "min": min(values),
                    "max": max(values),
                    "latest": values[-1],
                    "trend": self._calculate_trend(values)
                }
        
        # 최근 알림
        recent_alerts = [
            {
                "severity": alert.severity,
                "message": alert.message,
                "timestamp": alert.timestamp.isoformat(),
                "source": alert.source
            }
            for alert in self.alerts_history
            if alert.timestamp >= cutoff_time
        ]
        summary["recent_alerts"] = recent_alerts
        
        # 전체 건강 상태 평가
        critical_alerts = [a for a in recent_alerts if a["severity"] == "critical"]
        warning_alerts = [a for a in recent_alerts if a["severity"] == "warning"]
        
        if critical_alerts:
            summary["overall_health"] = "critical"
        elif warning_alerts:
            summary["overall_health"] = "warning"
        elif recent_alerts:
            summary["overall_health"] = "attention_needed"
        
        return summary
    
    def _calculate_trend(self, values: List[float]) -> str:
        """값들의 트렌드 계산"""
        if len(values) < 2:
            return "stable"
        
        # 단순 선형 회귀로 트렌드 계산
        n = len(values)
        x = list(range(n))
        
        x_mean = statistics.mean(x)
        y_mean = statistics.mean(values)
        
        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
        
        if denominator == 0:
            return "stable"
        
        slope = numerator / denominator
        
        if slope > 0.1:
            return "increasing"
        elif slope < -0.1:
            return "decreasing"
        else:
            return "stable"
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """현재 메트릭 값들 반환"""
        current_metrics = {}
        
        for metric_type in PerformanceMetricType:
            if self.metrics_history[metric_type]:
                latest_metric = self.metrics_history[metric_type][-1]
                current_metrics[metric_type.value] = latest_metric.value
                
                # 특별한 이름으로 매핑
                if metric_type == PerformanceMetricType.MEMORY_USAGE:
                    current_metrics["memory_usage_mb"] = latest_metric.value
                elif metric_type == PerformanceMetricType.CPU_USAGE:
                    current_metrics["cpu_usage_percent"] = latest_metric.value
        
        # 활성 연결 수 추가 (예시)
        current_metrics["active_connections"] = len(current_metrics)
        
        return current_metrics
    
    def generate_report(self, output_dir: str = "logs") -> str:
        """성능 보고서 생성"""
        import os
        import json
        
        os.makedirs(output_dir, exist_ok=True)
        
        report_data = {
            "generated_at": datetime.now().isoformat(),
            "performance_summary": self.get_performance_summary(),
            "monitoring_statistics": self.get_monitoring_statistics(),
            "current_metrics": self.get_current_metrics()
        }
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = os.path.join(output_dir, f"performance_report_{timestamp}.json")
        
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, indent=2, default=str, ensure_ascii=False)
        
        return report_file
    
    def get_monitoring_statistics(self) -> Dict[str, Any]:
        """모니터링 시스템 통계 반환"""
        uptime = None
        if self.performance_stats["monitoring_start_time"]:
            uptime = (datetime.now() - self.performance_stats["monitoring_start_time"]).total_seconds()
        
        return {
            **self.performance_stats,
            "monitoring_active": self.monitoring_active,
            "monitoring_uptime_seconds": uptime,
            "monitoring_interval_seconds": self.monitoring_interval,
            "metrics_history_size": {
                metric_type.value: len(self.metrics_history[metric_type])
                for metric_type in PerformanceMetricType
            },
            "alerts_history_size": len(self.alerts_history)
        }
    
    def update_thresholds(self, new_thresholds: Dict[str, Dict[str, float]]):
        """임계값 업데이트"""
        for metric_name, thresholds in new_thresholds.items():
            try:
                metric_type = PerformanceMetricType(metric_name)
                self.thresholds[metric_type].update(thresholds)
                logger.info(f"임계값 업데이트: {metric_name} -> {thresholds}")
            except ValueError:
                logger.warning(f"알 수 없는 메트릭 타입: {metric_name}")


# 전역 성능 모니터 인스턴스
_performance_monitor: Optional[PerformanceMonitor] = None


def get_performance_monitor() -> PerformanceMonitor:
    """전역 성능 모니터 인스턴스 반환 (싱글톤)"""
    global _performance_monitor
    
    if _performance_monitor is None:
        _performance_monitor = PerformanceMonitor()
    
    return _performance_monitor


def reset_performance_monitor():
    """성능 모니터 인스턴스 재설정 (테스트용)"""
    global _performance_monitor
    
    if _performance_monitor and _performance_monitor.monitoring_active:
        _performance_monitor.stop_monitoring()
    
    _performance_monitor = None