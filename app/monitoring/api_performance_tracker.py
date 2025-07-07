"""
API 성능 추적 시스템

개별 API 호출의 성능을 추적하고 분석하는 모듈입니다.
"""

import logging
import time
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum
import statistics
import threading

from app.core.multi_api_key_manager import APIProvider

logger = logging.getLogger(__name__)


class APICallStatus(Enum):
    """API 호출 상태"""
    SUCCESS = "success"
    ERROR = "error"
    TIMEOUT = "timeout"
    RATE_LIMITED = "rate_limited"
    AUTH_FAILED = "auth_failed"


@dataclass
class APICallRecord:
    """API 호출 기록"""
    call_id: str
    provider: str
    endpoint: str
    timestamp: datetime
    duration_ms: float
    status: APICallStatus
    status_code: Optional[int] = None
    response_size_bytes: int = 0
    error_message: Optional[str] = None
    request_params: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class APIEndpointStats:
    """API 엔드포인트 통계"""
    provider: str
    endpoint: str
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    avg_response_time_ms: float = 0.0
    min_response_time_ms: float = float('inf')
    max_response_time_ms: float = 0.0
    total_response_size_bytes: int = 0
    error_rate: float = 0.0
    last_called: Optional[datetime] = None
    recent_errors: List[str] = field(default_factory=list)


class APIPerformanceTracker:
    """API 성능 추적기"""
    
    def __init__(self, max_records_per_endpoint: int = 1000):
        """
        API 성능 추적기 초기화
        
        Args:
            max_records_per_endpoint: 엔드포인트당 최대 기록 수
        """
        self.max_records_per_endpoint = max_records_per_endpoint
        
        # API 호출 기록 저장소
        self.call_records: Dict[str, deque] = defaultdict(
            lambda: deque(maxlen=max_records_per_endpoint)
        )
        
        # 엔드포인트별 통계
        self.endpoint_stats: Dict[str, APIEndpointStats] = {}
        
        # 제공자별 통계
        self.provider_stats: Dict[str, Dict[str, Any]] = defaultdict(
            lambda: {
                "total_calls": 0,
                "successful_calls": 0,
                "failed_calls": 0,
                "avg_response_time_ms": 0.0,
                "error_rate": 0.0,
                "last_called": None,
                "active_endpoints": set()
            }
        )
        
        # 실시간 성능 메트릭
        self.real_time_metrics = {
            "current_rps": 0.0,  # requests per second
            "avg_response_time_1min": 0.0,
            "error_rate_1min": 0.0,
            "active_calls": 0,
            "last_metric_update": datetime.now()
        }
        
        # 호출 시간 윈도우 (1분)
        self.call_timestamps: deque = deque(maxlen=1000)
        
        # 스레드 안전성을 위한 락
        self.lock = threading.RLock()
        
        # 추적 시작 시간
        self.tracking_start_time = datetime.now()
        
        logger.info("API 성능 추적기 초기화 완료")
    
    def record_api_call(self, call_record: APICallRecord):
        """API 호출 기록"""
        with self.lock:
            endpoint_key = f"{call_record.provider}_{call_record.endpoint}"
            
            # 호출 기록 저장
            self.call_records[endpoint_key].append(call_record)
            
            # 호출 시간 기록 (실시간 메트릭용)
            self.call_timestamps.append(call_record.timestamp)
            
            # 엔드포인트 통계 업데이트
            self._update_endpoint_stats(call_record)
            
            # 제공자 통계 업데이트
            self._update_provider_stats(call_record)
            
            # 실시간 메트릭 업데이트
            self._update_real_time_metrics()
    
    def _update_endpoint_stats(self, call_record: APICallRecord):
        """엔드포인트 통계 업데이트"""
        endpoint_key = f"{call_record.provider}_{call_record.endpoint}"
        
        if endpoint_key not in self.endpoint_stats:
            self.endpoint_stats[endpoint_key] = APIEndpointStats(
                provider=call_record.provider,
                endpoint=call_record.endpoint
            )
        
        stats = self.endpoint_stats[endpoint_key]
        
        # 기본 통계 업데이트
        stats.total_calls += 1
        stats.last_called = call_record.timestamp
        
        if call_record.status == APICallStatus.SUCCESS:
            stats.successful_calls += 1
        else:
            stats.failed_calls += 1
            
            # 최근 에러 기록 (최대 10개)
            if call_record.error_message:
                stats.recent_errors.append(call_record.error_message)
                if len(stats.recent_errors) > 10:
                    stats.recent_errors.pop(0)
        
        # 응답 시간 통계
        stats.min_response_time_ms = min(stats.min_response_time_ms, call_record.duration_ms)
        stats.max_response_time_ms = max(stats.max_response_time_ms, call_record.duration_ms)
        
        # 평균 응답 시간 계산 (가중 평균)
        if stats.total_calls == 1:
            stats.avg_response_time_ms = call_record.duration_ms
        else:
            # 지수 이동 평균 사용
            alpha = 0.1  # 평활화 계수
            stats.avg_response_time_ms = (
                alpha * call_record.duration_ms + 
                (1 - alpha) * stats.avg_response_time_ms
            )
        
        # 응답 크기 통계
        stats.total_response_size_bytes += call_record.response_size_bytes
        
        # 에러율 계산
        stats.error_rate = (stats.failed_calls / stats.total_calls) * 100
    
    def _update_provider_stats(self, call_record: APICallRecord):
        """제공자 통계 업데이트"""
        provider_stat = self.provider_stats[call_record.provider]
        
        provider_stat["total_calls"] += 1
        provider_stat["last_called"] = call_record.timestamp
        provider_stat["active_endpoints"].add(call_record.endpoint)
        
        if call_record.status == APICallStatus.SUCCESS:
            provider_stat["successful_calls"] += 1
        else:
            provider_stat["failed_calls"] += 1
        
        # 평균 응답 시간 업데이트
        if provider_stat["total_calls"] == 1:
            provider_stat["avg_response_time_ms"] = call_record.duration_ms
        else:
            alpha = 0.1
            provider_stat["avg_response_time_ms"] = (
                alpha * call_record.duration_ms + 
                (1 - alpha) * provider_stat["avg_response_time_ms"]
            )
        
        # 에러율 계산
        provider_stat["error_rate"] = (
            provider_stat["failed_calls"] / provider_stat["total_calls"]
        ) * 100
    
    def _update_real_time_metrics(self):
        """실시간 메트릭 업데이트"""
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        
        # 1분 내 호출 수 계산
        recent_calls = [
            ts for ts in self.call_timestamps 
            if ts >= one_minute_ago
        ]
        
        # RPS 계산
        self.real_time_metrics["current_rps"] = len(recent_calls) / 60.0
        
        # 1분 내 호출들의 평균 응답시간과 에러율
        if recent_calls:
            recent_records = []
            for endpoint_key, records in self.call_records.items():
                recent_records.extend([
                    record for record in records 
                    if record.timestamp >= one_minute_ago
                ])
            
            if recent_records:
                # 평균 응답시간
                response_times = [r.duration_ms for r in recent_records]
                self.real_time_metrics["avg_response_time_1min"] = statistics.mean(response_times)
                
                # 에러율
                error_count = sum(1 for r in recent_records if r.status != APICallStatus.SUCCESS)
                self.real_time_metrics["error_rate_1min"] = (error_count / len(recent_records)) * 100
        
        self.real_time_metrics["last_metric_update"] = now
    
    def get_endpoint_performance(self, provider: str, endpoint: str, 
                                time_window_minutes: int = 60) -> Dict[str, Any]:
        """특정 엔드포인트의 성능 정보 반환"""
        endpoint_key = f"{provider}_{endpoint}"
        
        with self.lock:
            if endpoint_key not in self.endpoint_stats:
                return {"error": "엔드포인트를 찾을 수 없습니다"}
            
            stats = self.endpoint_stats[endpoint_key]
            records = list(self.call_records[endpoint_key])
            
            # 시간 윈도우 필터링
            cutoff_time = datetime.now() - timedelta(minutes=time_window_minutes)
            recent_records = [r for r in records if r.timestamp >= cutoff_time]
            
            # 상세 통계 계산
            performance_data = {
                "basic_stats": {
                    "provider": stats.provider,
                    "endpoint": stats.endpoint,
                    "total_calls": stats.total_calls,
                    "successful_calls": stats.successful_calls,
                    "failed_calls": stats.failed_calls,
                    "error_rate": stats.error_rate,
                    "last_called": stats.last_called.isoformat() if stats.last_called else None
                },
                "response_time_stats": {
                    "avg_ms": stats.avg_response_time_ms,
                    "min_ms": stats.min_response_time_ms if stats.min_response_time_ms != float('inf') else 0,
                    "max_ms": stats.max_response_time_ms
                },
                "recent_performance": self._calculate_recent_performance(recent_records),
                "error_analysis": {
                    "recent_errors": stats.recent_errors[-5:],  # 최근 5개 에러
                    "error_distribution": self._calculate_error_distribution(recent_records)
                },
                "time_window_minutes": time_window_minutes,
                "data_points": len(recent_records)
            }
            
            return performance_data
    
    def _calculate_recent_performance(self, records: List[APICallRecord]) -> Dict[str, Any]:
        """최근 성능 지표 계산"""
        if not records:
            return {
                "avg_response_time_ms": 0,
                "median_response_time_ms": 0,
                "p95_response_time_ms": 0,
                "success_rate": 0,
                "calls_per_minute": 0
            }
        
        response_times = [r.duration_ms for r in records]
        successful_calls = sum(1 for r in records if r.status == APICallStatus.SUCCESS)
        
        # 시간 범위 계산
        if len(records) > 1:
            time_span = (records[-1].timestamp - records[0].timestamp).total_seconds() / 60
            calls_per_minute = len(records) / max(time_span, 1)
        else:
            calls_per_minute = 0
        
        return {
            "avg_response_time_ms": statistics.mean(response_times),
            "median_response_time_ms": statistics.median(response_times),
            "p95_response_time_ms": self._calculate_percentile(response_times, 95),
            "success_rate": (successful_calls / len(records)) * 100,
            "calls_per_minute": calls_per_minute
        }
    
    def _calculate_error_distribution(self, records: List[APICallRecord]) -> Dict[str, int]:
        """에러 분포 계산"""
        error_counts = defaultdict(int)
        
        for record in records:
            if record.status != APICallStatus.SUCCESS:
                error_counts[record.status.value] += 1
        
        return dict(error_counts)
    
    def _calculate_percentile(self, values: List[float], percentile: float) -> float:
        """백분위수 계산"""
        if not values:
            return 0.0
        
        sorted_values = sorted(values)
        index = (percentile / 100) * (len(sorted_values) - 1)
        
        if index.is_integer():
            return sorted_values[int(index)]
        else:
            lower_index = int(index)
            upper_index = lower_index + 1
            weight = index - lower_index
            return sorted_values[lower_index] * (1 - weight) + sorted_values[upper_index] * weight
    
    def get_provider_performance(self, provider: str) -> Dict[str, Any]:
        """제공자별 성능 정보 반환"""
        with self.lock:
            if provider not in self.provider_stats:
                return {"error": "제공자를 찾을 수 없습니다"}
            
            provider_stat = self.provider_stats[provider]
            
            # 활성 엔드포인트 목록과 성능 정보
            active_endpoints = []
            for endpoint in provider_stat["active_endpoints"]:
                endpoint_key = f"{provider}_{endpoint}"
                if endpoint_key in self.endpoint_stats:
                    endpoint_stats = self.endpoint_stats[endpoint_key]
                    active_endpoints.append({
                        "endpoint": endpoint,
                        "total_calls": endpoint_stats.total_calls,
                        "error_rate": endpoint_stats.error_rate,
                        "avg_response_time_ms": endpoint_stats.avg_response_time_ms,
                        "last_called": endpoint_stats.last_called.isoformat() if endpoint_stats.last_called else None
                    })
            
            return {
                "provider": provider,
                "summary": {
                    "total_calls": provider_stat["total_calls"],
                    "successful_calls": provider_stat["successful_calls"],
                    "failed_calls": provider_stat["failed_calls"],
                    "error_rate": provider_stat["error_rate"],
                    "avg_response_time_ms": provider_stat["avg_response_time_ms"],
                    "last_called": provider_stat["last_called"].isoformat() if provider_stat["last_called"] else None,
                    "active_endpoints_count": len(provider_stat["active_endpoints"])
                },
                "active_endpoints": sorted(active_endpoints, key=lambda x: x["total_calls"], reverse=True)
            }
    
    def get_real_time_metrics(self) -> Dict[str, Any]:
        """실시간 메트릭 반환"""
        with self.lock:
            return {
                **self.real_time_metrics,
                "tracking_uptime_seconds": (datetime.now() - self.tracking_start_time).total_seconds()
            }
    
    def get_overall_performance(self) -> Dict[str, Any]:
        """전체 성능 요약 반환"""
        with self.lock:
            total_calls = sum(stats["total_calls"] for stats in self.provider_stats.values())
            total_success = sum(stats["successful_calls"] for stats in self.provider_stats.values())
            total_failures = sum(stats["failed_calls"] for stats in self.provider_stats.values())
            
            # 전체 평균 응답시간 (가중 평균)
            weighted_response_time = 0
            if total_calls > 0:
                for provider, stats in self.provider_stats.items():
                    weight = stats["total_calls"] / total_calls
                    weighted_response_time += stats["avg_response_time_ms"] * weight
            
            return {
                "summary": {
                    "total_api_calls": total_calls,
                    "successful_calls": total_success,
                    "failed_calls": total_failures,
                    "overall_success_rate": (total_success / max(total_calls, 1)) * 100,
                    "overall_error_rate": (total_failures / max(total_calls, 1)) * 100,
                    "avg_response_time_ms": weighted_response_time,
                    "active_providers": len(self.provider_stats),
                    "active_endpoints": sum(len(stats["active_endpoints"]) for stats in self.provider_stats.values()),
                    "tracking_duration_seconds": (datetime.now() - self.tracking_start_time).total_seconds()
                },
                "real_time_metrics": self.get_real_time_metrics(),
                "provider_breakdown": {
                    provider: {
                        "calls": stats["total_calls"],
                        "error_rate": stats["error_rate"],
                        "avg_response_time_ms": stats["avg_response_time_ms"]
                    }
                    for provider, stats in self.provider_stats.items()
                }
            }
    
    def detect_performance_issues(self) -> List[Dict[str, Any]]:
        """성능 이슈 감지"""
        issues = []
        
        with self.lock:
            # 1. 높은 에러율 감지
            for endpoint_key, stats in self.endpoint_stats.items():
                if stats.error_rate > 10:  # 10% 이상 에러율
                    issues.append({
                        "type": "high_error_rate",
                        "severity": "critical" if stats.error_rate > 20 else "warning",
                        "endpoint": f"{stats.provider}/{stats.endpoint}",
                        "error_rate": stats.error_rate,
                        "total_calls": stats.total_calls,
                        "message": f"높은 에러율 감지: {stats.error_rate:.1f}%"
                    })
            
            # 2. 느린 응답 시간 감지
            for endpoint_key, stats in self.endpoint_stats.items():
                if stats.avg_response_time_ms > 5000:  # 5초 이상
                    issues.append({
                        "type": "slow_response",
                        "severity": "critical" if stats.avg_response_time_ms > 10000 else "warning",
                        "endpoint": f"{stats.provider}/{stats.endpoint}",
                        "avg_response_time_ms": stats.avg_response_time_ms,
                        "message": f"느린 응답 시간: {stats.avg_response_time_ms:.0f}ms"
                    })
            
            # 3. 낮은 처리량 감지
            current_rps = self.real_time_metrics["current_rps"]
            if current_rps < 1 and any(stats["total_calls"] > 0 for stats in self.provider_stats.values()):
                issues.append({
                    "type": "low_throughput",
                    "severity": "warning",
                    "current_rps": current_rps,
                    "message": f"낮은 처리량: {current_rps:.2f} RPS"
                })
        
        return issues
    
    def reset_statistics(self):
        """통계 초기화"""
        with self.lock:
            self.call_records.clear()
            self.endpoint_stats.clear()
            self.provider_stats.clear()
            self.call_timestamps.clear()
            
            self.real_time_metrics = {
                "current_rps": 0.0,
                "avg_response_time_1min": 0.0,
                "error_rate_1min": 0.0,
                "active_calls": 0,
                "last_metric_update": datetime.now()
            }
            
            self.tracking_start_time = datetime.now()
            
            logger.info("API 성능 추적 통계 초기화 완료")


# 전역 API 성능 추적기 인스턴스
_api_performance_tracker: Optional[APIPerformanceTracker] = None


def get_api_performance_tracker() -> APIPerformanceTracker:
    """전역 API 성능 추적기 인스턴스 반환 (싱글톤)"""
    global _api_performance_tracker
    
    if _api_performance_tracker is None:
        _api_performance_tracker = APIPerformanceTracker()
    
    return _api_performance_tracker


def reset_api_performance_tracker():
    """API 성능 추적기 인스턴스 재설정 (테스트용)"""
    global _api_performance_tracker
    _api_performance_tracker = None


# 편의 함수들
def record_api_call_simple(provider: str, endpoint: str, duration_ms: float, 
                         status: APICallStatus, status_code: Optional[int] = None,
                         error_message: Optional[str] = None, response_size_bytes: int = 0):
    """간단한 API 호출 기록"""
    tracker = get_api_performance_tracker()
    
    call_record = APICallRecord(
        call_id=f"{provider}_{endpoint}_{int(time.time() * 1000)}",
        provider=provider,
        endpoint=endpoint,
        timestamp=datetime.now(),
        duration_ms=duration_ms,
        status=status,
        status_code=status_code,
        response_size_bytes=response_size_bytes,
        error_message=error_message
    )
    
    tracker.record_api_call(call_record)