"""
캐시 모니터링 및 성능 분석 시스템

Redis 캐시의 성능을 실시간으로 모니터링하고 분석하는 모듈입니다.
- 실시간 성능 메트릭 수집
- 캐시 히트율 추이 분석
- 메모리 사용량 모니터링
- 성능 알람 시스템
- 캐시 최적화 제안
"""

import asyncio
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, NamedTuple
from dataclasses import dataclass, asdict
from collections import deque, defaultdict
from enum import Enum

from utils.redis_client import RedisClient
from app.core.advanced_cache_manager import get_advanced_cache_manager, CacheMetrics


class AlertLevel(Enum):
    """알람 레벨"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class MetricType(Enum):
    """메트릭 타입"""
    HIT_RATE = "hit_rate"
    MEMORY_USAGE = "memory_usage"
    RESPONSE_TIME = "response_time"
    ERROR_RATE = "error_rate"
    EVICTION_RATE = "eviction_rate"


@dataclass
class AlertRule:
    """알람 규칙"""
    metric_type: MetricType
    threshold: float
    level: AlertLevel
    duration_minutes: int = 5  # 지속 시간 (분)
    enabled: bool = True
    description: str = ""


@dataclass
class PerformanceSnapshot:
    """성능 스냅샷"""
    timestamp: datetime
    hit_rate: float
    memory_usage_percent: float
    avg_response_time_ms: float
    total_requests: int
    error_count: int
    evictions: int
    cache_size_mb: float
    connected_clients: int


@dataclass
class CacheOptimizationSuggestion:
    """캐시 최적화 제안"""
    priority: str  # high, medium, low
    category: str  # memory, performance, configuration
    title: str
    description: str
    impact: str
    implementation: str


class CacheMonitor:
    """캐시 모니터링 시스템"""
    
    def __init__(self, redis_client: RedisClient = None):
        self.redis_client = redis_client or RedisClient()
        self.cache_manager = get_advanced_cache_manager()
        self.logger = logging.getLogger(__name__)
        
        # 성능 데이터 버퍼 (최근 24시간)
        self.performance_history: deque = deque(maxlen=1440)  # 1분마다 * 24시간
        self.error_log: deque = deque(maxlen=1000)
        
        # 알람 규칙
        self.alert_rules = self._initialize_alert_rules()
        self.active_alerts: Dict[str, datetime] = {}
        
        # 모니터링 설정
        self.monitoring_interval = 60  # 1분마다
        self.is_monitoring = False
        self._monitoring_task = None
        
        # 최적화 제안 캐시
        self._last_suggestions: List[CacheOptimizationSuggestion] = []
        self._suggestions_generated_at: Optional[datetime] = None
    
    def _initialize_alert_rules(self) -> List[AlertRule]:
        """기본 알람 규칙 초기화"""
        return [
            AlertRule(
                metric_type=MetricType.HIT_RATE,
                threshold=0.7,  # 70% 미만
                level=AlertLevel.WARNING,
                duration_minutes=5,
                description="캐시 히트율이 70% 미만으로 떨어짐"
            ),
            AlertRule(
                metric_type=MetricType.HIT_RATE,
                threshold=0.5,  # 50% 미만
                level=AlertLevel.CRITICAL,
                duration_minutes=3,
                description="캐시 히트율이 50% 미만으로 급락"
            ),
            AlertRule(
                metric_type=MetricType.MEMORY_USAGE,
                threshold=85.0,  # 85% 이상
                level=AlertLevel.WARNING,
                duration_minutes=5,
                description="메모리 사용량이 85% 이상"
            ),
            AlertRule(
                metric_type=MetricType.MEMORY_USAGE,
                threshold=95.0,  # 95% 이상
                level=AlertLevel.CRITICAL,
                duration_minutes=2,
                description="메모리 사용량이 95% 이상 (위험)"
            ),
            AlertRule(
                metric_type=MetricType.RESPONSE_TIME,
                threshold=100.0,  # 100ms 이상
                level=AlertLevel.WARNING,
                duration_minutes=10,
                description="평균 응답 시간이 100ms 이상"
            ),
            AlertRule(
                metric_type=MetricType.EVICTION_RATE,
                threshold=100,  # 시간당 100건 이상
                level=AlertLevel.WARNING,
                duration_minutes=5,
                description="캐시 축출이 빈번하게 발생"
            )
        ]
    
    async def start_monitoring(self):
        """모니터링 시작"""
        if self.is_monitoring:
            self.logger.warning("캐시 모니터링이 이미 실행 중입니다")
            return
        
        self.is_monitoring = True
        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.logger.info("캐시 모니터링이 시작되었습니다")
    
    async def stop_monitoring(self):
        """모니터링 중지"""
        self.is_monitoring = False
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
        self.logger.info("캐시 모니터링이 중지되었습니다")
    
    async def _monitoring_loop(self):
        """모니터링 루프"""
        try:
            while self.is_monitoring:
                await self._collect_performance_snapshot()
                await self._check_alert_rules()
                await asyncio.sleep(self.monitoring_interval)
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error(f"모니터링 루프 오류: {e}")
            await asyncio.sleep(self.monitoring_interval)
            if self.is_monitoring:
                # 재시작 시도
                asyncio.create_task(self._monitoring_loop())
    
    async def _collect_performance_snapshot(self):
        """성능 스냅샷 수집"""
        try:
            # 캐시 매니저에서 메트릭 수집
            metrics = await self.cache_manager.get_cache_metrics()
            
            # Redis 정보 수집
            info = await self.redis_client.client.info()
            
            # 스냅샷 생성
            snapshot = PerformanceSnapshot(
                timestamp=datetime.now(),
                hit_rate=metrics.hit_rate,
                memory_usage_percent=metrics.memory_usage_percent,
                avg_response_time_ms=metrics.avg_response_time_ms,
                total_requests=metrics.total_requests,
                error_count=0,  # 별도 추적 필요
                evictions=metrics.evictions,
                cache_size_mb=metrics.cache_size_mb,
                connected_clients=info.get('connected_clients', 0)
            )
            
            # 히스토리에 추가
            self.performance_history.append(snapshot)
            
            # 주기적으로 성능 데이터 저장 (Redis에)
            if len(self.performance_history) % 10 == 0:  # 10분마다
                await self._save_performance_data(snapshot)
            
        except Exception as e:
            self.logger.error(f"성능 스냅샷 수집 실패: {e}")
            self.error_log.append({
                'timestamp': datetime.now(),
                'error': str(e),
                'context': 'performance_snapshot'
            })
    
    async def _save_performance_data(self, snapshot: PerformanceSnapshot):
        """성능 데이터를 Redis에 저장"""
        try:
            # 일별 성능 데이터 키
            date_key = snapshot.timestamp.strftime('%Y-%m-%d')
            hour_minute_key = snapshot.timestamp.strftime('%H:%M')
            
            performance_key = f"cache_monitoring:performance:{date_key}"
            
            # 성능 데이터 저장 (해시)
            await self.redis_client.client.hset(
                performance_key,
                hour_minute_key,
                json.dumps(asdict(snapshot), default=str)
            )
            
            # 7일 후 만료
            await self.redis_client.client.expire(performance_key, 7 * 24 * 3600)
            
        except Exception as e:
            self.logger.error(f"성능 데이터 저장 실패: {e}")
    
    async def _check_alert_rules(self):
        """알람 규칙 확인"""
        if not self.performance_history:
            return
        
        current_snapshot = self.performance_history[-1]
        
        for rule in self.alert_rules:
            if not rule.enabled:
                continue
            
            # 규칙별 메트릭 값 추출
            current_value = self._extract_metric_value(current_snapshot, rule.metric_type)
            
            # 임계값 확인
            should_alert = self._should_trigger_alert(rule, current_value)
            
            alert_key = f"{rule.metric_type.value}_{rule.level.value}"
            
            if should_alert:
                if alert_key not in self.active_alerts:
                    # 새로운 알람 발생
                    self.active_alerts[alert_key] = datetime.now()
                    await self._trigger_alert(rule, current_value)
                else:
                    # 지속적인 알람 상태 확인
                    alert_duration = datetime.now() - self.active_alerts[alert_key]
                    if alert_duration.total_seconds() >= rule.duration_minutes * 60:
                        # 지속 시간 초과 - 알람 재발송
                        await self._trigger_alert(rule, current_value, is_sustained=True)
            else:
                # 알람 해제
                if alert_key in self.active_alerts:
                    del self.active_alerts[alert_key]
                    await self._resolve_alert(rule, current_value)
    
    def _extract_metric_value(self, snapshot: PerformanceSnapshot, metric_type: MetricType) -> float:
        """스냅샷에서 메트릭 값 추출"""
        if metric_type == MetricType.HIT_RATE:
            return snapshot.hit_rate
        elif metric_type == MetricType.MEMORY_USAGE:
            return snapshot.memory_usage_percent
        elif metric_type == MetricType.RESPONSE_TIME:
            return snapshot.avg_response_time_ms
        elif metric_type == MetricType.EVICTION_RATE:
            # 최근 1시간 축출율 계산
            recent_snapshots = [s for s in self.performance_history 
                              if (snapshot.timestamp - s.timestamp).total_seconds() <= 3600]
            if len(recent_snapshots) >= 2:
                return recent_snapshots[-1].evictions - recent_snapshots[0].evictions
            return 0
        else:
            return 0
    
    def _should_trigger_alert(self, rule: AlertRule, current_value: float) -> bool:
        """알람 트리거 여부 판단"""
        if rule.metric_type in [MetricType.HIT_RATE]:
            # 히트율은 임계값 미만일 때 알람
            return current_value < rule.threshold
        else:
            # 나머지는 임계값 초과 시 알람
            return current_value > rule.threshold
    
    async def _trigger_alert(self, rule: AlertRule, current_value: float, is_sustained: bool = False):
        """알람 발생"""
        alert_message = {
            'timestamp': datetime.now().isoformat(),
            'level': rule.level.value,
            'metric_type': rule.metric_type.value,
            'threshold': rule.threshold,
            'current_value': current_value,
            'description': rule.description,
            'is_sustained': is_sustained
        }
        
        # 로그 기록
        self.logger.warning(f"캐시 알람 발생: {rule.description} (현재값: {current_value}, 임계값: {rule.threshold})")
        
        # 알람 저장 (Redis)
        await self._save_alert(alert_message)
        
        # TODO: 외부 알람 시스템 연동 (예: Slack, Email)
    
    async def _resolve_alert(self, rule: AlertRule, current_value: float):
        """알람 해제"""
        self.logger.info(f"캐시 알람 해제: {rule.description} (현재값: {current_value})")
        
        # 해제 정보 저장
        resolution_message = {
            'timestamp': datetime.now().isoformat(),
            'metric_type': rule.metric_type.value,
            'current_value': current_value,
            'description': f"{rule.description} - 해제됨"
        }
        
        await self._save_alert(resolution_message, is_resolution=True)
    
    async def _save_alert(self, alert_data: Dict[str, Any], is_resolution: bool = False):
        """알람 정보 저장"""
        try:
            # 일별 알람 로그
            date_key = datetime.now().strftime('%Y-%m-%d')
            alert_type = "resolution" if is_resolution else "alert"
            alert_key = f"cache_monitoring:alerts:{date_key}"
            
            # 리스트에 추가
            await self.redis_client.client.lpush(
                alert_key,
                json.dumps(alert_data, ensure_ascii=False)
            )
            
            # 최대 1000개 유지
            await self.redis_client.client.ltrim(alert_key, 0, 999)
            
            # 30일 후 만료
            await self.redis_client.client.expire(alert_key, 30 * 24 * 3600)
            
        except Exception as e:
            self.logger.error(f"알람 저장 실패: {e}")
    
    async def get_performance_summary(self, hours: int = 24) -> Dict[str, Any]:
        """성능 요약 조회"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            recent_snapshots = [
                s for s in self.performance_history 
                if s.timestamp >= cutoff_time
            ]
            
            if not recent_snapshots:
                return {"error": "성능 데이터가 없습니다"}
            
            # 통계 계산
            hit_rates = [s.hit_rate for s in recent_snapshots]
            memory_usages = [s.memory_usage_percent for s in recent_snapshots]
            response_times = [s.avg_response_time_ms for s in recent_snapshots]
            
            return {
                "period_hours": hours,
                "total_snapshots": len(recent_snapshots),
                "hit_rate": {
                    "current": hit_rates[-1] if hit_rates else 0,
                    "average": sum(hit_rates) / len(hit_rates) if hit_rates else 0,
                    "min": min(hit_rates) if hit_rates else 0,
                    "max": max(hit_rates) if hit_rates else 0
                },
                "memory_usage": {
                    "current": memory_usages[-1] if memory_usages else 0,
                    "average": sum(memory_usages) / len(memory_usages) if memory_usages else 0,
                    "peak": max(memory_usages) if memory_usages else 0
                },
                "response_time": {
                    "current": response_times[-1] if response_times else 0,
                    "average": sum(response_times) / len(response_times) if response_times else 0,
                    "peak": max(response_times) if response_times else 0
                },
                "active_alerts": len(self.active_alerts),
                "total_requests": recent_snapshots[-1].total_requests if recent_snapshots else 0
            }
            
        except Exception as e:
            self.logger.error(f"성능 요약 조회 실패: {e}")
            return {"error": str(e)}
    
    async def generate_optimization_suggestions(self) -> List[CacheOptimizationSuggestion]:
        """캐시 최적화 제안 생성"""
        try:
            # 1시간마다 한 번만 생성
            if (self._suggestions_generated_at and 
                datetime.now() - self._suggestions_generated_at < timedelta(hours=1)):
                return self._last_suggestions
            
            suggestions = []
            
            # 성능 데이터 분석
            if not self.performance_history:
                return suggestions
            
            recent_snapshots = list(self.performance_history)[-60:]  # 최근 1시간
            
            if not recent_snapshots:
                return suggestions
            
            # 히트율 분석
            avg_hit_rate = sum(s.hit_rate for s in recent_snapshots) / len(recent_snapshots)
            if avg_hit_rate < 0.7:
                suggestions.append(CacheOptimizationSuggestion(
                    priority="high",
                    category="performance",
                    title="낮은 캐시 히트율 개선 필요",
                    description=f"현재 평균 히트율 {avg_hit_rate:.1%}로 70% 미만입니다.",
                    impact="응답 시간 단축 및 외부 API 호출 감소",
                    implementation="TTL 증가, 캐시 워밍 활성화, 캐시 키 전략 재검토"
                ))
            
            # 메모리 사용량 분석
            avg_memory = sum(s.memory_usage_percent for s in recent_snapshots) / len(recent_snapshots)
            if avg_memory > 80:
                suggestions.append(CacheOptimizationSuggestion(
                    priority="high",
                    category="memory",
                    title="높은 메모리 사용량 최적화 필요",
                    description=f"현재 평균 메모리 사용량 {avg_memory:.1f}%로 80% 초과입니다.",
                    impact="메모리 부족으로 인한 성능 저하 방지",
                    implementation="불필요한 캐시 데이터 정리, TTL 단축, 메모리 증설 고려"
                ))
            
            # 응답 시간 분석
            avg_response_time = sum(s.avg_response_time_ms for s in recent_snapshots) / len(recent_snapshots)
            if avg_response_time > 50:
                suggestions.append(CacheOptimizationSuggestion(
                    priority="medium",
                    category="performance",
                    title="캐시 응답 시간 최적화",
                    description=f"현재 평균 응답 시간 {avg_response_time:.1f}ms로 최적화 여지가 있습니다.",
                    impact="전체 시스템 응답 성능 향상",
                    implementation="Redis 설정 튜닝, 네트워크 최적화, 파이프라인 사용 확대"
                ))
            
            # 축출 빈도 분석
            if recent_snapshots:
                recent_evictions = recent_snapshots[-1].evictions
                if len(recent_snapshots) > 30:  # 30분 이상 데이터
                    eviction_rate = (recent_evictions - recent_snapshots[-30].evictions) / 30 * 60  # 시간당
                    if eviction_rate > 10:
                        suggestions.append(CacheOptimizationSuggestion(
                            priority="medium",
                            category="configuration",
                            title="빈번한 캐시 축출 최적화",
                            description=f"시간당 약 {eviction_rate:.0f}건의 캐시 축출이 발생하고 있습니다.",
                            impact="캐시 효율성 향상 및 불필요한 재연산 방지",
                            implementation="메모리 증설, maxmemory-policy 조정, TTL 재검토"
                        ))
            
            # 캐시 사용 패턴 분석
            if len(self.performance_history) > 1440:  # 24시간 이상 데이터
                suggestions.append(CacheOptimizationSuggestion(
                    priority="low",
                    category="optimization",
                    title="캐시 사용 패턴 기반 최적화",
                    description="충분한 모니터링 데이터가 축적되어 상세 분석이 가능합니다.",
                    impact="데이터 패턴 기반 맞춤형 최적화",
                    implementation="시간대별 TTL 차별화, 피크 시간 대비 캐시 워밍 강화"
                ))
            
            self._last_suggestions = suggestions
            self._suggestions_generated_at = datetime.now()
            
            return suggestions
            
        except Exception as e:
            self.logger.error(f"최적화 제안 생성 실패: {e}")
            return []
    
    async def get_alert_history(self, days: int = 7) -> List[Dict[str, Any]]:
        """알람 히스토리 조회"""
        try:
            alerts = []
            
            for i in range(days):
                date = datetime.now() - timedelta(days=i)
                date_key = date.strftime('%Y-%m-%d')
                alert_key = f"cache_monitoring:alerts:{date_key}"
                
                daily_alerts = await self.redis_client.client.lrange(alert_key, 0, -1)
                for alert_data in daily_alerts:
                    try:
                        alerts.append(json.loads(alert_data))
                    except json.JSONDecodeError:
                        continue
            
            # 시간순 정렬 (최신순)
            alerts.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            
            return alerts
            
        except Exception as e:
            self.logger.error(f"알람 히스토리 조회 실패: {e}")
            return []


# 싱글톤 인스턴스
_cache_monitor: Optional[CacheMonitor] = None


def get_cache_monitor() -> CacheMonitor:
    """캐시 모니터 싱글톤 인스턴스 반환"""
    global _cache_monitor
    if _cache_monitor is None:
        _cache_monitor = CacheMonitor()
    return _cache_monitor