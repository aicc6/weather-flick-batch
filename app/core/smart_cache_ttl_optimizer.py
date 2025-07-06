"""
스마트 캐시 TTL 최적화 시스템

API별, 데이터 타입별, 사용 패턴별로 최적화된 TTL을 동적으로 계산하고 적용하는 모듈입니다.
- 데이터 유형별 최적 TTL 계산
- 사용 패턴 기반 TTL 조정
- 시간대별 차별화된 TTL 적용
- 자동 TTL 튜닝 시스템
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict
from enum import Enum
from collections import defaultdict, deque

from utils.redis_client import RedisClient
from app.core.multi_api_key_manager import APIProvider


class DataFreshness(Enum):
    """데이터 신선도 요구사항"""
    REAL_TIME = "real_time"        # 실시간 (< 5분)
    NEAR_REAL_TIME = "near_real_time"  # 준실시간 (5-15분)
    FRESH = "fresh"                # 신선 (15분-1시간)
    MODERATE = "moderate"          # 적당 (1-6시간)
    STABLE = "stable"              # 안정 (6-24시간)
    STATIC = "static"              # 정적 (1일+)


class AccessPattern(Enum):
    """접근 패턴"""
    HIGH_FREQUENCY = "high_frequency"    # 고빈도 (분당 10+회)
    MODERATE_FREQUENCY = "moderate_frequency"  # 중빈도 (분당 1-10회)
    LOW_FREQUENCY = "low_frequency"      # 저빈도 (분당 < 1회)
    BATCH_ONLY = "batch_only"            # 배치 전용
    PEAK_TIME_ONLY = "peak_time_only"    # 피크 시간만


@dataclass
class TTLRule:
    """TTL 규칙"""
    pattern: str
    base_ttl: int
    data_freshness: DataFreshness
    access_pattern: AccessPattern
    time_sensitive: bool = False
    multiplier_factors: Dict[str, float] = None
    
    def __post_init__(self):
        if self.multiplier_factors is None:
            self.multiplier_factors = {}


@dataclass
class CacheUsageStats:
    """캐시 사용 통계"""
    key_pattern: str
    total_accesses: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    avg_access_interval: float = 0  # 평균 접근 간격 (초)
    peak_hour_accesses: Dict[int, int] = None  # 시간대별 접근 수
    last_access_time: Optional[datetime] = None
    data_staleness_tolerance: float = 0.8  # 데이터 신선도 허용 임계값
    
    def __post_init__(self):
        if self.peak_hour_accesses is None:
            self.peak_hour_accesses = defaultdict(int)
    
    @property
    def hit_rate(self) -> float:
        """캐시 히트율"""
        total = self.cache_hits + self.cache_misses
        return self.cache_hits / total if total > 0 else 0
    
    @property
    def access_frequency(self) -> AccessPattern:
        """접근 빈도 패턴 추정"""
        if self.avg_access_interval == 0:
            return AccessPattern.BATCH_ONLY
        
        accesses_per_minute = 60 / self.avg_access_interval
        
        if accesses_per_minute >= 10:
            return AccessPattern.HIGH_FREQUENCY
        elif accesses_per_minute >= 1:
            return AccessPattern.MODERATE_FREQUENCY
        else:
            return AccessPattern.LOW_FREQUENCY


class SmartCacheTTLOptimizer:
    """스마트 캐시 TTL 최적화 시스템"""
    
    def __init__(self, redis_client: RedisClient = None):
        self.redis_client = redis_client or RedisClient()
        self.logger = logging.getLogger(__name__)
        
        # TTL 규칙 정의
        self.ttl_rules = self._initialize_ttl_rules()
        
        # 사용 통계 추적
        self.usage_stats: Dict[str, CacheUsageStats] = {}
        self.stats_window = deque(maxlen=1000)  # 최근 1000건 추적
        
        # 최적화 설정
        self.optimization_enabled = True
        self.auto_tuning_enabled = True
        self.min_ttl = 60      # 최소 TTL (1분)
        self.max_ttl = 86400   # 최대 TTL (24시간)
        
        # 성능 추적
        self.optimization_metrics = {
            "total_optimizations": 0,
            "ttl_adjustments": 0,
            "hit_rate_improvements": 0,
            "memory_savings": 0
        }
    
    def _initialize_ttl_rules(self) -> List[TTLRule]:
        """기본 TTL 규칙 초기화"""
        return [
            # 날씨 데이터 규칙
            TTLRule(
                pattern="api_cache:kma:getUltraSrtNcst:*",
                base_ttl=900,  # 15분
                data_freshness=DataFreshness.NEAR_REAL_TIME,
                access_pattern=AccessPattern.HIGH_FREQUENCY,
                time_sensitive=True,
                multiplier_factors={"peak_hour": 0.5, "off_peak": 1.5}
            ),
            TTLRule(
                pattern="api_cache:kma:getVilageFcst:*",
                base_ttl=3600,  # 1시간
                data_freshness=DataFreshness.FRESH,
                access_pattern=AccessPattern.MODERATE_FREQUENCY,
                time_sensitive=True,
                multiplier_factors={"peak_hour": 0.8, "off_peak": 1.2}
            ),
            TTLRule(
                pattern="api_cache:weather:current:*",
                base_ttl=1800,  # 30분
                data_freshness=DataFreshness.NEAR_REAL_TIME,
                access_pattern=AccessPattern.HIGH_FREQUENCY,
                time_sensitive=True
            ),
            TTLRule(
                pattern="api_cache:weather:forecast:*",
                base_ttl=7200,  # 2시간
                data_freshness=DataFreshness.FRESH,
                access_pattern=AccessPattern.MODERATE_FREQUENCY,
                time_sensitive=True
            ),
            
            # 관광 데이터 규칙
            TTLRule(
                pattern="api_cache:kto:areaCode:*",
                base_ttl=604800,  # 7일
                data_freshness=DataFreshness.STATIC,
                access_pattern=AccessPattern.LOW_FREQUENCY,
                time_sensitive=False,
                multiplier_factors={"batch_mode": 2.0}
            ),
            TTLRule(
                pattern="api_cache:kto:areaBasedList:*",
                base_ttl=43200,  # 12시간
                data_freshness=DataFreshness.STABLE,
                access_pattern=AccessPattern.MODERATE_FREQUENCY,
                time_sensitive=False
            ),
            TTLRule(
                pattern="api_cache:kto:detailCommon:*",
                base_ttl=86400,  # 24시간
                data_freshness=DataFreshness.STABLE,
                access_pattern=AccessPattern.LOW_FREQUENCY,
                time_sensitive=False
            ),
            
            # 처리된 데이터 규칙
            TTLRule(
                pattern="weather_scores:*",
                base_ttl=3600,  # 1시간
                data_freshness=DataFreshness.FRESH,
                access_pattern=AccessPattern.HIGH_FREQUENCY,
                time_sensitive=True,
                multiplier_factors={"high_demand": 0.5, "low_demand": 2.0}
            ),
            TTLRule(
                pattern="recommendations:*",
                base_ttl=1800,  # 30분
                data_freshness=DataFreshness.FRESH,
                access_pattern=AccessPattern.HIGH_FREQUENCY,
                time_sensitive=True
            ),
            TTLRule(
                pattern="tourism_data:processed:*",
                base_ttl=14400,  # 4시간
                data_freshness=DataFreshness.MODERATE,
                access_pattern=AccessPattern.MODERATE_FREQUENCY,
                time_sensitive=False
            ),
            
            # 메타데이터 규칙
            TTLRule(
                pattern="metadata:regions:*",
                base_ttl=604800,  # 7일
                data_freshness=DataFreshness.STATIC,
                access_pattern=AccessPattern.LOW_FREQUENCY,
                time_sensitive=False
            ),
            TTLRule(
                pattern="config:*",
                base_ttl=3600,  # 1시간
                data_freshness=DataFreshness.MODERATE,
                access_pattern=AccessPattern.LOW_FREQUENCY,
                time_sensitive=False,
                multiplier_factors={"config_stable": 5.0}
            )
        ]
    
    def get_optimal_ttl(
        self, 
        cache_key: str,
        api_provider: Optional[APIProvider] = None,
        endpoint: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> int:
        """최적 TTL 계산"""
        try:
            # 1. 기본 규칙 매칭
            matched_rule = self._find_matching_rule(cache_key)
            if not matched_rule:
                return self._get_default_ttl(api_provider, endpoint)
            
            # 2. 사용 통계 기반 조정
            usage_stats = self._get_usage_stats(cache_key)
            base_ttl = matched_rule.base_ttl
            
            # 3. 시간대별 조정
            time_factor = self._calculate_time_factor(matched_rule, context)
            
            # 4. 접근 패턴 기반 조정
            access_factor = self._calculate_access_factor(matched_rule, usage_stats)
            
            # 5. 데이터 신선도 기반 조정
            freshness_factor = self._calculate_freshness_factor(matched_rule, usage_stats)
            
            # 6. 종합 TTL 계산
            optimal_ttl = int(base_ttl * time_factor * access_factor * freshness_factor)
            
            # 7. 범위 제한
            optimal_ttl = max(self.min_ttl, min(optimal_ttl, self.max_ttl))
            
            self.logger.debug(
                f"최적 TTL 계산: {cache_key} -> {optimal_ttl}초 "
                f"(기본: {base_ttl}, 시간: {time_factor:.2f}, "
                f"접근: {access_factor:.2f}, 신선도: {freshness_factor:.2f})"
            )
            
            return optimal_ttl
            
        except Exception as e:
            self.logger.error(f"최적 TTL 계산 실패 [{cache_key}]: {e}")
            return self._get_default_ttl(api_provider, endpoint)
    
    def _find_matching_rule(self, cache_key: str) -> Optional[TTLRule]:
        """캐시 키에 매칭되는 TTL 규칙 찾기"""
        for rule in self.ttl_rules:
            if self._pattern_matches(cache_key, rule.pattern):
                return rule
        return None
    
    def _pattern_matches(self, cache_key: str, pattern: str) -> bool:
        """패턴 매칭 확인"""
        # 간단한 wildcard 매칭 (* 지원)
        import fnmatch
        return fnmatch.fnmatch(cache_key, pattern)
    
    def _get_default_ttl(self, api_provider: Optional[APIProvider], endpoint: Optional[str]) -> int:
        """기본 TTL 반환"""
        if api_provider == APIProvider.KMA:
            if endpoint and "Ultra" in endpoint:
                return 900   # 15분 (초단기)
            else:
                return 3600  # 1시간 (단기예보)
        elif api_provider == APIProvider.KTO:
            return 21600     # 6시간
        elif api_provider == APIProvider.WEATHER:
            return 1800      # 30분
        else:
            return 3600      # 기본 1시간
    
    def _calculate_time_factor(self, rule: TTLRule, context: Optional[Dict[str, Any]]) -> float:
        """시간대별 조정 계수 계산"""
        if not rule.time_sensitive:
            return 1.0
        
        current_hour = datetime.now().hour
        
        # 피크 시간 (오전 7-9시, 오후 6-9시)
        peak_hours = list(range(7, 10)) + list(range(18, 22))
        is_peak_time = current_hour in peak_hours
        
        # 배치 작업 시간 (새벽 1-5시)
        batch_hours = list(range(1, 6))
        is_batch_time = current_hour in batch_hours
        
        if is_peak_time:
            # 피크 시간에는 TTL 단축 (더 자주 갱신)
            return rule.multiplier_factors.get("peak_hour", 0.7)
        elif is_batch_time:
            # 배치 시간에는 TTL 연장 (덜 자주 갱신)
            return rule.multiplier_factors.get("batch_mode", 1.5)
        else:
            # 일반 시간
            return rule.multiplier_factors.get("off_peak", 1.0)
    
    def _calculate_access_factor(self, rule: TTLRule, usage_stats: Optional[CacheUsageStats]) -> float:
        """접근 패턴 기반 조정 계수 계산"""
        if not usage_stats:
            return 1.0
        
        hit_rate = usage_stats.hit_rate
        access_pattern = usage_stats.access_frequency
        
        # 히트율이 높고 접근이 빈번하면 TTL 연장
        if hit_rate > 0.9 and access_pattern in [AccessPattern.HIGH_FREQUENCY, AccessPattern.MODERATE_FREQUENCY]:
            return 1.5
        
        # 히트율이 낮으면 TTL 단축
        elif hit_rate < 0.5:
            return 0.7
        
        # 접근 빈도별 조정
        if access_pattern == AccessPattern.HIGH_FREQUENCY:
            return rule.multiplier_factors.get("high_demand", 1.2)
        elif access_pattern == AccessPattern.LOW_FREQUENCY:
            return rule.multiplier_factors.get("low_demand", 0.8)
        else:
            return 1.0
    
    def _calculate_freshness_factor(self, rule: TTLRule, usage_stats: Optional[CacheUsageStats]) -> float:
        """데이터 신선도 기반 조정 계수 계산"""
        # 신선도 요구사항별 기본 계수
        freshness_factors = {
            DataFreshness.REAL_TIME: 0.5,
            DataFreshness.NEAR_REAL_TIME: 0.7,
            DataFreshness.FRESH: 1.0,
            DataFreshness.MODERATE: 1.3,
            DataFreshness.STABLE: 1.5,
            DataFreshness.STATIC: 2.0
        }
        
        base_factor = freshness_factors.get(rule.data_freshness, 1.0)
        
        # 사용 통계 기반 미세 조정
        if usage_stats and usage_stats.data_staleness_tolerance:
            # 신선도 허용 임계값이 높으면 TTL 연장 가능
            tolerance_factor = usage_stats.data_staleness_tolerance
            base_factor *= (1 + tolerance_factor * 0.3)
        
        return base_factor
    
    def _get_usage_stats(self, cache_key: str) -> Optional[CacheUsageStats]:
        """캐시 키의 사용 통계 조회"""
        # 패턴 매칭으로 통계 찾기
        for pattern, stats in self.usage_stats.items():
            if self._pattern_matches(cache_key, pattern):
                return stats
        return None
    
    async def update_usage_stats(
        self, 
        cache_key: str, 
        was_hit: bool, 
        access_time: Optional[datetime] = None
    ):
        """사용 통계 업데이트"""
        try:
            if access_time is None:
                access_time = datetime.now()
            
            # 패턴 추출
            pattern = self._extract_pattern(cache_key)
            
            if pattern not in self.usage_stats:
                self.usage_stats[pattern] = CacheUsageStats(key_pattern=pattern)
            
            stats = self.usage_stats[pattern]
            
            # 통계 업데이트
            stats.total_accesses += 1
            if was_hit:
                stats.cache_hits += 1
            else:
                stats.cache_misses += 1
            
            # 접근 간격 계산
            if stats.last_access_time:
                interval = (access_time - stats.last_access_time).total_seconds()
                if stats.avg_access_interval == 0:
                    stats.avg_access_interval = interval
                else:
                    # 지수 가중 이동 평균
                    stats.avg_access_interval = stats.avg_access_interval * 0.9 + interval * 0.1
            
            stats.last_access_time = access_time
            
            # 시간대별 통계
            hour = access_time.hour
            stats.peak_hour_accesses[hour] += 1
            
            # 통계 저장 (주기적)
            if stats.total_accesses % 100 == 0:
                await self._save_usage_stats(pattern, stats)
            
        except Exception as e:
            self.logger.error(f"사용 통계 업데이트 실패 [{cache_key}]: {e}")
    
    def _extract_pattern(self, cache_key: str) -> str:
        """캐시 키에서 패턴 추출"""
        # 기본적인 패턴 추출 로직
        parts = cache_key.split(":")
        if len(parts) >= 3:
            # api_cache:provider:endpoint:* 형태
            return ":".join(parts[:3]) + ":*"
        elif len(parts) >= 2:
            # provider:type:* 형태
            return ":".join(parts[:2]) + ":*"
        else:
            return cache_key + ":*"
    
    async def _save_usage_stats(self, pattern: str, stats: CacheUsageStats):
        """사용 통계를 Redis에 저장"""
        try:
            stats_key = f"cache_stats:usage:{pattern.replace(':', '_').replace('*', 'wildcard')}"
            stats_data = asdict(stats)
            
            # datetime 객체 처리
            if stats_data.get('last_access_time'):
                stats_data['last_access_time'] = stats_data['last_access_time'].isoformat()
            
            await self.redis_client.client.setex(
                stats_key,
                86400,  # 24시간 보관
                json.dumps(stats_data, default=str)
            )
            
        except Exception as e:
            self.logger.error(f"사용 통계 저장 실패 [{pattern}]: {e}")
    
    async def auto_tune_ttl_rules(self):
        """TTL 규칙 자동 튜닝"""
        if not self.auto_tuning_enabled:
            return
        
        try:
            self.logger.info("TTL 규칙 자동 튜닝을 시작합니다")
            
            tuned_count = 0
            
            for rule in self.ttl_rules:
                # 해당 패턴의 사용 통계 수집
                pattern_stats = None
                for pattern, stats in self.usage_stats.items():
                    if pattern == rule.pattern or self._pattern_matches(pattern, rule.pattern):
                        pattern_stats = stats
                        break
                
                if not pattern_stats or pattern_stats.total_accesses < 100:
                    continue  # 충분한 통계가 없음
                
                # 성능 분석
                hit_rate = pattern_stats.hit_rate
                access_frequency = pattern_stats.access_frequency
                
                # 튜닝 규칙 적용
                old_ttl = rule.base_ttl
                new_ttl = old_ttl
                
                # 히트율 기반 조정
                if hit_rate > 0.95:
                    # 매우 높은 히트율 -> TTL 증가
                    new_ttl = int(old_ttl * 1.5)
                elif hit_rate < 0.7:
                    # 낮은 히트율 -> TTL 감소
                    new_ttl = int(old_ttl * 0.8)
                
                # 접근 빈도 기반 조정
                if access_frequency == AccessPattern.HIGH_FREQUENCY and hit_rate > 0.8:
                    new_ttl = int(new_ttl * 1.2)
                elif access_frequency == AccessPattern.LOW_FREQUENCY:
                    new_ttl = int(new_ttl * 0.9)
                
                # 범위 제한
                new_ttl = max(self.min_ttl, min(new_ttl, self.max_ttl))
                
                # 변경사항 적용
                if new_ttl != old_ttl:
                    rule.base_ttl = new_ttl
                    tuned_count += 1
                    
                    self.logger.info(
                        f"TTL 규칙 튜닝: {rule.pattern} "
                        f"{old_ttl}초 -> {new_ttl}초 "
                        f"(히트율: {hit_rate:.1%}, 빈도: {access_frequency.value})"
                    )
            
            self.optimization_metrics["ttl_adjustments"] += tuned_count
            self.logger.info(f"TTL 자동 튜닝 완료: {tuned_count}개 규칙 조정")
            
        except Exception as e:
            self.logger.error(f"TTL 자동 튜닝 실패: {e}")
    
    async def analyze_cache_efficiency(self) -> Dict[str, Any]:
        """캐시 효율성 분석"""
        try:
            analysis = {
                "timestamp": datetime.now().isoformat(),
                "total_patterns": len(self.usage_stats),
                "optimization_metrics": self.optimization_metrics.copy(),
                "pattern_analysis": [],
                "recommendations": []
            }
            
            for pattern, stats in self.usage_stats.items():
                if stats.total_accesses < 10:
                    continue
                
                pattern_analysis = {
                    "pattern": pattern,
                    "total_accesses": stats.total_accesses,
                    "hit_rate": stats.hit_rate,
                    "access_frequency": stats.access_frequency.value,
                    "avg_access_interval": stats.avg_access_interval,
                    "peak_hours": sorted(stats.peak_hour_accesses.items(), key=lambda x: x[1], reverse=True)[:3],
                    "efficiency_score": self._calculate_efficiency_score(stats)
                }
                
                analysis["pattern_analysis"].append(pattern_analysis)
            
            # 권장사항 생성
            analysis["recommendations"] = self._generate_ttl_recommendations(analysis["pattern_analysis"])
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"캐시 효율성 분석 실패: {e}")
            return {"error": str(e)}
    
    def _calculate_efficiency_score(self, stats: CacheUsageStats) -> float:
        """효율성 점수 계산 (0-100)"""
        try:
            # 히트율 점수 (50점)
            hit_score = stats.hit_rate * 50
            
            # 접근 빈도 점수 (30점)
            frequency_scores = {
                AccessPattern.HIGH_FREQUENCY: 30,
                AccessPattern.MODERATE_FREQUENCY: 25,
                AccessPattern.LOW_FREQUENCY: 15,
                AccessPattern.BATCH_ONLY: 10,
                AccessPattern.PEAK_TIME_ONLY: 20
            }
            frequency_score = frequency_scores.get(stats.access_frequency, 15)
            
            # 일관성 점수 (20점)
            # 접근 간격의 일관성을 평가
            consistency_score = 20 if stats.avg_access_interval > 0 else 0
            
            total_score = hit_score + frequency_score + consistency_score
            return min(100, max(0, total_score))
            
        except Exception:
            return 50  # 기본값
    
    def _generate_ttl_recommendations(self, pattern_analysis: List[Dict]) -> List[str]:
        """TTL 최적화 권장사항 생성"""
        recommendations = []
        
        try:
            # 낮은 히트율 패턴 식별
            low_hit_rate_patterns = [p for p in pattern_analysis if p["hit_rate"] < 0.7]
            if low_hit_rate_patterns:
                recommendations.append(
                    f"낮은 히트율 패턴 {len(low_hit_rate_patterns)}개 발견: TTL 단축 고려"
                )
            
            # 고빈도 접근 패턴 식별
            high_freq_patterns = [p for p in pattern_analysis if p["access_frequency"] == "high_frequency"]
            if high_freq_patterns:
                recommendations.append(
                    f"고빈도 접근 패턴 {len(high_freq_patterns)}개: TTL 연장으로 성능 향상 가능"
                )
            
            # 효율성 점수 기반 권장사항
            low_efficiency_patterns = [p for p in pattern_analysis if p["efficiency_score"] < 60]
            if low_efficiency_patterns:
                recommendations.append(
                    f"낮은 효율성 패턴 {len(low_efficiency_patterns)}개: TTL 전략 재검토 필요"
                )
            
            # 시간대별 접근 패턴 분석
            patterns_with_peak = [p for p in pattern_analysis if p["peak_hours"]]
            if patterns_with_peak:
                recommendations.append(
                    f"시간대별 접근 패턴 {len(patterns_with_peak)}개: 시간대별 차별화 TTL 적용 고려"
                )
            
            if not recommendations:
                recommendations.append("현재 TTL 설정이 최적화된 상태입니다")
            
        except Exception as e:
            self.logger.error(f"권장사항 생성 실패: {e}")
            recommendations.append("권장사항 분석 중 오류가 발생했습니다")
        
        return recommendations
    
    async def apply_time_based_ttl_strategy(self):
        """시간대 기반 TTL 전략 적용"""
        try:
            current_hour = datetime.now().hour
            
            # 시간대별 전략
            strategies = {
                "deep_night": (list(range(0, 6)), {"multiplier": 2.0, "reason": "심야시간 - 접근 낮음"}),
                "morning_peak": (list(range(7, 10)), {"multiplier": 0.7, "reason": "아침 피크 - 빈번한 접근"}),
                "daytime": (list(range(10, 18)), {"multiplier": 1.0, "reason": "일반 시간"}),
                "evening_peak": (list(range(18, 22)), {"multiplier": 0.8, "reason": "저녁 피크 - 높은 접근"}),
                "night": (list(range(22, 24)), {"multiplier": 1.5, "reason": "야간 - 접근 감소"})
            }
            
            active_strategy = None
            for strategy_name, (hours, config) in strategies.items():
                if current_hour in hours:
                    active_strategy = (strategy_name, config)
                    break
            
            if active_strategy:
                strategy_name, config = active_strategy
                self.logger.info(f"시간대 기반 TTL 전략 적용: {strategy_name} ({config['reason']})")
                
                # 시간대별 TTL 조정 정보를 Redis에 저장
                time_strategy_key = "cache_optimization:time_strategy"
                strategy_data = {
                    "strategy": strategy_name,
                    "multiplier": config["multiplier"],
                    "reason": config["reason"],
                    "applied_at": datetime.now().isoformat(),
                    "active_hour": current_hour
                }
                
                await self.redis_client.client.setex(
                    time_strategy_key,
                    3600,  # 1시간
                    json.dumps(strategy_data)
                )
            
        except Exception as e:
            self.logger.error(f"시간대 기반 TTL 전략 적용 실패: {e}")


# 싱글톤 인스턴스
_smart_ttl_optimizer: Optional[SmartCacheTTLOptimizer] = None


def get_smart_ttl_optimizer() -> SmartCacheTTLOptimizer:
    """스마트 TTL 최적화 시스템 싱글톤 인스턴스 반환"""
    global _smart_ttl_optimizer
    if _smart_ttl_optimizer is None:
        _smart_ttl_optimizer = SmartCacheTTLOptimizer()
    return _smart_ttl_optimizer


# 편의 함수
async def get_optimal_cache_ttl(
    cache_key: str,
    api_provider: Optional[APIProvider] = None,
    endpoint: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> int:
    """최적 캐시 TTL 조회 편의 함수"""
    optimizer = get_smart_ttl_optimizer()
    return optimizer.get_optimal_ttl(cache_key, api_provider, endpoint, context)


async def update_cache_access_stats(cache_key: str, was_hit: bool):
    """캐시 접근 통계 업데이트 편의 함수"""
    optimizer = get_smart_ttl_optimizer()
    await optimizer.update_usage_stats(cache_key, was_hit)