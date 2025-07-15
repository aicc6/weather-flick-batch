"""
고급 캐시 관리 시스템

Redis 캐시의 성능을 최적화하고 고급 캐싱 전략을 제공하는 모듈입니다.
- 캐시 워밍 (Cache Warming)
- 스마트 무효화 (Smart Invalidation)
- 분산 락 (Distributed Lock)
- 배치 캐시 작업 (Batch Cache Operations)
- 캐시 성능 모니터링 (Performance Monitoring)
"""

import asyncio
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass
from contextlib import asynccontextmanager
from enum import Enum

from utils.redis_client import RedisClient


class CacheStrategy(Enum):
    """캐시 전략 열거형"""
    WRITE_THROUGH = "write_through"  # 쓰기 즉시 캐시 업데이트
    WRITE_BEHIND = "write_behind"    # 비동기 캐시 업데이트
    CACHE_ASIDE = "cache_aside"      # 필요 시 캐시 로드
    REFRESH_AHEAD = "refresh_ahead"  # 만료 전 미리 갱신


@dataclass
class CacheMetrics:
    """캐시 성능 메트릭"""
    hit_count: int = 0
    miss_count: int = 0
    total_requests: int = 0
    total_response_time_ms: float = 0
    cache_size_mb: float = 0
    memory_usage_percent: float = 0
    evictions: int = 0
    expired_keys: int = 0
    
    @property
    def hit_rate(self) -> float:
        """캐시 히트율"""
        if self.total_requests == 0:
            return 0.0
        return self.hit_count / self.total_requests
    
    @property
    def avg_response_time_ms(self) -> float:
        """평균 응답 시간"""
        if self.total_requests == 0:
            return 0.0
        return self.total_response_time_ms / self.total_requests


@dataclass 
class CacheWarming:
    """캐시 워밍 설정"""
    enabled: bool = True
    patterns: List[str] = None
    schedule_cron: str = "0 1 * * *"  # 매일 새벽 1시
    concurrent_workers: int = 5
    timeout_seconds: int = 300
    retry_attempts: int = 3
    
    def __post_init__(self):
        if self.patterns is None:
            self.patterns = [
                "api_cache:kma:*",
                "api_cache:kto:*",
                "weather_scores:*",
                "tourism_data:*"
            ]


@dataclass
class CacheInvalidation:
    """캐시 무효화 설정"""
    enabled: bool = True
    dependency_mapping: Dict[str, List[str]] = None
    batch_size: int = 100
    async_invalidation: bool = True
    cascade_invalidation: bool = True
    
    def __post_init__(self):
        if self.dependency_mapping is None:
            self.dependency_mapping = {
                "weather_data": ["weather_scores:*", "recommendations:*"],
                "tourist_attractions": ["tourism_data:*", "recommendations:*"],
                "regions": ["weather_data:*", "tourism_data:*"]
            }


class AdvancedCacheManager:
    """고급 캐시 관리자"""
    
    def __init__(self, redis_client: RedisClient = None):
        self.redis_client = redis_client or RedisClient()
        self.logger = logging.getLogger(__name__)
        
        # 성능 메트릭
        self._metrics = CacheMetrics()
        self._last_metrics_reset = datetime.now()
        
        # 설정
        self.warming_config = CacheWarming()
        self.invalidation_config = CacheInvalidation()
        
        # 분산 락 접두사
        self.lock_prefix = "cache_lock:"
        self.lock_timeout = 60  # 기본 락 타임아웃 60초
        
        # 배치 작업 큐
        self._batch_queue: List[Dict[str, Any]] = []
        self._batch_lock = asyncio.Lock()
        
    async def get_with_refresh_ahead(
        self, 
        key: str, 
        ttl: int,
        refresh_threshold: float = 0.8,
        refresh_func: Callable = None
    ) -> Optional[Any]:
        """Refresh-Ahead 전략으로 캐시 조회"""
        start_time = time.time()
        
        try:
            # 캐시에서 데이터와 TTL 확인
            pipe = self.redis_client.client.pipeline()
            pipe.get(key)
            pipe.ttl(key)
            cached_data, remaining_ttl = await pipe.execute()
            
            if cached_data:
                # 히트 카운트 증가
                self._metrics.hit_count += 1
                
                # TTL 기반 refresh-ahead 판단
                if remaining_ttl > 0 and remaining_ttl < (ttl * refresh_threshold):
                    # 백그라운드에서 미리 갱신
                    if refresh_func:
                        asyncio.create_task(self._background_refresh(key, refresh_func, ttl))
                
                return json.loads(cached_data) if cached_data else None
            else:
                # 미스 카운트 증가
                self._metrics.miss_count += 1
                return None
                
        except Exception as e:
            self.logger.error(f"Refresh-ahead 캐시 조회 실패 [{key}]: {e}")
            return None
        finally:
            # 성능 메트릭 업데이트
            response_time = (time.time() - start_time) * 1000
            self._update_metrics(response_time)
    
    async def _background_refresh(self, key: str, refresh_func: Callable, ttl: int):
        """백그라운드 캐시 갱신"""
        try:
            # 분산 락으로 중복 갱신 방지
            lock_key = f"{self.lock_prefix}refresh:{key}"
            async with self.acquire_lock(lock_key, timeout=30):
                
                # 갱신 함수 실행
                if asyncio.iscoroutinefunction(refresh_func):
                    new_data = await refresh_func()
                else:
                    new_data = refresh_func()
                
                # 캐시 업데이트
                if new_data is not None:
                    await self.set_cache(key, new_data, ttl)
                    self.logger.debug(f"백그라운드 캐시 갱신 완료: {key}")
                    
        except Exception as e:
            self.logger.warning(f"백그라운드 캐시 갱신 실패 [{key}]: {e}")
    
    @asynccontextmanager
    async def acquire_lock(self, lock_key: str, timeout: int = None):
        """분산 락 획득"""
        timeout = timeout or self.lock_timeout
        lock_value = f"{datetime.now().timestamp()}:{id(self)}"
        acquired = False
        
        try:
            # 락 획득 시도
            acquired = await self.redis_client.client.set(
                lock_key, 
                lock_value, 
                nx=True,  # 키가 없을 때만 설정
                ex=timeout  # 만료 시간 설정
            )
            
            if not acquired:
                raise RuntimeError(f"분산 락 획득 실패: {lock_key}")
            
            self.logger.debug(f"분산 락 획득: {lock_key}")
            yield
            
        finally:
            if acquired:
                # 락 해제 (원자적 해제를 위한 Lua 스크립트)
                lua_script = """
                if redis.call("GET", KEYS[1]) == ARGV[1] then
                    return redis.call("DEL", KEYS[1])
                else
                    return 0
                end
                """
                await self.redis_client.client.eval(lua_script, 1, lock_key, lock_value)
                self.logger.debug(f"분산 락 해제: {lock_key}")
    
    async def batch_set(self, items: Dict[str, Dict[str, Any]], pipeline_size: int = 100):
        """배치 캐시 설정"""
        try:
            items_list = list(items.items())
            
            # 파이프라인으로 배치 처리
            for i in range(0, len(items_list), pipeline_size):
                batch = items_list[i:i + pipeline_size]
                
                pipe = self.redis_client.client.pipeline()
                for key, cache_item in batch:
                    ttl = cache_item.get('ttl', 3600)
                    data = cache_item.get('data')
                    
                    if data is not None:
                        pipe.setex(key, ttl, json.dumps(data))
                
                await pipe.execute()
                
            self.logger.info(f"배치 캐시 설정 완료: {len(items)}건")
            
        except Exception as e:
            self.logger.error(f"배치 캐시 설정 실패: {e}")
    
    async def batch_delete(self, patterns: List[str], batch_size: int = 100):
        """배치 캐시 삭제"""
        try:
            all_keys = set()
            
            # 패턴별 키 수집
            for pattern in patterns:
                keys = await self.redis_client.client.keys(pattern)
                all_keys.update(keys)
            
            if not all_keys:
                self.logger.info("삭제할 캐시 키가 없습니다")
                return 0
            
            # 배치 삭제
            keys_list = list(all_keys)
            deleted_count = 0
            
            for i in range(0, len(keys_list), batch_size):
                batch = keys_list[i:i + batch_size]
                if batch:
                    deleted_count += await self.redis_client.client.delete(*batch)
            
            self.logger.info(f"배치 캐시 삭제 완료: {deleted_count}건")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"배치 캐시 삭제 실패: {e}")
            return 0
    
    async def invalidate_by_dependency(self, changed_key: str):
        """의존성 기반 캐시 무효화"""
        if not self.invalidation_config.enabled:
            return
        
        try:
            # 의존성 매핑에서 무효화할 패턴 찾기
            invalidation_patterns = []
            
            for dependency_key, patterns in self.invalidation_config.dependency_mapping.items():
                if dependency_key in changed_key:
                    invalidation_patterns.extend(patterns)
            
            if invalidation_patterns:
                if self.invalidation_config.async_invalidation:
                    # 비동기 무효화
                    asyncio.create_task(self.batch_delete(invalidation_patterns))
                else:
                    # 동기 무효화
                    await self.batch_delete(invalidation_patterns)
                
                self.logger.info(f"의존성 기반 캐시 무효화: {changed_key} -> {len(invalidation_patterns)} 패턴")
            
        except Exception as e:
            self.logger.error(f"의존성 기반 캐시 무효화 실패: {e}")
    
    async def warm_cache(
        self, 
        warming_functions: Dict[str, Callable],
        patterns: List[str] = None
    ):
        """캐시 워밍 실행"""
        if not self.warming_config.enabled:
            return
        
        try:
            patterns = patterns or self.warming_config.patterns
            
            # 기존 캐시 키 확인
            existing_keys = set()
            for pattern in patterns:
                keys = await self.redis_client.client.keys(pattern)
                existing_keys.update(keys)
            
            # 워밍할 키 결정
            warming_tasks = []
            
            for func_name, warming_func in warming_functions.items():
                # 워밍 함수 실행하여 데이터 생성
                if asyncio.iscoroutinefunction(warming_func):
                    warming_data = await warming_func()
                else:
                    warming_data = warming_func()
                
                if warming_data:
                    warming_tasks.append(
                        self._execute_warming_task(func_name, warming_data)
                    )
            
            # 동시 실행 (워커 수 제한)
            if warming_tasks:
                semaphore = asyncio.Semaphore(self.warming_config.concurrent_workers)
                
                async def limited_task(task):
                    async with semaphore:
                        return await task
                
                results = await asyncio.gather(
                    *[limited_task(task) for task in warming_tasks],
                    return_exceptions=True
                )
                
                # 결과 분석
                success_count = sum(1 for r in results if not isinstance(r, Exception))
                error_count = len(results) - success_count
                
                self.logger.info(
                    f"캐시 워밍 완료: 성공 {success_count}건, 실패 {error_count}건"
                )
            
        except Exception as e:
            self.logger.error(f"캐시 워밍 실패: {e}")
    
    async def _execute_warming_task(self, task_name: str, warming_data: Dict[str, Any]):
        """개별 워밍 작업 실행"""
        try:
            cache_items = {}
            
            for key, data in warming_data.items():
                # TTL 결정 (데이터 타입별 차별화)
                if "weather" in key:
                    ttl = 1800  # 30분
                elif "tourism" in key:
                    ttl = 7200  # 2시간
                elif "recommendation" in key:
                    ttl = 3600  # 1시간
                else:
                    ttl = 3600  # 기본 1시간
                
                cache_items[key] = {'data': data, 'ttl': ttl}
            
            # 배치 캐시 설정
            await self.batch_set(cache_items)
            
            self.logger.debug(f"워밍 작업 완료: {task_name}, {len(cache_items)}건")
            return True
            
        except Exception as e:
            self.logger.error(f"워밍 작업 실패 [{task_name}]: {e}")
            return False
    
    async def get_cache_metrics(self) -> CacheMetrics:
        """캐시 성능 메트릭 조회"""
        try:
            # 동기 Redis 클라이언트를 통한 정보 수집
            info = self.redis_client.get_info()
            
            if info:
                # 메모리 사용량 계산 (간단한 방식)
                used_memory_human = info.get('used_memory_human', '0B')
                
                # 메트릭 업데이트
                self._metrics.evictions = info.get('evicted_keys', 0)
                self._metrics.expired_keys = info.get('expired_keys', 0)
            
            return self._metrics
            
        except Exception as e:
            self.logger.error(f"캐시 메트릭 조회 실패: {e}")
            return self._metrics
    
    def _update_metrics(self, response_time_ms: float):
        """메트릭 업데이트"""
        self._metrics.total_requests += 1
        self._metrics.total_response_time_ms += response_time_ms
    
    async def reset_metrics(self):
        """메트릭 초기화"""
        self._metrics = CacheMetrics()
        self._last_metrics_reset = datetime.now()
        self.logger.info("캐시 성능 메트릭이 초기화되었습니다")
    
    async def set(self, key: str, value: Any, ttl: int = 3600) -> bool:
        """
        캐시에 데이터 저장 (기본 set 메서드)
        
        Args:
            key: 캐시 키
            value: 저장할 데이터
            ttl: 만료 시간 (초)
            
        Returns:
            bool: 저장 성공 여부
        """
        start_time = time.time()
        
        try:
            # 동기 Redis 클라이언트 사용
            result = self.redis_client.set_cache(key, value, ttl)
            
            # 의존성 기반 무효화 트리거 (비동기 처리)
            if self.invalidation_config.enabled:
                asyncio.create_task(self.invalidate_by_dependency(key))
            
            if result:
                self.logger.debug(f"캐시 저장 성공: {key} (TTL: {ttl}초)")
            else:
                self.logger.warning(f"캐시 저장 실패: {key}")
            
            return result
                
        except Exception as e:
            self.logger.error(f"캐시 저장 실패 [{key}]: {e}")
            return False
        finally:
            # 성능 메트릭 업데이트
            response_time = (time.time() - start_time) * 1000
            self._update_metrics(response_time)
    
    async def get(self, key: str, default: Any = None) -> Any:
        """
        캐시에서 데이터 조회 (기본 get 메서드)
        
        Args:
            key: 캐시 키
            default: 키가 없을 때 반환할 기본값
            
        Returns:
            Any: 캐시된 데이터 또는 기본값
        """
        start_time = time.time()
        
        try:
            # 동기 Redis 클라이언트 사용
            cached_data = self.redis_client.get_cache(key)
            
            if cached_data is not None:
                # 히트 카운트 증가
                self._metrics.hit_count += 1
                self.logger.debug(f"캐시 조회 성공: {key}")
                return cached_data
            else:
                # 미스 카운트 증가
                self._metrics.miss_count += 1
                self.logger.debug(f"캐시 미스: {key}")
                return default
                
        except Exception as e:
            self.logger.error(f"캐시 조회 실패 [{key}]: {e}")
            self._metrics.miss_count += 1
            return default
        finally:
            # 성능 메트릭 업데이트
            response_time = (time.time() - start_time) * 1000
            self._update_metrics(response_time)
    
    async def delete(self, key: str) -> bool:
        """
        캐시에서 특정 키 삭제
        
        Args:
            key: 삭제할 캐시 키
            
        Returns:
            bool: 삭제 성공 여부
        """
        try:
            # 동기 Redis 클라이언트 사용
            result = self.redis_client.delete_cache(key)
            
            if result:
                self.logger.debug(f"캐시 삭제 성공: {key}")
                
                # 의존성 기반 무효화 트리거 (비동기 처리)
                if self.invalidation_config.enabled:
                    asyncio.create_task(self.invalidate_by_dependency(key))
                
                return True
            else:
                self.logger.debug(f"캐시 키가 존재하지 않음: {key}")
                return False
                
        except Exception as e:
            self.logger.error(f"캐시 삭제 실패 [{key}]: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        캐시 키 존재 여부 확인
        
        Args:
            key: 확인할 캐시 키
            
        Returns:
            bool: 키 존재 여부
        """
        try:
            # 동기 Redis 클라이언트 사용
            result = self.redis_client.exists(key)
            return result
        except Exception as e:
            self.logger.error(f"캐시 키 존재 확인 실패 [{key}]: {e}")
            return False
    
    async def expire(self, key: str, ttl: int) -> bool:
        """
        캐시 키의 만료 시간 설정
        
        Args:
            key: 캐시 키
            ttl: 만료 시간 (초)
            
        Returns:
            bool: 설정 성공 여부
        """
        try:
            # 동기 Redis 클라이언트를 통해 직접 접근
            client = self.redis_client.get_client()
            if not client:
                self.logger.warning(f"Redis 클라이언트 없음, 만료 시간 설정 실패: {key}")
                return False
                
            result = client.expire(key, ttl)
            
            if result:
                self.logger.debug(f"캐시 만료 시간 설정: {key} (TTL: {ttl}초)")
                return True
            else:
                self.logger.warning(f"캐시 키가 존재하지 않아 만료 시간 설정 실패: {key}")
                return False
                
        except Exception as e:
            self.logger.error(f"캐시 만료 시간 설정 실패 [{key}]: {e}")
            return False
    
    async def get_ttl(self, key: str) -> int:
        """
        캐시 키의 남은 만료 시간 조회
        
        Args:
            key: 캐시 키
            
        Returns:
            int: 남은 만료 시간 (초), -1: 만료 시간 없음, -2: 키 없음
        """
        try:
            # 동기 Redis 클라이언트를 통해 직접 접근
            client = self.redis_client.get_client()
            if not client:
                self.logger.warning(f"Redis 클라이언트 없음, TTL 조회 실패: {key}")
                return -2
                
            result = client.ttl(key)
            return result
        except Exception as e:
            self.logger.error(f"캐시 TTL 조회 실패 [{key}]: {e}")
            return -2
    
    async def set_cache(self, key: str, data: Any, ttl: int = 3600) -> bool:
        """
        캐시 설정 (기존 호환성을 위한 메서드)
        
        Args:
            key: 캐시 키
            data: 저장할 데이터
            ttl: 만료 시간 (초)
            
        Returns:
            bool: 저장 성공 여부
        """
        return await self.set(key, data, ttl)
    
    async def get_cache(self, key: str, default: Any = None) -> Any:
        """
        캐시 조회 (기존 호환성을 위한 메서드)
        
        Args:
            key: 캐시 키
            default: 기본값
            
        Returns:
            Any: 캐시된 데이터 또는 기본값
        """
        return await self.get(key, default)

    async def get_cache_health(self) -> Dict[str, Any]:
        """캐시 시스템 건강 상태 조회"""
        try:
            metrics = await self.get_cache_metrics()
            info = self.redis_client.get_info()
            
            # Redis 클라이언트 상태 확인
            client = self.redis_client.get_client()
            is_healthy = False
            
            if client:
                try:
                    client.ping()
                    is_healthy = True
                except Exception:
                    is_healthy = False
            
            return {
                "status": "healthy" if is_healthy else "unhealthy",
                "hit_rate": metrics.hit_rate,
                "memory_usage_percent": metrics.memory_usage_percent,
                "connected_clients": info.get('connected_clients', 0),
                "total_commands_processed": info.get('total_commands_processed', 0),
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0),
                "evicted_keys": info.get('evicted_keys', 0),
                "last_metrics_reset": self._last_metrics_reset.isoformat(),
                "redis_version": info.get('redis_version', 'unknown')
            }
            
        except Exception as e:
            self.logger.error(f"캐시 건강 상태 조회 실패: {e}")
            return {"status": "error", "error": str(e)}


# 싱글톤 인스턴스
_advanced_cache_manager: Optional[AdvancedCacheManager] = None


def get_advanced_cache_manager() -> AdvancedCacheManager:
    """고급 캐시 매니저 싱글톤 인스턴스 반환"""
    global _advanced_cache_manager
    if _advanced_cache_manager is None:
        _advanced_cache_manager = AdvancedCacheManager()
    return _advanced_cache_manager


# 편의 함수들
async def set_cache(key: str, data: Any, ttl: int = 3600):
    """캐시 설정 편의 함수"""
    manager = get_advanced_cache_manager()
    return await manager.redis_client.set_cache(key, data, ttl)


async def get_cache(key: str) -> Optional[Any]:
    """캐시 조회 편의 함수"""
    manager = get_advanced_cache_manager()
    return await manager.redis_client.get_cache(key)


async def invalidate_cache(pattern: str):
    """캐시 무효화 편의 함수"""
    manager = get_advanced_cache_manager()
    return await manager.batch_delete([pattern])