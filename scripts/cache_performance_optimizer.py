#!/usr/bin/env python3
"""
캐시 성능 최적화 스크립트

Redis 캐시 시스템의 성능을 분석하고 최적화하는 통합 도구입니다.
- 캐시 성능 분석 및 보고서 생성
- 캐시 워밍 실행
- TTL 최적화 제안
- 메모리 사용량 최적화
- 캐시 무효화 전략 테스트

실행 방법:
python scripts/cache_performance_optimizer.py [--action analyze|warm|optimize|test] [--config-level aggressive]

옵션:
--action: 실행할 작업 (analyze, warm, optimize, test, monitor)
--config-level: 최적화 레벨 (conservative, balanced, aggressive)
--duration: 모니터링 지속 시간 (분, 기본: 60)
--report: 보고서 저장 (기본: True)
"""

import sys
import argparse
import asyncio
import time
import json
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from app.core.advanced_cache_manager import (
    get_advanced_cache_manager, 
    CacheWarming
)
from app.core.cache_monitoring import get_cache_monitor
from app.core.logger import get_logger
from utils.redis_client import RedisClient


class CachePerformanceOptimizer:
    """캐시 성능 최적화 도구"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.cache_manager = get_advanced_cache_manager()
        self.cache_monitor = get_cache_monitor()
        self.redis_client = RedisClient()
        
        # 최적화 설정
        self.optimization_levels = {
            "conservative": {
                "batch_size": 50,
                "concurrent_workers": 2,
                "warming_enabled": True,
                "aggressive_ttl": False,
                "memory_threshold": 70
            },
            "balanced": {
                "batch_size": 100,
                "concurrent_workers": 5,
                "warming_enabled": True,
                "aggressive_ttl": True,
                "memory_threshold": 80
            },
            "aggressive": {
                "batch_size": 200,
                "concurrent_workers": 10,
                "warming_enabled": True,
                "aggressive_ttl": True,
                "memory_threshold": 90
            }
        }
    
    async def analyze_cache_performance(self, save_report: bool = True) -> Dict[str, Any]:
        """캐시 성능 종합 분석"""
        self.logger.info("캐시 성능 분석을 시작합니다...")
        
        analysis_result = {
            "timestamp": datetime.now().isoformat(),
            "redis_info": {},
            "performance_metrics": {},
            "optimization_suggestions": [],
            "health_status": {},
            "memory_analysis": {},
            "hit_rate_analysis": {},
            "bottleneck_analysis": {}
        }
        
        try:
            # 1. Redis 기본 정보 수집
            analysis_result["redis_info"] = await self._analyze_redis_info()
            
            # 2. 성능 메트릭 수집
            analysis_result["performance_metrics"] = await self._collect_performance_metrics()
            
            # 3. 메모리 사용량 분석
            analysis_result["memory_analysis"] = await self._analyze_memory_usage()
            
            # 4. 히트율 분석
            analysis_result["hit_rate_analysis"] = await self._analyze_hit_rate()
            
            # 5. 병목 현상 분석
            analysis_result["bottleneck_analysis"] = await self._analyze_bottlenecks()
            
            # 6. 건강 상태 확인
            analysis_result["health_status"] = await self.cache_manager.get_cache_health()
            
            # 7. 최적화 제안 생성
            analysis_result["optimization_suggestions"] = await self.cache_monitor.generate_optimization_suggestions()
            
            # 8. 보고서 저장
            if save_report:
                await self._save_analysis_report(analysis_result)
            
            self.logger.info("캐시 성능 분석이 완료되었습니다")
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"캐시 성능 분석 실패: {e}")
            analysis_result["error"] = str(e)
            return analysis_result
    
    async def _analyze_redis_info(self) -> Dict[str, Any]:
        """Redis 기본 정보 분석"""
        try:
            info = await self.redis_client.client.info()
            
            return {
                "version": info.get("redis_version", "unknown"),
                "mode": info.get("redis_mode", "unknown"),
                "uptime_days": info.get("uptime_in_seconds", 0) / 86400,
                "connected_clients": info.get("connected_clients", 0),
                "blocked_clients": info.get("blocked_clients", 0),
                "used_memory_mb": info.get("used_memory", 0) / 1024 / 1024,
                "used_memory_peak_mb": info.get("used_memory_peak", 0) / 1024 / 1024,
                "maxmemory_mb": info.get("maxmemory", 0) / 1024 / 1024,
                "total_commands_processed": info.get("total_commands_processed", 0),
                "instantaneous_ops_per_sec": info.get("instantaneous_ops_per_sec", 0),
                "keyspace_hits": info.get("keyspace_hits", 0),
                "keyspace_misses": info.get("keyspace_misses", 0),
                "evicted_keys": info.get("evicted_keys", 0),
                "expired_keys": info.get("expired_keys", 0)
            }
            
        except Exception as e:
            self.logger.error(f"Redis 정보 수집 실패: {e}")
            return {"error": str(e)}
    
    async def _collect_performance_metrics(self) -> Dict[str, Any]:
        """성능 메트릭 수집"""
        try:
            # 캐시 매니저 메트릭
            cache_metrics = await self.cache_manager.get_cache_metrics()
            
            # 성능 요약 (최근 1시간)
            performance_summary = await self.cache_monitor.get_performance_summary(hours=1)
            
            return {
                "cache_metrics": {
                    "hit_count": cache_metrics.hit_count,
                    "miss_count": cache_metrics.miss_count,
                    "hit_rate": cache_metrics.hit_rate,
                    "total_requests": cache_metrics.total_requests,
                    "avg_response_time_ms": cache_metrics.avg_response_time_ms,
                    "cache_size_mb": cache_metrics.cache_size_mb,
                    "memory_usage_percent": cache_metrics.memory_usage_percent
                },
                "performance_summary": performance_summary
            }
            
        except Exception as e:
            self.logger.error(f"성능 메트릭 수집 실패: {e}")
            return {"error": str(e)}
    
    async def _analyze_memory_usage(self) -> Dict[str, Any]:
        """메모리 사용량 분석"""
        try:
            info = await self.redis_client.client.info("memory")
            
            used_memory = info.get("used_memory", 0)
            used_memory_peak = info.get("used_memory_peak", 0)
            maxmemory = info.get("maxmemory", 0)
            
            # 데이터베이스별 키 개수
            db_info = await self.redis_client.client.info("keyspace")
            keyspace_analysis = {}
            
            for key, value in db_info.items():
                if key.startswith("db"):
                    # 예: db0: keys=1000,expires=100,avg_ttl=3600
                    keyspace_analysis[key] = value
            
            # 메모리 효율성 계산
            memory_efficiency = "good"
            usage_percent = (used_memory / maxmemory * 100) if maxmemory > 0 else 0
            
            if usage_percent > 90:
                memory_efficiency = "critical"
            elif usage_percent > 80:
                memory_efficiency = "warning"
            elif usage_percent > 70:
                memory_efficiency = "moderate"
            
            return {
                "used_memory_mb": used_memory / 1024 / 1024,
                "used_memory_peak_mb": used_memory_peak / 1024 / 1024,
                "maxmemory_mb": maxmemory / 1024 / 1024,
                "usage_percent": usage_percent,
                "memory_efficiency": memory_efficiency,
                "fragmentation_ratio": info.get("mem_fragmentation_ratio", 0),
                "keyspace_analysis": keyspace_analysis,
                "rss_overhead_bytes": info.get("used_memory_rss", 0) - used_memory,
                "dataset_overhead_bytes": info.get("used_memory_overhead", 0)
            }
            
        except Exception as e:
            self.logger.error(f"메모리 사용량 분석 실패: {e}")
            return {"error": str(e)}
    
    async def _analyze_hit_rate(self) -> Dict[str, Any]:
        """히트율 분석"""
        try:
            info = await self.redis_client.client.info("stats")
            
            hits = info.get("keyspace_hits", 0)
            misses = info.get("keyspace_misses", 0)
            total_requests = hits + misses
            
            hit_rate = (hits / total_requests * 100) if total_requests > 0 else 0
            
            # 히트율 품질 평가
            hit_quality = "excellent"
            if hit_rate < 50:
                hit_quality = "poor"
            elif hit_rate < 70:
                hit_quality = "fair"
            elif hit_rate < 85:
                hit_quality = "good"
            
            # 패턴별 히트율 분석 (예상)
            pattern_analysis = await self._analyze_key_patterns()
            
            return {
                "total_hits": hits,
                "total_misses": misses,
                "total_requests": total_requests,
                "hit_rate_percent": hit_rate,
                "hit_quality": hit_quality,
                "pattern_analysis": pattern_analysis,
                "improvement_potential": 100 - hit_rate if hit_rate < 95 else 0
            }
            
        except Exception as e:
            self.logger.error(f"히트율 분석 실패: {e}")
            return {"error": str(e)}
    
    async def _analyze_key_patterns(self) -> Dict[str, Any]:
        """키 패턴 분석"""
        try:
            # 주요 패턴별 키 개수 조사
            patterns = {
                "api_cache:kma:*": "기상청 API 캐시",
                "api_cache:kto:*": "관광공사 API 캐시", 
                "weather_scores:*": "날씨 점수 캐시",
                "tourism_data:*": "관광 데이터 캐시",
                "recommendations:*": "추천 데이터 캐시"
            }
            
            pattern_stats = {}
            
            for pattern, description in patterns.items():
                keys = await self.redis_client.client.keys(pattern)
                
                # 샘플 키들의 TTL 확인
                ttl_stats = []
                if keys:
                    sample_size = min(10, len(keys))
                    sample_keys = keys[:sample_size]
                    
                    for key in sample_keys:
                        ttl = await self.redis_client.client.ttl(key)
                        if ttl > 0:
                            ttl_stats.append(ttl)
                
                pattern_stats[pattern] = {
                    "description": description,
                    "key_count": len(keys),
                    "avg_ttl": sum(ttl_stats) / len(ttl_stats) if ttl_stats else 0,
                    "has_expiry": len(ttl_stats) > 0
                }
            
            return pattern_stats
            
        except Exception as e:
            self.logger.error(f"키 패턴 분석 실패: {e}")
            return {"error": str(e)}
    
    async def _analyze_bottlenecks(self) -> Dict[str, Any]:
        """병목 현상 분석"""
        try:
            info = await self.redis_client.client.info()
            
            # CPU 사용률 (Redis 내부)
            used_cpu_sys = info.get("used_cpu_sys", 0)
            used_cpu_user = info.get("used_cpu_user", 0)
            
            # 네트워크 처리량
            total_net_input_bytes = info.get("total_net_input_bytes", 0)
            total_net_output_bytes = info.get("total_net_output_bytes", 0)
            
            # 명령어 처리 통계
            total_commands_processed = info.get("total_commands_processed", 0)
            instantaneous_ops_per_sec = info.get("instantaneous_ops_per_sec", 0)
            
            # 클라이언트 연결
            connected_clients = info.get("connected_clients", 0)
            blocked_clients = info.get("blocked_clients", 0)
            
            bottlenecks = []
            
            # 병목 상황 감지
            if connected_clients > 100:
                bottlenecks.append("높은 클라이언트 연결 수")
            
            if blocked_clients > 0:
                bottlenecks.append("블록된 클라이언트 존재")
            
            if instantaneous_ops_per_sec > 10000:
                bottlenecks.append("높은 초당 명령어 처리량")
            
            # 메모리 압박
            used_memory = info.get("used_memory", 0)
            maxmemory = info.get("maxmemory", 0)
            if maxmemory > 0 and (used_memory / maxmemory) > 0.9:
                bottlenecks.append("메모리 사용량 90% 초과")
            
            return {
                "cpu_usage": {
                    "system": used_cpu_sys,
                    "user": used_cpu_user
                },
                "network_throughput": {
                    "input_bytes": total_net_input_bytes,
                    "output_bytes": total_net_output_bytes
                },
                "command_processing": {
                    "total_processed": total_commands_processed,
                    "ops_per_sec": instantaneous_ops_per_sec
                },
                "client_connections": {
                    "connected": connected_clients,
                    "blocked": blocked_clients
                },
                "identified_bottlenecks": bottlenecks,
                "performance_score": max(0, 100 - len(bottlenecks) * 20)
            }
            
        except Exception as e:
            self.logger.error(f"병목 현상 분석 실패: {e}")
            return {"error": str(e)}
    
    async def execute_cache_warming(self, config_level: str = "balanced") -> Dict[str, Any]:
        """캐시 워밍 실행"""
        self.logger.info(f"캐시 워밍을 시작합니다 (레벨: {config_level})")
        
        try:
            config = self.optimization_levels.get(config_level, self.optimization_levels["balanced"])
            
            # 워밍 함수들 정의
            warming_functions = {
                "weather_data": self._warm_weather_cache,
                "tourism_data": self._warm_tourism_cache,
                "api_metadata": self._warm_api_metadata_cache
            }
            
            # 캐시 워밍 설정 업데이트
            warming_config = CacheWarming(
                enabled=config["warming_enabled"],
                concurrent_workers=config["concurrent_workers"]
            )
            
            self.cache_manager.warming_config = warming_config
            
            # 워밍 실행
            start_time = time.time()
            await self.cache_manager.warm_cache(warming_functions)
            execution_time = time.time() - start_time
            
            # 워밍 후 성능 확인
            post_warming_metrics = await self.cache_manager.get_cache_metrics()
            
            result = {
                "success": True,
                "config_level": config_level,
                "execution_time_seconds": execution_time,
                "functions_executed": list(warming_functions.keys()),
                "post_warming_metrics": {
                    "cache_size_mb": post_warming_metrics.cache_size_mb,
                    "memory_usage_percent": post_warming_metrics.memory_usage_percent
                }
            }
            
            self.logger.info(f"캐시 워밍 완료: {execution_time:.2f}초 소요")
            return result
            
        except Exception as e:
            self.logger.error(f"캐시 워밍 실패: {e}")
            return {"success": False, "error": str(e)}
    
    async def _warm_weather_cache(self) -> Dict[str, Any]:
        """날씨 데이터 캐시 워밍"""
        try:
            # 주요 지역의 날씨 데이터 미리 캐싱
            regions = ["서울", "부산", "대구", "인천", "광주", "대전", "울산"]
            warming_data = {}
            
            for region in regions:
                # 현재 날씨 캐시 키
                current_key = f"weather_cache:current:{region}"
                warming_data[current_key] = {
                    "region": region,
                    "temperature": 20.0,
                    "humidity": 60,
                    "condition": "맑음",
                    "cached_at": datetime.now().isoformat()
                }
                
                # 예보 데이터 캐시 키
                forecast_key = f"weather_cache:forecast:{region}"
                warming_data[forecast_key] = {
                    "region": region,
                    "forecasts": [
                        {"date": "2024-01-01", "temp": 15, "condition": "흐림"},
                        {"date": "2024-01-02", "temp": 18, "condition": "맑음"}
                    ],
                    "cached_at": datetime.now().isoformat()
                }
            
            return warming_data
            
        except Exception as e:
            self.logger.error(f"날씨 캐시 워밍 실패: {e}")
            return {}
    
    async def _warm_tourism_cache(self) -> Dict[str, Any]:
        """관광 데이터 캐시 워밍"""
        try:
            # 인기 관광지 데이터 미리 캐싱
            attractions = ["경복궁", "제주도", "부산해변", "설악산", "한라산"]
            warming_data = {}
            
            for attraction in attractions:
                cache_key = f"tourism_cache:attraction:{attraction}"
                warming_data[cache_key] = {
                    "name": attraction,
                    "category": "관광지",
                    "rating": 4.5,
                    "cached_at": datetime.now().isoformat()
                }
            
            return warming_data
            
        except Exception as e:
            self.logger.error(f"관광 캐시 워밍 실패: {e}")
            return {}
    
    async def _warm_api_metadata_cache(self) -> Dict[str, Any]:
        """API 메타데이터 캐시 워밍"""
        try:
            # API 설정 및 메타데이터 캐싱
            warming_data = {
                "api_config:kma": {
                    "endpoint": "weather",
                    "rate_limit": 1000,
                    "cached_at": datetime.now().isoformat()
                },
                "api_config:kto": {
                    "endpoint": "tourism",
                    "rate_limit": 1000,
                    "cached_at": datetime.now().isoformat()
                }
            }
            
            return warming_data
            
        except Exception as e:
            self.logger.error(f"API 메타데이터 캐시 워밍 실패: {e}")
            return {}
    
    async def optimize_ttl_settings(self, config_level: str = "balanced") -> Dict[str, Any]:
        """TTL 설정 최적화"""
        self.logger.info(f"TTL 설정 최적화를 시작합니다 (레벨: {config_level})")
        
        try:
            config = self.optimization_levels.get(config_level, self.optimization_levels["balanced"])
            
            # 현재 키 패턴별 TTL 분석
            pattern_analysis = await self._analyze_key_patterns()
            
            # 최적화된 TTL 제안
            ttl_recommendations = {}
            
            if config["aggressive_ttl"]:
                # 공격적 TTL (더 긴 캐시 유지)
                ttl_recommendations = {
                    "api_cache:kma:*": 7200,    # 2시간 (기존 30분에서 증가)
                    "api_cache:kto:*": 86400,   # 24시간 (기존 6시간에서 증가)
                    "weather_scores:*": 10800,  # 3시간 (기존 1시간에서 증가)
                    "tourism_data:*": 21600,    # 6시간 (기존 2시간에서 증가)
                    "recommendations:*": 7200   # 2시간 (기존 30분에서 증가)
                }
            else:
                # 보수적 TTL (적절한 캐시 유지)
                ttl_recommendations = {
                    "api_cache:kma:*": 3600,    # 1시간
                    "api_cache:kto:*": 14400,   # 4시간
                    "weather_scores:*": 1800,   # 30분
                    "tourism_data:*": 7200,     # 2시간
                    "recommendations:*": 3600   # 1시간
                }
            
            # TTL 최적화 적용 (기존 키들의 TTL 업데이트)
            optimization_results = []
            
            for pattern, recommended_ttl in ttl_recommendations.items():
                keys = await self.redis_client.client.keys(pattern)
                updated_count = 0
                
                for key in keys[:100]:  # 배치 크기 제한
                    try:
                        await self.redis_client.client.expire(key, recommended_ttl)
                        updated_count += 1
                    except Exception as e:
                        self.logger.warning(f"TTL 업데이트 실패 [{key}]: {e}")
                
                optimization_results.append({
                    "pattern": pattern,
                    "recommended_ttl": recommended_ttl,
                    "keys_found": len(keys),
                    "keys_updated": updated_count
                })
            
            return {
                "success": True,
                "config_level": config_level,
                "ttl_recommendations": ttl_recommendations,
                "optimization_results": optimization_results,
                "pattern_analysis": pattern_analysis
            }
            
        except Exception as e:
            self.logger.error(f"TTL 최적화 실패: {e}")
            return {"success": False, "error": str(e)}
    
    async def test_cache_invalidation(self) -> Dict[str, Any]:
        """캐시 무효화 전략 테스트"""
        self.logger.info("캐시 무효화 전략 테스트를 시작합니다")
        
        try:
            # 테스트용 캐시 데이터 생성
            test_keys = [
                "test_cache:data:1",
                "test_cache:data:2", 
                "test_cache:dependency:1",
                "test_cache:dependency:2"
            ]
            
            # 테스트 데이터 설정
            for key in test_keys:
                await self.cache_manager.set_cache(key, {"test": True, "created": datetime.now().isoformat()}, 300)
            
            # 무효화 전 상태 확인
            before_invalidation = {}
            for key in test_keys:
                before_invalidation[key] = await self.cache_manager.get_cache(key) is not None
            
            # 의존성 기반 무효화 테스트
            await self.cache_manager.invalidate_by_dependency("test_cache:data")
            
            # 무효화 후 상태 확인
            after_invalidation = {}
            for key in test_keys:
                after_invalidation[key] = await self.cache_manager.get_cache(key) is not None
            
            # 배치 삭제 테스트
            await self.cache_manager.batch_delete(["test_cache:*"])
            
            # 최종 상태 확인
            after_batch_delete = {}
            for key in test_keys:
                after_batch_delete[key] = await self.cache_manager.get_cache(key) is not None
            
            return {
                "success": True,
                "test_keys": test_keys,
                "before_invalidation": before_invalidation,
                "after_invalidation": after_invalidation,
                "after_batch_delete": after_batch_delete,
                "invalidation_effective": sum(before_invalidation.values()) > sum(after_invalidation.values()),
                "batch_delete_effective": sum(after_batch_delete.values()) == 0
            }
            
        except Exception as e:
            self.logger.error(f"캐시 무효화 테스트 실패: {e}")
            return {"success": False, "error": str(e)}
    
    async def monitor_real_time(self, duration_minutes: int = 60) -> Dict[str, Any]:
        """실시간 모니터링"""
        self.logger.info(f"실시간 캐시 모니터링을 시작합니다 ({duration_minutes}분)")
        
        try:
            # 모니터링 시작
            await self.cache_monitor.start_monitoring()
            
            # 지정된 시간만큼 대기
            await asyncio.sleep(duration_minutes * 60)
            
            # 모니터링 중지
            await self.cache_monitor.stop_monitoring()
            
            # 모니터링 결과 수집
            performance_summary = await self.cache_monitor.get_performance_summary(hours=duration_minutes/60)
            optimization_suggestions = await self.cache_monitor.generate_optimization_suggestions()
            alert_history = await self.cache_monitor.get_alert_history(days=1)
            
            return {
                "success": True,
                "duration_minutes": duration_minutes,
                "performance_summary": performance_summary,
                "optimization_suggestions": optimization_suggestions,
                "alert_history": alert_history,
                "monitoring_completed": True
            }
            
        except Exception as e:
            self.logger.error(f"실시간 모니터링 실패: {e}")
            await self.cache_monitor.stop_monitoring()
            return {"success": False, "error": str(e)}
    
    async def _save_analysis_report(self, analysis_result: Dict[str, Any]):
        """분석 보고서 저장"""
        try:
            # 보고서 파일명
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = project_root / "logs" / f"cache_analysis_report_{timestamp}.json"
            report_file.parent.mkdir(exist_ok=True)
            
            # JSON 형태로 저장
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(analysis_result, f, indent=2, ensure_ascii=False, default=str)
            
            # 텍스트 요약 보고서도 생성
            summary_file = project_root / "logs" / f"cache_analysis_summary_{timestamp}.txt"
            await self._generate_text_summary(analysis_result, summary_file)
            
            self.logger.info(f"분석 보고서 저장: {report_file}")
            self.logger.info(f"요약 보고서 저장: {summary_file}")
            
        except Exception as e:
            self.logger.error(f"분석 보고서 저장 실패: {e}")
    
    async def _generate_text_summary(self, analysis_result: Dict[str, Any], output_file: Path):
        """텍스트 요약 보고서 생성"""
        try:
            lines = []
            lines.append("="*80)
            lines.append("Redis 캐시 성능 분석 보고서")
            lines.append(f"생성일시: {analysis_result.get('timestamp', 'N/A')}")
            lines.append("="*80)
            
            # Redis 기본 정보
            redis_info = analysis_result.get('redis_info', {})
            if redis_info and 'error' not in redis_info:
                lines.append("\n📊 Redis 기본 정보:")
                lines.append(f"  버전: {redis_info.get('version', 'N/A')}")
                lines.append(f"  가동 시간: {redis_info.get('uptime_days', 0):.1f}일")
                lines.append(f"  연결된 클라이언트: {redis_info.get('connected_clients', 0)}개")
                lines.append(f"  메모리 사용량: {redis_info.get('used_memory_mb', 0):.1f}MB")
                lines.append(f"  최대 메모리: {redis_info.get('maxmemory_mb', 0):.1f}MB")
                lines.append(f"  초당 명령어 처리: {redis_info.get('instantaneous_ops_per_sec', 0)}개")
            
            # 성능 메트릭
            perf_metrics = analysis_result.get('performance_metrics', {})
            if perf_metrics and 'error' not in perf_metrics:
                cache_metrics = perf_metrics.get('cache_metrics', {})
                lines.append("\n📈 성능 메트릭:")
                lines.append(f"  캐시 히트율: {cache_metrics.get('hit_rate', 0):.1%}")
                lines.append(f"  총 요청 수: {cache_metrics.get('total_requests', 0)}건")
                lines.append(f"  평균 응답 시간: {cache_metrics.get('avg_response_time_ms', 0):.1f}ms")
                lines.append(f"  캐시 크기: {cache_metrics.get('cache_size_mb', 0):.1f}MB")
            
            # 메모리 분석
            memory_analysis = analysis_result.get('memory_analysis', {})
            if memory_analysis and 'error' not in memory_analysis:
                lines.append("\n💾 메모리 분석:")
                lines.append(f"  사용률: {memory_analysis.get('usage_percent', 0):.1f}%")
                lines.append(f"  효율성: {memory_analysis.get('memory_efficiency', 'N/A')}")
                lines.append(f"  단편화 비율: {memory_analysis.get('fragmentation_ratio', 0):.2f}")
            
            # 히트율 분석
            hit_rate_analysis = analysis_result.get('hit_rate_analysis', {})
            if hit_rate_analysis and 'error' not in hit_rate_analysis:
                lines.append("\n🎯 히트율 분석:")
                lines.append(f"  전체 히트율: {hit_rate_analysis.get('hit_rate_percent', 0):.1f}%")
                lines.append(f"  품질 평가: {hit_rate_analysis.get('hit_quality', 'N/A')}")
                lines.append(f"  개선 여지: {hit_rate_analysis.get('improvement_potential', 0):.1f}%")
            
            # 최적화 제안
            suggestions = analysis_result.get('optimization_suggestions', [])
            if suggestions:
                lines.append("\n💡 최적화 제안:")
                for i, suggestion in enumerate(suggestions[:5], 1):
                    lines.append(f"  {i}. [{suggestion.get('priority', 'medium').upper()}] {suggestion.get('title', 'N/A')}")
                    lines.append(f"     {suggestion.get('description', 'N/A')}")
            
            # 건강 상태
            health_status = analysis_result.get('health_status', {})
            if health_status and 'error' not in health_status:
                lines.append("\n🏥 건강 상태:")
                lines.append(f"  상태: {health_status.get('status', 'unknown')}")
                lines.append(f"  히트율: {health_status.get('hit_rate', 0):.1%}")
                lines.append(f"  메모리 사용률: {health_status.get('memory_usage_percent', 0):.1f}%")
            
            lines.append("\n" + "="*80)
            
            # 파일에 저장
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))
            
        except Exception as e:
            self.logger.error(f"텍스트 요약 보고서 생성 실패: {e}")


async def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='캐시 성능 최적화 도구')
    parser.add_argument('--action', choices=['analyze', 'warm', 'optimize', 'test', 'monitor'], 
                       default='analyze', help='실행할 작업')
    parser.add_argument('--config-level', choices=['conservative', 'balanced', 'aggressive'],
                       default='balanced', help='최적화 레벨')
    parser.add_argument('--duration', type=int, default=60,
                       help='모니터링 지속 시간 (분)')
    parser.add_argument('--report', type=bool, default=True,
                       help='보고서 저장 여부')
    
    args = parser.parse_args()
    
    optimizer = CachePerformanceOptimizer()
    
    try:
        if args.action == 'analyze':
            print("📊 캐시 성능 분석을 시작합니다...")
            result = await optimizer.analyze_cache_performance(save_report=args.report)
            
            if 'error' not in result:
                print("✅ 캐시 성능 분석이 완료되었습니다.")
                
                # 주요 지표 출력
                health = result.get('health_status', {})
                if health:
                    print(f"   상태: {health.get('status', 'unknown')}")
                    print(f"   히트율: {health.get('hit_rate', 0):.1%}")
                    print(f"   메모리: {health.get('memory_usage_percent', 0):.1f}%")
                
                suggestions = result.get('optimization_suggestions', [])
                if suggestions:
                    print(f"   최적화 제안: {len(suggestions)}건")
            else:
                print(f"❌ 분석 실패: {result['error']}")
        
        elif args.action == 'warm':
            print(f"🔥 캐시 워밍을 시작합니다 (레벨: {args.config_level})...")
            result = await optimizer.execute_cache_warming(args.config_level)
            
            if result.get('success'):
                print(f"✅ 캐시 워밍 완료: {result.get('execution_time_seconds', 0):.2f}초")
            else:
                print(f"❌ 워밍 실패: {result.get('error', 'Unknown')}")
        
        elif args.action == 'optimize':
            print(f"⚡ TTL 최적화를 시작합니다 (레벨: {args.config_level})...")
            result = await optimizer.optimize_ttl_settings(args.config_level)
            
            if result.get('success'):
                optimizations = result.get('optimization_results', [])
                total_updated = sum(opt.get('keys_updated', 0) for opt in optimizations)
                print(f"✅ TTL 최적화 완료: {total_updated}개 키 업데이트")
            else:
                print(f"❌ 최적화 실패: {result.get('error', 'Unknown')}")
        
        elif args.action == 'test':
            print("🧪 캐시 무효화 전략 테스트를 시작합니다...")
            result = await optimizer.test_cache_invalidation()
            
            if result.get('success'):
                print("✅ 무효화 테스트 완료")
                print(f"   의존성 무효화: {'✅' if result.get('invalidation_effective') else '❌'}")
                print(f"   배치 삭제: {'✅' if result.get('batch_delete_effective') else '❌'}")
            else:
                print(f"❌ 테스트 실패: {result.get('error', 'Unknown')}")
        
        elif args.action == 'monitor':
            print(f"📡 실시간 모니터링을 시작합니다 ({args.duration}분)...")
            result = await optimizer.monitor_real_time(args.duration)
            
            if result.get('success'):
                print("✅ 모니터링 완료")
                perf = result.get('performance_summary', {})
                if perf:
                    hit_rate = perf.get('hit_rate', {})
                    print(f"   평균 히트율: {hit_rate.get('average', 0):.1%}")
                    print(f"   총 요청: {perf.get('total_requests', 0)}건")
            else:
                print(f"❌ 모니터링 실패: {result.get('error', 'Unknown')}")
        
        return 0
        
    except Exception as e:
        print(f"❌ 실행 실패: {e}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)