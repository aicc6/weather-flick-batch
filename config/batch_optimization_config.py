"""
배치 최적화 설정 모듈

이 모듈은 weather-flick-batch 시스템의 배치 INSERT 최적화 설정을 관리합니다.
환경별, 작업별로 최적화된 배치 설정을 제공합니다.
"""

import os
from dataclasses import dataclass
from typing import Dict, Any
from enum import Enum

from app.core.batch_insert_optimizer import BatchConfig


class BatchOptimizationLevel(Enum):
    """배치 최적화 레벨"""
    CONSERVATIVE = "conservative"  # 안전 우선 (작은 배치, 낮은 메모리)
    BALANCED = "balanced"         # 균형 (기본 설정)
    AGGRESSIVE = "aggressive"     # 성능 우선 (큰 배치, 높은 메모리)
    MEMORY_CONSTRAINED = "memory_constrained"  # 메모리 제약 환경


@dataclass
class TableSpecificConfig:
    """테이블별 특화 설정"""
    table_name: str
    batch_size: int
    max_memory_mb: int
    transaction_timeout: int
    retry_attempts: int
    parallel_workers: int = 1
    use_upsert: bool = True
    conflict_columns: list = None
    
    def __post_init__(self):
        if self.conflict_columns is None:
            self.conflict_columns = []


class BatchOptimizationConfig:
    """배치 최적화 설정 관리자"""
    
    # 최적화 레벨별 기본 설정
    OPTIMIZATION_PROFILES = {
        BatchOptimizationLevel.CONSERVATIVE: {
            "batch_size": 500,
            "max_memory_mb": 50,
            "transaction_timeout": 30,
            "retry_attempts": 5,
            "retry_delay": 2.0
        },
        BatchOptimizationLevel.BALANCED: {
            "batch_size": 1000,
            "max_memory_mb": 100,
            "transaction_timeout": 60,
            "retry_attempts": 3,
            "retry_delay": 1.0
        },
        BatchOptimizationLevel.AGGRESSIVE: {
            "batch_size": 2000,
            "max_memory_mb": 200,
            "transaction_timeout": 120,
            "retry_attempts": 2,
            "retry_delay": 0.5
        },
        BatchOptimizationLevel.MEMORY_CONSTRAINED: {
            "batch_size": 200,
            "max_memory_mb": 25,
            "transaction_timeout": 20,
            "retry_attempts": 3,
            "retry_delay": 1.5
        }
    }
    
    # 테이블별 특화 설정
    TABLE_SPECIFIC_CONFIGS = {
        # 날씨 데이터 테이블들
        "current_weather": TableSpecificConfig(
            table_name="current_weather",
            batch_size=1000,
            max_memory_mb=80,
            transaction_timeout=45,
            retry_attempts=3,
            parallel_workers=2,
            use_upsert=True,
            conflict_columns=["region_code", "observed_at"]
        ),
        
        "weather_forecast": TableSpecificConfig(
            table_name="weather_forecast",
            batch_size=1500,
            max_memory_mb=120,
            transaction_timeout=60,
            retry_attempts=3,
            parallel_workers=3,
            use_upsert=True,
            conflict_columns=["region_code", "forecast_date", "forecast_time"]
        ),
        
        "historical_weather_daily": TableSpecificConfig(
            table_name="historical_weather_daily",
            batch_size=2000,
            max_memory_mb=150,
            transaction_timeout=90,
            retry_attempts=2,
            parallel_workers=2,
            use_upsert=True,
            conflict_columns=["region_code", "weather_date"]
        ),
        
        # 관광지 데이터 테이블들
        "tourist_attractions": TableSpecificConfig(
            table_name="tourist_attractions",
            batch_size=1000,
            max_memory_mb=100,
            transaction_timeout=60,
            retry_attempts=3,
            parallel_workers=2,
            use_upsert=True,
            conflict_columns=["content_id"]
        ),
        
        "accommodations": TableSpecificConfig(
            table_name="accommodations",
            batch_size=800,
            max_memory_mb=80,
            transaction_timeout=45,
            retry_attempts=3,
            parallel_workers=2,
            use_upsert=True,
            conflict_columns=["content_id"]
        ),
        
        "festivals_events": TableSpecificConfig(
            table_name="festivals_events",
            batch_size=1200,
            max_memory_mb=90,
            transaction_timeout=50,
            retry_attempts=3,
            parallel_workers=2,
            use_upsert=True,
            conflict_columns=["content_id"]
        ),
        
        "restaurants": TableSpecificConfig(
            table_name="restaurants",
            batch_size=1500,
            max_memory_mb=120,
            transaction_timeout=60,
            retry_attempts=3,
            parallel_workers=3,
            use_upsert=True,
            conflict_columns=["content_id"]
        ),
        
        # 메타데이터 테이블들
        "api_raw_data": TableSpecificConfig(
            table_name="api_raw_data",
            batch_size=500,
            max_memory_mb=60,
            transaction_timeout=30,
            retry_attempts=2,
            parallel_workers=1,
            use_upsert=False,
            conflict_columns=[]
        ),
        
        "batch_job_logs": TableSpecificConfig(
            table_name="batch_job_logs",
            batch_size=300,
            max_memory_mb=40,
            transaction_timeout=20,
            retry_attempts=2,
            parallel_workers=1,
            use_upsert=False,
            conflict_columns=[]
        )
    }
    
    @classmethod
    def get_batch_config(
        cls, 
        table_name: str = None,
        optimization_level: BatchOptimizationLevel = BatchOptimizationLevel.BALANCED,
        override_settings: Dict[str, Any] = None
    ) -> BatchConfig:
        """배치 설정 생성"""
        
        # 기본 프로필 설정
        base_config = cls.OPTIMIZATION_PROFILES[optimization_level].copy()
        
        # 테이블별 설정 적용
        if table_name and table_name in cls.TABLE_SPECIFIC_CONFIGS:
            table_config = cls.TABLE_SPECIFIC_CONFIGS[table_name]
            base_config.update({
                "batch_size": table_config.batch_size,
                "max_memory_mb": table_config.max_memory_mb,
                "transaction_timeout": table_config.transaction_timeout,
                "retry_attempts": table_config.retry_attempts
            })
        
        # 환경 변수 설정 적용
        env_overrides = cls._get_env_overrides()
        base_config.update(env_overrides)
        
        # 직접 override 설정 적용
        if override_settings:
            base_config.update(override_settings)
        
        return BatchConfig(**base_config)
    
    @classmethod
    def get_table_config(cls, table_name: str) -> TableSpecificConfig:
        """테이블별 특화 설정 조회"""
        return cls.TABLE_SPECIFIC_CONFIGS.get(
            table_name, 
            TableSpecificConfig(
                table_name=table_name,
                batch_size=1000,
                max_memory_mb=100,
                transaction_timeout=60,
                retry_attempts=3
            )
        )
    
    @classmethod
    def _get_env_overrides(cls) -> Dict[str, Any]:
        """환경 변수에서 설정 override"""
        overrides = {}
        
        # 환경 변수 매핑
        env_mappings = {
            "BATCH_SIZE": ("batch_size", int),
            "BATCH_MAX_MEMORY_MB": ("max_memory_mb", int),
            "BATCH_TRANSACTION_TIMEOUT": ("transaction_timeout", int),
            "BATCH_RETRY_ATTEMPTS": ("retry_attempts", int),
            "BATCH_RETRY_DELAY": ("retry_delay", float)
        }
        
        for env_key, (config_key, type_func) in env_mappings.items():
            env_value = os.getenv(env_key)
            if env_value:
                try:
                    overrides[config_key] = type_func(env_value)
                except (ValueError, TypeError):
                    # 환경 변수 형변환 실패 시 무시
                    pass
        
        return overrides
    
    @classmethod
    def get_optimization_level_from_env(cls) -> BatchOptimizationLevel:
        """환경 변수에서 최적화 레벨 결정"""
        env_level = os.getenv("BATCH_OPTIMIZATION_LEVEL", "balanced").lower()
        
        level_mapping = {
            "conservative": BatchOptimizationLevel.CONSERVATIVE,
            "balanced": BatchOptimizationLevel.BALANCED,
            "aggressive": BatchOptimizationLevel.AGGRESSIVE,
            "memory_constrained": BatchOptimizationLevel.MEMORY_CONSTRAINED,
            "safe": BatchOptimizationLevel.CONSERVATIVE,
            "fast": BatchOptimizationLevel.AGGRESSIVE,
            "low_memory": BatchOptimizationLevel.MEMORY_CONSTRAINED
        }
        
        return level_mapping.get(env_level, BatchOptimizationLevel.BALANCED)
    
    @classmethod
    def validate_config(cls, config: BatchConfig, table_name: str = None) -> bool:
        """배치 설정 유효성 검증"""
        
        # 기본 범위 검증
        if config.batch_size <= 0 or config.batch_size > 10000:
            return False
        
        if config.max_memory_mb <= 0 or config.max_memory_mb > 1000:
            return False
        
        if config.transaction_timeout <= 0 or config.transaction_timeout > 600:
            return False
        
        if config.retry_attempts < 0 or config.retry_attempts > 10:
            return False
        
        # 테이블별 추가 검증
        if table_name:
            # 대용량 테이블의 경우 최소 배치 크기 권장
            high_volume_tables = ["weather_forecast", "historical_weather_daily", "restaurants"]
            if table_name in high_volume_tables and config.batch_size < 500:
                return False
        
        return True


# 편의 함수들
def get_weather_batch_config(
    table_name: str = "weather_forecast",
    optimization_level: BatchOptimizationLevel = None
) -> BatchConfig:
    """날씨 데이터용 배치 설정"""
    if optimization_level is None:
        optimization_level = BatchOptimizationConfig.get_optimization_level_from_env()
    
    return BatchOptimizationConfig.get_batch_config(table_name, optimization_level)


def get_tourism_batch_config(
    table_name: str = "tourist_attractions",
    optimization_level: BatchOptimizationLevel = None
) -> BatchConfig:
    """관광지 데이터용 배치 설정"""
    if optimization_level is None:
        optimization_level = BatchOptimizationConfig.get_optimization_level_from_env()
    
    return BatchOptimizationConfig.get_batch_config(table_name, optimization_level)


def get_default_batch_config() -> BatchConfig:
    """기본 배치 설정"""
    optimization_level = BatchOptimizationConfig.get_optimization_level_from_env()
    return BatchOptimizationConfig.get_batch_config(optimization_level=optimization_level)


def get_memory_optimized_config() -> BatchConfig:
    """메모리 최적화 배치 설정"""
    return BatchOptimizationConfig.get_batch_config(
        optimization_level=BatchOptimizationLevel.MEMORY_CONSTRAINED
    )


def get_performance_optimized_config() -> BatchConfig:
    """성능 최적화 배치 설정"""
    return BatchOptimizationConfig.get_batch_config(
        optimization_level=BatchOptimizationLevel.AGGRESSIVE
    )