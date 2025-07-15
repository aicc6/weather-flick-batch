"""
API 원본 데이터 저장 정책 설정

각 API 제공자별로 저장 정책을 정의하고 관리합니다.
"""

from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum


class StoragePolicy(Enum):
    """저장 정책 타입"""
    ALWAYS = "always"           # 항상 저장
    SELECTIVE = "selective"     # 선택적 저장 (조건 기반)
    ERROR_ONLY = "error_only"   # 오류 응답만 저장
    NEVER = "never"            # 저장 안함


@dataclass
class EndpointConfig:
    """엔드포인트별 세부 설정"""
    store: bool = True
    ttl_days: int = 90
    max_response_size_mb: Optional[float] = None
    store_on_error: bool = True
    compression: bool = True
    priority: int = 1  # 1: 높음, 2: 중간, 3: 낮음


@dataclass 
class ProviderConfig:
    """API 제공자별 설정"""
    enabled: bool = True
    default_policy: StoragePolicy = StoragePolicy.SELECTIVE
    default_ttl_days: int = 90
    max_response_size_mb: float = 10.0
    store_errors: bool = True
    compression_enabled: bool = True
    endpoints: Dict[str, EndpointConfig] = None
    
    def __post_init__(self):
        if self.endpoints is None:
            self.endpoints = {}


# =============================================================================
# API 제공자별 저장 정책 설정
# =============================================================================

API_STORAGE_POLICIES: Dict[str, ProviderConfig] = {
    
    # 기상청 (Korea Meteorological Administration)
    "KMA": ProviderConfig(
        enabled=True,
        default_policy=StoragePolicy.ALWAYS,
        default_ttl_days=90,
        max_response_size_mb=5.0,
        store_errors=True,
        compression_enabled=True,
        endpoints={
            # 단기 예보구역 API
            "fct_shrt_reg": EndpointConfig(
                store=True,
                ttl_days=180,  # 예보구역 정보는 오래 보관
                max_response_size_mb=1.0,
                priority=1
            ),
            
            # 초단기실황 API
            "getUltraSrtNcst": EndpointConfig(
                store=True,
                ttl_days=30,   # 실황 데이터는 단기 보관
                max_response_size_mb=2.0,
                priority=2
            ),
            
            # 단기예보 API
            "getUltraSrtFcst": EndpointConfig(
                store=True,
                ttl_days=60,
                max_response_size_mb=3.0,
                priority=1
            ),
            
            # 동네예보 API
            "getVilageFcst": EndpointConfig(
                store=True,
                ttl_days=60,
                max_response_size_mb=3.0,
                priority=1
            ),
            
            # 헬스체크 등 단순 API
            "health": EndpointConfig(
                store=False,
                ttl_days=7
            ),
        }
    ),
    
    # 한국관광공사 (Korea Tourism Organization)
    "KTO": ProviderConfig(
        enabled=True,
        default_policy=StoragePolicy.SELECTIVE,
        default_ttl_days=180,
        max_response_size_mb=20.0,  # 이미지 등 대용량 응답 고려
        store_errors=True,
        compression_enabled=True,
        endpoints={
            # 지역 기반 리스트 조회
            "areaBasedList2": EndpointConfig(
                store=True,
                ttl_days=180,
                max_response_size_mb=15.0,
                priority=1
            ),
            
            # 지역 코드 조회
            "areaCode2": EndpointConfig(
                store=True,
                ttl_days=365,  # 코드 정보는 장기 보관
                max_response_size_mb=1.0,
                priority=1
            ),
            
            # 법정동 코드 조회
            "ldongCode2": EndpointConfig(
                store=True,
                ttl_days=365,
                max_response_size_mb=2.0,
                priority=1
            ),
            
            # 상세 정보 조회
            "detailCommon2": EndpointConfig(
                store=True,
                ttl_days=180,
                max_response_size_mb=5.0,
                priority=2
            ),
            
            # 이미지 정보 조회 (대용량)
            "detailImage2": EndpointConfig(
                store=True,
                ttl_days=90,
                max_response_size_mb=30.0,
                priority=3
            ),
            
            # 소개 정보 조회
            "detailIntro2": EndpointConfig(
                store=True,
                ttl_days=180,
                max_response_size_mb=5.0,
                priority=2
            ),
            
            # 반려동물 동반 여행 정보
            "detailPetTour2": EndpointConfig(
                store=True,
                ttl_days=90,
                max_response_size_mb=3.0,
                priority=3
            ),
            
            # 동기화 리스트 조회
            "areaBasedSyncList2": EndpointConfig(
                store=True,
                ttl_days=30,
                max_response_size_mb=10.0,
                priority=2
            ),
        }
    ),
    
    # 기타 날씨 API
    "WEATHER": ProviderConfig(
        enabled=True,
        default_policy=StoragePolicy.SELECTIVE,
        default_ttl_days=30,
        max_response_size_mb=5.0,
        store_errors=True,
        compression_enabled=True,
        endpoints={
            "forecast": EndpointConfig(
                store=True,
                ttl_days=30,
                max_response_size_mb=3.0,
                priority=2
            ),
            
            "weather": EndpointConfig(
                store=True,
                ttl_days=30,
                max_response_size_mb=3.0,
                priority=2
            ),
        }
    ),
    
    # 시스템 모니터링 API (저장 안함)
    "MONITORING": ProviderConfig(
        enabled=False,
        default_policy=StoragePolicy.ERROR_ONLY,
        default_ttl_days=7,
        max_response_size_mb=1.0,
        store_errors=True,
        compression_enabled=False,
        endpoints={
            "health": EndpointConfig(store=False),
            "metrics": EndpointConfig(store=False),
            "status": EndpointConfig(store=False),
        }
    ),
}


# =============================================================================
# 전역 설정
# =============================================================================

GLOBAL_STORAGE_CONFIG = {
    # 저장 시스템 전체 활성화 여부
    "storage_enabled": True,
    
    # 비동기 저장 설정
    "async_storage": {
        "enabled": True,
        "queue_size": 1000,
        "worker_count": 2,
        "batch_size": 50,
        "flush_interval_seconds": 10,
    },
    
    # 압축 설정
    "compression": {
        "algorithm": "gzip",
        "level": 6,
        "min_size_bytes": 1024,  # 1KB 미만은 압축 안함
    },
    
    # 자동 정리 설정
    "auto_cleanup": {
        "enabled": True,
        "schedule_cron": "0 2 * * *",  # 매일 새벽 2시
        "batch_size": 1000,
        "dry_run": False,
    },
    
    # 모니터링 설정
    "monitoring": {
        "metrics_enabled": True,
        "alert_thresholds": {
            "storage_size_gb": 100,
            "daily_growth_gb": 10,
            "error_rate_percent": 5,
        },
    },
    
    # 긴급 정리 설정
    "emergency_cleanup": {
        "disk_usage_threshold_percent": 85,
        "emergency_ttl_days": 7,
        "priority_cleanup": True,
    },
}


# =============================================================================
# 유틸리티 함수
# =============================================================================

def get_provider_config(provider: str) -> Optional[ProviderConfig]:
    """API 제공자 설정 조회"""
    return API_STORAGE_POLICIES.get(provider.upper())


def get_endpoint_config(provider: str, endpoint: str) -> Optional[EndpointConfig]:
    """특정 엔드포인트 설정 조회"""
    provider_config = get_provider_config(provider)
    if not provider_config:
        return None
    
    return provider_config.endpoints.get(endpoint)


def should_store_response(provider: str, endpoint: str, response_size_mb: float = 0, 
                         status_code: int = 200) -> bool:
    """응답 저장 여부 결정"""
    
    if not GLOBAL_STORAGE_CONFIG["storage_enabled"]:
        return False
    
    provider_config = get_provider_config(provider)
    if not provider_config or not provider_config.enabled:
        return False
    
    endpoint_config = get_endpoint_config(provider, endpoint)
    
    # 오류 응답 처리
    if status_code >= 400:
        if endpoint_config:
            return endpoint_config.store_on_error
        return provider_config.store_errors
    
    # 엔드포인트별 설정이 있는 경우
    if endpoint_config:
        if not endpoint_config.store:
            return False
        
        # 응답 크기 체크
        if (endpoint_config.max_response_size_mb and 
            response_size_mb > endpoint_config.max_response_size_mb):
            return False
    
    # 제공자 기본 설정 체크
    else:
        if provider_config.default_policy == StoragePolicy.NEVER:
            return False
        
        if (response_size_mb > provider_config.max_response_size_mb):
            return False
    
    return True


def get_ttl_days(provider: str, endpoint: str) -> int:
    """TTL 일수 조회"""
    endpoint_config = get_endpoint_config(provider, endpoint)
    if endpoint_config:
        return endpoint_config.ttl_days
    
    provider_config = get_provider_config(provider)
    if provider_config:
        return provider_config.default_ttl_days
    
    return 90  # 기본값


def get_priority(provider: str, endpoint: str) -> int:
    """우선순위 조회 (1: 높음, 3: 낮음)"""
    endpoint_config = get_endpoint_config(provider, endpoint)
    if endpoint_config:
        return endpoint_config.priority
    
    return 2  # 기본값 (중간)