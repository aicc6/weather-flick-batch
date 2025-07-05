"""
다중 API 키 관리자

여러 API 키를 순환 사용하여 API 한도 초과 문제를 해결합니다.
"""

import os
import time
import json
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum


class APIProvider(Enum):
    """API 제공자 유형"""

    KTO = "KTO"  # 한국관광공사
    KMA = "KMA"  # 기상청
    WEATHER = "WEATHER"  # 날씨 API 제공자 추가
    GOOGLE = "GOOGLE"  # 구글 API
    NAVER = "NAVER"  # 네이버 API


@dataclass
class APIKeyInfo:
    """API 키 정보"""

    key: str
    provider: APIProvider
    daily_limit: int = 1000
    current_usage: int = 0
    last_used: Optional[datetime] = None
    is_active: bool = True
    rate_limit_reset_time: Optional[datetime] = None
    error_count: int = 0
    last_error_time: Optional[datetime] = None


@dataclass
class APIKeyStats:
    """API 키 사용 통계"""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rate_limit_errors: int = 0
    last_reset_time: datetime = field(default_factory=datetime.now)


class MultiAPIKeyManager:
    """다중 API 키 관리자"""

    def __init__(self, cache_file: str = "data/cache/api_key_cache.json"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.cache_file = cache_file
        self.api_keys: Dict[APIProvider, List[APIKeyInfo]] = {
            APIProvider.KTO: [],
            APIProvider.KMA: [],
        }
        self.stats: Dict[str, APIKeyStats] = {}
        self.current_key_index: Dict[APIProvider, int] = {
            APIProvider.KTO: 0,
            APIProvider.KMA: 0,
        }

        # 환경 변수에서 키 로드
        self._load_api_keys_from_env()

        # 캐시에서 사용량 정보 로드
        self._load_cache()

    def _load_api_keys_from_env(self):
        """환경 변수에서 API 키들 로드"""
        # KTO API 키들 로드
        kto_keys = self._parse_api_keys("KTO_API_KEY")
        for key in kto_keys:
            if key and key.strip() and "your_kto_api_key_here" not in key:
                key_info = APIKeyInfo(
                    key=key.strip(),
                    provider=APIProvider.KTO,
                    daily_limit=int(os.getenv("KTO_API_DAILY_LIMIT", "1000")),
                )
                self.api_keys[APIProvider.KTO].append(key_info)

        # KMA API 키들 로드
        kma_keys = self._parse_api_keys("KMA_API_KEY")
        for key in kma_keys:
            if key and key.strip() and "your_kma_api_key_here" not in key:
                key_info = APIKeyInfo(
                    key=key.strip(),
                    provider=APIProvider.KMA,
                    daily_limit=int(os.getenv("KMA_API_DAILY_LIMIT", "1000")),
                )
                self.api_keys[APIProvider.KMA].append(key_info)

        # 로드된 키 개수 로깅
        kto_count = len(self.api_keys[APIProvider.KTO])
        kma_count = len(self.api_keys[APIProvider.KMA])

        self.logger.info(f"🔑 KTO API 키 {kto_count}개 로드됨")
        self.logger.info(f"🔑 KMA API 키 {kma_count}개 로드됨")

        if kto_count == 0:
            self.logger.warning("⚠️ KTO API 키가 설정되지 않았습니다.")
        if kma_count == 0:
            self.logger.warning("⚠️ KMA API 키가 설정되지 않았습니다.")

    def _parse_api_keys(self, env_var_prefix: str) -> List[str]:
        """환경 변수에서 API 키들 파싱 (쉼표로 구분된 여러 키 지원)"""
        keys = []

        # .env 파일을 강제로 다시 로드 (tourism 작업 실행 시 필요)
        try:
            from dotenv import load_dotenv

            load_dotenv(override=True)  # 기존 환경변수 덮어쓰기
        except ImportError:
            pass

        # 환경 변수에서 키 가져오기
        main_key = os.getenv(env_var_prefix, "")
        if main_key:
            # 쉼표로 구분된 여러 키 지원
            if "," in main_key:
                keys.extend([k.strip() for k in main_key.split(",") if k.strip()])
            else:
                # 단일 키
                keys.append(main_key.strip())

        return keys

    def get_active_key(self, provider: APIProvider) -> Optional[APIKeyInfo]:
        """사용 가능한 API 키 반환"""
        if provider not in self.api_keys or not self.api_keys[provider]:
            self.logger.error(f"❌ {provider.value} API 키가 설정되지 않았습니다.")
            return None

        keys = self.api_keys[provider]
        start_index = self.current_key_index[provider]

        # 모든 키를 순환하면서 사용 가능한 키 찾기
        for i in range(len(keys)):
            current_index = (start_index + i) % len(keys)
            key_info = keys[current_index]

            if self._is_key_available(key_info):
                self.current_key_index[provider] = current_index
                return key_info

        # 모든 키가 한도 초과인 경우
        self.logger.warning(f"⚠️ 모든 {provider.value} API 키가 한도에 도달했습니다.")
        return self._get_least_used_key(provider)

    def _is_key_available(self, key_info: APIKeyInfo) -> bool:
        """키가 사용 가능한지 확인"""
        if not key_info.is_active:
            return False

        # 일일 한도 확인
        if key_info.current_usage >= key_info.daily_limit:
            return False

        # Rate limit 시간 확인
        if (
            key_info.rate_limit_reset_time
            and datetime.now() < key_info.rate_limit_reset_time
        ):
            return False

        # 최근 오류 확인 (오류 발생 후 일정 시간 대기)
        if (
            key_info.last_error_time
            and datetime.now() - key_info.last_error_time < timedelta(minutes=10)
        ):
            return False

        return True

    def _get_least_used_key(self, provider: APIProvider) -> Optional[APIKeyInfo]:
        """가장 적게 사용된 키 반환"""
        keys = self.api_keys[provider]
        if not keys:
            return None

        # 활성화된 키 중에서 가장 적게 사용된 키 선택
        active_keys = [k for k in keys if k.is_active]
        if not active_keys:
            return None

        least_used = min(active_keys, key=lambda k: k.current_usage)
        return least_used

    def record_api_call(
        self,
        provider: APIProvider,
        key: str,
        success: bool = True,
        is_rate_limited: bool = False,
    ):
        """API 호출 기록"""
        key_info = self._find_key_info(provider, key)
        if not key_info:
            return

        key_info.current_usage += 1
        key_info.last_used = datetime.now()

        # 통계 업데이트
        if key not in self.stats:
            self.stats[key] = APIKeyStats()

        stats = self.stats[key]
        stats.total_requests += 1

        if success:
            stats.successful_requests += 1
        else:
            stats.failed_requests += 1
            key_info.error_count += 1
            key_info.last_error_time = datetime.now()

        if is_rate_limited:
            stats.rate_limit_errors += 1
            # Rate limit 발생 시 1시간 후 재시도
            key_info.rate_limit_reset_time = datetime.now() + timedelta(hours=1)
            self.logger.warning(f"⚠️ {provider.value} API 키 한도 초과: {key[:10]}...")

        # 오류가 많이 발생한 키는 일시 비활성화
        if key_info.error_count >= 5:
            key_info.is_active = False
            self.logger.warning(
                f"⚠️ {provider.value} API 키 비활성화 (오류 {key_info.error_count}회): {key[:10]}..."
            )

        # 캐시 저장
        self._save_cache()

    def _find_key_info(self, provider: APIProvider, key: str) -> Optional[APIKeyInfo]:
        """키 정보 찾기"""
        if provider not in self.api_keys:
            return None

        for key_info in self.api_keys[provider]:
            if key_info.key == key:
                return key_info
        return None

    def rotate_to_next_key(self, provider: APIProvider):
        """다음 키로 순환"""
        if provider not in self.api_keys or not self.api_keys[provider]:
            return

        self.current_key_index[provider] = (self.current_key_index[provider] + 1) % len(
            self.api_keys[provider]
        )
        self.logger.info(
            f"🔄 {provider.value} API 키 순환: 인덱스 {self.current_key_index[provider]}"
        )

    def are_all_keys_rate_limited(self, provider: APIProvider) -> bool:
        """특정 제공자의 모든 키가 제한되었는지 확인"""
        if provider not in self.api_keys or not self.api_keys[provider]:
            return True
        
        current_time = datetime.now()
        
        for key_info in self.api_keys[provider]:
            # 활성화된 키가 있고, 제한 시간이 지났거나 제한되지 않은 키가 있으면 False
            if (key_info.is_active and 
                (key_info.rate_limit_reset_time is None or 
                 current_time >= key_info.rate_limit_reset_time)):
                return False
        
        return True
    
    def get_next_reset_time(self, provider: APIProvider) -> Optional[datetime]:
        """특정 제공자의 다음 제한 해제 시간 반환"""
        if provider not in self.api_keys or not self.api_keys[provider]:
            return None
        
        earliest_reset = None
        
        for key_info in self.api_keys[provider]:
            if key_info.rate_limit_reset_time:
                if earliest_reset is None or key_info.rate_limit_reset_time < earliest_reset:
                    earliest_reset = key_info.rate_limit_reset_time
        
        return earliest_reset
    
    def get_rate_limit_status(self, provider: APIProvider) -> Dict:
        """API 키 제한 상태 상세 정보 반환"""
        if provider not in self.api_keys:
            return {"all_limited": True, "active_keys": 0, "total_keys": 0, "next_reset": None}
        
        keys = self.api_keys[provider]
        current_time = datetime.now()
        
        total_keys = len(keys)
        active_keys = 0
        limited_keys = 0
        next_reset = None
        
        for key_info in keys:
            if key_info.is_active:
                if (key_info.rate_limit_reset_time is None or 
                    current_time >= key_info.rate_limit_reset_time):
                    active_keys += 1
                else:
                    limited_keys += 1
                    if next_reset is None or key_info.rate_limit_reset_time < next_reset:
                        next_reset = key_info.rate_limit_reset_time
        
        return {
            "all_limited": active_keys == 0,
            "active_keys": active_keys,
            "limited_keys": limited_keys,
            "total_keys": total_keys,
            "next_reset": next_reset
        }

    def reset_daily_usage(self):
        """일일 사용량 초기화"""
        current_time = datetime.now()

        for provider_keys in self.api_keys.values():
            for key_info in provider_keys:
                key_info.current_usage = 0
                key_info.error_count = 0
                key_info.is_active = True
                key_info.rate_limit_reset_time = None
                key_info.last_error_time = None

        # 통계 초기화
        for stats in self.stats.values():
            stats.total_requests = 0
            stats.successful_requests = 0
            stats.failed_requests = 0
            stats.rate_limit_errors = 0
            stats.last_reset_time = current_time

        self.logger.info("🔄 일일 API 키 사용량이 초기화되었습니다.")
        self._save_cache()

    def get_usage_stats(self) -> Dict:
        """사용량 통계 반환"""
        stats = {"providers": {}, "total_keys": 0, "active_keys": 0}

        for provider, keys in self.api_keys.items():
            provider_stats = {
                "total_keys": len(keys),
                "active_keys": len([k for k in keys if k.is_active]),
                "total_usage": sum(k.current_usage for k in keys),
                "total_limit": sum(k.daily_limit for k in keys),
                "keys": [],
            }

            for i, key_info in enumerate(keys):
                key_stats = {
                    "index": i,
                    "key_preview": (
                        key_info.key[:10] + "..."
                        if len(key_info.key) > 10
                        else key_info.key
                    ),
                    "usage": key_info.current_usage,
                    "limit": key_info.daily_limit,
                    "usage_percent": (
                        (key_info.current_usage / key_info.daily_limit * 100)
                        if key_info.daily_limit > 0
                        else 0
                    ),
                    "is_active": key_info.is_active,
                    "error_count": key_info.error_count,
                    "last_used": (
                        key_info.last_used.isoformat() if key_info.last_used else None
                    ),
                }
                provider_stats["keys"].append(key_stats)

            stats["providers"][provider.value] = provider_stats
            stats["total_keys"] += provider_stats["total_keys"]
            stats["active_keys"] += provider_stats["active_keys"]

        return stats

    def _load_cache(self):
        """캐시에서 사용량 정보 로드"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    cache_data = json.load(f)

                # 오늘 날짜 확인
                today = datetime.now().date()
                cache_date = datetime.fromisoformat(cache_data.get("date", "")).date()

                if cache_date != today:
                    self.logger.info("새로운 날짜로 API 키 사용량 초기화")
                    return

                # 키 사용량 복원
                for provider_name, provider_data in cache_data.get(
                    "providers", {}
                ).items():
                    try:
                        provider = APIProvider(provider_name)
                        if provider in self.api_keys:
                            for key_data in provider_data.get("keys", []):
                                key_info = self._find_key_info(
                                    provider, key_data["key"]
                                )
                                if key_info:
                                    key_info.current_usage = key_data.get("usage", 0)
                                    key_info.error_count = key_data.get(
                                        "error_count", 0
                                    )
                                    key_info.is_active = key_data.get("is_active", True)
                    except ValueError:
                        continue

                self.logger.info("📁 API 키 캐시 로드 완료")
        except Exception as e:
            self.logger.warning(f"캐시 로드 실패: {e}")

    def _save_cache(self):
        """캐시에 사용량 정보 저장"""
        try:
            cache_data = {"date": datetime.now().isoformat(), "providers": {}}

            for provider, keys in self.api_keys.items():
                provider_data = {"keys": []}

                for key_info in keys:
                    key_data = {
                        "key": key_info.key,
                        "usage": key_info.current_usage,
                        "error_count": key_info.error_count,
                        "is_active": key_info.is_active,
                    }
                    provider_data["keys"].append(key_data)

                cache_data["providers"][provider.value] = provider_data

            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            self.logger.warning(f"캐시 저장 실패: {e}")


# 전역 인스턴스
_api_key_manager = None


def get_api_key_manager() -> MultiAPIKeyManager:
    """API 키 매니저 싱글톤 인스턴스 반환"""
    global _api_key_manager
    if _api_key_manager is None:
        _api_key_manager = MultiAPIKeyManager()
    return _api_key_manager


def reset_api_key_manager():
    """API 키 매니저 싱글톤 인스턴스 리셋 (환경 변수 재로드용)"""
    global _api_key_manager
    _api_key_manager = None
