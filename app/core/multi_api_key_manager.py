"""
다중 API 키 관리자

여러 API 키를 순환 사용하여 API 한도 초과 문제를 해결합니다.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
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
        """사용 가능한 API 키 반환 (상세 로깅 포함)"""
        if provider not in self.api_keys or not self.api_keys[provider]:
            self.logger.error(f"❌ {provider.value} API 키가 설정되지 않았습니다.")
            return None

        keys = self.api_keys[provider]
        start_index = self.current_key_index[provider]
        
        # 키 상태 요약 로깅
        active_count = len([k for k in keys if k.is_active])
        total_count = len(keys)
        self.logger.debug(
            f"🗝️ {provider.value} 키 상태 확인: {active_count}/{total_count}개 활성"
        )

        # 모든 키를 순환하면서 사용 가능한 키 찾기
        for i in range(len(keys)):
            current_index = (start_index + i) % len(keys)
            key_info = keys[current_index]
            key_preview = key_info.key[:10] + "..."
            
            # 키별 상태 확인 로깅
            availability_reason = self._get_key_unavailable_reason(key_info)
            if availability_reason:
                self.logger.debug(
                    f"⏭️ {provider.value} 키 #{current_index} 건너뜀: {key_preview} - {availability_reason}"
                )
                continue

            # 사용 가능한 키 발견
            self.current_key_index[provider] = current_index
            self.logger.info(
                f"✅ {provider.value} 키 #{current_index} 선택: {key_preview} "
                f"(사용량: {key_info.current_usage}/{key_info.daily_limit}, "
                f"오류: {key_info.error_count}회)"
            )
            return key_info

        # 모든 키가 사용 불가능한 경우
        self._log_all_keys_status(provider)
        self.logger.warning(f"⚠️ 모든 {provider.value} API 키가 사용 불가능합니다.")
        return self._get_least_used_key(provider)

    def _is_key_available(self, key_info: APIKeyInfo) -> bool:
        """키가 사용 가능한지 확인"""
        return self._get_key_unavailable_reason(key_info) is None
    
    def _get_key_unavailable_reason(self, key_info: APIKeyInfo) -> Optional[str]:
        """키가 사용 불가능한 이유 반환 (None이면 사용 가능)"""
        if not key_info.is_active:
            return f"비활성화됨 (오류 {key_info.error_count}회)"

        # 일일 한도 확인
        if key_info.current_usage >= key_info.daily_limit:
            return f"일일 한도 초과 ({key_info.current_usage}/{key_info.daily_limit})"

        # Rate limit 시간 확인
        if (
            key_info.rate_limit_reset_time
            and datetime.now() < key_info.rate_limit_reset_time
        ):
            remaining_time = key_info.rate_limit_reset_time - datetime.now()
            minutes = int(remaining_time.total_seconds() // 60)
            return f"Rate limit 제한 중 ({minutes}분 후 해제)"

        # 최근 오류 확인 (오류 발생 후 일정 시간 대기)
        if (
            key_info.last_error_time
            and datetime.now() - key_info.last_error_time < timedelta(minutes=10)
        ):
            elapsed_time = datetime.now() - key_info.last_error_time
            remaining_minutes = 10 - int(elapsed_time.total_seconds() // 60)
            return f"최근 오류로 대기 중 ({remaining_minutes}분 후 재시도)"

        return None

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
        error_details: str = None,
    ):
        """API 호출 기록 (상세 오류 정보 포함)"""
        key_info = self._find_key_info(provider, key)
        if not key_info:
            self.logger.warning(f"🔍 알 수 없는 API 키: {provider.value} - {key[:10]}...")
            return

        key_info.current_usage += 1
        key_info.last_used = datetime.now()

        # 키별 로깅
        key_preview = key[:10] + "..."
        key_index = self._get_key_index(provider, key)
        
        # 통계 업데이트
        if key not in self.stats:
            self.stats[key] = APIKeyStats()

        stats = self.stats[key]
        stats.total_requests += 1

        if success:
            stats.successful_requests += 1
            self.logger.debug(
                f"✅ {provider.value} API 키 #{key_index} 호출 성공: {key_preview} "
                f"(사용량: {key_info.current_usage}/{key_info.daily_limit})"
            )
        else:
            stats.failed_requests += 1
            key_info.error_count += 1
            key_info.last_error_time = datetime.now()
            
            # 상세 오류 로깅
            error_msg = f"❌ {provider.value} API 키 #{key_index} 호출 실패: {key_preview}"
            if error_details:
                error_msg += f" - 오류: {error_details}"
            error_msg += f" (연속 오류: {key_info.error_count}회)"
            self.logger.warning(error_msg)

        if is_rate_limited:
            stats.rate_limit_errors += 1
            # Rate limit 발생 시 1시간 후 재시도
            key_info.rate_limit_reset_time = datetime.now() + timedelta(hours=1)
            self.logger.warning(
                f"🚫 {provider.value} API 키 #{key_index} 한도 초과: {key_preview} "
                f"(재시도 가능: {key_info.rate_limit_reset_time.strftime('%H:%M:%S')})"
            )

        # 오류가 많이 발생한 키는 일시 비활성화
        if key_info.error_count >= 5:
            key_info.is_active = False
            self.logger.error(
                f"🚨 {provider.value} API 키 #{key_index} 자동 비활성화: {key_preview} "
                f"(누적 오류: {key_info.error_count}회)"
            )
            
            # 활성 키 개수 확인 및 경고
            active_keys = [k for k in self.api_keys[provider] if k.is_active]
            self.logger.warning(
                f"⚠️ {provider.value} 활성 키 개수: {len(active_keys)}/{len(self.api_keys[provider])}개"
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
    
    def _get_key_index(self, provider: APIProvider, key: str) -> int:
        """키의 인덱스 번호 반환"""
        if provider not in self.api_keys:
            return -1
        
        for i, key_info in enumerate(self.api_keys[provider]):
            if key_info.key == key:
                return i
        return -1
    
    def _log_all_keys_status(self, provider: APIProvider):
        """모든 키의 상태를 상세 로깅"""
        if provider not in self.api_keys:
            return
        
        self.logger.warning(f"🔍 {provider.value} API 키 상태 상세 조회:")
        
        for i, key_info in enumerate(self.api_keys[provider]):
            key_preview = key_info.key[:10] + "..."
            status = "활성" if key_info.is_active else "비활성"
            reason = self._get_key_unavailable_reason(key_info) or "사용 가능"
            
            usage_percent = (
                (key_info.current_usage / key_info.daily_limit * 100)
                if key_info.daily_limit > 0 else 0
            )
            
            self.logger.warning(
                f"  키 #{i}: {key_preview} | {status} | {reason} | "
                f"사용량: {key_info.current_usage}/{key_info.daily_limit} ({usage_percent:.1f}%) | "
                f"오류: {key_info.error_count}회"
            )
    
    def get_detailed_key_status(self, provider: APIProvider) -> Dict:
        """키별 상세 상태 정보 반환"""
        if provider not in self.api_keys:
            return {"error": f"{provider.value} 키가 설정되지 않음"}
        
        keys_status = []
        
        for i, key_info in enumerate(self.api_keys[provider]):
            key_preview = key_info.key[:10] + "..."
            unavailable_reason = self._get_key_unavailable_reason(key_info)
            
            key_status = {
                "index": i,
                "key_preview": key_preview,
                "is_active": key_info.is_active,
                "is_available": unavailable_reason is None,
                "unavailable_reason": unavailable_reason,
                "current_usage": key_info.current_usage,
                "daily_limit": key_info.daily_limit,
                "usage_percent": (
                    (key_info.current_usage / key_info.daily_limit * 100)
                    if key_info.daily_limit > 0 else 0
                ),
                "error_count": key_info.error_count,
                "last_used": key_info.last_used.isoformat() if key_info.last_used else None,
                "last_error_time": key_info.last_error_time.isoformat() if key_info.last_error_time else None,
                "rate_limit_reset_time": key_info.rate_limit_reset_time.isoformat() if key_info.rate_limit_reset_time else None,
            }
            keys_status.append(key_status)
        
        active_keys = len([k for k in self.api_keys[provider] if k.is_active])
        available_keys = len([k for k in self.api_keys[provider] if self._get_key_unavailable_reason(k) is None])
        
        return {
            "provider": provider.value,
            "total_keys": len(self.api_keys[provider]),
            "active_keys": active_keys,
            "available_keys": available_keys,
            "current_key_index": self.current_key_index.get(provider, 0),
            "keys": keys_status
        }
    
    def force_deactivate_key(self, provider: APIProvider, key_preview: str, reason: str = "수동 비활성화"):
        """특정 키 강제 비활성화"""
        if provider not in self.api_keys:
            return False
        
        for key_info in self.api_keys[provider]:
            if key_info.key.startswith(key_preview.replace("...", "")):
                key_info.is_active = False
                key_info.error_count += 10  # 높은 오류 수로 설정
                key_info.last_error_time = datetime.now()
                
                self.logger.warning(
                    f"🚨 {provider.value} API 키 강제 비활성화: {key_preview} - {reason}"
                )
                
                self._save_cache()
                return True
        
        return False
    
    def reactivate_key(self, provider: APIProvider, key_preview: str):
        """특정 키 재활성화 (오류 카운트 리셋)"""
        if provider not in self.api_keys:
            return False
        
        for key_info in self.api_keys[provider]:
            if key_info.key.startswith(key_preview.replace("...", "")):
                key_info.is_active = True
                key_info.error_count = 0
                key_info.last_error_time = None
                key_info.rate_limit_reset_time = None
                
                self.logger.info(
                    f"✅ {provider.value} API 키 재활성화: {key_preview}"
                )
                
                self._save_cache()
                return True
        
        return False
    
    def get_available_keys(self, provider: APIProvider) -> List[APIKeyInfo]:
        """
        사용 가능한 API 키 목록 반환
        
        Args:
            provider: API 제공자
            
        Returns:
            List[APIKeyInfo]: 사용 가능한 키 목록
        """
        if provider not in self.api_keys:
            return []
        
        available_keys = []
        
        for key_info in self.api_keys[provider]:
            # 사용 불가능한 이유가 없는 키만 반환
            if self._get_key_unavailable_reason(key_info) is None:
                available_keys.append(key_info)
        
        return available_keys
    
    def get_all_available_keys(self) -> Dict[APIProvider, List[APIKeyInfo]]:
        """
        모든 제공자의 사용 가능한 API 키 목록 반환
        
        Returns:
            Dict[APIProvider, List[APIKeyInfo]]: 제공자별 사용 가능한 키 목록
        """
        available_keys = {}
        
        for provider in self.api_keys.keys():
            available_keys[provider] = self.get_available_keys(provider)
        
        return available_keys
    
    def get_key_availability_summary(self) -> Dict[str, Any]:
        """
        API 키 가용성 요약 정보 반환
        
        Returns:
            Dict[str, Any]: 가용성 요약 정보
        """
        summary = {
            "timestamp": datetime.now().isoformat(),
            "providers": {},
            "total_summary": {
                "total_keys": 0,
                "active_keys": 0,
                "available_keys": 0,
                "exhausted_keys": 0,
                "error_keys": 0
            }
        }
        
        for provider, keys in self.api_keys.items():
            available_keys = self.get_available_keys(provider)
            active_keys = [k for k in keys if k.is_active]
            exhausted_keys = [k for k in keys if k.current_usage >= k.daily_limit]
            error_keys = [k for k in keys if k.error_count >= 5]
            
            provider_summary = {
                "total_keys": len(keys),
                "active_keys": len(active_keys),
                "available_keys": len(available_keys),
                "exhausted_keys": len(exhausted_keys),
                "error_keys": len(error_keys),
                "availability_rate": (len(available_keys) / len(keys) * 100) if keys else 0
            }
            
            summary["providers"][provider.value] = provider_summary
            
            # 전체 요약에 추가
            summary["total_summary"]["total_keys"] += provider_summary["total_keys"]
            summary["total_summary"]["active_keys"] += provider_summary["active_keys"]
            summary["total_summary"]["available_keys"] += provider_summary["available_keys"]
            summary["total_summary"]["exhausted_keys"] += provider_summary["exhausted_keys"]
            summary["total_summary"]["error_keys"] += provider_summary["error_keys"]
        
        # 전체 가용성 비율 계산
        total_keys = summary["total_summary"]["total_keys"]
        if total_keys > 0:
            summary["total_summary"]["availability_rate"] = (
                summary["total_summary"]["available_keys"] / total_keys * 100
            )
        else:
            summary["total_summary"]["availability_rate"] = 0
        
        return summary

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
