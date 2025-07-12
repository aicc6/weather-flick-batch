"""
통합 외부 API 클라이언트

모든 외부 API 호출을 중앙화하여 원본 데이터 저장, 캐싱, 오류 처리를 통합 관리합니다.
"""

import os
import time
import hashlib
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass
from enum import Enum

import aiohttp
import json
from urllib.parse import urlencode

from app.core.database_manager_extension import get_extended_database_manager
from app.core.multi_api_key_manager import get_api_key_manager, APIProvider
from app.core.smart_cache_ttl_optimizer import get_smart_ttl_optimizer, get_optimal_cache_ttl, update_cache_access_stats
from app.core.selective_storage_manager import get_storage_manager, StorageRequest
from app.archiving.archival_engine import get_archival_engine
from app.archiving.backup_manager import get_backup_manager


@dataclass
class APIResponse:
    """API 응답 데이터 클래스"""

    success: bool
    data: Optional[Dict] = None
    raw_data_id: Optional[str] = None
    duration_ms: Optional[int] = None
    error: Optional[str] = None
    from_cache: bool = False
    response_status: Optional[int] = None

    @classmethod
    def from_cache(cls, cached_data: Dict) -> "APIResponse":
        """캐시된 데이터로부터 응답 객체 생성"""
        return cls(success=True, data=cached_data, from_cache=True)

    @classmethod
    def error_response(cls, error_message: str) -> "APIResponse":
        """오류 응답 객체 생성"""
        return cls(success=False, error=error_message)


class UnifiedAPIClient:
    """통합 외부 API 클라이언트"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_manager = get_extended_database_manager()

        # Redis 캐시 매니저 (선택적)
        self.cache_manager = None
        try:
            from app.utils.redis_client import get_redis_client

            self.cache_manager = get_redis_client()
            self.logger.info("Redis 캐시 매니저 초기화 완료")
        except ImportError:
            self.logger.warning(
                "Redis 클라이언트를 사용할 수 없습니다. 파일 캐시를 사용합니다."
            )

        # API 키 매니저
        self.key_manager = get_api_key_manager()
        
        # 스마트 TTL 최적화 매니저
        self.smart_ttl_optimizer = get_smart_ttl_optimizer()
        
        # 선택적 저장 매니저
        self.storage_manager = get_storage_manager()
        
        # 아카이빙 시스템
        self.archival_engine = get_archival_engine()
        self.backup_manager = get_backup_manager()

        # 파일 매니저
        try:
            from app.utils.file_manager import FileManager

            self.file_manager = FileManager()
        except ImportError:
            self.file_manager = None
            self.logger.warning("파일 매니저를 사용할 수 없습니다.")

        # HTTP 세션 설정
        self.session = None

        # 만료 시간 설정 (API별) - 스마트 TTL과 함께 사용
        self.expiry_settings = {
            APIProvider.KTO: timedelta(days=7),  # KTO 데이터는 7일
            APIProvider.KMA: timedelta(hours=6),  # 날씨 데이터는 6시간
            APIProvider.GOOGLE: timedelta(days=30),  # Google 데이터는 30일
            APIProvider.NAVER: timedelta(days=1),  # Naver 데이터는 1일
        }

    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            headers={
                "User-Agent": "WeatherFlick-Batch/1.0 (Weather Travel Recommendation Service)"
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self.session:
            await self.session.close()

    def _generate_cache_key(
        self, api_provider: str, endpoint: str, params: Dict
    ) -> str:
        """캐시 키 생성"""
        # 파라미터를 정렬하여 일관된 키 생성
        sorted_params = sorted(params.items()) if params else []
        params_str = urlencode(sorted_params)

        # 캐시 키 생성
        key_input = f"{api_provider}:{endpoint}:{params_str}"
        key_hash = hashlib.md5(key_input.encode()).hexdigest()

        return f"api_cache:{api_provider.lower()}:{key_hash}"

    def _generate_api_key_hash(self, api_key: str) -> str:
        """API 키 해시 생성 (보안)"""
        return hashlib.sha256(api_key.encode()).hexdigest()[:16]

    def _calculate_expiry_time(self, api_provider: APIProvider) -> datetime:
        """API별 만료 시간 계산"""
        expiry_delta = self.expiry_settings.get(api_provider, timedelta(hours=24))
        return datetime.utcnow() + expiry_delta

    async def _get_cached_data(self, cache_key: str) -> Optional[Dict]:
        """캐시된 데이터 조회"""
        if not self.cache_manager:
            return None

        try:
            cached_data = await self.cache_manager.get(cache_key)
            if cached_data:
                self.logger.debug(f"캐시 히트: {cache_key}")
                return cached_data
        except Exception as e:
            self.logger.warning(f"캐시 조회 실패: {e}")

        return None

    async def _set_cached_data(self, cache_key: str, data: Dict, ttl: int = 3600):
        """데이터 캐시 저장"""
        if not self.cache_manager:
            return

        try:
            await self.cache_manager.set(cache_key, data, ttl)
            self.logger.debug(f"캐시 저장: {cache_key}")
        except Exception as e:
            self.logger.warning(f"캐시 저장 실패: {e}")

    def _store_raw_data(
        self,
        api_provider: APIProvider,
        endpoint: str,
        request_params: Dict,
        response_data: Dict,
        response_status: int,
        duration_ms: int,
        api_key: str,
    ) -> Optional[str]:
        """원본 데이터를 데이터베이스에 저장"""

        try:
            # 파일 시스템 백업 경로 생성
            file_path = None
            if self.file_manager:
                file_path = self._backup_to_file(api_provider, endpoint, response_data)

            # 데이터베이스 저장용 데이터 준비
            raw_data = {
                "api_provider": api_provider.value,
                "endpoint": endpoint,
                "request_method": "GET",
                "request_params": request_params,
                "response_status": response_status,
                "raw_response": response_data,
                "response_size": len(
                    json.dumps(response_data, ensure_ascii=False).encode("utf-8")
                ),
                "request_duration": duration_ms,
                "api_key_hash": self._generate_api_key_hash(api_key),
                "expires_at": self._calculate_expiry_time(api_provider),
                "file_path": file_path,
            }

            # 데이터베이스에 저장
            raw_data_id = self.db_manager.insert_raw_data(raw_data)

            # API별 메타데이터 저장
            self._store_api_metadata(
                api_provider, raw_data_id, endpoint, request_params
            )

            self.logger.debug(f"원본 데이터 저장 완료: {raw_data_id}")
            return raw_data_id

        except Exception as e:
            self.logger.error(f"원본 데이터 저장 실패: {e}")
            return None

    def _store_api_metadata(
        self, api_provider: APIProvider, raw_data_id: str, endpoint: str, params: Dict
    ):
        """API별 메타데이터 저장"""

        try:
            if api_provider == APIProvider.KTO:
                metadata = {
                    "raw_data_id": raw_data_id,
                    "content_type_id": params.get("contentTypeId"),
                    "area_code": params.get("areaCode"),
                    "sigungu_code": params.get("sigunguCode"),
                    "page_no": params.get("pageNo", 1),
                    "num_of_rows": params.get("numOfRows", 10),
                    "sync_batch_id": params.get("sync_batch_id"),
                }
                self.db_manager.insert_kto_metadata(metadata)

            elif api_provider == APIProvider.KMA:
                metadata = {
                    "raw_data_id": raw_data_id,
                    "base_date": params.get("base_date"),
                    "base_time": params.get("base_time"),
                    "nx": params.get("nx"),
                    "ny": params.get("ny"),
                    "forecast_type": self._determine_kma_forecast_type(endpoint),
                    "region_name": params.get("region_name"),
                }
                self.db_manager.insert_kma_metadata(metadata)

        except Exception as e:
            self.logger.error(f"메타데이터 저장 실패: {e}")

    def _determine_kma_forecast_type(self, endpoint: str) -> str:
        """KMA 엔드포인트로부터 예보 타입 결정"""
        if "getUltraSrtNcst" in endpoint:
            return "ultra_srt_ncst"
        elif "getUltraSrtFcst" in endpoint:
            return "ultra_srt_fcst"
        elif "getVilageFcst" in endpoint:
            return "vilage_fcst"
        else:
            return "unknown"

    def _backup_to_file(
        self, api_provider: APIProvider, endpoint: str, response_data: Dict
    ) -> Optional[str]:
        """응답 데이터를 파일 시스템에 백업"""

        if not self.file_manager:
            return None

        try:
            # 파일 경로 생성
            today = datetime.now().strftime("%Y-%m-%d")
            timestamp = datetime.now().strftime("%H%M%S")

            filename = f"{api_provider.value.lower()}_{endpoint}_{timestamp}.json"
            file_path = f"raw/{api_provider.value.lower()}/{today}/{filename}"

            # 파일 저장
            self.file_manager.save_json(file_path, response_data)

            return file_path

        except Exception as e:
            self.logger.error(f"파일 백업 실패: {e}")
            return None

    async def _execute_api_call(
        self, api_provider: APIProvider, endpoint: str, params: Dict
    ) -> Dict:
        """실제 API 호출 실행"""

        # API 키 획득
        if api_provider in [APIProvider.KTO, APIProvider.KMA]:
            api_key_info = self.key_manager.get_active_key(api_provider)
            if not api_key_info:
                error_msg = f"{api_provider.value} API 키가 설정되지 않았습니다."
                self.logger.error(error_msg)
                raise ValueError(error_msg)

            api_key = api_key_info.key
            params = params.copy()
            params["serviceKey"] = api_key
        elif api_provider == APIProvider.WEATHER:
            # OpenWeatherMap API의 경우 appid 파라미터 사용
            weather_api_key = os.getenv("WEATHER_API_KEY")
            if not weather_api_key:
                raise ValueError(f"{api_provider.value} API 키가 설정되지 않았습니다.")

            params = params.copy()
            params["appid"] = weather_api_key

        # Base URL 설정
        base_urls = {
            APIProvider.KTO: os.getenv(
                "KTO_API_BASE_URL", "http://apis.data.go.kr/B551011/KorService2"
            ),
            APIProvider.KMA: os.getenv(
                "KMA_API_BASE_URL",
                "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0",
            ),
            APIProvider.WEATHER: os.getenv(
                "WEATHER_API_BASE_URL", "http://api.openweathermap.org/data/2.5"
            ),
        }

        base_url = base_urls.get(api_provider)
        if not base_url:
            raise ValueError(
                f"{api_provider.value}에 대한 Base URL이 설정되지 않았습니다."
            )

        # 전체 URL 구성
        url = f"{base_url}/{endpoint}"

        # HTTP 요청 실행
        async with self.session.get(url, params=params) as response:
            response_text = await response.text()

            # JSON 응답 파싱
            try:
                response_data = json.loads(response_text)
            except json.JSONDecodeError:
                # XML 응답일 수 있음 (KTO/KMA API 오류 응답)
                if response_text.startswith("<"):
                    raise ValueError(f"XML 오류 응답: {response_text[:200]}...")
                else:
                    raise ValueError(f"JSON 파싱 실패: {response_text[:200]}...")

            # 응답 상태 확인
            if response.status != 200:
                raise ValueError(
                    f"HTTP 오류: {response.status} - {response_text[:200]}..."
                )

            # API별 성공 응답 확인
            if api_provider in [APIProvider.KTO, APIProvider.KMA]:
                # 디버깅을 위한 로그 추가
                self.logger.debug(f"Response data type: {type(response_data)}")
                self.logger.debug(f"Response data keys: {list(response_data.keys()) if isinstance(response_data, dict) else 'Not dict'}")
                
                # 정상 응답 형태 확인
                if "response" in response_data:
                    # 정상 JSON 응답
                    result_code = (
                        response_data.get("response", {})
                        .get("header", {})
                        .get("resultCode")
                    )
                    
                    self.logger.debug(f"Normal response result_code: {result_code}")
                    
                    if result_code not in ["00", "0000"]:
                        result_msg = (
                            response_data.get("response", {})
                            .get("header", {})
                            .get("resultMsg", "알 수 없는 오류")
                        )
                        raise ValueError(f"API 오류 ({result_code}): {result_msg}")
                        
                    # KTO/KMA API는 body 부분만 반환
                    return response_data.get("response", {}).get("body", {})
                    
                elif "resultCode" in response_data:
                    # 오류 응답 (XML에서 JSON으로 파싱된 경우)
                    result_code = response_data.get("resultCode")
                    result_msg = response_data.get("resultMsg", "알 수 없는 오류")
                    
                    self.logger.debug(f"Error response result_code: {result_code}")
                    
                    raise ValueError(f"API 오류 ({result_code}): {result_msg}")
                    
                else:
                    raise ValueError(f"알 수 없는 응답 형태: {response_data}")
                
            elif api_provider == APIProvider.WEATHER:
                # OpenWeatherMap API 오류 확인
                if "cod" in response_data:
                    cod = response_data["cod"]
                    # cod가 문자열일 수 있음 (예: "200")
                    if isinstance(cod, str):
                        cod = int(cod) if cod.isdigit() else 0
                    if cod != 200:
                        error_msg = response_data.get("message", "알 수 없는 오류")
                        raise ValueError(f"Weather API 오류 ({cod}): {error_msg}")

            return response_data

    async def call_api(
        self,
        api_provider: Union[APIProvider, str],
        endpoint: str,
        params: Optional[Dict] = None,
        store_raw: bool = True,
        cache_ttl: int = 3600,
        use_cache: bool = True,
    ) -> APIResponse:
        """
        통합 API 호출 메서드

        Args:
            api_provider: API 제공자 ('KTO', 'KMA', 'GOOGLE', 'NAVER')
            endpoint: API 엔드포인트
            params: 요청 파라미터
            store_raw: 원본 데이터 저장 여부
            cache_ttl: 캐시 TTL (초)
            use_cache: 캐시 사용 여부

        Returns:
            APIResponse: API 응답 결과
        """

        # API 제공자 타입 변환
        if isinstance(api_provider, str):
            try:
                api_provider = APIProvider(api_provider.upper())
            except ValueError:
                return APIResponse.error_response(
                    f"지원하지 않는 API 제공자: {api_provider}"
                )

        if params is None:
            params = {}

        # 1. 캐시 확인
        cache_key = self._generate_cache_key(api_provider.value, endpoint, params)
        if use_cache:
            cached_response = await self._get_cached_data(cache_key)
            if cached_response:
                # 캐시 히트 통계 업데이트
                await update_cache_access_stats(cache_key, was_hit=True)
                return APIResponse.from_cache(cached_response)
            else:
                # 캐시 미스 통계 업데이트
                await update_cache_access_stats(cache_key, was_hit=False)

        # 2. API 호출 실행
        start_time = time.time()

        try:
            if not self.session:
                return APIResponse.error_response(
                    "HTTP 세션이 초기화되지 않았습니다. async with 구문을 사용하세요."
                )

            response_data = await self._execute_api_call(api_provider, endpoint, params)
            duration_ms = int((time.time() - start_time) * 1000)

            # API 키 매니저에 성공 기록
            if api_provider in [APIProvider.KTO, APIProvider.KMA]:
                api_key_info = self.key_manager.get_active_key(api_provider)
                if api_key_info:
                    self.key_manager.record_api_call(
                        provider=api_provider,
                        key=api_key_info.key,
                        success=True,
                        is_rate_limited=False,
                        error_details=None
                    )

            # 3. 선택적 원본 데이터 저장
            raw_data_id = None
            if store_raw:
                raw_data_id = await self._store_raw_data_selective(
                    api_provider,
                    endpoint, 
                    params,
                    response_data,
                    200,
                    duration_ms
                )

            # 4. 캐시 저장 (스마트 TTL 최적화 적용)
            if use_cache:
                # 스마트 TTL 계산
                optimal_ttl = await get_optimal_cache_ttl(
                    cache_key, 
                    api_provider, 
                    endpoint,
                    context={"params": params, "response_size": len(str(response_data))}
                )
                
                # 사용자 지정 TTL과 최적 TTL 중 더 적절한 값 선택
                final_ttl = optimal_ttl if cache_ttl == 3600 else min(cache_ttl, optimal_ttl * 1.2)
                
                await self._set_cached_data(cache_key, response_data, final_ttl)
                
                self.logger.debug(f"캐시 저장: {cache_key}, TTL: {final_ttl}초 (최적화: {optimal_ttl}초)")

            self.logger.info(
                f"API 호출 성공: {api_provider.value}/{endpoint} ({duration_ms}ms)"
            )

            return APIResponse(
                success=True,
                data=response_data,
                raw_data_id=raw_data_id,
                duration_ms=duration_ms,
                response_status=200,
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            error_details = str(e)
            
            self.logger.error(
                f"API 호출 실패: {api_provider.value}/{endpoint} - {error_details} ({duration_ms}ms)"
            )

            # API 키 매니저에 오류 정보 기록
            if api_provider in [APIProvider.KTO, APIProvider.KMA]:
                api_key_info = self.key_manager.get_active_key(api_provider)
                if api_key_info:
                    # Rate limit 오류 확인
                    is_rate_limited = any(keyword in error_details.lower() for keyword in [
                        "rate limit", "quota", "한도", "초과", "제한", "429"
                    ])
                    
                    self.key_manager.record_api_call(
                        provider=api_provider,
                        key=api_key_info.key,
                        success=False,
                        is_rate_limited=is_rate_limited,
                        error_details=error_details
                    )

            # 오류도 원본 데이터로 저장 (선택적)
            if store_raw:
                error_response = {
                    "error": error_details,
                    "timestamp": datetime.utcnow().isoformat(),
                    "endpoint": endpoint,
                    "params": params
                }

                try:
                    api_key = "unknown"
                    if api_provider in [APIProvider.KTO, APIProvider.KMA]:
                        api_key_info = self.key_manager.get_active_key(api_provider)
                        if api_key_info:
                            api_key = api_key_info.key
                    elif api_provider == APIProvider.WEATHER:
                        api_key = os.getenv("WEATHER_API_KEY", "unknown")

                    self._store_raw_data(
                        api_provider,
                        endpoint,
                        params,
                        error_response,
                        500,
                        duration_ms,
                        api_key,
                    )
                except:
                    pass  # 오류 저장 실패는 무시

            return APIResponse.error_response(error_details)

    async def get_raw_data(self, raw_data_id: str) -> Optional[Dict]:
        """저장된 원본 데이터 조회"""
        try:
            return self.db_manager.get_raw_data(raw_data_id)
        except Exception as e:
            self.logger.error(f"원본 데이터 조회 실패: {e}")
            return None

    async def cleanup_expired_data(self) -> int:
        """만료된 원본 데이터 정리"""
        try:
            return self.db_manager.cleanup_expired_raw_data()
        except Exception as e:
            self.logger.error(f"만료 데이터 정리 실패: {e}")
            return 0
    
    async def run_archival_process(self, api_provider: str = None, endpoint: str = None, 
                                 dry_run: bool = False) -> Dict[str, Any]:
        """
        아카이빙 프로세스 실행
        
        Args:
            api_provider: 특정 API 제공자만 아카이빙 (예: KTO, KMA)
            endpoint: 특정 엔드포인트만 아카이빙
            dry_run: 실제 실행 없이 분석만 수행
        
        Returns:
            아카이빙 결과 요약
        """
        try:
            summary = await self.archival_engine.run_archival_process(
                api_provider=api_provider,
                endpoint=endpoint,
                dry_run=dry_run
            )
            
            self.logger.info(f"아카이빙 프로세스 완료: "
                           f"후보 {summary.total_candidates}개, "
                           f"성공 {summary.successful_backups}개, "
                           f"압축률 {summary.average_compression_ratio:.1f}%")
            
            return {
                "success": True,
                "summary": {
                    "total_candidates": summary.total_candidates,
                    "processed_items": summary.processed_items,
                    "successful_backups": summary.successful_backups,
                    "failed_backups": summary.failed_backups,
                    "skipped_items": summary.skipped_items,
                    "total_original_size_mb": summary.total_original_size_mb,
                    "total_compressed_size_mb": summary.total_compressed_size_mb,
                    "average_compression_ratio": summary.average_compression_ratio,
                    "processing_time_seconds": summary.processing_time_seconds
                }
            }
            
        except Exception as e:
            self.logger.error(f"아카이빙 프로세스 실패: {e}")
            return {"success": False, "error": str(e)}
    
    async def restore_archived_data(self, data_id: str) -> Optional[Dict[str, Any]]:
        """
        아카이빙된 데이터 복원
        
        Args:
            data_id: 복원할 데이터의 ID
        
        Returns:
            복원된 데이터 또는 None
        """
        try:
            restored_data = await self.archival_engine.restore_archived_data(data_id)
            
            if restored_data:
                self.logger.info(f"아카이빙된 데이터 복원 완료: {data_id}")
            else:
                self.logger.warning(f"아카이빙된 데이터를 찾을 수 없음: {data_id}")
            
            return restored_data
            
        except Exception as e:
            self.logger.error(f"아카이빙된 데이터 복원 실패: {data_id}, 오류: {e}")
            return None
    
    def get_archival_statistics(self) -> Dict[str, Any]:
        """
        아카이빙 시스템 통계 반환
        
        Returns:
            아카이빙 엔진 및 백업 관리자 통계
        """
        try:
            archival_stats = self.archival_engine.get_archival_statistics()
            backup_stats = self.backup_manager.get_backup_statistics()
            
            return {
                "archival_engine": archival_stats,
                "backup_manager": backup_stats,
                "combined_summary": {
                    "total_api_calls_archived": archival_stats["engine_statistics"]["total_items_processed"],
                    "total_backups_created": backup_stats["total_backups"],
                    "total_data_archived_mb": archival_stats["engine_statistics"]["total_data_archived_mb"],
                    "average_compression_ratio": backup_stats["average_compression_ratio"],
                    "last_archival_run": archival_stats["engine_statistics"]["last_run_time"]
                }
            }
            
        except Exception as e:
            self.logger.error(f"아카이빙 통계 조회 실패: {e}")
            return {"error": str(e)}
    
    async def _store_raw_data_selective(self, api_provider: APIProvider, endpoint: str,
                                      params: Dict, response_data: Dict, status_code: int,
                                      duration_ms: int) -> Optional[str]:
        """
        선택적 저장 시스템을 사용한 원본 데이터 저장
        
        Args:
            api_provider: API 제공자
            endpoint: API 엔드포인트
            params: 요청 파라미터
            response_data: 응답 데이터
            status_code: HTTP 상태 코드
            duration_ms: 실행 시간 (밀리초)
        
        Returns:
            저장된 데이터의 ID (선택적 저장되지 않은 경우 None)
        """
        try:
            # 응답 크기 계산
            import json
            response_json = json.dumps(response_data, ensure_ascii=False)
            response_size_bytes = len(response_json.encode('utf-8'))
            
            # 저장 요청 객체 생성
            storage_request = StorageRequest(
                provider=api_provider.value,
                endpoint=endpoint,
                request_params=params,
                response_data=response_data,
                response_size_bytes=response_size_bytes,
                status_code=status_code,
                execution_time_ms=float(duration_ms),
                created_at=datetime.now(),
                request_id=f"{api_provider.value}_{endpoint}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                additional_metadata={
                    "api_provider": api_provider.value,
                    "endpoint": endpoint,
                    "call_type": "unified_api_client"
                }
            )
            
            # 저장 여부 결정 및 실행
            should_store, reason, storage_metadata = self.storage_manager.should_store_response(storage_request)
            
            if should_store:
                stored_uuid = self.storage_manager.store_api_response(storage_request, storage_metadata)
                if stored_uuid:
                    self.logger.debug(f"선택적 저장 완료: {api_provider.value}/{endpoint} -> UUID: {stored_uuid}")
                    return stored_uuid
                else:
                    self.logger.warning(f"선택적 저장 실패: {api_provider.value}/{endpoint}")
                    return None
            else:
                self.logger.debug(f"선택적 저장 생략: {api_provider.value}/{endpoint} - {reason}")
                return None
                
        except Exception as e:
            self.logger.error(f"선택적 저장 중 오류: {e}")
            return None
    
    def _build_request_url(self, api_provider: APIProvider, endpoint: str, params: Dict) -> str:
        """요청 URL 빌드"""
        try:
            base_urls = {
                APIProvider.KTO: "https://apis.data.go.kr/B551011/KorService1",
                APIProvider.KMA: "https://apihub.kma.go.kr/api/typ01",
                APIProvider.WEATHER: "https://api.openweathermap.org/data/2.5"
            }
            
            base_url = base_urls.get(api_provider, "unknown://unknown")
            
            if params:
                from urllib.parse import urlencode
                query_string = urlencode(params)
                return f"{base_url}/{endpoint}?{query_string}"
            else:
                return f"{base_url}/{endpoint}"
                
        except Exception as e:
            self.logger.error(f"URL 빌드 실패: {e}")
            return f"{api_provider.value}://{endpoint}"


# 싱글톤 인스턴스
_unified_api_client = None


def get_unified_api_client() -> UnifiedAPIClient:
    """통합 API 클라이언트 인스턴스 반환"""
    global _unified_api_client
    if _unified_api_client is None:
        _unified_api_client = UnifiedAPIClient()
    return _unified_api_client


def reset_unified_api_client():
    """통합 API 클라이언트 싱글톤 인스턴스 리셋"""
    global _unified_api_client
    _unified_api_client = None
