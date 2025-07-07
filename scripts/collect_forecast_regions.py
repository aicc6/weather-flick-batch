#!/usr/bin/env python3
"""
기상청 예보구역 데이터 수집 스크립트

기상청 API 허브의 단기예보구역 API를 활용하여 예보구역 정보를 수집하고
기존 지역정보와 통합하는 스크립트입니다.

Usage:
    python scripts/collect_forecast_regions.py --mode all
    python scripts/collect_forecast_regions.py --mode update
    python scripts/collect_forecast_regions.py --test
"""

import sys
import os
import argparse
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import aiohttp
import json

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_app_settings, get_api_config
from app.core.database_manager import DatabaseManager
from app.core.database_manager_extension import extend_database_manager
from app.core.multi_api_key_manager import MultiAPIKeyManager, APIProvider
from app.core.selective_storage_manager import get_storage_manager, StorageRequest

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ForecastRegionCollector:
    """기상청 예보구역 데이터 수집기"""
    
    def __init__(self):
        self.app_settings = get_app_settings()
        self.api_config = get_api_config()
        self.db_manager = extend_database_manager(DatabaseManager().sync_manager)
        self.api_key_manager = MultiAPIKeyManager()
        self.storage_manager = get_storage_manager()
        self.session: Optional[aiohttp.ClientSession] = None
        
        # 기상청 예보구역 API 설정
        self.forecast_region_api = "https://apihub.kma.go.kr/api/typ01/url/fct_shrt_reg.php"
        self.grid_conversion_api = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-dfs_xy_lonlat"
        
    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self.session:
            await self.session.close()
    
    async def collect_forecast_regions(self) -> List[Dict[str, Any]]:
        """전국 예보구역 정보 수집"""
        try:
            logger.info("기상청 예보구역 데이터 수집 시작")
            
            # 현재 시간 설정 (6시간 전 발표 기준)
            current_time = datetime.now()
            forecast_times = ["0500", "1100", "1700", "2300"]
            base_time = self._get_latest_forecast_time(current_time, forecast_times)
            base_date = current_time.strftime("%Y%m%d")
            
            # 전국 예보구역 코드 목록 (기상청 표준)
            region_codes = await self._get_all_region_codes()
            
            all_regions = []
            for region_code in region_codes:
                try:
                    region_data = await self._fetch_region_data(
                        region_code=region_code,
                        base_date=base_date,
                        base_time=base_time
                    )
                    
                    if region_data:
                        all_regions.append(region_data)
                        logger.info(f"예보구역 수집 완료: {region_code} - {region_data.get('region_name', 'Unknown')}")
                    
                    # API 호출 제한 고려 (0.1초 대기)
                    await asyncio.sleep(0.1)
                    
                except Exception as e:
                    logger.error(f"예보구역 {region_code} 수집 실패: {e}")
                    continue
            
            logger.info(f"총 {len(all_regions)}개 예보구역 수집 완료")
            return all_regions
            
        except Exception as e:
            logger.error(f"예보구역 데이터 수집 실패: {e}")
            return []
    
    async def _get_all_region_codes(self) -> List[str]:
        """전국 예보구역 코드 목록 반환"""
        # 기상청 표준 예보구역 코드 (일부 예시)
        # 실제 운영 시에는 기상청 API를 통해 동적으로 가져와야 함
        return [
            "11B00000",  # 서울
            "11B10101",  # 종로구
            "11B10102",  # 중구
            "11B10103",  # 용산구
            "11B10104",  # 성동구
            "11B10105",  # 광진구
            "26000000",  # 부산
            "26110000",  # 중구
            "26140000",  # 서구
            "26170000",  # 동구
            "27000000",  # 대구
            "27110000",  # 중구
            "27140000",  # 동구
            "27170000",  # 서구
            "28000000",  # 인천
            "28110000",  # 중구
            "28140000",  # 동구
            "28177000",  # 미추홀구
            "29000000",  # 광주
            "29110000",  # 동구
            "29140000",  # 서구
            "29155000",  # 남구
            "30000000",  # 대전
            "30110000",  # 동구
            "30140000",  # 중구
            "30170000",  # 서구
            "31000000",  # 울산
            "31110000",  # 중구
            "31140000",  # 남구
            "31170000",  # 동구
            "36000000",  # 세종
            "41000000",  # 경기
            "42000000",  # 강원
            "43000000",  # 충북
            "44000000",  # 충남
            "45000000",  # 전북
            "46000000",  # 전남
            "47000000",  # 경북
            "48000000",  # 경남
            "49000000",  # 제주
        ]
    
    def _get_latest_forecast_time(self, current_time: datetime, forecast_times: List[str]) -> str:
        """현재 시간 기준 최신 발표 시간 반환"""
        current_hour_min = current_time.strftime("%H%M")
        
        for i in range(len(forecast_times) - 1, -1, -1):
            if current_hour_min >= forecast_times[i]:
                return forecast_times[i]
        
        # 현재 시간이 모든 발표 시간보다 이른 경우, 전날 마지막 발표 시간
        return forecast_times[-1]
    
    async def _fetch_region_data(self, region_code: str, base_date: str, base_time: str) -> Optional[Dict[str, Any]]:
        """특정 예보구역 데이터 수집"""
        try:
            api_key_info = self.api_key_manager.get_active_key(APIProvider.KMA)
            if not api_key_info:
                logger.error("사용 가능한 API 키가 없습니다")
                return None
            
            api_key = api_key_info.key
            
            params = {
                "authKey": api_key,
                "reg": region_code,
                "tmfc": f"{base_date}{base_time}",
                "help": "1"  # 도움말 포함
            }
            
            logger.debug(f"API 호출 URL: {self.forecast_region_api}")
            logger.debug(f"API 파라미터: {params}")
            
            async with self.session.get(self.forecast_region_api, params=params) as response:
                response_text = await response.text()
                
                logger.debug(f"응답 상태: {response.status}")
                logger.debug(f"응답 헤더: {dict(response.headers)}")
                logger.debug(f"응답 내용 (처음 200자): {response_text[:200]}")
                
                # API 원본 응답 저장 시스템 추가
                await self._store_raw_api_response(
                    region_code=region_code,
                    request_url=str(response.url),
                    request_params=params,
                    response_text=response_text,
                    status_code=response.status,
                    response_headers=dict(response.headers)
                )
                
                if response.status == 200:
                    # 빈 응답 체크
                    if not response_text or response_text.strip() == "":
                        logger.warning(f"예보구역 {region_code}: 빈 응답")
                        return None
                    
                    # XML 또는 JSON 응답 처리
                    content_type = response.headers.get('content-type', '')
                    
                    if 'application/json' in content_type:
                        try:
                            data = json.loads(response_text)
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON 파싱 실패: {e}")
                            return None
                    else:
                        # XML 응답을 JSON으로 변환
                        data = self._parse_xml_response(response_text)
                    
                    if not data:
                        logger.warning(f"예보구역 {region_code}: 파싱된 데이터 없음")
                        return None
                    
                    return self._process_region_data(data, region_code)
                
                elif response.status == 401:
                    logger.error(f"API 인증 실패: {response_text}")
                    self.api_key_manager.mark_key_error(api_key, "Unauthorized")
                    return None
                
                else:
                    logger.error(f"API 호출 실패: {response.status} - {response_text}")
                    return None
                    
        except Exception as e:
            logger.error(f"예보구역 {region_code} 데이터 수집 중 오류: {e}")
            return None
    
    def _parse_xml_response(self, xml_data: str) -> Dict[str, Any]:
        """XML 응답을 파싱하여 딕셔너리로 변환"""
        try:
            import xml.etree.ElementTree as ET
            root = ET.fromstring(xml_data)
            
            # XML 구조에 따라 파싱 로직 구현
            # 실제 기상청 API 응답 구조에 맞게 수정 필요
            result = {}
            
            for item in root.findall('.//item'):
                for child in item:
                    result[child.tag] = child.text
            
            return result
            
        except Exception as e:
            logger.error(f"XML 파싱 실패: {e}")
            return {}
    
    def _process_region_data(self, raw_data: Dict[str, Any], region_code: str) -> Dict[str, Any]:
        """원시 데이터를 표준 형식으로 변환"""
        try:
            # 기상청 API 응답 구조에 맞게 데이터 추출
            processed_data = {
                "region_code": region_code,
                "region_name": raw_data.get("reg_name", ""),
                "region_name_full": raw_data.get("reg_full_name", ""),
                "parent_region_code": raw_data.get("parent_reg", ""),
                "forecast_office": raw_data.get("stn", ""),
                "forecast_office_name": raw_data.get("stn_name", ""),
                "center_latitude": self._safe_float(raw_data.get("lat")),
                "center_longitude": self._safe_float(raw_data.get("lon")),
                "grid_x": self._safe_int(raw_data.get("nx")),
                "grid_y": self._safe_int(raw_data.get("ny")),
                "region_level": self._determine_region_level(region_code),
                "administrative_code": self._map_to_administrative_code(region_code),
                "is_active": True,
                "forecast_region_type": "short_term",
                "collected_at": datetime.now(),
                "raw_data": raw_data
            }
            
            return processed_data
            
        except Exception as e:
            logger.error(f"데이터 처리 실패: {e}")
            return {}
    
    def _safe_float(self, value: Any) -> Optional[float]:
        """안전한 float 변환"""
        try:
            return float(value) if value is not None else None
        except (ValueError, TypeError):
            return None
    
    def _safe_int(self, value: Any) -> Optional[int]:
        """안전한 int 변환"""
        try:
            return int(value) if value is not None else None
        except (ValueError, TypeError):
            return None
    
    def _determine_region_level(self, region_code: str) -> int:
        """예보구역 코드를 기반으로 지역 레벨 결정"""
        if len(region_code) == 8:
            if region_code.endswith("00000"):
                return 1  # 시도 레벨
            elif region_code.endswith("000"):
                return 2  # 시군구 레벨
            else:
                return 3  # 읍면동 레벨
        return 0  # 알 수 없음
    
    def _map_to_administrative_code(self, region_code: str) -> Optional[str]:
        """예보구역 코드를 행정구역 코드로 매핑"""
        # 기상청 예보구역 코드와 행정구역 코드 매핑
        mapping = {
            "11B00000": "11",     # 서울
            "26000000": "26",     # 부산
            "27000000": "27",     # 대구
            "28000000": "28",     # 인천
            "29000000": "29",     # 광주
            "30000000": "30",     # 대전
            "31000000": "31",     # 울산
            "36000000": "36",     # 세종
            "41000000": "41",     # 경기
            "42000000": "42",     # 강원
            "43000000": "43",     # 충북
            "44000000": "44",     # 충남
            "45000000": "45",     # 전북
            "46000000": "46",     # 전남
            "47000000": "47",     # 경북
            "48000000": "48",     # 경남
            "49000000": "49",     # 제주
        }
        
        # 시도 코드 추출 (처음 2자리)
        if len(region_code) >= 2:
            sido_code = region_code[:2]
            return mapping.get(f"{sido_code}000000", region_code[:2])
        
        return None
    
    async def save_forecast_regions(self, regions: List[Dict[str, Any]]) -> int:
        """예보구역 데이터를 데이터베이스에 저장"""
        try:
            saved_count = 0
            
            for region in regions:
                try:
                    success = self._upsert_forecast_region(region)
                    if success:
                        saved_count += 1
                        
                except Exception as e:
                    logger.error(f"예보구역 저장 실패 {region.get('region_code')}: {e}")
                    continue
            
            logger.info(f"예보구역 저장 완료: {saved_count}/{len(regions)}")
            return saved_count
            
        except Exception as e:
            logger.error(f"예보구역 배치 저장 실패: {e}")
            return 0
    
    def _upsert_forecast_region(self, region_data: Dict[str, Any]) -> bool:
        """예보구역 데이터 UPSERT"""
        try:
            # 필수 데이터 검증
            region_code = region_data.get("region_code")
            region_name = region_data.get("region_name", "")
            latitude = region_data.get("center_latitude")
            longitude = region_data.get("center_longitude")
            
            if not region_code:
                logger.warning("예보구역 코드가 없어 저장을 건너뜁니다")
                return False
            
            # 좌표가 없는 경우 기본값 설정 또는 저장 건너뛰기
            if latitude is None or longitude is None:
                logger.warning(f"예보구역 {region_code}: 좌표 정보 없음, 저장 건너뜀")
                return False
            
            # weather_regions 테이블에 예보구역 정보 추가
            query = """
            INSERT INTO weather_regions (
                region_code, region_name, latitude, longitude, 
                grid_x, grid_y, is_active, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT (region_code) DO UPDATE SET
                region_name = EXCLUDED.region_name,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                grid_x = EXCLUDED.grid_x,
                grid_y = EXCLUDED.grid_y,
                is_active = EXCLUDED.is_active,
                updated_at = CURRENT_TIMESTAMP
            """
            
            params = (
                region_code,
                region_name,
                float(latitude),
                float(longitude),
                region_data.get("grid_x"),
                region_data.get("grid_y"),
                region_data.get("is_active", True)
            )
            
            self.db_manager.execute_update(query, params)
            logger.info(f"예보구역 저장 성공: {region_code} - {region_name}")
            return True
            
        except Exception as e:
            logger.error(f"예보구역 UPSERT 실패: {e}")
            logger.error(f"데이터: {region_data}")
            return False
    
    async def _store_raw_api_response(self, region_code: str, request_url: str, 
                                    request_params: Dict[str, Any], response_text: str,
                                    status_code: int, response_headers: Dict[str, str]):
        """
        기상청 예보구역 API 원본 응답을 선택적 저장 시스템에 저장
        
        Args:
            region_code: 예보구역 코드
            request_url: 요청 URL
            request_params: 요청 파라미터
            response_text: 응답 텍스트
            status_code: HTTP 상태 코드
            response_headers: 응답 헤더
        """
        try:
            # 응답 데이터 파싱 (JSON 또는 XML)
            response_data = {}
            content_type = response_headers.get('content-type', '')
            
            if 'application/json' in content_type:
                try:
                    response_data = json.loads(response_text)
                except json.JSONDecodeError:
                    response_data = {"raw_text": response_text}
            else:
                # XML 또는 기타 형식
                response_data = {"raw_text": response_text, "content_type": content_type}
            
            # 응답 크기 계산
            response_size_bytes = len(response_text.encode('utf-8'))
            
            # 저장 요청 객체 생성
            storage_request = StorageRequest(
                provider="KMA",
                endpoint="fct_shrt_reg",
                request_url=request_url,
                request_params=request_params,
                response_data=response_data,
                response_size_bytes=response_size_bytes,
                status_code=status_code,
                execution_time_ms=0.0,  # 비동기 환경에서는 측정하지 않음
                created_at=datetime.now(),
                request_id=f"forecast_region_{region_code}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                additional_metadata={
                    "region_code": region_code,
                    "api_type": "forecast_region",
                    "content_type": content_type,
                    "response_headers": response_headers
                }
            )
            
            # 저장 여부 결정 및 실행
            should_store, reason, storage_metadata = self.storage_manager.should_store_response(storage_request)
            
            if should_store:
                success = self.storage_manager.store_api_response(storage_request, storage_metadata)
                if success:
                    logger.debug(f"예보구역 {region_code} 원본 응답 저장 완료")
                else:
                    logger.warning(f"예보구역 {region_code} 원본 응답 저장 실패")
            else:
                logger.debug(f"예보구역 {region_code} 원본 응답 저장 생략: {reason}")
                
        except Exception as e:
            logger.error(f"예보구역 {region_code} 원본 응답 저장 중 오류: {e}")
    
    def get_storage_statistics(self) -> Dict[str, Any]:
        """
        저장 시스템 통계 반환
        
        Returns:
            저장 시스템 성능 및 사용량 통계
        """
        return self.storage_manager.get_statistics()


async def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="기상청 예보구역 데이터 수집")
    parser.add_argument("--mode", choices=["all", "update", "test"], default="all",
                       help="수집 모드: all(전체), update(업데이트), test(테스트)")
    parser.add_argument("--regions", help="특정 예보구역 코드 (쉼표로 구분)")
    parser.add_argument("--verbose", "-v", action="store_true", help="상세 로그 출력")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        async with ForecastRegionCollector() as collector:
            if args.mode == "test":
                logger.info("테스트 모드: 소수 예보구역만 수집")
                regions = await collector.collect_forecast_regions()
                regions = regions[:5]  # 처음 5개만 테스트
            else:
                logger.info(f"{args.mode} 모드로 예보구역 수집 시작")
                regions = await collector.collect_forecast_regions()
            
            if regions:
                saved_count = await collector.save_forecast_regions(regions)
                
                # 저장 시스템 통계 출력
                storage_stats = collector.get_storage_statistics()
                logger.info(f"예보구역 수집 완료: {saved_count}개 저장")
                logger.info(f"원본 응답 저장 통계: "
                          f"요청 {storage_stats.get('total_requests', 0)}개, "
                          f"저장 {storage_stats.get('storage_executions', 0)}개, "
                          f"성공률 {storage_stats.get('storage_success_rate', 0):.1f}%")
            else:
                logger.warning("수집된 예보구역 데이터가 없습니다")
                
    except Exception as e:
        logger.error(f"예보구역 수집 작업 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())