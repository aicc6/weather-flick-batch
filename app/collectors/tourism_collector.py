"""
한국관광공사 API 데이터 수집기 (리팩토링됨)

통합된 UnifiedKTOClient를 사용하여 관광정보 API 데이터를 수집합니다.
기존 인터페이스와의 호환성을 유지하면서 중복 코드를 제거했습니다.
"""

from typing import Dict, List, Optional, Any
import logging

from config.settings import get_api_config
from app.collectors.unified_kto_client import UnifiedKTOClient


class TourismDataCollector:
    """한국관광공사 API 데이터 수집기 (통합 클라이언트 사용)"""

    def __init__(self):
        self.settings = get_api_config()
        self.api_key = self.settings.kto_api_key
        self.logger = logging.getLogger(__name__)

        # 통합 KTO 클라이언트 사용
        self.client = UnifiedKTOClient(
            api_key=self.api_key, base_url="http://apis.data.go.kr/B551011/KorService2"
        )

        # 호환성을 위한 속성들 (클라이언트에서 제공)
        self.content_types = self.client.content_types
        self.area_codes = self.client.area_codes

    # ========== 통합 클라이언트로 위임하는 메서드들 ==========

    def get_area_codes(self) -> List[Dict]:
        """지역 코드 목록 조회"""
        return self.client.get_area_codes()

    def get_sigungu_codes(self, area_code: str) -> List[Dict]:
        """시군구 코드 목록 조회"""
        return self.client.get_sigungu_codes(area_code)

    def get_tourist_attractions(
        self,
        area_code: str,
        sigungu_code: str = None,
        content_type: str = "12",
        page_no: int = 1,
        num_of_rows: int = 100,
    ) -> List[Dict]:
        """관광지 정보 조회"""
        return self.client.get_tourist_attractions(
            area_code=area_code,
            sigungu_code=sigungu_code,
            content_type_id=content_type,
            page_no=page_no,
            num_of_rows=num_of_rows,
        )

    def get_attraction_detail(
        self, content_id: str, content_type: str = "12"
    ) -> Optional[Dict]:
        """관광지 상세 정보 조회"""
        return self.client.get_attraction_detail(content_id, content_type)

    def get_all_attractions_by_region(
        self, area_code: str, content_types: List[str] = None
    ) -> List[Dict]:
        """특정 지역의 모든 관광지 정보 수집"""
        return self.client.get_all_attractions_by_region(area_code, content_types)

    def get_festival_events(
        self, start_date: str, end_date: str, area_code: str = None
    ) -> List[Dict]:
        """축제/행사 정보 조회"""
        return self.client.get_festivals_events(
            start_date=start_date, end_date=end_date, area_code=area_code
        )

    # ========== 호환성을 위한 추가 메서드들 ==========

    def _safe_float(self, value: Any) -> Optional[float]:
        """안전한 float 변환 (호환성)"""
        return self.client._safe_float(value)

    def _is_api_key_valid(self) -> bool:
        """API 키 유효성 검사 (호환성)"""
        return self.client._is_api_key_valid()

    def get_request_stats(self) -> Dict[str, Any]:
        """API 요청 통계 조회"""
        return self.client.get_request_stats()

    def reset_daily_counter(self):
        """일일 요청 카운터 초기화"""
        self.client.reset_daily_counter()

    # ========== 프로퍼티로 클라이언트 통계 노출 ==========

    @property
    def daily_request_count(self) -> int:
        """일일 요청 횟수"""
        return self.client.daily_request_count

    @property
    def max_daily_requests(self) -> int:
        """최대 일일 요청 횟수"""
        return self.client.max_daily_requests

    @property
    def rate_limit_count(self) -> int:
        """API 한도 초과 횟수"""
        return self.client.rate_limit_count
