"""
통합된 한국관광공사 API 클라이언트

기존의 kto_api.py와 tourism_collector.py의 기능을 통합하여
모든 KTO API 요청을 일관된 방식으로 처리합니다.
"""

from typing import Dict, List, Optional, Any
from datetime import datetime

from app.core.base_api_client import KTOAPIClient


class KTODataCollector(KTOAPIClient):
    """한국관광공사 데이터 수집기"""

    def __init__(self, api_key: str, base_url: str = None):
        super().__init__(api_key, base_url)

        # 컨텐츠 타입 정의
        self.content_types = {
            "12": "관광지",
            "14": "문화시설",
            "15": "축제공연행사",
            "25": "여행코스",
            "28": "레포츠",
            "32": "숙박",
            "38": "쇼핑",
            "39": "음식점",
        }

        # 지역 코드 정의 (시도 코드)
        self.area_codes = {
            "1": "서울",
            "2": "인천",
            "3": "대전",
            "4": "대구",
            "5": "광주",
            "6": "부산",
            "7": "울산",
            "8": "세종",
            "31": "경기",
            "32": "강원",
            "33": "충북",
            "34": "충남",
            "35": "경북",
            "36": "경남",
            "37": "전북",
            "38": "전남",
            "39": "제주",
        }

    def get_request_stats(self) -> Dict[str, Any]:
        """API 요청 통계 반환"""
        return {
            "daily_request_count": self.daily_request_count,
            "max_daily_requests": self.max_daily_requests,
            "rate_limit_count": self.rate_limit_count,
            "remaining_requests": max(
                0, self.max_daily_requests - self.daily_request_count
            ),
        }


    # ========== 관광지 정보 관련 메서드 ==========

    def get_tourist_attractions(
        self,
        area_code: str = None,
        sigungu_code: str = None,
        content_type_id: str = "12",
        page_no: int = 1,
        num_of_rows: int = 100,
    ) -> List[Dict]:
        """관광지 정보 조회"""
        params = {
            "contentTypeId": content_type_id,
            "numOfRows": num_of_rows,
            "pageNo": page_no,
            "arrange": "A",  # 제목순 정렬
        }

        if area_code:
            params["areaCode"] = area_code
        if sigungu_code:
            params["sigunguCode"] = sigungu_code

        body = self.make_request("areaBasedList2", params)
        if not body or "items" not in body:
            self.logger.debug(
                f"관광지 조회 결과가 없습니다. (지역: {area_code}, 타입: {content_type_id})"
            )
            return []

        items = body["items"].get("item", [])
        if isinstance(items, dict):
            items = [items]

        attractions = []
        for item in items:
            attraction_data = {
                "attraction_id": item.get("contentid"),
                "content_id": item.get("contentid"),  # 호환성 위해 중복 필드
                "area_code": area_code,
                "region_code": area_code,  # 호환성 위해 중복 필드
                "sigungu_code": sigungu_code,
                "content_type_id": content_type_id,
                "category_code": content_type_id,  # 호환성 위해 중복 필드
                "content_type_name": self.content_types.get(content_type_id, "기타"),
                "title": item.get("title", "").strip(),
                "attraction_name": item.get("title", "").strip(),  # 호환성
                "address": item.get("addr1", "").strip(),
                "address_detail": item.get("addr2", "").strip(),
                "latitude": self._safe_float(item.get("mapy")),
                "longitude": self._safe_float(item.get("mapx")),
                "phone": item.get("tel", "").strip(),
                "image_url": item.get("firstimage", "").strip(),
                "thumbnail_url": item.get("firstimage2", "").strip(),
                "created_date": item.get("createdtime"),
                "modified_date": item.get("modifiedtime"),
                "ml_level": item.get("mlevel", "1"),
                "cat1": item.get("cat1"),
                "cat2": item.get("cat2"),
                "cat3": item.get("cat3"),
                "booktour": item.get("booktour"),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            attractions.append(attraction_data)

        self.logger.debug(
            f"관광지 {len(attractions)}개 수집 완료 (지역: {area_code}, 타입: {content_type_id})"
        )
        return attractions

    def get_all_attractions_by_region(
        self, area_code: str, content_types: List[str] = None
    ) -> List[Dict]:
        """특정 지역의 모든 관광지 정보 수집"""
        if content_types is None:
            content_types = ["12", "14", "15", "25", "28"]  # 주요 관광 콘텐츠만

        all_attractions = []

        for content_type in content_types:
            page_attractions = self.fetch_paginated_data(
                "areaBasedList2",
                {
                    "areaCode": area_code,
                    "contentTypeId": content_type,
                    "arrange": "A",
                },
            )

            # 데이터 변환
            for item in page_attractions:
                attraction_data = {
                    "attraction_id": item.get("contentid"),
                    "content_id": item.get("contentid"),
                    "area_code": area_code,
                    "region_code": area_code,
                    "content_type_id": content_type,
                    "category_code": content_type,
                    "content_type_name": self.content_types.get(content_type, "기타"),
                    "title": item.get("title", "").strip(),
                    "attraction_name": item.get("title", "").strip(),
                    "address": item.get("addr1", "").strip(),
                    "address_detail": item.get("addr2", "").strip(),
                    "latitude": self._safe_float(item.get("mapy")),
                    "longitude": self._safe_float(item.get("mapx")),
                    "phone": item.get("tel", "").strip(),
                    "image_url": item.get("firstimage", "").strip(),
                    "thumbnail_url": item.get("firstimage2", "").strip(),
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
                all_attractions.append(attraction_data)

        area_name = self.area_codes.get(area_code, area_code)
        self.logger.info(f"{area_name} 지역 관광지 {len(all_attractions)}개 수집 완료")
        return all_attractions

    def get_attraction_detail(
        self, content_id: str, content_type: str = "12"
    ) -> Optional[Dict]:
        """관광지 상세 정보 조회"""
        params = {
            "contentId": content_id,
            "contentTypeId": content_type,
            "defaultYN": "Y",
            "firstImageYN": "Y",
            "areacodeYN": "Y",
            "catcodeYN": "Y",
            "addrinfoYN": "Y",
            "mapinfoYN": "Y",
            "overviewYN": "Y",
        }

        item = self.fetch_single_item("detailCommon2", params)
        if not item:
            return None

        detail_data = {
            "attraction_id": content_id,
            "content_id": content_id,
            "title": item.get("title", "").strip(),
            "description": item.get("overview", "").strip(),
            "homepage": item.get("homepage", "").strip(),
            "tel": item.get("tel", "").strip(),
            "address": item.get("addr1", "").strip(),
            "address_detail": item.get("addr2", "").strip(),
            "zipcode": item.get("zipcode", "").strip(),
            "latitude": self._safe_float(item.get("mapy")),
            "longitude": self._safe_float(item.get("mapx")),
            "image_url": item.get("firstimage", "").strip(),
            "thumbnail_url": item.get("firstimage2", "").strip(),
            "updated_at": datetime.now(),
        }

        self.logger.debug(f"관광지 상세정보 수집 완료: {content_id}")
        return detail_data

    # ========== 축제/행사 관련 메서드 ==========

    def get_festivals_events(
        self,
        start_date: str = None,
        end_date: str = None,
        area_code: str = None,
        event_start_date: str = None,
        event_end_date: str = None,
    ) -> List[Dict]:
        """축제/행사 정보 조회"""
        params = {
            "numOfRows": 100,
            "pageNo": 1,
            "arrange": "A",
        }

        # 파라미터 호환성 처리
        if start_date or event_start_date:
            params["eventStartDate"] = start_date or event_start_date
        if end_date or event_end_date:
            params["eventEndDate"] = end_date or event_end_date
        if area_code:
            params["areaCode"] = area_code

        body = self.make_request("searchFestival2", params)
        if not body or "items" not in body:
            self.logger.debug("축제/행사 조회 결과가 없습니다.")
            return []

        items = body["items"].get("item", [])
        if isinstance(items, dict):
            items = [items]

        festivals = []
        for item in items:
            festival_data = {
                "content_id": item.get("contentid"),
                "title": item.get("title", "").strip(),
                "event_start_date": item.get("eventstartdate"),
                "event_end_date": item.get("eventenddate"),
                "area_code": item.get("areacode"),
                "sigungu_code": item.get("sigungucode"),
                "address": item.get("addr1", "").strip(),
                "latitude": self._safe_float(item.get("mapy")),
                "longitude": self._safe_float(item.get("mapx")),
                "image_url": item.get("firstimage", "").strip(),
                "tel": item.get("tel", "").strip(),
                "created_at": datetime.now(),
            }
            festivals.append(festival_data)

        self.logger.info(f"축제/행사 {len(festivals)}개 수집 완료")
        return festivals

    # ========== 기타 메서드 ==========

    def get_category_codes(self, content_type_id: str = None) -> List[Dict]:
        """카테고리 코드 조회"""
        params = {
            "numOfRows": 100,
        }
        if content_type_id:
            params["contentTypeId"] = content_type_id

        body = self.make_request("categoryCode2", params)
        if not body or "items" not in body:
            return []

        items = body["items"].get("item", [])
        if isinstance(items, dict):
            items = [items]

        self.logger.info(f"카테고리 코드 {len(items)}개 수집 완료")
        return items

    def search_keyword(
        self, keyword: str, area_code: str = None, content_type_id: str = None
    ) -> List[Dict]:
        """키워드 검색"""
        params = {
            "arrange": "A",
            "keyword": keyword,
        }
        if area_code:
            params["areaCode"] = area_code
        if content_type_id:
            params["contentTypeId"] = content_type_id

        return self.fetch_paginated_data("searchKeyword2", params)

    def get_location_based_list(
        self, mapx: str, mapy: str, radius: str = "1000", content_type_id: str = None
    ) -> List[Dict]:
        """위치 기반 관광정보 조회"""
        params = {
            "arrange": "A",
            "mapX": mapx,
            "mapY": mapy,
            "radius": radius,
        }
        if content_type_id:
            params["contentTypeId"] = content_type_id

        return self.fetch_paginated_data("locationBasedList2", params)

    def _safe_float(self, value: Any) -> Optional[float]:
        """안전한 float 변환"""
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    # ========== 종합 데이터 수집 메서드 ==========

    def collect_comprehensive_data(self, area_codes: List[str] = None) -> Dict:
        """종합적인 관광 데이터 수집"""
        if not area_codes:
            area_codes = [
                "1",   # 서울
                "2",   # 인천
                "3",   # 대전
                "4",   # 대구
                "5",   # 광주
                "6",   # 부산
                "7",   # 울산
                "8",   # 세종
                "31",  # 경기도
                "32",  # 강원도
                "33",  # 충청북도
                "34",  # 충청남도
                "35",  # 경상북도
                "36",  # 경상남도
                "37",  # 전라북도
                "38",  # 전라남도
                "39",  # 제주도
            ]

        comprehensive_data = {
            "category_codes": [],
            "tourist_attractions": [],
            "cultural_facilities": [],
            "festivals_events": [],
            "travel_courses": [],
            "leisure_sports": [],
            "accommodations": [],
            "shopping": [],
            "restaurants": [],
        }

        content_types = {
            "12": "tourist_attractions",
            "14": "cultural_facilities",
            "15": "festivals_events",
            "25": "travel_courses",
            "28": "leisure_sports",
            "32": "accommodations",
            "38": "shopping",
            "39": "restaurants",
        }

        self.logger.info("=== 종합 관광 데이터 수집 시작 ===")


        # 1. 카테고리 코드 수집
        self.logger.info("1. 카테고리 코드 수집")
        comprehensive_data["category_codes"] = self.get_category_codes()

        # 2. 컨텐츠 타입별 데이터 수집
        self.logger.info("2. 컨텐츠 타입별 데이터 수집")
        for content_type_id, data_key in content_types.items():
            self.logger.info(f"- {data_key} 수집 (타입: {content_type_id})")
            for area_code in area_codes:
                attractions = self.get_all_attractions_by_region(
                    area_code, [content_type_id]
                )
                comprehensive_data[data_key].extend(attractions)

        # 3. 축제/행사 정보 수집 (현재 날짜 기준)
        self.logger.info("3. 축제/행사 정보 수집")
        current_date = datetime.now().strftime("%Y%m%d")
        festivals = self.get_festivals_events(start_date=current_date)
        comprehensive_data["festivals_events"].extend(festivals)

        self.logger.info("=== 종합 관광 데이터 수집 완료 ===")
        return comprehensive_data
