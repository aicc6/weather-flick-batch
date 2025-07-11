"""
한국관광공사 API 확장 데이터 수집 모듈

기존 KTODataCollector에서 누락된 엔드포인트들을 추가로 구현합니다.
"""

from typing import Dict, List, Optional
from app.collectors.kto_api import KTODataCollector


class ExtendedKTODataCollector(KTODataCollector):
    """확장된 한국관광공사 데이터 수집기"""
    
    def __init__(self):
        super().__init__()
        self.logger.info("확장된 KTO 데이터 수집기 초기화")
    
    # ===== 무장애 여행 정보 =====
    def get_detail_with_tour(self, content_id: str) -> Optional[Dict]:
        """무장애 여행정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
        }
        return self._fetch_single_item("detailWithTour2", params)
    
    # ===== 주차장 정보 =====
    def get_detail_parking(self, content_id: str) -> List[Dict]:
        """주차장 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
        }
        return self._fetch_item_list("detailParking2", params)
    
    # ===== 반려동물 동반 여행 목록 =====
    def get_pet_tour_list(
        self, 
        area_code: str = None, 
        sigungu_code: str = None,
        content_type_id: str = None
    ) -> List[Dict]:
        """반려동물 동반 가능 여행정보 목록 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
        }
        if area_code:
            params["areaCode"] = area_code
        if sigungu_code:
            params["sigunguCode"] = sigungu_code
        if content_type_id:
            params["contentTypeId"] = content_type_id
        return self._fetch_paginated_data("petTour2", params)
    
    # ===== 친환경 여행 정보 =====
    def get_green_tour_list(
        self,
        area_code: str = None,
        sigungu_code: str = None
    ) -> List[Dict]:
        """친환경 여행정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
        }
        if area_code:
            params["areaCode"] = area_code
        if sigungu_code:
            params["sigunguCode"] = sigungu_code
        return self._fetch_paginated_data("greenTour2", params)
    
    # ===== 웰니스 관광 정보 =====
    def get_healing_tour_list(
        self,
        area_code: str = None,
        sigungu_code: str = None
    ) -> List[Dict]:
        """웰니스 관광정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
        }
        if area_code:
            params["areaCode"] = area_code
        if sigungu_code:
            params["sigunguCode"] = sigungu_code
        return self._fetch_paginated_data("healingTour2", params)
    
    # ===== 추천 코스 정보 =====
    def get_course_list(
        self,
        area_code: str = None,
        sigungu_code: str = None,
        course_type: str = None
    ) -> List[Dict]:
        """추천 코스 목록 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
        }
        if area_code:
            params["areaCode"] = area_code
        if sigungu_code:
            params["sigunguCode"] = sigungu_code
        if course_type:
            params["courseType"] = course_type
        return self._fetch_paginated_data("courseList2", params)
    
    def get_course_detail(self, content_id: str) -> Optional[Dict]:
        """추천 코스 상세 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
        }
        return self._fetch_single_item("courseDetail2", params)
    
    # ===== 테마별 관광지 =====
    def get_theme_list(
        self,
        theme_code: str,
        area_code: str = None,
        sigungu_code: str = None
    ) -> List[Dict]:
        """테마별 관광지 목록 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
            "themeCode": theme_code,
        }
        if area_code:
            params["areaCode"] = area_code
        if sigungu_code:
            params["sigunguCode"] = sigungu_code
        return self._fetch_paginated_data("themeList2", params)
    
    # ===== 대중교통 정보 =====
    def get_public_transport(
        self,
        content_id: str,
        transport_type: str = None
    ) -> List[Dict]:
        """대중교통 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
        }
        if transport_type:
            params["transportType"] = transport_type
        return self._fetch_item_list("publicTransport2", params)
    
    def get_nearby_station(
        self,
        mapx: str,
        mapy: str,
        radius: str = "1000"
    ) -> List[Dict]:
        """인근 역/정류장 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "mapX": mapx,
            "mapY": mapy,
            "radius": radius,
        }
        return self._fetch_item_list("nearbyStation2", params)
    
    # ===== 리뷰 정보 =====
    def get_review_list(
        self,
        content_id: str,
        sort_type: str = "latest"
    ) -> List[Dict]:
        """리뷰 목록 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
            "sortType": sort_type,
        }
        return self._fetch_paginated_data("reviewList2", params)
    
    def get_review_detail(self, review_id: str) -> Optional[Dict]:
        """리뷰 상세 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "reviewId": review_id,
        }
        return self._fetch_single_item("reviewDetail2", params)
    
    # ===== 실시간 정보 =====
    def get_congestion_info(self, content_id: str) -> Optional[Dict]:
        """혼잡도 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
        }
        return self._fetch_single_item("congestionInfo2", params)
    
    def get_weather_info(self, content_id: str) -> Optional[Dict]:
        """관광지 날씨 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
        }
        return self._fetch_single_item("weatherInfo2", params)
    
    # ===== 교통시설 정보 =====
    def get_transport_facilities(
        self,
        area_code: str = None,
        sigungu_code: str = None
    ) -> List[Dict]:
        """교통시설 정보 조회 (콘텐츠타입 40)"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
            "contentTypeId": "40",  # 교통시설
        }
        if area_code:
            params["areaCode"] = area_code
        if sigungu_code:
            params["sigunguCode"] = sigungu_code
        return self._fetch_paginated_data("areaBasedList2", params)
    
    def collect_extended_comprehensive_data(self, area_codes: List[str] = None) -> Dict:
        """확장된 종합 관광 데이터 수집"""
        # 기본 데이터 수집
        comprehensive_data = super().collect_comprehensive_data(area_codes)
        
        # 확장 데이터 추가
        extended_data = {
            "transport_facilities": [],  # 교통시설
            "pet_tour_list": [],         # 반려동물 동반 여행
            "green_tour_list": [],       # 친환경 여행
            "healing_tour_list": [],     # 웰니스 관광
            "course_list": [],           # 추천 코스
            "theme_lists": {},           # 테마별 관광지
        }
        
        if not area_codes:
            area_codes = ["1", "39"]  # 서울, 제주 우선 수집
        
        self.logger.info("\n=== 확장 데이터 수집 시작 ===")
        
        # 1. 교통시설 정보
        self.logger.info("\n1. 교통시설 정보 수집")
        for area_code in area_codes:
            facilities = self.get_transport_facilities(area_code=area_code)
            extended_data["transport_facilities"].extend(facilities)
            time.sleep(0.2)
        
        # 2. 반려동물 동반 여행
        self.logger.info("\n2. 반려동물 동반 여행정보 수집")
        for area_code in area_codes:
            pet_tours = self.get_pet_tour_list(area_code=area_code)
            extended_data["pet_tour_list"].extend(pet_tours)
            time.sleep(0.2)
        
        # 3. 친환경 여행정보
        self.logger.info("\n3. 친환경 여행정보 수집")
        for area_code in area_codes:
            green_tours = self.get_green_tour_list(area_code=area_code)
            extended_data["green_tour_list"].extend(green_tours)
            time.sleep(0.2)
        
        # 4. 웰니스 관광정보
        self.logger.info("\n4. 웰니스 관광정보 수집")
        for area_code in area_codes:
            healing_tours = self.get_healing_tour_list(area_code=area_code)
            extended_data["healing_tour_list"].extend(healing_tours)
            time.sleep(0.2)
        
        # 5. 추천 코스
        self.logger.info("\n5. 추천 코스 정보 수집")
        for area_code in area_codes:
            courses = self.get_course_list(area_code=area_code)
            extended_data["course_list"].extend(courses)
            time.sleep(0.2)
        
        # 6. 테마별 관광지 (주요 테마만)
        self.logger.info("\n6. 테마별 관광지 수집")
        theme_codes = ["TC01", "TC02", "TC03"]  # 예시 테마 코드
        for theme_code in theme_codes:
            theme_data = []
            for area_code in area_codes:
                theme_attractions = self.get_theme_list(
                    theme_code=theme_code,
                    area_code=area_code
                )
                theme_data.extend(theme_attractions)
                time.sleep(0.2)
            extended_data["theme_lists"][theme_code] = theme_data
        
        # 기존 데이터와 병합
        comprehensive_data.update(extended_data)
        
        self.logger.info("\n=== 확장 데이터 수집 완료 ===")
        return comprehensive_data


if __name__ == "__main__":
    import logging
    import time
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    collector = ExtendedKTODataCollector()
    
    print("=== 확장된 한국관광공사 데이터 수집 테스트 ===")
    
    # 1. 반려동물 동반 여행 테스트
    print("\n1. 반려동물 동반 여행정보 테스트")
    pet_tours = collector.get_pet_tour_list(area_code="1")  # 서울
    print(f"   수집된 반려동물 동반 여행지: {len(pet_tours)}개")
    if pet_tours:
        print(f"   예시: {pet_tours[0].get('title', 'N/A')}")
    
    # 2. 교통시설 테스트
    print("\n2. 교통시설 정보 테스트")
    transport = collector.get_transport_facilities(area_code="1")
    print(f"   수집된 교통시설: {len(transport)}개")
    
    # 3. 무장애 여행정보 테스트 (특정 관광지)
    print("\n3. 무장애 여행정보 테스트")
    if pet_tours and len(pet_tours) > 0:
        content_id = pet_tours[0].get('contentid')
        if content_id:
            with_tour = collector.get_detail_with_tour(content_id)
            if with_tour:
                print("   ✅ 무장애 여행정보 조회 성공")
            else:
                print("   ⚠️ 무장애 여행정보 없음")
    
    print("\n=== 테스트 완료 ===")