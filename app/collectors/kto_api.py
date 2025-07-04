"""
한국관광공사 API 데이터 수집 모듈

한국관광공사의 관광정보 서비스 API를 사용하여 지역 코드, 관광지 정보 등을 수집합니다.
"""

import os
import requests
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

from app.core.multi_api_key_manager import get_api_key_manager, APIProvider

# 환경 변수 로드
load_dotenv(override=True)


class KTODataCollector:
    """한국관광공사 데이터 수집기"""

    def __init__(self):
        self.base_url = os.getenv(
            "KTO_API_BASE_URL", "http://apis.data.go.kr/B551011/KorService2"
        )

        self.logger = logging.getLogger(__name__)

        # 다중 API 키 시스템 사용
        self.key_manager = get_api_key_manager()
        active_key = self.key_manager.get_active_key(APIProvider.KTO)
        self.api_key = active_key.key if active_key else None

        if not self.api_key:
            self.logger.warning(
                "KTO_API_KEY가 설정되지 않았습니다. 테스트 데이터를 생성합니다."
            )
            self.api_key = "test_key"
        else:
            self.logger.info(f"다중 키 시스템에서 API 키 로드: {self.api_key[:10]}...")

        import requests.adapters
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        self.session = requests.Session()
        # SSL 인증서 검증을 활성화합니다. 시스템의 기본 인증서를 사용합니다.
        # 특정 인증서가 필요한 경우, os.getenv('SSL_CERT_PATH') 등으로 경로를 설정할 수 있습니다.
        self.session.verify = True
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

        from urllib3.util.retry import Retry

        retry_strategy = Retry(
            total=3, status_forcelist=[429, 500, 502, 503, 504], backoff_factor=1
        )
        adapter = requests.adapters.HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.logger.info(f"KTO API 수집기 초기화 완료 - Base URL: {self.base_url}")

    def _make_request(self, url: str, params: Dict) -> Optional[Dict]:
        """API 요청을 보내고 응답의 body 부분을 반환하는 헬퍼 메소드"""
        try:
            self.logger.debug(f"API 요청 URL: {url}")
            self.logger.debug(f"API 요청 파라미터: {params}")

            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()

            if not response.text.strip():
                self.logger.error("API 응답이 비어있습니다.")
                return None

            # XML 응답인지 먼저 확인 (오류 응답은 보통 XML 형태)
            response_text = response.text.strip()
            if response_text.startswith("<OpenAPI_ServiceResponse>"):
                return self._handle_xml_error_response(response_text)

            # JSON 응답 처리
            try:
                data = response.json()
            except ValueError as e:
                self.logger.error(f"JSON 파싱 오류: {e}")
                self.logger.error(f"응답 내용: {response.text[:500]}...")
                return None

            if data.get("response", {}).get("header", {}).get("resultCode") == "0000":
                return data.get("response", {}).get("body", {})
            else:
                error_msg = (
                    data.get("response", {})
                    .get("header", {})
                    .get("resultMsg", "알 수 없는 오류")
                )
                self.logger.error(f"API 오류: {error_msg} (URL: {url})")
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"API 호출 오류: {url}, {e}")
            return None
        except Exception as e:
            self.logger.error(f"알 수 없는 오류 발생: {url}, {e}")
            return None

    def _handle_xml_error_response(self, xml_response: str) -> Optional[Dict]:
        """XML 오류 응답 처리"""
        try:
            import xml.etree.ElementTree as ET

            root = ET.fromstring(xml_response)

            # 오류 정보 추출
            err_msg = root.find(".//errMsg")
            return_auth_msg = root.find(".//returnAuthMsg")
            return_reason_code = root.find(".//returnReasonCode")

            error_message = err_msg.text if err_msg is not None else "Unknown error"
            auth_message = return_auth_msg.text if return_auth_msg is not None else ""
            reason_code = (
                return_reason_code.text if return_reason_code is not None else ""
            )

            # API 요청 한도 초과 처리
            if "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR" in auth_message:
                self.logger.warning("⚠️ API 요청 한도 초과 - 잠시 대기 후 재시도합니다.")
                self._handle_rate_limit_exceeded()
                return None

            # 기타 인증 오류 처리
            elif "INVALID_REQUEST_PARAMETER_ERROR" in auth_message:
                self.logger.error("❌ 잘못된 요청 파라미터 오류")
                return None

            elif "SERVICE_KEY_IS_NOT_REGISTERED_ERROR" in auth_message:
                self.logger.error("❌ 등록되지 않은 서비스 키 오류")
                return None

            elif "TEMPORARILY_DISABLE_THE_SERVICEKEY_ERROR" in auth_message:
                self.logger.error("❌ 서비스 키 일시 비활성화 오류")
                return None

            else:
                self.logger.error(
                    f"❌ API 오류 - {error_message}: {auth_message} (코드: {reason_code})"
                )
                return None

        except ET.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
            self.logger.error(f"응답 내용: {xml_response[:500]}...")
            return None
        except Exception as e:
            self.logger.error(f"XML 오류 응답 처리 실패: {e}")
            return None

    def _handle_rate_limit_exceeded(self):
        """API 요청 한도 초과 시 처리"""
        # 요청 간격을 늘려서 재시도
        delay_seconds = 60  # 1분 대기
        self.logger.info(f"🕐 API 요청 한도 초과로 {delay_seconds}초 대기합니다...")
        time.sleep(delay_seconds)

        # 추가적인 대기 시간 설정 (점진적 백오프)
        if not hasattr(self, "_rate_limit_count"):
            self._rate_limit_count = 0

        self._rate_limit_count += 1
        if self._rate_limit_count > 3:
            # 3번 이상 한도 초과 시 더 긴 대기
            extra_delay = min(300, self._rate_limit_count * 60)  # 최대 5분
            self.logger.warning(
                f"⏳ 연속 한도 초과로 추가 {extra_delay}초 대기합니다..."
            )
            time.sleep(extra_delay)

    def _fetch_paginated_data(self, endpoint: str, params: Dict) -> List[Dict]:
        """페이징 처리된 API로부터 모든 데이터를 수집하는 헬퍼 메소드"""
        url = f"{self.base_url}/{endpoint}"
        all_items = []
        page_no = 1
        retry_count = 0
        max_retries = 3

        num_of_rows = params.get("numOfRows", 100)
        params["numOfRows"] = num_of_rows

        while True:
            params["pageNo"] = page_no

            body = self._make_request(url, params)

            if not body:
                # API 한도 초과로 인한 실패일 수 있으므로 재시도
                if retry_count < max_retries:
                    retry_count += 1
                    self.logger.info(
                        f"🔄 페이지 {page_no} 재시도 {retry_count}/{max_retries}"
                    )
                    time.sleep(5)  # 5초 대기 후 재시도
                    continue
                else:
                    self.logger.warning(
                        f"⚠️ 페이지 {page_no} 최대 재시도 횟수 초과, 다음 페이지로 진행"
                    )
                    break

            total_count = body.get("totalCount", 0)
            if total_count == 0:
                self.logger.info(f"수집할 데이터가 없습니다. (Endpoint: {endpoint})")
                break

            items_data = body.get("items", {})
            if not items_data or "item" not in items_data:
                self.logger.info(f"페이지 {page_no}에 더 이상 아이템이 없습니다.")
                break

            page_items = items_data["item"]
            if isinstance(page_items, dict):
                page_items = [page_items]

            all_items.extend(page_items)

            self.logger.info(
                f"[{endpoint}] 페이지 {page_no}: {len(page_items)}개 수집 (전체: {len(all_items)}/{total_count})"
            )

            # 성공적으로 데이터를 가져왔으므로 retry_count 초기화
            retry_count = 0

            if len(all_items) >= total_count or len(page_items) < num_of_rows:
                break

            page_no += 1
            # API 요청 간격을 늘려서 한도 초과 방지
            time.sleep(0.5)

        self.logger.info(f"[{endpoint}] 총 {len(all_items)}개 데이터 수집 완료")
        return all_items

    def _fetch_single_item(self, endpoint: str, params: Dict) -> Optional[Dict]:
        """단일 아이템을 반환하는 API를 호출하는 헬퍼 메소드"""
        url = f"{self.base_url}/{endpoint}"
        body = self._make_request(url, params)

        if not body:
            return None

        items = body.get("items", {}).get("item")
        if not items:
            self.logger.warning(f"응답에 items가 없습니다. (Endpoint: {endpoint})")
            return None

        if isinstance(items, list):
            return items[0] if items else None
        return items

    def _fetch_item_list(self, endpoint: str, params: Dict) -> List[Dict]:
        """아이템 리스트를 반환하는 API를 호출하는 헬퍼 메소드 (페이징 없음)"""
        url = f"{self.base_url}/{endpoint}"
        body = self._make_request(url, params)

        if not body:
            return []

        items = body.get("items", {}).get("item", [])
        if isinstance(items, dict):
            items = [items]

        self.logger.info(f"[{endpoint}] {len(items)}개 데이터 수집 완료")
        return items

    def get_area_codes(self, area_code: str = None) -> List[Dict]:
        """지역 코드 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "numOfRows": 50,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
        }
        if area_code:
            params["areaCode"] = area_code
        return self._fetch_item_list("areaCode2", params)

    def get_detailed_area_codes(self, area_code: str) -> List[Dict]:
        """세부 지역 코드 정보 조회 (시군구)"""
        params = {
            "serviceKey": self.api_key,
            "numOfRows": 100,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "areaCode": area_code,
        }
        return self._fetch_item_list("areaCode2", params)

    def get_tourist_attractions(
        self,
        area_code: str = None,
        sigungu_code: str = None,
        content_type_id: str = "12",
    ) -> List[Dict]:
        """관광지 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
            "contentTypeId": content_type_id,
        }
        if area_code:
            params["areaCode"] = area_code
        if sigungu_code:
            params["sigunguCode"] = sigungu_code
        return self._fetch_paginated_data("areaBasedList2", params)

    def get_attraction_detail(self, content_id: str) -> Optional[Dict]:
        """관광지 상세 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
            "defaultYN": "Y",
            "firstImageYN": "Y",
            "areacodeYN": "Y",
            "catcodeYN": "Y",
            "addrinfoYN": "Y",
            "mapinfoYN": "Y",
            "overviewYN": "Y",
        }
        return self._fetch_single_item("detailCommon2", params)

    def get_festivals_events(
        self,
        area_code: str = None,
        event_start_date: str = None,
        event_end_date: str = None,
    ) -> List[Dict]:
        """축제/행사 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
        }
        if area_code:
            params["areaCode"] = area_code
        if event_start_date:
            params["eventStartDate"] = event_start_date
        if event_end_date:
            params["eventEndDate"] = event_end_date
        return self._fetch_paginated_data("searchFestival2", params)

    def get_category_codes(self, content_type_id: str = None) -> List[Dict]:
        """카테고리 코드 조회"""
        params = {
            "serviceKey": self.api_key,
            "numOfRows": 100,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
        }
        if content_type_id:
            params["contentTypeId"] = content_type_id
        return self._fetch_item_list("categoryCode2", params)

    def get_location_based_list(
        self, mapx: str, mapy: str, radius: str = "1000", content_type_id: str = None
    ) -> List[Dict]:
        """위치 기반 관광정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
            "mapX": mapx,
            "mapY": mapy,
            "radius": radius,
        }
        if content_type_id:
            params["contentTypeId"] = content_type_id
        return self._fetch_paginated_data("locationBasedList2", params)

    def search_keyword(
        self, keyword: str, area_code: str = None, content_type_id: str = None
    ) -> List[Dict]:
        """키워드 검색"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "arrange": "A",
            "keyword": keyword,
        }
        if area_code:
            params["areaCode"] = area_code
        if content_type_id:
            params["contentTypeId"] = content_type_id
        return self._fetch_paginated_data("searchKeyword2", params)

    def get_stay_info(
        self, area_code: str = None, sigungu_code: str = None
    ) -> List[Dict]:
        """숙박 정보 조회"""
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
        return self._fetch_paginated_data("searchStay2", params)

    def get_detail_intro(self, content_id: str, content_type_id: str) -> Optional[Dict]:
        """관광지 상세 소개 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
            "contentTypeId": content_type_id,
        }
        return self._fetch_single_item("detailIntro2", params)

    def get_detail_info(self, content_id: str, content_type_id: str) -> List[Dict]:
        """추가 관광정보 상세 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
            "contentTypeId": content_type_id,
        }
        return self._fetch_item_list("detailInfo2", params)

    def get_image_info(self, content_id: str) -> List[Dict]:
        """관광지 이미지 정보 조회"""
        params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "contentId": content_id,
            "imageYN": "Y",
        }
        return self._fetch_item_list("detailImage2", params)

    def collect_comprehensive_data(self, area_codes: List[str] = None) -> Dict:
        """종합적인 관광 데이터 수집"""
        if not area_codes:
            area_codes = [
                "1",
                "6",
                "31",
                "32",
                "33",
                "34",
                "35",
                "36",
                "37",
                "38",
                "39",
            ]

        comprehensive_data = {
            "area_codes": [],
            "detailed_area_codes": [],
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

        self.logger.info("\n1. 지역 코드 수집")
        comprehensive_data["area_codes"] = self.get_area_codes()

        self.logger.info("\n2. 세부 지역 코드 수집")
        for area_code in area_codes:
            detailed_codes = self.get_detailed_area_codes(area_code)
            comprehensive_data["detailed_area_codes"].extend(detailed_codes)
            time.sleep(0.1)

        self.logger.info("\n3. 카테고리 코드 수집")
        comprehensive_data["category_codes"] = self.get_category_codes()

        self.logger.info("\n4. 컨텐츠 타입별 데이터 수집")
        for content_type_id, data_key in content_types.items():
            self.logger.info(f"\n- {data_key} 수집 (타입: {content_type_id})")
            for area_code in area_codes:
                attractions = self.get_tourist_attractions(
                    area_code=area_code, content_type_id=content_type_id
                )
                comprehensive_data[data_key].extend(attractions)
                time.sleep(0.2)

        self.logger.info("\n5. 축제/행사 정보 수집")
        current_date = datetime.now().strftime("%Y%m%d")
        festivals = self.get_festivals_events(event_start_date=current_date)
        comprehensive_data["festivals_events"].extend(festivals)

        self.logger.info("\n6. 숙박 정보 수집")
        for area_code in area_codes:
            stays = self.get_stay_info(area_code=area_code)
            comprehensive_data["accommodations"].extend(stays)
            time.sleep(0.2)

        return comprehensive_data

    def save_data_to_json(self, data: List[Dict], filename: str):
        """데이터를 JSON 파일로 저장"""
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=str)
            self.logger.info(f"데이터가 {filename}에 저장되었습니다.")
        except Exception as e:
            self.logger.error(f"파일 저장 오류: {e}")

    def generate_sample_data(self):
        """API가 작동하지 않을 때 사용할 샘플 데이터 생성"""
        sample_area_codes = [
            {"code": "1", "name": "서울"},
            {"code": "6", "name": "부산"},
            {"code": "39", "name": "제주특별자치도"},
        ]
        sample_attractions = [
            {
                "contentid": "126508",
                "title": "경복궁",
                "addr1": "서울특별시 종로구 사직로 161",
            },
            {
                "contentid": "264302",
                "title": "해운대해수욕장",
                "addr1": "부산광역시 해운대구 우동",
            },
        ]
        sample_festivals = [
            {
                "contentid": "2671577",
                "title": "서울 한강 여름축제",
                "eventstartdate": "20250701",
            },
        ]
        return sample_area_codes, sample_attractions, sample_festivals


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    collector = KTODataCollector()

    print("=== 한국관광공사 종합 데이터 수집 시작 ===")

    try:
        comprehensive_data = collector.collect_comprehensive_data()

        for data_type, data_list in comprehensive_data.items():
            if data_list:
                filename = f"{data_type}.json"
                collector.save_data_to_json(data_list, filename)

        print("\n=== 종합 데이터 수집 완료 ===")
        total_count = sum(
            len(data_list) for data_list in comprehensive_data.values() if data_list
        )
        print(f"총 수집 데이터: {total_count:,}개")

    except Exception as e:
        print(f"종합 데이터 수집 중 오류 발생: {e}")
        print("샘플 데이터 생성으로 전환합니다.")

        sample_area_codes, sample_attractions, sample_festivals = (
            collector.generate_sample_data()
        )

        collector.save_data_to_json(sample_area_codes, "area_codes.json")
        collector.save_data_to_json(sample_attractions, "tourist_attractions.json")
        collector.save_data_to_json(sample_festivals, "festivals_events.json")

        print("\n=== 샘플 데이터 수집 완료 ===")
