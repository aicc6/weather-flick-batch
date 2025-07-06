"""
데이터 변환 파이프라인

원본 API 데이터를 시스템에서 사용할 수 있는 표준화된 형태로 변환합니다.
"""

import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Type
from dataclasses import dataclass
from abc import ABC, abstractmethod

from app.core.database_manager_extension import get_extended_database_manager


@dataclass
class ValidationResult:
    """데이터 검증 결과"""

    is_valid: bool
    quality_score: float  # 0-100 점수
    errors: List[Dict]
    warnings: List[Dict]

    @property
    def has_errors(self) -> bool:
        return len(self.errors) > 0

    @property
    def has_warnings(self) -> bool:
        return len(self.warnings) > 0


@dataclass
class TransformationResult:
    """데이터 변환 결과"""

    success: bool
    processed_data: Optional[List[Dict]] = None
    quality_score: Optional[float] = None
    errors: Optional[List[Dict]] = None
    warnings: Optional[List[Dict]] = None
    transformation_time_ms: Optional[int] = None
    input_count: int = 0
    output_count: int = 0

    @classmethod
    def error_result(cls, error_message: str) -> "TransformationResult":
        """오류 결과 생성"""
        return cls(
            success=False,
            errors=[{"type": "transformation_error", "message": error_message}],
        )


class BaseDataTransformer(ABC):
    """데이터 변환기 기본 클래스"""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractmethod
    def transform(self, endpoint: str, raw_response: Dict) -> List[Dict]:
        """원본 데이터를 변환"""
        pass

    @abstractmethod
    def get_target_table(self, endpoint: str) -> str:
        """대상 테이블명 반환"""
        pass

    @abstractmethod
    def get_rule_name(self) -> str:
        """변환 규칙명 반환"""
        pass

    def _extract_items(self, raw_response: Dict) -> List[Dict]:
        """API 응답에서 아이템 목록 추출 (KTO/KMA 공통)"""
        try:
            # UnifiedAPIClient가 이미 body 부분만 반환하므로 직접 items에 접근
            # 기존 전체 응답 구조도 지원하기 위해 두 가지 경로 시도
            items = None
            
            # 1. UnifiedAPIClient에서 반환한 body 데이터인 경우
            if "items" in raw_response:
                items = raw_response.get("items", {})
            # 2. 전체 응답 구조인 경우 (하위 호환성)
            elif "response" in raw_response:
                items = raw_response.get("response", {}).get("body", {}).get("items", {})
            
            if not items:
                self.logger.debug(f"items가 없습니다. raw_response 키: {list(raw_response.keys())}")
                return []

            item_list = items.get("item", [])

            # 단일 아이템인 경우 리스트로 변환
            if isinstance(item_list, dict):
                return [item_list]
            elif isinstance(item_list, list):
                return item_list
            else:
                self.logger.debug(f"item이 없거나 잘못된 타입입니다. items 키: {list(items.keys()) if isinstance(items, dict) else type(items)}")
                return []

        except Exception as e:
            self.logger.error(f"아이템 추출 실패: {e}")
            return []

    def _clean_value(self, value: Any) -> Any:
        """값 정리 (공백 제거, None 처리 등)"""
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None
        return value

    def _is_valid_korea_coordinate(self, longitude: float, latitude: float) -> bool:
        """한국 영역 내 좌표인지 확인"""
        # 한국 영역 대략적 범위
        return (124.0 <= longitude <= 132.0) and (33.0 <= latitude <= 39.0)


class KTODataTransformer(BaseDataTransformer):
    """KTO API 데이터 변환기"""

    # 필드 매핑 정의
    FIELD_MAPPING = {
        "tourist_attractions": {
            "contentid": "content_id",
            "title": "attraction_name",
            "addr1": "address",
            "addr2": "address_detail",
            "mapx": "longitude",
            "mapy": "latitude",
            "firstimage": "first_image",
            "firstimage2": "thumbnail_url",
            "areacode": "region_code",
            "sigungucode": "sigungu_code",
            "cat1": "category_large_code",
            "cat2": "category_medium_code",
            "cat3": "category_small_code",
            "modifiedtime": "modifiedtime",
            "tel": "phone_number",
            "overview": "description",
            "homepage": "homepage_url",
            # 새로 추가된 필드들
            "booktour": "booktour",
            "createdtime": "createdtime",
            "telname": "telname",
            "faxno": "faxno",
            "zipcode": "zipcode",
            "mlevel": "mlevel",
        },
        "accommodations": {
            "contentid": "content_id",
            "title": "accommodation_name",
            "addr1": "address",
            "mapx": "longitude",
            "mapy": "latitude",
            "firstimage": "first_image",
            "areacode": "region_code",
            "sigungucode": "sigungu_code",
            "tel": "phone_number",
            "modifiedtime": "modifiedtime",
            # 새로 추가된 필드들
            "booktour": "booktour",
            "createdtime": "createdtime",
            "telname": "telname",
            "faxno": "faxno",
            "zipcode": "zipcode",
            "mlevel": "mlevel",
            "homepage": "homepage",
            "overview": "overview",
        },
        "festivals_events": {
            "contentid": "content_id",
            "title": "event_name",
            "addr1": "address",
            "mapx": "longitude",
            "mapy": "latitude",
            "firstimage": "first_image",
            "areacode": "region_code",
            "eventstartdate": "start_date",
            "eventenddate": "end_date",
            "tel": "phone_number",
            "modifiedtime": "modifiedtime",
            # 새로 추가된 필드들
            "booktour": "booktour",
            "createdtime": "createdtime",
            "telname": "telname",
            "faxno": "faxno",
            "zipcode": "zipcode",
            "mlevel": "mlevel",
            "homepage": "homepage",
            "overview": "overview",
        },
        "restaurants": {
            "contentid": "content_id",
            "title": "restaurant_name",
            "addr1": "address",
            "addr2": "address_detail",
            "mapx": "longitude",
            "mapy": "latitude",
            "firstimage": "first_image",
            "firstimage2": "thumbnail_url",
            "areacode": "region_code",
            "sigungucode": "sigungu_code",
            "cat1": "category_large_code",
            "cat2": "category_medium_code",
            "cat3": "category_small_code",
            "modifiedtime": "modifiedtime",
            "tel": "phone_number",
            "overview": "description",
            "homepage": "homepage_url",
            # 새로 추가된 필드들
            "booktour": "booktour",
            "createdtime": "createdtime",
            "telname": "telname",
            "faxno": "faxno",
            "zipcode": "zipcode",
            "mlevel": "mlevel",
        },
        "pet_tour_info": {
            "contentid": "content_id",
            "acmpyTypeCd": "pet_acpt_abl",  # 동반 가능 유형
            "acmpyNeedMtr": "title",        # 임시로 title에 필요사항 저장
            "etcAcmpyInfo": "pet_info",     # 기타 동반 정보
            "acmpyPsblCpam": "overview",    # 임시로 overview에 동반 가능 동물 저장
        },
    }

    def get_rule_name(self) -> str:
        return "KTO_API_STANDARD_TRANSFORM"

    def get_target_table(self, endpoint: str) -> str:
        """엔드포인트별 대상 테이블 결정"""
        endpoint_mapping = {
            "areaBasedList2": "tourist_attractions",  # 기본값, contentTypeId로 세분화됨
            "searchStay2": "accommodations",
            "searchFestival2": "festivals_events",
            "locationBasedList2": "tourist_attractions",
            "searchKeyword2": "tourist_attractions",
            "detailPetTour2": "pet_tour_info",  # 반려동물 동반여행 정보
        }

        # areaBasedList2의 경우 contentTypeId에 따라 결정
        if "areaBasedList2" in endpoint:
            return "dynamic"  # 나중에 contentTypeId로 결정
        
        for key, table in endpoint_mapping.items():
            if key in endpoint:
                return table

        return "tourist_attractions"  # 기본값
    
    def get_target_table_by_content_type(self, content_type_id: str) -> str:
        """contentTypeId에 따른 대상 테이블 결정"""
        content_type_mapping = {
            "12": "tourist_attractions",  # 관광지
            "14": "tourist_attractions",  # 문화시설 -> 관광지로 통합
            "15": "festivals_events",     # 축제공연행사
            "25": "tourist_attractions",  # 여행코스 -> 관광지로 통합
            "28": "tourist_attractions",  # 레포츠 -> 관광지로 통합
            "32": "accommodations",       # 숙박
            "38": "tourist_attractions",  # 쇼핑 -> 관광지로 통합
            "39": "restaurants",          # 음식점
        }
        
        return content_type_mapping.get(content_type_id, "tourist_attractions")

    def transform(self, endpoint: str, raw_response: Dict) -> List[Dict]:
        """KTO API 응답을 표준 형식으로 변환"""

        # 1. 응답에서 아이템 추출
        items = self._extract_items(raw_response)
        if not items:
            self.logger.warning(f"변환할 아이템이 없습니다: {endpoint}")
            return []

        # 2. 대상 테이블 결정
        target_table = self.get_target_table(endpoint)
        
        # areaBasedList2의 경우 첫 번째 아이템의 contentTypeId로 테이블 결정
        if target_table == "dynamic" and items:
            content_type_id = items[0].get("contenttypeid")
            if content_type_id:
                target_table = self.get_target_table_by_content_type(str(content_type_id))
                self.logger.debug(f"contentTypeId {content_type_id}로 테이블 결정: {target_table}")
            else:
                target_table = "tourist_attractions"  # 기본값

        # 3. 테이블별 변환 로직 적용
        if target_table == "tourist_attractions":
            return self._transform_tourist_attractions(items)
        elif target_table == "accommodations":
            return self._transform_accommodations(items)
        elif target_table == "festivals_events":
            return self._transform_festivals_events(items)
        elif target_table == "restaurants":
            return self._transform_restaurants(items)
        elif target_table == "pet_tour_info":
            return self._transform_pet_tour_info(items)
        else:
            raise ValueError(f"지원하지 않는 테이블: {target_table}")

    def _transform_tourist_attractions(self, items: List[Dict]) -> List[Dict]:
        """관광지 데이터 변환"""
        transformed_items = []
        mapping = self.FIELD_MAPPING["tourist_attractions"]

        for item in items:
            transformed = self._apply_field_mapping(item, mapping)

            # 좌표 데이터 검증 및 변환
            transformed = self._process_coordinates(transformed)

            # 카테고리 정보 통합
            transformed["category_info"] = {
                "large_code": transformed.get("category_large_code"),
                "medium_code": transformed.get("category_medium_code"),
                "small_code": transformed.get("category_small_code"),
            }

            # 상세 정보 처리 (JSONB 필드)
            transformed = self._process_detailed_info(transformed, item)

            # 메타데이터 추가
            transformed.update(self._add_metadata("KTO_API"))

            transformed_items.append(transformed)

        return transformed_items

    def _transform_accommodations(self, items: List[Dict]) -> List[Dict]:
        """숙박 데이터 변환"""
        transformed_items = []
        mapping = self.FIELD_MAPPING["accommodations"]

        for item in items:
            transformed = self._apply_field_mapping(item, mapping)
            transformed = self._process_coordinates(transformed)
            transformed.update(self._add_metadata("KTO_API"))
            transformed_items.append(transformed)

        return transformed_items

    def _transform_festivals_events(self, items: List[Dict]) -> List[Dict]:
        """축제/행사 데이터 변환"""
        transformed_items = []
        mapping = self.FIELD_MAPPING["festivals_events"]

        for item in items:
            transformed = self._apply_field_mapping(item, mapping)
            transformed = self._process_coordinates(transformed)

            # 날짜 데이터 처리
            transformed = self._process_event_dates(transformed)

            transformed.update(self._add_metadata("KTO_API"))
            transformed_items.append(transformed)

        return transformed_items

    def _transform_restaurants(self, items: List[Dict]) -> List[Dict]:
        """음식점 데이터 변환"""
        transformed_items = []
        mapping = self.FIELD_MAPPING["restaurants"]

        for item in items:
            transformed = self._apply_field_mapping(item, mapping)

            # 좌표 데이터 검증 및 변환
            transformed = self._process_coordinates(transformed)

            # 카테고리 정보 통합
            transformed["category_info"] = {
                "large_code": transformed.get("category_large_code"),
                "medium_code": transformed.get("category_medium_code"),
                "small_code": transformed.get("category_small_code"),
            }

            # 메타데이터 추가
            transformed.update(self._add_metadata("KTO_API"))

            transformed_items.append(transformed)

        return transformed_items

    def _transform_pet_tour_info(self, items: List[Dict]) -> List[Dict]:
        """반려동물 동반여행 정보 변환"""
        transformed_items = []
        mapping = self.FIELD_MAPPING["pet_tour_info"]

        for item in items:
            transformed = self._apply_field_mapping(item, mapping)

            # 좌표 데이터 검증 및 변환
            transformed = self._process_coordinates(transformed)

            # 반려동물 관련 정보 처리
            if transformed.get("pet_acpt_abl"):
                # Y/N 값을 boolean으로 변환하지 않고 문자열 그대로 유지
                transformed["pet_acpt_abl"] = str(transformed["pet_acpt_abl"]).strip()

            # 메타데이터 추가
            transformed.update(self._add_metadata("KTO_API"))

            transformed_items.append(transformed)

        return transformed_items

    def _apply_field_mapping(self, item: Dict, mapping: Dict) -> Dict:
        """필드 매핑 적용"""
        transformed = {}

        for api_field, db_field in mapping.items():
            if api_field in item:
                transformed[db_field] = self._clean_value(item[api_field])

        return transformed

    def _process_coordinates(self, transformed: Dict) -> Dict:
        """좌표 데이터 처리"""
        if "longitude" in transformed and "latitude" in transformed:
            try:
                longitude = (
                    float(transformed["longitude"])
                    if transformed["longitude"]
                    else None
                )
                latitude = (
                    float(transformed["latitude"]) if transformed["latitude"] else None
                )

                if longitude and latitude:
                    transformed["longitude"] = longitude
                    transformed["latitude"] = latitude

                    # 좌표 유효성 검증
                    if not self._is_valid_korea_coordinate(longitude, latitude):
                        transformed["coordinate_validation_error"] = True
                        self.logger.warning(
                            f"유효하지 않은 좌표: {longitude}, {latitude}"
                        )
                else:
                    transformed["longitude"] = None
                    transformed["latitude"] = None

            except (ValueError, TypeError) as e:
                transformed["longitude"] = None
                transformed["latitude"] = None
                transformed["coordinate_parsing_error"] = True
                self.logger.warning(f"좌표 파싱 오류: {e}")

        return transformed

    def _process_event_dates(self, transformed: Dict) -> Dict:
        """이벤트 날짜 처리"""
        for date_field in ["start_date", "end_date"]:
            if date_field in transformed and transformed[date_field]:
                try:
                    # YYYYMMDD 형식을 YYYY-MM-DD로 변환
                    date_str = str(transformed[date_field])
                    if len(date_str) == 8 and date_str.isdigit():
                        formatted_date = (
                            f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                        )
                        transformed[date_field] = formatted_date
                except Exception as e:
                    self.logger.warning(f"날짜 형식 변환 실패 ({date_field}): {e}")

        return transformed

    def _add_metadata(self, data_source: str) -> Dict:
        """메타데이터 추가"""
        return {
            "data_source": data_source,
            "processed_at": datetime.utcnow().isoformat(),
            "processing_status": "processed",
        }

    def _process_detailed_info(self, transformed: Dict, original_item: Dict) -> Dict:
        """상세 정보 처리 (JSONB 필드 생성)"""
        
        # detail_intro_info 처리 (detailIntro2 API 응답)
        intro_info = {}
        intro_fields = [
            'chkbabycarriage', 'chkcreditcard', 'chkpet', 'expagerange', 
            'expguide', 'heritage1', 'heritage2', 'heritage3', 'infocenter',
            'opendate', 'parking', 'restdate', 'useseason', 'usetime'
        ]
        
        for field in intro_fields:
            if field in original_item:
                intro_info[field] = self._clean_value(original_item[field])
        
        if intro_info:
            transformed["detail_intro_info"] = intro_info
        
        # detail_additional_info 처리 (detailInfo2 API 응답)
        additional_info = {}
        additional_fields = [
            'contentid', 'fldgubun', 'infoname', 'infotext', 'serialnum'
        ]
        
        for field in additional_fields:
            if field in original_item:
                additional_info[field] = self._clean_value(original_item[field])
        
        if additional_info:
            transformed["detail_additional_info"] = additional_info
        
        # 새로운 필드들 후처리
        transformed = self._process_new_fields(transformed)
        
        return transformed

    def _process_new_fields(self, transformed: Dict) -> Dict:
        """새로 추가된 필드들의 후처리"""
        
        # booktour 필드 정규화 (Y/N 값 확인)
        if "booktour" in transformed:
            booktour_value = transformed["booktour"]
            if booktour_value and str(booktour_value).upper() not in ['Y', 'N']:
                transformed["booktour"] = 'N'  # 기본값
        
        # 날짜 형식 검증 (createdtime, modifiedtime)
        for date_field in ['createdtime', 'modifiedtime']:
            if date_field in transformed and transformed[date_field]:
                date_value = str(transformed[date_field])
                if len(date_value) == 14 and date_value.isdigit():
                    # YYYYMMDDHHMMSS 형식 유지
                    pass
                else:
                    # 잘못된 형식이면 None으로 설정
                    transformed[date_field] = None
        
        # mlevel 정수 변환
        if "mlevel" in transformed and transformed["mlevel"]:
            try:
                transformed["mlevel"] = int(transformed["mlevel"])
            except (ValueError, TypeError):
                transformed["mlevel"] = None
        
        # 전화번호/팩스번호 형식 정리
        for phone_field in ['telname', 'faxno']:
            if phone_field in transformed and transformed[phone_field]:
                phone_value = str(transformed[phone_field]).strip()
                # 빈 문자열이면 None으로
                if not phone_value:
                    transformed[phone_field] = None
        
        # 우편번호 형식 검증
        if "zipcode" in transformed and transformed["zipcode"]:
            zipcode_value = str(transformed["zipcode"]).strip()
            if len(zipcode_value) not in [5, 6] or not zipcode_value.isdigit():
                transformed["zipcode"] = None
        
        return transformed


class KMADataTransformer(BaseDataTransformer):
    """KMA API 데이터 변환기"""

    # KMA 카테고리 코드 매핑 (DB 컬럼명에 맞춤)
    CATEGORY_MAPPING = {
        "POP": "precipitation_probability",  # 강수확률
        "PTY": "precipitation_type",  # 강수형태 (sky_condition에 매핑)
        "PCP": "precipitation",  # 강수량
        "REH": "humidity",  # 습도
        "SNO": "snow_amount",  # 적설량
        "SKY": "sky_condition",  # 하늘상태
        "TMP": "temperature",  # 기온
        "TMN": "min_temp",  # 최저기온
        "TMX": "max_temp",  # 최고기온
        "UUU": "wind_u_component",  # 풍속(동서성분)
        "VVV": "wind_v_component",  # 풍속(남북성분)
        "WAV": "wave_height",  # 파고
        "VEC": "wind_direction",  # 풍향
        "WSD": "wind_speed",  # 풍속
        "T1H": "temperature",  # 기온(1시간)
        "RN1": "precipitation",  # 1시간 강수량
        "LGT": "lightning",  # 낙뢰
    }

    def get_rule_name(self) -> str:
        return "KMA_API_STANDARD_TRANSFORM"

    def get_target_table(self, endpoint: str) -> str:
        """엔드포인트별 대상 테이블 결정"""
        if "getUltraSrtNcst" in endpoint:
            return "current_weather"
        elif "getUltraSrtFcst" in endpoint or "getVilageFcst" in endpoint:
            return "weather_forecasts"
        else:
            return "weather_forecasts"  # 기본값

    def transform(self, endpoint: str, raw_response: Dict) -> List[Dict]:
        """KMA API 응답을 표준 형식으로 변환"""

        # 1. 응답에서 아이템 추출
        items = self._extract_items(raw_response)
        if not items:
            return []

        # 2. 대상 테이블별 변환
        target_table = self.get_target_table(endpoint)

        if target_table == "current_weather":
            return self._transform_current_weather(items)
        elif target_table == "weather_forecasts":
            return self._transform_weather_forecast(items)
        else:
            raise ValueError(f"지원하지 않는 테이블: {target_table}")

    def _transform_current_weather(self, items: List[Dict]) -> List[Dict]:
        """현재 날씨 데이터 변환"""
        # 카테고리별로 데이터 그룹화
        weather_data = {}

        for item in items:
            base_date = item.get("baseDate")
            base_time = item.get("baseTime")
            nx = item.get("nx")
            ny = item.get("ny")
            category = item.get("category")
            value = item.get("obsrValue")

            # 고유 키 생성
            key = f"{base_date}_{base_time}_{nx}_{ny}"

            if key not in weather_data:
                weather_data[key] = {
                    "base_date": base_date,
                    "base_time": base_time,
                    "nx": nx,
                    "ny": ny,
                    "observation_time": self._format_datetime(base_date, base_time),
                    "data_source": "KMA_API",
                    "processed_at": datetime.utcnow().isoformat(),
                }

            # 카테고리별 값 매핑
            if category in self.CATEGORY_MAPPING:
                field_name = self.CATEGORY_MAPPING[category]
                weather_data[key][field_name] = self._convert_weather_value(
                    category, value
                )

        return list(weather_data.values())

    def _transform_weather_forecast(self, items: List[Dict]) -> List[Dict]:
        """날씨 예보 데이터 변환"""
        forecast_data = {}

        for item in items:
            base_date = item.get("baseDate")
            base_time = item.get("baseTime")
            fcst_date = item.get("fcstDate")
            fcst_time = item.get("fcstTime")
            nx = item.get("nx")
            ny = item.get("ny")
            category = item.get("category")
            value = item.get("fcstValue")

            # 고유 키 생성 (예보 날짜/시간별)
            key = f"{fcst_date}_{fcst_time}_{nx}_{ny}"

            if key not in forecast_data:
                forecast_data[key] = {
                    "base_date": base_date,
                    "base_time": base_time,
                    "forecast_date": fcst_date,  # YYYYMMDD 형식 유지 (DB 스키마에 맞춤)
                    "forecast_time": fcst_time,
                    "nx": int(nx) if nx else None,
                    "ny": int(ny) if ny else None,
                    "region_code": self._get_region_code_from_grid(nx, ny),
                    "forecast_type": "short",
                    "data_source": "KMA_API",
                    "processed_at": datetime.utcnow().isoformat(),
                }

            # 카테고리별 값 매핑
            if category in self.CATEGORY_MAPPING:
                field_name = self.CATEGORY_MAPPING[category]
                forecast_data[key][field_name] = self._convert_weather_value(
                    category, value
                )

        return list(forecast_data.values())

    def _format_datetime(self, date_str: str, time_str: str) -> str:
        """날짜/시간 문자열을 ISO 형식으로 변환"""
        try:
            # YYYYMMDD HHMM -> YYYY-MM-DD HH:MM:00
            date_part = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
            time_part = f"{time_str[:2]}:{time_str[2:4]}:00"
            return f"{date_part} {time_part}"
        except Exception:
            return f"{date_str} {time_str}"

    def _format_date(self, date_str: str) -> str:
        """날짜 문자열을 ISO 형식으로 변환"""
        try:
            return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        except Exception:
            return date_str

    def _convert_weather_value(self, category: str, value: str) -> Any:
        """날씨 데이터 값 변환"""
        if not value or value.strip() == "":
            return None

        try:
            # 숫자형 데이터
            if category in ["POP", "REH", "TMP", "TMN", "TMX", "WSD", "VEC", "T1H"]:
                return float(value)

            # 강수량/적설량 (특수 처리)
            elif category in ["PCP", "SNO", "RN1"]:
                if value == "강수없음" or value == "적설없음":
                    return 0.0
                else:
                    # mm 단위 숫자 추출
                    numeric_value = "".join(
                        filter(lambda x: x.isdigit() or x == ".", value)
                    )
                    return float(numeric_value) if numeric_value else 0.0

            # 코드형 데이터는 문자열 그대로
            else:
                return value.strip()

        except Exception as e:
            self.logger.warning(f"날씨 값 변환 실패 [{category}: {value}]: {e}")
            return None

    def _get_region_code_from_grid(self, nx: int, ny: int) -> str:
        """격자 좌표에서 지역 코드 변환"""
        # 기본적인 격자-지역 매핑 (실제로는 더 정교한 매핑이 필요)
        grid_to_region = {
            (60, 127): "1100000000",  # 서울
            (98, 76): "2600000000",   # 부산
            (89, 90): "2700000000",   # 대구
            (55, 124): "2800000000",  # 인천
            (67, 100): "2900000000",  # 광주
            (68, 100): "3000000000",  # 대전
            (102, 84): "3100000000",  # 울산
            (52, 38): "5000000000",   # 제주
        }
        
        # 격자 좌표로 지역 코드 찾기
        if nx and ny:
            region_code = grid_to_region.get((int(nx), int(ny)))
            if region_code:
                return region_code
            else:
                # 기본 지역 코드 (서울)
                return "1100000000"
        
        return "1100000000"  # 기본값


class DataValidatorRegistry:
    """데이터 검증기 레지스트리"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def validate(self, api_provider: str, data: List[Dict]) -> ValidationResult:
        """데이터 검증 실행"""

        if api_provider == "KTO":
            return self._validate_kto_data(data)
        elif api_provider == "KMA":
            return self._validate_kma_data(data)
        else:
            return ValidationResult(
                is_valid=True,
                quality_score=50.0,
                errors=[],
                warnings=[
                    {
                        "type": "unknown_provider",
                        "message": f"알 수 없는 API 제공자: {api_provider}",
                    }
                ],
            )

    def _validate_kto_data(self, data: List[Dict]) -> ValidationResult:
        """KTO 데이터 검증"""
        errors = []
        warnings = []

        if not data:
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                errors=[{"type": "empty_data", "message": "데이터가 비어있습니다"}],
                warnings=[],
            )

        required_fields = ["content_id", "attraction_name"]
        valid_count = 0

        for i, item in enumerate(data):
            # 필수 필드 검증
            missing_fields = [field for field in required_fields if not item.get(field)]
            if missing_fields:
                errors.append(
                    {
                        "type": "missing_required_fields",
                        "message": f"레코드 {i}: 필수 필드 누락 - {missing_fields}",
                        "record_index": i,
                    }
                )
            else:
                valid_count += 1

            # 좌표 검증
            if item.get("coordinate_validation_error"):
                warnings.append(
                    {
                        "type": "invalid_coordinates",
                        "message": f"레코드 {i}: 유효하지 않은 좌표",
                        "record_index": i,
                    }
                )

        # 품질 점수 계산
        completeness_score = (valid_count / len(data)) * 100

        return ValidationResult(
            is_valid=len(errors) == 0,
            quality_score=completeness_score,
            errors=errors,
            warnings=warnings,
        )

    def _validate_kma_data(self, data: List[Dict]) -> ValidationResult:
        """KMA 데이터 검증"""
        errors = []
        warnings = []

        if not data:
            return ValidationResult(
                is_valid=False,
                quality_score=0.0,
                errors=[{"type": "empty_data", "message": "데이터가 비어있습니다"}],
                warnings=[],
            )

        valid_count = 0

        for i, item in enumerate(data):
            # 기본 필드 검증
            if not item.get("base_date") or not item.get("base_time"):
                errors.append(
                    {
                        "type": "missing_datetime",
                        "message": f"레코드 {i}: 날짜/시간 정보 누락",
                        "record_index": i,
                    }
                )
            else:
                valid_count += 1

        # 품질 점수 계산
        quality_score = (valid_count / len(data)) * 100

        return ValidationResult(
            is_valid=len(errors) == 0,
            quality_score=quality_score,
            errors=errors,
            warnings=warnings,
        )


class DataTransformationPipeline:
    """데이터 변환 파이프라인"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_manager = get_extended_database_manager()

        # 변환기 등록
        self.transformers = {
            "KTO": KTODataTransformer(),
            "KMA": KMADataTransformer(),
        }

        # 검증기 등록
        self.validators = DataValidatorRegistry()

    def _extract_items(self, raw_response: Dict) -> List[Dict]:
        """API 응답에서 아이템 목록 추출 (KTO/KMA 공통)"""
        try:
            # UnifiedAPIClient가 이미 body 부분만 반환하므로 직접 items에 접근
            # 기존 전체 응답 구조도 지원하기 위해 두 가지 경로 시도
            items = None
            
            # 1. UnifiedAPIClient에서 반환한 body 데이터인 경우
            if "items" in raw_response:
                items = raw_response.get("items", {})
            # 2. 전체 응답 구조인 경우 (하위 호환성)
            elif "response" in raw_response:
                items = raw_response.get("response", {}).get("body", {}).get("items", {})
            
            if not items:
                self.logger.debug(f"items가 없습니다. raw_response 키: {list(raw_response.keys())}")
                return []

            item_list = items.get("item", [])

            # 단일 아이템인 경우 리스트로 변환
            if isinstance(item_list, dict):
                return [item_list]
            elif isinstance(item_list, list):
                return item_list
            else:
                self.logger.debug(f"item이 없거나 잘못된 타입입니다. items 키: {list(items.keys()) if isinstance(items, dict) else type(items)}")
                return []

        except Exception as e:
            self.logger.error(f"아이템 추출 실패: {e}")
            return []

    def _clean_value(self, value):
        """값 정리 (빈 문자열을 None으로 변환)"""
        if value is None:
            return None
        if isinstance(value, str):
            cleaned = value.strip()
            return cleaned if cleaned else None
        return value

    async def transform_raw_data(self, raw_data_id: str) -> TransformationResult:
        """원본 데이터를 가공 데이터로 변환"""

        transformation_start = time.time()

        try:
            # 1. 원본 데이터 로드
            raw_data = self.db_manager.get_raw_data(raw_data_id)
            if not raw_data:
                return TransformationResult.error_result(
                    "원본 데이터를 찾을 수 없습니다"
                )

            # 2. 적절한 변환기 선택
            api_provider = raw_data.get("api_provider")
            transformer = self.transformers.get(api_provider)

            if not transformer:
                return TransformationResult.error_result(
                    f"지원하지 않는 API 제공자: {api_provider}"
                )

            # 3. 데이터 변환 실행
            endpoint = raw_data.get("endpoint", "")
            raw_response = raw_data.get("raw_response", {})

            processed_data = transformer.transform(endpoint, raw_response)

            # 4. 데이터 유효성 검증
            validation_result = self.validators.validate(api_provider, processed_data)

            # 5. 변환 로그 기록
            transformation_time_ms = int((time.time() - transformation_start) * 1000)

            transformation_log = {
                "raw_data_id": raw_data_id,
                "target_table": transformer.get_target_table(endpoint),
                "transformation_rule": transformer.get_rule_name(),
                "input_record_count": len(
                    raw_response.get("response", {})
                    .get("body", {})
                    .get("items", {})
                    .get("item", [])
                ),
                "output_record_count": len(processed_data),
                "error_count": len(validation_result.errors),
                "transformation_time_ms": transformation_time_ms,
                "status": (
                    "success" if validation_result.is_valid else "partial_failure"
                ),
                "error_details": {
                    "errors": validation_result.errors,
                    "warnings": validation_result.warnings,
                },
                "quality_score": validation_result.quality_score,
            }

            self.db_manager.log_transformation(transformation_log)

            self.logger.info(
                f"데이터 변환 완료: {raw_data_id} -> {len(processed_data)}건 (품질점수: {validation_result.quality_score:.1f})"
            )

            return TransformationResult(
                success=True,
                processed_data=processed_data,
                quality_score=validation_result.quality_score,
                errors=validation_result.errors,
                warnings=validation_result.warnings,
                transformation_time_ms=transformation_time_ms,
                input_count=transformation_log["input_record_count"],
                output_count=len(processed_data),
            )

        except Exception as e:
            transformation_time_ms = int((time.time() - transformation_start) * 1000)
            self.logger.error(f"데이터 변환 실패: {raw_data_id} - {e}")

            # 실패 로그 기록
            try:
                error_log = {
                    "raw_data_id": raw_data_id,
                    "target_table": "unknown",
                    "transformation_rule": "unknown",
                    "transformation_time_ms": transformation_time_ms,
                    "status": "failure",
                    "error_details": {
                        "errors": [
                            {"type": "transformation_exception", "message": str(e)}
                        ]
                    },
                }
                self.db_manager.log_transformation(error_log)
            except:
                pass  # 로그 기록 실패는 무시

            return TransformationResult.error_result(str(e))

    def get_supported_providers(self) -> List[str]:
        """지원하는 API 제공자 목록 반환"""
        return list(self.transformers.keys())

    def transform_data(self, api_provider: str, endpoint: str, raw_response: Dict) -> TransformationResult:
        """원본 API 응답 데이터를 직접 변환 (테스트용)"""
        
        try:
            transformation_start = time.time()
            
            # 1. 적절한 변환기 선택
            transformer = self.transformers.get(api_provider)
            if not transformer:
                return TransformationResult.error_result(
                    f"지원하지 않는 API 제공자: {api_provider}"
                )

            # 2. 데이터 변환 실행
            processed_data = transformer.transform(endpoint, raw_response)

            # 3. 데이터 유효성 검증
            validation_result = self.validators.validate(api_provider, processed_data)

            # 4. 변환 시간 계산
            transformation_time_ms = int((time.time() - transformation_start) * 1000)

            self.logger.info(
                f"데이터 변환 완료 (테스트): {len(processed_data)}건 (품질점수: {validation_result.quality_score:.1f})"
            )

            return TransformationResult(
                success=True,
                processed_data=processed_data,
                quality_score=validation_result.quality_score,
                errors=validation_result.errors,
                warnings=validation_result.warnings,
                transformation_time_ms=transformation_time_ms,
                input_count=len(raw_response.get("response", {}).get("body", {}).get("items", {}).get("item", [])),
                output_count=len(processed_data),
            )

        except Exception as e:
            transformation_time_ms = int((time.time() - transformation_start) * 1000)
            self.logger.error(f"데이터 변환 실패 (테스트): {e}")

            return TransformationResult.error_result(str(e))

    async def process_detailed_api_response(
        self, 
        api_name: str, 
        content_id: str, 
        content_type_id: str, 
        raw_response: Dict,
        raw_data_id: Optional[str] = None
    ) -> Dict:
        """상세 정보 API 응답 처리 및 데이터베이스 저장"""
        
        try:
            if api_name == "detailCommon2":
                return await self._process_detail_common(content_id, content_type_id, raw_response, raw_data_id)
            elif api_name == "detailIntro2":
                return await self._process_detail_intro(content_id, content_type_id, raw_response, raw_data_id)
            elif api_name == "detailInfo2":
                return await self._process_detail_info(content_id, content_type_id, raw_response, raw_data_id)
            elif api_name == "detailImage2":
                return await self._process_detail_images(content_id, content_type_id, raw_response, raw_data_id)
            else:
                return {
                    'success': False,
                    'error': f'지원하지 않는 API: {api_name}'
                }
                
        except Exception as e:
            self.logger.error(f"{api_name} 응답 처리 실패 ({content_id}): {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    async def _process_detail_common(self, content_id: str, content_type_id: str, raw_response: Dict, raw_data_id: Optional[str]) -> Dict:
        """detailCommon2 API 응답 처리"""
        
        items = self._extract_items(raw_response)
        if not items:
            return {'success': False, 'error': 'detailCommon2 응답에 데이터가 없습니다'}
        
        item = items[0]  # detailCommon2는 단일 아이템 반환
        
        # 기존 테이블 업데이트를 위한 필드 추출
        update_fields = {
            'homepage': self._clean_value(item.get('homepage')),
            'booktour': self._clean_value(item.get('booktour')),
            'createdtime': self._clean_value(item.get('createdtime')),
            'modifiedtime': self._clean_value(item.get('modifiedtime')),
            'telname': self._clean_value(item.get('tel')),  # 전화번호 정보
            'faxno': self._clean_value(item.get('fax')),
            'zipcode': self._clean_value(item.get('zipcode')),
            'mlevel': int(item.get('mlevel', 0)) if item.get('mlevel') else None
        }
        
        # 컨텐츠 타입별 테이블 업데이트
        content_type_table_map = {
            '12': 'tourist_attractions',
            '14': 'cultural_facilities', 
            '15': 'festivals_events',
            '25': 'travel_courses',
            '28': 'leisure_sports',
            '32': 'accommodations',
            '38': 'shopping',
            '39': 'restaurants'
        }
        
        table_name = content_type_table_map.get(content_type_id)
        if not table_name:
            return {'success': False, 'error': f'알 수 없는 컨텐츠 타입: {content_type_id}'}
        
        # 기존 레코드 업데이트
        try:
            update_query = f"""
            UPDATE {table_name} 
            SET homepage = %s, booktour = %s, createdtime = %s, modifiedtime = %s,
                telname = %s, faxno = %s, zipcode = %s, mlevel = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE content_id = %s
            """
            
            params = (
                update_fields['homepage'],
                update_fields['booktour'],
                update_fields['createdtime'],
                update_fields['modifiedtime'],
                update_fields['telname'],
                update_fields['faxno'],
                update_fields['zipcode'],
                update_fields['mlevel'],
                content_id
            )
            
            self.db_manager.execute_update(update_query, params)
            
            self.logger.debug(f"✅ {table_name} 테이블 detailCommon2 정보 업데이트: {content_id}")
            
            return {
                'success': True,
                'updated_table': table_name,
                'updated_fields': list(update_fields.keys())
            }
            
        except Exception as e:
            self.logger.error(f"❌ {table_name} 테이블 업데이트 실패 ({content_id}): {e}")
            return {'success': False, 'error': str(e)}
    
    async def _process_detail_intro(self, content_id: str, content_type_id: str, raw_response: Dict, raw_data_id: Optional[str]) -> Dict:
        """detailIntro2 API 응답 처리"""
        
        items = self._extract_items(raw_response)
        if not items:
            return {'success': False, 'error': 'detailIntro2 응답에 데이터가 없습니다'}
        
        item = items[0]  # detailIntro2는 단일 아이템 반환
        
        # intro 정보를 JSONB로 저장
        intro_info = {}
        intro_fields = [
            'heritage1', 'heritage2', 'heritage3', 'opendate', 'restdate',
            'expguide', 'expagerange', 'accomcount', 'useseason', 'usetime',
            'parking', 'chkbabycarriage', 'chkpet', 'chkcreditcard',
            'infocenter', 'scale', 'facilities', 'program'
        ]
        
        for field in intro_fields:
            if field in item and item[field]:
                intro_info[field] = self._clean_value(item[field])
        
        # 컨텐츠 타입별 테이블 매핑
        content_type_table_map = {
            '12': 'tourist_attractions',
            '14': 'cultural_facilities', 
            '15': 'festivals_events',
            '25': 'travel_courses',
            '28': 'leisure_sports',
            '32': 'accommodations',
            '38': 'shopping',
            '39': 'restaurants'
        }
        
        table_name = content_type_table_map.get(content_type_id)
        if not table_name:
            return {'success': False, 'error': f'알 수 없는 컨텐츠 타입: {content_type_id}'}
        
        # detail_intro_info 필드 업데이트
        try:
            update_query = f"""
            UPDATE {table_name} 
            SET detail_intro_info = %s, updated_at = CURRENT_TIMESTAMP
            WHERE content_id = %s
            """
            
            self.db_manager.execute_update(
                update_query, 
                (self.db_manager.serialize_for_db(intro_info), content_id)
            )
            
            self.logger.debug(f"✅ {table_name} 테이블 detailIntro2 정보 업데이트: {content_id}")
            
            return {
                'success': True,
                'updated_table': table_name,
                'intro_fields_count': len(intro_info)
            }
            
        except Exception as e:
            self.logger.error(f"❌ {table_name} 테이블 intro 정보 업데이트 실패 ({content_id}): {e}")
            return {'success': False, 'error': str(e)}
    
    async def _process_detail_info(self, content_id: str, content_type_id: str, raw_response: Dict, raw_data_id: Optional[str]) -> Dict:
        """detailInfo2 API 응답 처리"""
        
        items = self._extract_items(raw_response)
        if not items:
            return {'success': False, 'error': 'detailInfo2 응답에 데이터가 없습니다'}
        
        # content_detail_info 테이블에 저장할 데이터 준비
        detail_infos = []
        for item in items:
            detail_data = {
                'content_id': content_id,
                'content_type_id': content_type_id,
                'info_name': self._clean_value(item.get('infoname')),
                'info_text': self._clean_value(item.get('infotext')),
                'serial_num': int(item.get('serialnum', 1)) if item.get('serialnum') else 1,
                'raw_data_id': raw_data_id
            }
            detail_infos.append(detail_data)
        
        # content_detail_info 테이블에 배치 삽입
        try:
            success_count = self.db_manager.insert_content_detail_info_batch(detail_infos)
            
            # 기존 테이블의 detail_additional_info 필드도 업데이트
            additional_info = {}
            for item in items:
                serial_num = item.get('serialnum', '1')
                additional_info[f'info_{serial_num}'] = {
                    'name': self._clean_value(item.get('infoname')),
                    'text': self._clean_value(item.get('infotext'))
                }
            
            # 컨텐츠 타입별 테이블 업데이트
            content_type_table_map = {
                '12': 'tourist_attractions',
                '14': 'cultural_facilities', 
                '15': 'festivals_events',
                '25': 'travel_courses',
                '28': 'leisure_sports',
                '32': 'accommodations',
                '38': 'shopping',
                '39': 'restaurants'
            }
            
            table_name = content_type_table_map.get(content_type_id)
            if table_name:
                update_query = f"""
                UPDATE {table_name} 
                SET detail_additional_info = %s, updated_at = CURRENT_TIMESTAMP
                WHERE content_id = %s
                """
                
                self.db_manager.execute_update(
                    update_query, 
                    (self.db_manager.serialize_for_db(additional_info), content_id)
                )
            
            self.logger.debug(f"✅ detailInfo2 정보 저장: {content_id} ({success_count}건)")
            
            return {
                'success': True,
                'detail_info_count': success_count,
                'updated_table': table_name if table_name else None
            }
            
        except Exception as e:
            self.logger.error(f"❌ detailInfo2 정보 저장 실패 ({content_id}): {e}")
            return {'success': False, 'error': str(e)}
    
    async def _process_detail_images(self, content_id: str, content_type_id: str, raw_response: Dict, raw_data_id: Optional[str]) -> Dict:
        """detailImage2 API 응답 처리"""
        
        items = self._extract_items(raw_response)
        if not items:
            return {'success': False, 'error': 'detailImage2 응답에 데이터가 없습니다'}
        
        # content_images 테이블에 저장할 데이터 준비
        image_data = []
        for item in items:
            image_info = {
                'content_id': content_id,
                'content_type_id': content_type_id,
                'img_name': self._clean_value(item.get('imgname')),
                'origin_img_url': self._clean_value(item.get('originimgurl')),
                'small_image_url': self._clean_value(item.get('smallimageurl')),
                'serial_num': int(item.get('serialnum', 1)) if item.get('serialnum') else 1,
                'cpyrht_div_cd': self._clean_value(item.get('cpyrhtDivCd')),
                'raw_data_id': raw_data_id
            }
            image_data.append(image_info)
        
        # content_images 테이블에 배치 삽입
        try:
            success_count = self.db_manager.insert_content_images_batch(image_data)
            
            self.logger.debug(f"✅ detailImage2 정보 저장: {content_id} ({success_count}건)")
            
            return {
                'success': True,
                'image_count': success_count
            }
            
        except Exception as e:
            self.logger.error(f"❌ detailImage2 정보 저장 실패 ({content_id}): {e}")
            return {'success': False, 'error': str(e)}


# 싱글톤 인스턴스
_transformation_pipeline = None


def get_transformation_pipeline() -> DataTransformationPipeline:
    """데이터 변환 파이프라인 인스턴스 반환"""
    global _transformation_pipeline
    if _transformation_pipeline is None:
        _transformation_pipeline = DataTransformationPipeline()
    return _transformation_pipeline
