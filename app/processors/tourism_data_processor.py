"""
한국관광공사 API 데이터 통합 처리 모듈

API에서 수집한 관광정보를 데이터베이스 스키마에 맞게 변환하고 저장하는 모듈입니다.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from dotenv import load_dotenv

from app.core.database_manager import DatabaseManager

load_dotenv()


class TourismDataProcessor:
    """관광 데이터 통합 처리기"""

    DATA_TYPE_CONFIG = {
        "tourist_attractions": {
            "table_name": "tourist_attractions",
            "name_field": "attraction_name",
            "fields": {
                "contentid": "content_id",
                "areacode": "region_code",
                "sigungucode": "sigungu_code",
                "title": "attraction_name",
                "cat1": "category_code",
                "addr1": "address",
                "mapy": "latitude",
                "mapx": "longitude",
                "overview": "description",
                "firstimage": "image_url",
            },
        },
        "cultural_facilities": {
            "table_name": "cultural_facilities",
            "name_field": "facility_name",
            "fields": {
                "contentid": "content_id",
                "areacode": "region_code",
                "sigungucode": "sigungu_code",
                "title": "facility_name",
                "cat1": "category_code",
                "cat2": "sub_category_code",
                "addr1": "address",
                "addr2": "detail_address",
                "mapy": "latitude",
                "mapx": "longitude",
                "zipcode": "zipcode",
                "tel": "tel",
                "homepage": "homepage",
                "overview": "overview",
                "firstimage": "first_image",
                "firstimage2": "first_image_small",
            },
        },
        "festivals_events": {
            "table_name": "festivals_events",
            "name_field": "event_name",
            "fields": {
                "contentid": "content_id",
                "areacode": "region_code",
                "sigungucode": "sigungu_code",
                "title": "event_name",
                "cat1": "category_code",
                "cat2": "sub_category_code",
                "addr1": "address",
                "addr2": "detail_address",
                "mapy": "latitude",
                "mapx": "longitude",
                "zipcode": "zipcode",
                "tel": "tel",
                "homepage": "homepage",
                "overview": "overview",
                "firstimage": "first_image",
                "firstimage2": "first_image_small",
                "eventstartdate": "event_start_date",
                "eventenddate": "event_end_date",
                "eventplace": "event_place",
            },
        },
        "travel_courses": {
            "table_name": "travel_courses",
            "name_field": "course_name",
            "fields": {
                "contentid": "content_id",
                "areacode": "region_code",
                "sigungucode": "sigungu_code",
                "title": "course_name",
                "cat1": "category_code",
                "cat2": "sub_category_code",
                "addr1": "address",
                "addr2": "detail_address",
                "mapy": "latitude",
                "mapx": "longitude",
                "zipcode": "zipcode",
                "tel": "tel",
                "homepage": "homepage",
                "overview": "overview",
                "firstimage": "first_image",
                "firstimage2": "first_image_small",
            },
        },
        "leisure_sports": {
            "table_name": "leisure_sports",
            "name_field": "facility_name",
            "fields": {
                "contentid": "content_id",
                "areacode": "region_code",
                "sigungucode": "sigungu_code",
                "title": "facility_name",
                "cat1": "category_code",
                "cat2": "sub_category_code",
                "addr1": "address",
                "addr2": "detail_address",
                "mapy": "latitude",
                "mapx": "longitude",
                "zipcode": "zipcode",
                "tel": "tel",
                "homepage": "homepage",
                "overview": "overview",
                "firstimage": "first_image",
                "firstimage2": "first_image_small",
            },
        },
        "accommodations": {
            "table_name": "accommodations",
            "name_field": "accommodation_name",
            "fields": {
                "contentid": "content_id",
                "areacode": "region_code",
                "sigungucode": "sigungu_code",
                "title": "accommodation_name",
                "cat1": "category_code",
                "cat2": "sub_category_code",
                "addr1": "address",
                "addr2": "detail_address",
                "mapy": "latitude",
                "mapx": "longitude",
                "zipcode": "zipcode",
                "tel": "tel",
                "homepage": "homepage",
                "overview": "overview",
                "firstimage": "first_image",
                "firstimage2": "first_image_small",
            },
        },
        "shopping": {
            "table_name": "shopping",
            "name_field": "shop_name",
            "fields": {
                "contentid": "content_id",
                "areacode": "region_code",
                "sigungucode": "sigungu_code",
                "title": "shop_name",
                "cat1": "category_code",
                "cat2": "sub_category_code",
                "addr1": "address",
                "addr2": "detail_address",
                "mapy": "latitude",
                "mapx": "longitude",
                "zipcode": "zipcode",
                "tel": "tel",
                "homepage": "homepage",
                "overview": "overview",
                "firstimage": "first_image",
                "firstimage2": "first_image_small",
            },
        },
        "restaurants": {
            "table_name": "restaurants",
            "name_field": "restaurant_name",
            "fields": {
                "contentid": "content_id",
                "areacode": "region_code",
                "sigungucode": "sigungu_code",
                "title": "restaurant_name",
                "cat1": "category_code",
                "cat2": "sub_category_code",
                "addr1": "address",
                "addr2": "detail_address",
                "mapy": "latitude",
                "mapx": "longitude",
                "zipcode": "zipcode",
                "tel": "tel",
                "homepage": "homepage",
                "overview": "overview",
                "firstimage": "first_image",
                "firstimage2": "first_image_small",
            },
        },
    }

    def __init__(self):
        self.db_config = {
            "host": os.getenv("DB_HOST", "localhost"),
            "database": os.getenv("DB_NAME", "weather_travel_db"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "password"),
            "port": os.getenv("DB_PORT", "5432"),
        }
        self.logger = logging.getLogger(__name__)
        self.db_manager = DatabaseManager()

    def process_area_codes(self, area_codes: List[Dict]) -> int:
        data_to_upsert = [
            {
                "region_code": area.get("code") or area.get("rnum"),
                "region_name": area.get("name"),
                "region_level": 1,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            for area in area_codes
            if area.get("code") and area.get("name")
        ]
        return self.db_manager.upsert("regions", data_to_upsert, ["region_code"])

    def process_detailed_area_codes(self, detailed_codes: List[Dict]) -> int:
        data_to_upsert = [
            {
                "region_code": area.get("code"),
                "region_name": area.get("name"),
                "parent_region_code": area.get("areacode"),
                "region_level": 2,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            for area in detailed_codes
            if area.get("code") and area.get("name")
        ]
        return self.db_manager.upsert("regions", data_to_upsert, ["region_code"])

    def process_category_codes(self, category_codes: List[Dict]) -> int:
        data_to_upsert = [
            {
                "category_code": category.get("code")
                or category.get("cat1")
                or category.get("cat2")
                or category.get("cat3"),
                "category_name": category.get("name"),
                "content_type_id": category.get("contenttypeid"),
                "category_level": 1,  # Assuming level 1 for now, adjust if API provides this
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            for category in category_codes
            if category.get("code") and category.get("name")
        ]
        return self.db_manager.upsert(
            "category_codes", data_to_upsert, ["category_code"]
        )

    def _dynamic_field_mapper(self, item: Dict, data_type: str) -> Optional[Dict]:
        config = self.DATA_TYPE_CONFIG.get(data_type)
        if not config:
            return None

        mapped_data = {}
        for api_field, db_field in config["fields"].items():
            value = item.get(api_field)
            if value is not None:
                if db_field in ["latitude", "longitude"]:
                    value = self._safe_float(value)
                elif "date" in db_field:
                    value = self._parse_date(value)
                mapped_data[db_field] = value

        if not mapped_data.get("content_id") or not mapped_data.get(
            config["name_field"]
        ):
            return None

        mapped_data["created_at"] = datetime.now()
        mapped_data["updated_at"] = datetime.now()
        return mapped_data

    def process_content_data(self, data_list: List[Dict], data_type: str) -> int:
        if not data_list:
            return 0

        config = self.DATA_TYPE_CONFIG.get(data_type)
        if not config:
            self.logger.error(f"유효하지 않은 데이터 타입: {data_type}")
            return 0

        mapped_data = [
            self._dynamic_field_mapper(item, data_type) for item in data_list
        ]
        mapped_data = [d for d in mapped_data if d is not None]

        if not mapped_data:
            return 0

        return self.db_manager.upsert(config["table_name"], mapped_data, ["content_id"])

    def _safe_float(self, value) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _parse_date(self, date_str) -> Optional[datetime.date]:
        if not date_str:
            return None
        try:
            # Ensure date_str is a string before checking length
            s_date_str = str(date_str)
            if len(s_date_str) == 8:
                return datetime.strptime(s_date_str, "%Y%m%d").date()
            return None
        except (ValueError, TypeError):
            return None

    def process_comprehensive_data(self, comprehensive_data: Dict) -> Dict[str, int]:
        results = {}
        self.logger.info("=== 종합 관광 데이터 통합 처리 시작 ===")

        # Process area codes and detailed area codes first as they are dependencies
        if comprehensive_data.get("area_codes"):
            results["area_codes"] = self.process_area_codes(
                comprehensive_data["area_codes"]
            )

        if comprehensive_data.get("detailed_area_codes"):
            results["detailed_area_codes"] = self.process_detailed_area_codes(
                comprehensive_data["detailed_area_codes"]
            )

        if comprehensive_data.get("category_codes"):
            results["category_codes"] = self.process_category_codes(
                comprehensive_data["category_codes"]
            )

        # Process other content types using the generic process_content_data
        for data_type, config in self.DATA_TYPE_CONFIG.items():
            if comprehensive_data.get(data_type):
                results[data_type] = self.process_content_data(
                    comprehensive_data[data_type], data_type
                )

        total_processed = sum(results.values())
        self.logger.info(f"=== 종합 데이터 처리 완료: 총 {total_processed:,}개 ===")
        for data_type, count in results.items():
            if count > 0:
                self.logger.info(f"  - {data_type}: {count:,}개")

        return results

    def load_and_process_json_files(self, data_directory: str = ".") -> Dict[str, int]:
        comprehensive_data = {}

        # Load content type data based on DATA_TYPE_CONFIG
        for data_type, config in self.DATA_TYPE_CONFIG.items():
            filename = f"{data_type}.json"
            file_path = os.path.join(data_directory, filename)
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        comprehensive_data[data_type] = json.load(f)
                    self.logger.info(
                        f"{filename} 로드 완료: {len(comprehensive_data[data_type])}개"
                    )
                except Exception as e:
                    self.logger.error(f"{filename} 로드 오류: {e}")

        # Load area and category codes separately as they are not in DATA_TYPE_CONFIG
        for key in ["area_codes", "detailed_area_codes", "category_codes"]:
            filename = f"{key}.json"
            file_path = os.path.join(data_directory, filename)
            if os.path.exists(file_path):
                try:
                    with open(file_path, "r", encoding="utf-8") as f:
                        comprehensive_data[key] = json.load(f)
                    self.logger.info(
                        f"{filename} 로드 완료: {len(comprehensive_data[key])}개"
                    )
                except Exception as e:
                    self.logger.error(f"{filename} 로드 오류: {e}")

        return self.process_comprehensive_data(comprehensive_data)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    processor = TourismDataProcessor()
    results = processor.load_and_process_json_files()

    print("\n=== 처리 결과 ===")
    total = sum(results.values())
    print(f"총 처리된 데이터: {total:,}개")
    for data_type, count in results.items():
        if count > 0:
            print(f"  - {data_type}: {count:,}개")
