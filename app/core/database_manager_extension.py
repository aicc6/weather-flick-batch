"""
데이터베이스 매니저 확장

새로운 API 원데이터 관리 기능을 기존 database_manager에 확장
배치 INSERT 최적화 기능 포함
"""

import uuid
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Any
from app.core.database_manager import SyncDatabaseManager
from app.core.batch_insert_optimizer import (
    BatchInsertOptimizer, 
    optimize_tourism_data_insert
)
from config.batch_optimization_config import (
    get_tourism_batch_config,
    BatchOptimizationConfig
)


class DatabaseManagerExtension:
    """데이터베이스 매니저 확장 클래스 (배치 최적화 포함)"""

    def __init__(self, db_manager: SyncDatabaseManager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
        self.batch_optimizer = BatchInsertOptimizer(db_manager)

    def insert_raw_data(self, raw_data: Dict) -> str:
        """원본 API 데이터 삽입"""

        query = """
        INSERT INTO api_raw_data (
            api_provider, endpoint, request_method, request_params,
            response_status, raw_response, response_size, request_duration,
            api_key_hash, expires_at, file_path
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) RETURNING id
        """

        params = (
            raw_data.get("api_provider"),
            raw_data.get("endpoint"),
            raw_data.get("request_method"),
            self.db_manager.serialize_for_db(raw_data.get("request_params")),
            raw_data.get("response_status"),
            self.db_manager.serialize_for_db(raw_data.get("raw_response")),
            raw_data.get("response_size"),
            raw_data.get("request_duration"),
            raw_data.get("api_key_hash"),
            raw_data.get("expires_at"),
            raw_data.get("file_path"),
        )

        try:
            # execute_query 메서드 사용 (RETURNING 절 처리)
            result = self.db_manager.execute_query(query, params)
            
            # 결과 형태에 따른 처리
            if isinstance(result, list) and len(result) > 0:
                # fetch_all 형태로 반환된 경우
                first_result = result[0]
                if "id" in first_result:
                    raw_data_id = str(first_result["id"])
                    self.logger.debug(f"원본 데이터 삽입 완료: {raw_data_id}")
                    return raw_data_id
            elif isinstance(result, dict) and "id" in result:
                # fetch_one 형태로 반환된 경우
                raw_data_id = str(result["id"])
                self.logger.debug(f"원본 데이터 삽입 완료: {raw_data_id}")
                return raw_data_id
            
            # 결과가 올바르지 않은 경우
            self.logger.error(f"예상치 못한 결과 형태: {type(result)} - {result}")
            raise Exception("원본 데이터 삽입 결과를 받을 수 없습니다")

        except Exception as e:
            self.logger.error(f"원본 데이터 삽입 실패: {e}")
            raise

    def insert_kto_metadata(self, metadata: Dict) -> bool:
        """KTO API 메타데이터 삽입"""

        query = """
        INSERT INTO kto_api_metadata (
            raw_data_id, content_type_id, area_code, sigungu_code,
            total_count, page_no, num_of_rows, sync_batch_id
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        """

        params = (
            metadata.get("raw_data_id"),
            metadata.get("content_type_id"),
            metadata.get("area_code"),
            metadata.get("sigungu_code"),
            metadata.get("total_count"),
            metadata.get("page_no"),
            metadata.get("num_of_rows"),
            metadata.get("sync_batch_id"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"KTO 메타데이터 삽입 실패: {e}")
            return False

    def insert_kma_metadata(self, metadata: Dict) -> bool:
        """KMA API 메타데이터 삽입"""

        query = """
        INSERT INTO kma_api_metadata (
            raw_data_id, base_date, base_time, nx, ny,
            forecast_type, region_name
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s
        )
        """

        params = (
            metadata.get("raw_data_id"),
            metadata.get("base_date"),
            metadata.get("base_time"),
            metadata.get("nx"),
            metadata.get("ny"),
            metadata.get("forecast_type"),
            metadata.get("region_name"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"KMA 메타데이터 삽입 실패: {e}")
            return False

    def log_transformation(self, log_data: Dict) -> bool:
        """데이터 변환 로그 기록"""

        query = """
        INSERT INTO data_transformation_logs (
            raw_data_id, target_table, transformation_rule,
            input_record_count, output_record_count, error_count,
            transformation_time_ms, status, error_details, quality_score,
            completed_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
        )
        """

        params = (
            log_data.get("raw_data_id"),
            log_data.get("target_table"),
            self.db_manager.serialize_for_db(log_data.get("transformation_rule")),
            log_data.get("input_record_count"),
            log_data.get("output_record_count"),
            log_data.get("error_count"),
            log_data.get("transformation_time_ms"),
            log_data.get("status"),
            self.db_manager.serialize_for_db(log_data.get("error_details")),
            log_data.get("quality_score"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"변환 로그 기록 실패: {e}")
            return False

    def get_raw_data(self, raw_data_id: str) -> Optional[Dict]:
        """원본 데이터 조회"""

        query = """
        SELECT * FROM api_raw_data 
        WHERE id = %s
        """

        try:
            result = self.db_manager.fetch_one(query, (raw_data_id,))
            return result

        except Exception as e:
            self.logger.error(f"원본 데이터 조회 실패: {e}")
            return None

    def cleanup_expired_raw_data(self) -> int:
        """만료된 원본 데이터 정리"""

        # 1. 만료된 데이터 아카이브 표시
        archive_query = """
        UPDATE api_raw_data 
        SET is_archived = true 
        WHERE expires_at < CURRENT_TIMESTAMP 
        AND is_archived = false
        """

        # 2. 아카이브된 데이터 개수 조회
        count_query = """
        SELECT COUNT(*) as count 
        FROM api_raw_data 
        WHERE is_archived = true
        """

        try:
            self.db_manager.execute_update(archive_query)

            result = self.db_manager.fetch_one(count_query)
            archived_count = result["count"] if result else 0

            self.logger.info(f"만료된 원본 데이터 아카이브 완료: {archived_count}건")
            return archived_count

        except Exception as e:
            self.logger.error(f"만료 데이터 정리 실패: {e}")
            return 0

    def upsert_tourist_attraction(self, data: Dict) -> bool:
        """관광지 데이터 UPSERT (새로운 10개 필드 지원)"""

        query = """
        INSERT INTO tourist_attractions (
            content_id, region_code, attraction_name, category_code, category_name,
            address, latitude, longitude, description, image_url, homepage,
            booktour, createdtime, modifiedtime, telname, faxno, zipcode, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score, processing_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            attraction_name = EXCLUDED.attraction_name,
            category_code = EXCLUDED.category_code,
            category_name = EXCLUDED.category_name,
            address = EXCLUDED.address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            description = EXCLUDED.description,
            image_url = EXCLUDED.image_url,
            homepage = EXCLUDED.homepage,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            zipcode = EXCLUDED.zipcode,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            processing_status = EXCLUDED.processing_status,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("attraction_name"),
            data.get("category_code"),
            data.get("category_name"),
            data.get("address"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("description"),
            data.get("first_image") or data.get("image_url"),
            data.get("homepage"),
            # 새로 추가된 필드들
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("mlevel") or data.get("map_level"),
            self.db_manager.serialize_for_db(data.get("detail_intro_info") or data.get("intro_info")),
            self.db_manager.serialize_for_db(data.get("detail_additional_info") or data.get("additional_info")),
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
            data.get("processing_status"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"관광지 데이터 UPSERT 실패: {e}")
            return False

    def upsert_accommodation(self, data: Dict) -> bool:
        """숙박 데이터 UPSERT"""

        query = """
        INSERT INTO accommodations (
            content_id, region_code, accommodation_name, address, 
            latitude, longitude, first_image,
            homepage, booktour, createdtime, modifiedtime, telname, faxno, zipcode, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            accommodation_name = EXCLUDED.accommodation_name,
            address = EXCLUDED.address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            first_image = EXCLUDED.first_image,
            homepage = EXCLUDED.homepage,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            zipcode = EXCLUDED.zipcode,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("accommodation_name"),
            data.get("address"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("first_image"),
            # 새로 추가된 필드들
            data.get("homepage"),
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("mlevel") or data.get("map_level"),
            self.db_manager.serialize_for_db(data.get("detail_intro_info") or data.get("intro_info")),
            self.db_manager.serialize_for_db(data.get("detail_additional_info") or data.get("additional_info")),
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"숙박 데이터 UPSERT 실패: {e}")
            return False

    def upsert_festival_event(self, data: Dict) -> bool:
        """축제/행사 데이터 UPSERT"""

        query = """
        INSERT INTO festivals_events (
            content_id, region_code, event_name, address,
            latitude, longitude, first_image, event_start_date, event_end_date,
            homepage, booktour, createdtime, modifiedtime, telname, faxno, zipcode, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            event_name = EXCLUDED.event_name,
            address = EXCLUDED.address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            first_image = EXCLUDED.first_image,
            event_start_date = EXCLUDED.event_start_date,
            event_end_date = EXCLUDED.event_end_date,
            homepage = EXCLUDED.homepage,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            zipcode = EXCLUDED.zipcode,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("event_name"),
            data.get("address"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("first_image"),
            data.get("event_start_date") or data.get("start_date"),
            data.get("event_end_date") or data.get("end_date"),
            # 새로 추가된 필드들
            data.get("homepage"),
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("mlevel") or data.get("map_level"),
            self.db_manager.serialize_for_db(data.get("detail_intro_info") or data.get("intro_info")),
            self.db_manager.serialize_for_db(data.get("detail_additional_info") or data.get("additional_info")),
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"축제/행사 데이터 UPSERT 실패: {e}")
            return False

    def upsert_pet_tour_info(self, data: Dict) -> bool:
        """반려동물 동반여행 정보 UPSERT"""

        query = """
        INSERT INTO pet_tour_info (
            content_id, content_type_id, title, address, latitude, longitude,
            area_code, sigungu_code, tel, homepage, overview,
            cat1, cat2, cat3, first_image, first_image2,
            pet_acpt_abl, pet_info, raw_data_id, data_quality_score,
            processing_status, last_sync_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            content_type_id = EXCLUDED.content_type_id,
            title = EXCLUDED.title,
            address = EXCLUDED.address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            area_code = EXCLUDED.area_code,
            sigungu_code = EXCLUDED.sigungu_code,
            tel = EXCLUDED.tel,
            homepage = EXCLUDED.homepage,
            overview = EXCLUDED.overview,
            cat1 = EXCLUDED.cat1,
            cat2 = EXCLUDED.cat2,
            cat3 = EXCLUDED.cat3,
            first_image = EXCLUDED.first_image,
            first_image2 = EXCLUDED.first_image2,
            pet_acpt_abl = EXCLUDED.pet_acpt_abl,
            pet_info = EXCLUDED.pet_info,
            raw_data_id = EXCLUDED.raw_data_id,
            data_quality_score = EXCLUDED.data_quality_score,
            processing_status = EXCLUDED.processing_status,
            last_sync_at = EXCLUDED.last_sync_at,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("content_type_id"),
            data.get("title"),
            data.get("address"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("area_code"),
            data.get("sigungu_code"),
            data.get("tel"),
            data.get("homepage"),
            data.get("overview"),
            data.get("cat1"),
            data.get("cat2"),
            data.get("cat3"),
            data.get("first_image"),
            data.get("first_image2"),
            data.get("pet_acpt_abl"),
            data.get("pet_info"),
            data.get("raw_data_id"),
            data.get("data_quality_score"),
            data.get("processing_status"),
            data.get("last_sync_at"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"반려동물 동반여행 정보 UPSERT 실패: {e}")
            return False

    def upsert_restaurant(self, data: Dict) -> bool:
        """음식점 데이터 UPSERT"""

        query = """
        INSERT INTO restaurants (
            content_id, region_code, restaurant_name, address, detail_address,
            latitude, longitude, first_image, first_image_small, tel,
            category_code, sub_category_code, sigungu_code,
            overview, homepage,
            booktour, createdtime, modifiedtime, telname, faxno, zipcode, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            restaurant_name = EXCLUDED.restaurant_name,
            address = EXCLUDED.address,
            detail_address = EXCLUDED.detail_address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            first_image = EXCLUDED.first_image,
            first_image_small = EXCLUDED.first_image_small,
            tel = EXCLUDED.tel,
            category_code = EXCLUDED.category_code,
            sub_category_code = EXCLUDED.sub_category_code,
            sigungu_code = EXCLUDED.sigungu_code,
            overview = EXCLUDED.overview,
            homepage = EXCLUDED.homepage,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            zipcode = EXCLUDED.zipcode,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("restaurant_name"),
            data.get("address"),
            data.get("addr2"),  # KTO API의 addr2 필드 사용
            data.get("latitude"),
            data.get("longitude"),
            data.get("first_image"),      # 변환된 데이터에서 온 필드명
            data.get("thumbnail_url"),  # 변환된 데이터에서 온 필드명
            data.get("phone_number"),   # 변환된 데이터에서 온 필드명
            data.get("category_large_code"),   # 변환된 데이터에서 온 필드명
            data.get("category_medium_code"),  # 변환된 데이터에서 온 필드명
            data.get("sigungu_code"),
            data.get("description"),    # 변환된 데이터에서 온 필드명
            data.get("homepage_url"),   # 변환된 데이터에서 온 필드명
            # 새로 추가된 필드들
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("mlevel") or data.get("map_level"),
            self.db_manager.serialize_for_db(data.get("detail_intro_info") or data.get("intro_info")),
            self.db_manager.serialize_for_db(data.get("detail_additional_info") or data.get("additional_info")),
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"음식점 데이터 UPSERT 실패: {e}")
            return False

    def get_api_call_statistics(self, api_provider: str) -> Dict:
        """API 호출 통계 조회"""

        query = """
        SELECT 
            COUNT(*) FILTER (WHERE DATE(created_at) = CURRENT_DATE) as today_calls,
            COUNT(CASE WHEN response_status = 200 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as success_rate,
            AVG(request_duration) as avg_response_time_ms,
            MAX(created_at) as last_call_at,
            SUM(response_size) / 1024.0 / 1024.0 as total_raw_data_size_mb
        FROM api_raw_data 
        WHERE api_provider = %s
        AND created_at >= CURRENT_DATE - INTERVAL '7 days'
        """

        try:
            result = self.db_manager.fetch_one(query, (api_provider,))

            if result:
                stats = dict(result)
                # None 값을 0으로 변환
                for key, value in stats.items():
                    if value is None:
                        stats[key] = 0.0
                return stats

            return {}

        except Exception as e:
            self.logger.error(f"API 통계 조회 실패: {e}")
            return {}

    def get_data_quality_thresholds(self, table_name: str) -> Dict:
        """데이터 품질 임계값 조회"""

        query = """
        SELECT * FROM data_quality_thresholds 
        WHERE table_name = %s
        """

        try:
            result = self.db_manager.fetch_one(query, (table_name,))

            if result:
                return dict(result)

            # 기본값 반환
            return {
                "completeness_threshold": 0.90,
                "validity_threshold": 0.95,
                "consistency_threshold": 0.99,
                "overall_threshold": 0.85,
            }

        except Exception as e:
            self.logger.error(f"품질 임계값 조회 실패: {e}")
            return {}

    def upsert_content_images(self, image_data: Dict) -> bool:
        """컨텐츠 이미지 정보 UPSERT"""
        try:
            query = """
            INSERT INTO content_images (
                content_id, content_type_id, img_name, origin_img_url, 
                small_image_url, serial_num, cpyrht_div_cd, img_size,
                img_width, img_height, raw_data_id, data_quality_score,
                processing_status, last_sync_at, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (content_id, serial_num) 
            DO UPDATE SET 
                img_name = EXCLUDED.img_name,
                origin_img_url = EXCLUDED.origin_img_url,
                small_image_url = EXCLUDED.small_image_url,
                cpyrht_div_cd = EXCLUDED.cpyrht_div_cd,
                img_size = EXCLUDED.img_size,
                img_width = EXCLUDED.img_width,
                img_height = EXCLUDED.img_height,
                updated_at = CURRENT_TIMESTAMP
            """
            
            current_time = datetime.now()
            params = (
                image_data.get("content_id"),
                image_data.get("content_type_id"),
                image_data.get("img_name"),
                image_data.get("origin_img_url"),
                image_data.get("small_image_url"),
                image_data.get("serial_num", 1),
                image_data.get("cpyrht_div_cd"),
                image_data.get("img_size"),
                image_data.get("img_width"),
                image_data.get("img_height"),
                image_data.get("raw_data_id"),
                image_data.get("data_quality_score", 85.0),
                image_data.get("processing_status", "processed"),
                current_time,
                current_time,
                current_time
            )
            
            self.db_manager.execute_update(query, params)
            return True
            
        except Exception as e:
            self.logger.error(f"컨텐츠 이미지 저장 실패: {e}")
            return False

    def upsert_content_detail_info(self, detail_data: Dict) -> bool:
        """컨텐츠 상세 정보 UPSERT"""
        try:
            query = """
            INSERT INTO content_detail_info (
                content_id, content_type_id, info_name, info_text,
                serial_num, raw_data_id, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (content_id, serial_num)
            DO UPDATE SET
                info_name = EXCLUDED.info_name,
                info_text = EXCLUDED.info_text,
                updated_at = CURRENT_TIMESTAMP
            """
            
            current_time = datetime.now()
            params = (
                detail_data.get("content_id"),
                detail_data.get("content_type_id"),
                detail_data.get("info_name"),
                detail_data.get("info_text"),
                detail_data.get("serial_num", 1),
                detail_data.get("raw_data_id"),
                current_time,
                current_time
            )
            
            self.db_manager.execute_update(query, params)
            return True
            
        except Exception as e:
            self.logger.error(f"컨텐츠 상세 정보 저장 실패: {e}")
            return False

    def insert_content_images_batch(self, images: List[Dict]) -> int:
        """컨텐츠 이미지 정보 배치 삽입"""
        try:
            if not images:
                return 0
            
            success_count = 0
            for image in images:
                if self.upsert_content_images(image):
                    success_count += 1
            
            self.logger.info(f"컨텐츠 이미지 배치 삽입 완료: {success_count}/{len(images)}")
            return success_count
            
        except Exception as e:
            self.logger.error(f"컨텐츠 이미지 배치 삽입 실패: {e}")
            return 0

    def insert_content_detail_info_batch(self, details: List[Dict]) -> int:
        """컨텐츠 상세 정보 배치 삽입"""
        try:
            if not details:
                return 0
            
            success_count = 0
            for detail in details:
                if self.upsert_content_detail_info(detail):
                    success_count += 1
            
            self.logger.info(f"컨텐츠 상세 정보 배치 삽입 완료: {success_count}/{len(details)}")
            return success_count
            
        except Exception as e:
            self.logger.error(f"컨텐츠 상세 정보 배치 삽입 실패: {e}")
            return 0

    def upsert_cultural_facility(self, data: Dict) -> bool:
        """문화시설 데이터 UPSERT"""

        query = """
        INSERT INTO cultural_facilities (
            content_id, region_code, sigungu_code, facility_name, category_code, sub_category_code,
            address, detail_address, latitude, longitude, zipcode, tel,
            homepage, overview, first_image, first_image_small,
            facility_type, admission_fee, operating_hours, parking_info, rest_date, use_season, use_time,
            booktour, createdtime, modifiedtime, telname, faxno, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score, processing_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            sigungu_code = EXCLUDED.sigungu_code,
            facility_name = EXCLUDED.facility_name,
            category_code = EXCLUDED.category_code,
            sub_category_code = EXCLUDED.sub_category_code,
            address = EXCLUDED.address,
            detail_address = EXCLUDED.detail_address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            zipcode = EXCLUDED.zipcode,
            tel = EXCLUDED.tel,
            homepage = EXCLUDED.homepage,
            overview = EXCLUDED.overview,
            first_image = EXCLUDED.first_image,
            first_image_small = EXCLUDED.first_image_small,
            facility_type = EXCLUDED.facility_type,
            admission_fee = EXCLUDED.admission_fee,
            operating_hours = EXCLUDED.operating_hours,
            parking_info = EXCLUDED.parking_info,
            rest_date = EXCLUDED.rest_date,
            use_season = EXCLUDED.use_season,
            use_time = EXCLUDED.use_time,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            processing_status = EXCLUDED.processing_status,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("sigungu_code"),
            data.get("facility_name"),
            data.get("category_code"),
            data.get("sub_category_code"),
            data.get("address"),
            data.get("detail_address") or data.get("addr2"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("tel"),
            data.get("homepage"),
            data.get("overview"),
            data.get("first_image"),
            data.get("first_image_small"),
            data.get("facility_type"),
            data.get("admission_fee"),
            data.get("operating_hours"),
            data.get("parking_info"),
            data.get("rest_date"),
            data.get("use_season"),
            data.get("use_time"),
            # 새로 추가된 필드들
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("mlevel") or data.get("map_level"),
            self.db_manager.serialize_for_db(data.get("detail_intro_info") or data.get("intro_info")),
            self.db_manager.serialize_for_db(data.get("detail_additional_info") or data.get("additional_info")),
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
            data.get("processing_status"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"문화시설 데이터 UPSERT 실패: {e}")
            return False

    def upsert_travel_course(self, data: Dict) -> bool:
        """여행코스 데이터 UPSERT"""

        query = """
        INSERT INTO travel_courses (
            content_id, region_code, sigungu_code, course_name, category_code, sub_category_code,
            address, detail_address, latitude, longitude, zipcode, tel,
            homepage, overview, first_image, first_image_small,
            course_theme, course_distance, required_time, difficulty_level, schedule,
            booktour, createdtime, modifiedtime, telname, faxno, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score, processing_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            sigungu_code = EXCLUDED.sigungu_code,
            course_name = EXCLUDED.course_name,
            category_code = EXCLUDED.category_code,
            sub_category_code = EXCLUDED.sub_category_code,
            address = EXCLUDED.address,
            detail_address = EXCLUDED.detail_address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            zipcode = EXCLUDED.zipcode,
            tel = EXCLUDED.tel,
            homepage = EXCLUDED.homepage,
            overview = EXCLUDED.overview,
            first_image = EXCLUDED.first_image,
            first_image_small = EXCLUDED.first_image_small,
            course_theme = EXCLUDED.course_theme,
            course_distance = EXCLUDED.course_distance,
            required_time = EXCLUDED.required_time,
            difficulty_level = EXCLUDED.difficulty_level,
            schedule = EXCLUDED.schedule,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            processing_status = EXCLUDED.processing_status,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("sigungu_code"),
            data.get("course_name"),
            data.get("category_code"),
            data.get("sub_category_code"),
            data.get("address"),
            data.get("detail_address") or data.get("addr2"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("tel"),
            data.get("homepage"),
            data.get("overview"),
            data.get("first_image"),
            data.get("first_image_small"),
            data.get("course_theme"),
            data.get("course_distance"),
            data.get("required_time"),
            data.get("difficulty_level"),
            data.get("schedule"),
            # 새로 추가된 필드들
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("mlevel") or data.get("map_level"),
            self.db_manager.serialize_for_db(data.get("detail_intro_info") or data.get("intro_info")),
            self.db_manager.serialize_for_db(data.get("detail_additional_info") or data.get("additional_info")),
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
            data.get("processing_status"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"여행코스 데이터 UPSERT 실패: {e}")
            return False

    def upsert_leisure_sport(self, data: Dict) -> bool:
        """레포츠 데이터 UPSERT"""

        query = """
        INSERT INTO leisure_sports (
            content_id, region_code, sigungu_code, facility_name, category_code, sub_category_code,
            address, detail_address, latitude, longitude, zipcode, tel,
            homepage, overview, first_image, first_image_small,
            sports_type, reservation_info, operating_hours, admission_fee, parking_info, rental_info, capacity,
            booktour, createdtime, modifiedtime, telname, faxno, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score, processing_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            sigungu_code = EXCLUDED.sigungu_code,
            facility_name = EXCLUDED.facility_name,
            category_code = EXCLUDED.category_code,
            sub_category_code = EXCLUDED.sub_category_code,
            address = EXCLUDED.address,
            detail_address = EXCLUDED.detail_address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            zipcode = EXCLUDED.zipcode,
            tel = EXCLUDED.tel,
            homepage = EXCLUDED.homepage,
            overview = EXCLUDED.overview,
            first_image = EXCLUDED.first_image,
            first_image_small = EXCLUDED.first_image_small,
            sports_type = EXCLUDED.sports_type,
            reservation_info = EXCLUDED.reservation_info,
            operating_hours = EXCLUDED.operating_hours,
            admission_fee = EXCLUDED.admission_fee,
            parking_info = EXCLUDED.parking_info,
            rental_info = EXCLUDED.rental_info,
            capacity = EXCLUDED.capacity,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            processing_status = EXCLUDED.processing_status,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("sigungu_code"),
            data.get("facility_name"),
            data.get("category_code"),
            data.get("sub_category_code"),
            data.get("address"),
            data.get("detail_address") or data.get("addr2"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("tel"),
            data.get("homepage"),
            data.get("overview"),
            data.get("first_image"),
            data.get("first_image_small"),
            data.get("sports_type"),
            data.get("reservation_info"),
            data.get("operating_hours"),
            data.get("admission_fee"),
            data.get("parking_info"),
            data.get("rental_info"),
            data.get("capacity"),
            # 새로 추가된 필드들
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("mlevel") or data.get("map_level"),
            self.db_manager.serialize_for_db(data.get("detail_intro_info") or data.get("intro_info")),
            self.db_manager.serialize_for_db(data.get("detail_additional_info") or data.get("additional_info")),
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
            data.get("processing_status"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"레포츠 데이터 UPSERT 실패: {e}")
            return False

    def upsert_shopping(self, data: Dict) -> bool:
        """쇼핑 데이터 UPSERT"""

        query = """
        INSERT INTO shopping (
            content_id, region_code, sigungu_code, shop_name, category_code, sub_category_code,
            address, detail_address, latitude, longitude, zipcode, tel,
            homepage, overview, first_image, first_image_small,
            shop_type, opening_hours, rest_date, parking_info, credit_card, pet_allowed, baby_carriage, sale_item, fair_day,
            booktour, createdtime, modifiedtime, telname, faxno, mlevel,
            detail_intro_info, detail_additional_info,
            raw_data_id, last_sync_at, data_quality_score, processing_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            sigungu_code = EXCLUDED.sigungu_code,
            shop_name = EXCLUDED.shop_name,
            category_code = EXCLUDED.category_code,
            sub_category_code = EXCLUDED.sub_category_code,
            address = EXCLUDED.address,
            detail_address = EXCLUDED.detail_address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            zipcode = EXCLUDED.zipcode,
            tel = EXCLUDED.tel,
            homepage = EXCLUDED.homepage,
            overview = EXCLUDED.overview,
            first_image = EXCLUDED.first_image,
            first_image_small = EXCLUDED.first_image_small,
            shop_type = EXCLUDED.shop_type,
            opening_hours = EXCLUDED.opening_hours,
            rest_date = EXCLUDED.rest_date,
            parking_info = EXCLUDED.parking_info,
            credit_card = EXCLUDED.credit_card,
            pet_allowed = EXCLUDED.pet_allowed,
            baby_carriage = EXCLUDED.baby_carriage,
            sale_item = EXCLUDED.sale_item,
            fair_day = EXCLUDED.fair_day,
            booktour = EXCLUDED.booktour,
            createdtime = EXCLUDED.createdtime,
            modifiedtime = EXCLUDED.modifiedtime,
            telname = EXCLUDED.telname,
            faxno = EXCLUDED.faxno,
            mlevel = EXCLUDED.mlevel,
            detail_intro_info = EXCLUDED.detail_intro_info,
            detail_additional_info = EXCLUDED.detail_additional_info,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            processing_status = EXCLUDED.processing_status,
            updated_at = CURRENT_TIMESTAMP
        """

        params = (
            data.get("content_id"),
            data.get("region_code"),
            data.get("sigungu_code"),
            data.get("shop_name"),
            data.get("category_code"),
            data.get("sub_category_code"),
            data.get("address"),
            data.get("detail_address") or data.get("addr2"),
            data.get("latitude"),
            data.get("longitude"),
            data.get("zipcode") or data.get("zip_code"),
            data.get("tel"),
            data.get("homepage"),
            data.get("overview"),
            data.get("first_image"),
            data.get("first_image_small"),
            data.get("shop_type"),
            data.get("opening_hours"),
            data.get("rest_date"),
            data.get("parking_info"),
            data.get("credit_card"),
            data.get("pet_allowed"),
            data.get("baby_carriage"),
            data.get("sale_item"),
            data.get("fair_day"),
            # 새로 추가된 필드들
            data.get("booktour") or data.get("book_tour"),
            data.get("createdtime") or data.get("created_time"),
            data.get("modifiedtime") or data.get("modified_time"),
            data.get("telname") or data.get("tel_name"),
            data.get("faxno") or data.get("fax_no"),
            data.get("mlevel") or data.get("map_level"),
            self.db_manager.serialize_for_db(data.get("detail_intro_info") or data.get("intro_info")),
            self.db_manager.serialize_for_db(data.get("detail_additional_info") or data.get("additional_info")),
            # 메타데이터 필드들
            data.get("raw_data_id"),
            data.get("last_sync_at"),
            data.get("data_quality_score"),
            data.get("processing_status"),
        )

        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"쇼핑 데이터 UPSERT 실패: {e}")
            return False


# 기존 database_manager에 확장 기능 추가
def extend_database_manager(db_manager: SyncDatabaseManager) -> SyncDatabaseManager:
    """데이터베이스 매니저에 확장 기능 추가"""

    extension = DatabaseManagerExtension(db_manager)

    # 확장 메서드들을 db_manager에 동적으로 추가
    db_manager.insert_raw_data = extension.insert_raw_data
    db_manager.insert_kto_metadata = extension.insert_kto_metadata
    db_manager.insert_kma_metadata = extension.insert_kma_metadata
    db_manager.log_transformation = extension.log_transformation
    db_manager.get_raw_data = extension.get_raw_data
    db_manager.cleanup_expired_raw_data = extension.cleanup_expired_raw_data
    db_manager.upsert_tourist_attraction = extension.upsert_tourist_attraction
    db_manager.upsert_accommodation = extension.upsert_accommodation
    db_manager.upsert_festival_event = extension.upsert_festival_event
    db_manager.upsert_pet_tour_info = extension.upsert_pet_tour_info
    db_manager.upsert_restaurant = extension.upsert_restaurant
    db_manager.upsert_cultural_facility = extension.upsert_cultural_facility
    db_manager.upsert_travel_course = extension.upsert_travel_course
    db_manager.upsert_leisure_sport = extension.upsert_leisure_sport
    db_manager.upsert_shopping = extension.upsert_shopping
    db_manager.get_api_call_statistics = extension.get_api_call_statistics
    db_manager.get_data_quality_thresholds = extension.get_data_quality_thresholds
    
    # 새로운 테이블 지원 메서드 추가
    db_manager.upsert_content_images = extension.upsert_content_images
    db_manager.upsert_content_detail_info = extension.upsert_content_detail_info
    db_manager.insert_content_images_batch = extension.insert_content_images_batch
    db_manager.insert_content_detail_info_batch = extension.insert_content_detail_info_batch

    return db_manager


# 확장된 데이터베이스 매니저 인스턴스 제공
def get_extended_database_manager() -> SyncDatabaseManager:
    """확장된 데이터베이스 매니저 인스턴스 반환"""
    from app.core.database_manager import DatabaseManager

    # UnifiedDatabaseManager 인스턴스에서 sync_manager 가져오기
    unified_manager = DatabaseManager()
    return extend_database_manager(unified_manager.sync_manager)
