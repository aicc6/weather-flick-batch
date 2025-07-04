"""
데이터베이스 매니저 확장

새로운 API 원데이터 관리 기능을 기존 database_manager에 확장
"""

import uuid
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from app.core.database_manager import SyncDatabaseManager


class DatabaseManagerExtension:
    """데이터베이스 매니저 확장 클래스"""
    
    def __init__(self, db_manager: SyncDatabaseManager):
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
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
            raw_data.get('api_provider'),
            raw_data.get('endpoint'), 
            raw_data.get('request_method'),
            raw_data.get('request_params'),
            raw_data.get('response_status'),
            raw_data.get('raw_response'),
            raw_data.get('response_size'),
            raw_data.get('request_duration'),
            raw_data.get('api_key_hash'),
            raw_data.get('expires_at'),
            raw_data.get('file_path')
        )
        
        try:
            # INSERT...RETURNING 쿼리를 위한 특별한 처리
            with self.db_manager.get_cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                if result:
                    raw_data_id = str(result['id'])
                    self.logger.debug(f"원본 데이터 삽입 완료: {raw_data_id}")
                    return raw_data_id
                else:
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
            metadata.get('raw_data_id'),
            metadata.get('content_type_id'),
            metadata.get('area_code'),
            metadata.get('sigungu_code'),
            metadata.get('total_count'),
            metadata.get('page_no'),
            metadata.get('num_of_rows'),
            metadata.get('sync_batch_id')
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
            metadata.get('raw_data_id'),
            metadata.get('base_date'),
            metadata.get('base_time'),
            metadata.get('nx'),
            metadata.get('ny'),
            metadata.get('forecast_type'),
            metadata.get('region_name')
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
            log_data.get('raw_data_id'),
            log_data.get('target_table'),
            log_data.get('transformation_rule'),
            log_data.get('input_record_count'),
            log_data.get('output_record_count'),
            log_data.get('error_count'),
            log_data.get('transformation_time_ms'),
            log_data.get('status'),
            log_data.get('error_details'),
            log_data.get('quality_score')
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
            archived_count = result['count'] if result else 0
            
            self.logger.info(f"만료된 원본 데이터 아카이브 완료: {archived_count}건")
            return archived_count
            
        except Exception as e:
            self.logger.error(f"만료 데이터 정리 실패: {e}")
            return 0
    
    def upsert_tourist_attraction(self, data: Dict) -> bool:
        """관광지 데이터 UPSERT"""
        
        query = """
        INSERT INTO tourist_attractions (
            content_id, region_code, attraction_name, category_code, category_name,
            address, latitude, longitude, description, image_url, raw_data_id,
            last_sync_at, data_quality_score, processing_status
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            processing_status = EXCLUDED.processing_status,
            updated_at = CURRENT_TIMESTAMP
        """
        
        params = (
            data.get('content_id'),
            data.get('region_code'),
            data.get('attraction_name'),
            data.get('category_code'),
            data.get('category_name'),
            data.get('address'),
            data.get('latitude'),
            data.get('longitude'),
            data.get('description'),
            data.get('image_url'),
            data.get('raw_data_id'),
            data.get('last_sync_at'),
            data.get('data_quality_score'),
            data.get('processing_status')
        )
        
        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"관광지 데이터 UPSERT 실패: {e}")
            return False
    
    def upsert_accommodation(self, data: Dict) -> bool:
        """숙박 데이터 UPSERT"""
        
        # accommodations 테이블 구조에 맞게 조정
        query = """
        INSERT INTO accommodations (
            content_id, region_code, accommodation_name, address, 
            latitude, longitude, image_url, raw_data_id,
            last_sync_at, data_quality_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            accommodation_name = EXCLUDED.accommodation_name,
            address = EXCLUDED.address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            image_url = EXCLUDED.image_url,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            updated_at = CURRENT_TIMESTAMP
        """
        
        params = (
            data.get('content_id'),
            data.get('region_code'),
            data.get('accommodation_name'),
            data.get('address'),
            data.get('latitude'),
            data.get('longitude'),
            data.get('image_url'),
            data.get('raw_data_id'),
            data.get('last_sync_at'),
            data.get('data_quality_score')
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
            latitude, longitude, image_url, start_date, end_date,
            raw_data_id, last_sync_at, data_quality_score
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (content_id) DO UPDATE SET
            region_code = EXCLUDED.region_code,
            event_name = EXCLUDED.event_name,
            address = EXCLUDED.address,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            image_url = EXCLUDED.image_url,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            raw_data_id = EXCLUDED.raw_data_id,
            last_sync_at = EXCLUDED.last_sync_at,
            data_quality_score = EXCLUDED.data_quality_score,
            updated_at = CURRENT_TIMESTAMP
        """
        
        params = (
            data.get('content_id'),
            data.get('region_code'),
            data.get('event_name'),
            data.get('address'),
            data.get('latitude'),
            data.get('longitude'),
            data.get('image_url'),
            data.get('start_date'),
            data.get('end_date'),
            data.get('raw_data_id'),
            data.get('last_sync_at'),
            data.get('data_quality_score')
        )
        
        try:
            self.db_manager.execute_update(query, params)
            return True
        except Exception as e:
            self.logger.error(f"축제/행사 데이터 UPSERT 실패: {e}")
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
                'completeness_threshold': 0.90,
                'validity_threshold': 0.95,
                'consistency_threshold': 0.99,
                'overall_threshold': 0.85
            }
            
        except Exception as e:
            self.logger.error(f"품질 임계값 조회 실패: {e}")
            return {}


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
    db_manager.get_api_call_statistics = extension.get_api_call_statistics
    db_manager.get_data_quality_thresholds = extension.get_data_quality_thresholds
    
    return db_manager


# 확장된 데이터베이스 매니저 인스턴스 제공
def get_extended_database_manager() -> SyncDatabaseManager:
    """확장된 데이터베이스 매니저 인스턴스 반환"""
    from app.core.database_manager import DatabaseManager
    
    # UnifiedDatabaseManager 인스턴스에서 sync_manager 가져오기
    unified_manager = DatabaseManager()
    return extend_database_manager(unified_manager.sync_manager)