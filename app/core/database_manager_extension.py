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
    
    async def insert_raw_data(self, raw_data: Dict) -> str:
        """원본 API 데이터 삽입"""
        
        query = """
        INSERT INTO api_raw_data (
            api_provider, endpoint, request_method, request_params,
            response_status, raw_response, response_size, request_duration,
            api_key_hash, expires_at, file_path
        ) VALUES (
            %(api_provider)s, %(endpoint)s, %(request_method)s, %(request_params)s,
            %(response_status)s, %(raw_response)s, %(response_size)s, %(request_duration)s,
            %(api_key_hash)s, %(expires_at)s, %(file_path)s
        ) RETURNING id
        """
        
        try:
            result = await self.db_manager.execute_query(query, raw_data, fetch=True)
            if result:
                raw_data_id = str(result[0]['id'])
                self.logger.debug(f"원본 데이터 삽입 완료: {raw_data_id}")
                return raw_data_id
            else:
                raise Exception("원본 데이터 삽입 결과를 받을 수 없습니다")
                
        except Exception as e:
            self.logger.error(f"원본 데이터 삽입 실패: {e}")
            raise
    
    async def insert_kto_metadata(self, metadata: Dict) -> bool:
        """KTO API 메타데이터 삽입"""
        
        query = """
        INSERT INTO kto_api_metadata (
            raw_data_id, content_type_id, area_code, sigungu_code,
            total_count, page_no, num_of_rows, sync_batch_id
        ) VALUES (
            %(raw_data_id)s, %(content_type_id)s, %(area_code)s, %(sigungu_code)s,
            %(total_count)s, %(page_no)s, %(num_of_rows)s, %(sync_batch_id)s
        )
        """
        
        try:
            await self.db_manager.execute_query(query, metadata)
            return True
        except Exception as e:
            self.logger.error(f"KTO 메타데이터 삽입 실패: {e}")
            return False
    
    async def insert_kma_metadata(self, metadata: Dict) -> bool:
        """KMA API 메타데이터 삽입"""
        
        query = """
        INSERT INTO kma_api_metadata (
            raw_data_id, base_date, base_time, nx, ny,
            forecast_type, region_name
        ) VALUES (
            %(raw_data_id)s, %(base_date)s, %(base_time)s, %(nx)s, %(ny)s,
            %(forecast_type)s, %(region_name)s
        )
        """
        
        try:
            await self.db_manager.execute_query(query, metadata)
            return True
        except Exception as e:
            self.logger.error(f"KMA 메타데이터 삽입 실패: {e}")
            return False
    
    async def log_transformation(self, log_data: Dict) -> bool:
        """데이터 변환 로그 기록"""
        
        query = """
        INSERT INTO data_transformation_logs (
            raw_data_id, target_table, transformation_rule,
            input_record_count, output_record_count, error_count,
            transformation_time_ms, status, error_details, quality_score,
            completed_at
        ) VALUES (
            %(raw_data_id)s, %(target_table)s, %(transformation_rule)s,
            %(input_record_count)s, %(output_record_count)s, %(error_count)s,
            %(transformation_time_ms)s, %(status)s, %(error_details)s, %(quality_score)s,
            CURRENT_TIMESTAMP
        )
        """
        
        try:
            await self.db_manager.execute_query(query, log_data)
            return True
        except Exception as e:
            self.logger.error(f"변환 로그 기록 실패: {e}")
            return False
    
    async def get_raw_data(self, raw_data_id: str) -> Optional[Dict]:
        """원본 데이터 조회"""
        
        query = """
        SELECT * FROM api_raw_data 
        WHERE id = %(raw_data_id)s
        """
        
        try:
            result = await self.db_manager.execute_query(
                query, 
                {'raw_data_id': raw_data_id}, 
                fetch=True
            )
            
            if result:
                return dict(result[0])
            return None
            
        except Exception as e:
            self.logger.error(f"원본 데이터 조회 실패: {e}")
            return None
    
    async def cleanup_expired_raw_data(self) -> int:
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
            await self.db_manager.execute_query(archive_query)
            
            result = await self.db_manager.execute_query(count_query, fetch=True)
            archived_count = result[0]['count'] if result else 0
            
            self.logger.info(f"만료된 원본 데이터 아카이브 완료: {archived_count}건")
            return archived_count
            
        except Exception as e:
            self.logger.error(f"만료 데이터 정리 실패: {e}")
            return 0
    
    async def upsert_tourist_attraction(self, data: Dict) -> bool:
        """관광지 데이터 UPSERT"""
        
        query = """
        INSERT INTO tourist_attractions (
            content_id, region_code, attraction_name, category_code, category_name,
            address, latitude, longitude, description, image_url, raw_data_id,
            last_sync_at, data_quality_score, processing_status
        ) VALUES (
            %(content_id)s, %(region_code)s, %(attraction_name)s, %(category_code)s, %(category_name)s,
            %(address)s, %(latitude)s, %(longitude)s, %(description)s, %(image_url)s, %(raw_data_id)s,
            %(last_sync_at)s, %(data_quality_score)s, %(processing_status)s
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
        
        try:
            await self.db_manager.execute_query(query, data)
            return True
        except Exception as e:
            self.logger.error(f"관광지 데이터 UPSERT 실패: {e}")
            return False
    
    async def upsert_accommodation(self, data: Dict) -> bool:
        """숙박 데이터 UPSERT"""
        
        # accommodations 테이블 구조에 맞게 조정
        query = """
        INSERT INTO accommodations (
            content_id, region_code, accommodation_name, address, 
            latitude, longitude, image_url, raw_data_id,
            last_sync_at, data_quality_score
        ) VALUES (
            %(content_id)s, %(region_code)s, %(accommodation_name)s, %(address)s,
            %(latitude)s, %(longitude)s, %(image_url)s, %(raw_data_id)s,
            %(last_sync_at)s, %(data_quality_score)s
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
        
        try:
            await self.db_manager.execute_query(query, data)
            return True
        except Exception as e:
            self.logger.error(f"숙박 데이터 UPSERT 실패: {e}")
            return False
    
    async def upsert_festival_event(self, data: Dict) -> bool:
        """축제/행사 데이터 UPSERT"""
        
        query = """
        INSERT INTO festivals_events (
            content_id, region_code, event_name, address,
            latitude, longitude, image_url, start_date, end_date,
            raw_data_id, last_sync_at, data_quality_score
        ) VALUES (
            %(content_id)s, %(region_code)s, %(event_name)s, %(address)s,
            %(latitude)s, %(longitude)s, %(image_url)s, %(start_date)s, %(end_date)s,
            %(raw_data_id)s, %(last_sync_at)s, %(data_quality_score)s
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
        
        try:
            await self.db_manager.execute_query(query, data)
            return True
        except Exception as e:
            self.logger.error(f"축제/행사 데이터 UPSERT 실패: {e}")
            return False
    
    async def get_api_call_statistics(self, api_provider: str) -> Dict:
        """API 호출 통계 조회"""
        
        query = """
        SELECT 
            COUNT(*) FILTER (WHERE DATE(created_at) = CURRENT_DATE) as today_calls,
            COUNT(CASE WHEN response_status = 200 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as success_rate,
            AVG(request_duration) as avg_response_time_ms,
            MAX(created_at) as last_call_at,
            SUM(response_size) / 1024.0 / 1024.0 as total_raw_data_size_mb
        FROM api_raw_data 
        WHERE api_provider = %(api_provider)s
        AND created_at >= CURRENT_DATE - INTERVAL '7 days'
        """
        
        try:
            result = await self.db_manager.execute_query(
                query, 
                {'api_provider': api_provider}, 
                fetch=True
            )
            
            if result:
                stats = dict(result[0])
                # None 값을 0으로 변환
                for key, value in stats.items():
                    if value is None:
                        stats[key] = 0.0
                return stats
            
            return {}
            
        except Exception as e:
            self.logger.error(f"API 통계 조회 실패: {e}")
            return {}
    
    async def get_data_quality_thresholds(self, table_name: str) -> Dict:
        """데이터 품질 임계값 조회"""
        
        query = """
        SELECT * FROM data_quality_thresholds 
        WHERE table_name = %(table_name)s
        """
        
        try:
            result = await self.db_manager.execute_query(
                query, 
                {'table_name': table_name}, 
                fetch=True
            )
            
            if result:
                return dict(result[0])
            
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
    from app.core.database_manager import get_sync_database_manager
    
    db_manager = get_sync_database_manager()
    return extend_database_manager(db_manager)