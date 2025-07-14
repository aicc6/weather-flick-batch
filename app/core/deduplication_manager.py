"""
지역 정보 중복 방지 및 관리 시스템
외부 API에서 수집한 지역 정보의 중복을 방지하고 통합 관리
"""

import logging
import hashlib
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor

from ..core.database_connection_pool import DatabaseConnectionPool

logger = logging.getLogger(__name__)

@dataclass
class RegionData:
    """지역 데이터 구조"""
    region_code: str
    region_name: str
    region_name_full: Optional[str] = None
    region_name_en: Optional[str] = None
    region_level: int = 1
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    administrative_code: Optional[str] = None
    api_provider: str = 'UNKNOWN'
    api_region_code: Optional[str] = None
    additional_data: Optional[Dict] = None

class DeduplicationManager:
    """지역 정보 중복 제거 및 통합 관리자"""
    
    def __init__(self, connection_pool: DatabaseConnectionPool):
        self.connection_pool = connection_pool
        self.similarity_threshold = 0.85  # 문자열 유사도 임계값
        
    def add_region_data(self, region_data: RegionData) -> Tuple[bool, str]:
        """
        지역 데이터를 중복 검사 후 추가
        
        Args:
            region_data: 추가할 지역 데이터
            
        Returns:
            Tuple[bool, str]: (성공 여부, 메시지)
        """
        try:
            # 1. 중복 검사
            duplicate_check = self._check_duplicate(region_data)
            
            if duplicate_check['is_duplicate']:
                existing_region_id = duplicate_check['region_id']
                logger.info(f"중복 지역 발견: {region_data.region_name} -> {existing_region_id}")
                
                # 2. 기존 데이터 업데이트 (더 나은 정보가 있다면)
                updated = self._update_existing_region(existing_region_id, region_data)
                
                if updated:
                    return True, f"기존 지역 정보 업데이트: {region_data.region_name}"
                else:
                    return True, f"중복 지역 스킵: {region_data.region_name}"
            
            # 3. 새 지역 추가
            region_id = self._insert_new_region(region_data)
            
            # 4. API 매핑 정보 추가
            self._add_api_mapping(region_id, region_data)
            
            # 5. 좌표 정보 추가
            if region_data.latitude and region_data.longitude:
                self._add_coordinate_info(region_id, region_data)
            
            return True, f"새 지역 추가: {region_data.region_name}"
            
        except Exception as e:
            logger.error(f"지역 데이터 추가 실패: {e}")
            return False, f"추가 실패: {str(e)}"
    
    def _check_duplicate(self, region_data: RegionData) -> Dict[str, Any]:
        """중복 검사 수행"""
        
        with self.connection_pool.get_sync_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # 1. 정확한 일치 검사
                cursor.execute("""
                    SELECT region_id, region_name, region_code
                    FROM regions
                    WHERE region_name = %s 
                      AND region_level = %s
                      AND (administrative_code = %s OR %s IS NULL)
                """, (
                    region_data.region_name,
                    region_data.region_level,
                    region_data.administrative_code,
                    region_data.administrative_code
                ))
                
                exact_match = cursor.fetchone()
                if exact_match:
                    return {
                        'is_duplicate': True,
                        'region_id': exact_match['region_id'],
                        'match_type': 'exact',
                        'similarity': 1.0
                    }
                
                # 2. 부분 문자열 검사 (유사도 대신)
                cursor.execute("""
                    SELECT region_id, region_name, region_code
                    FROM regions
                    WHERE region_level = %s
                      AND (
                          region_name LIKE %s OR 
                          %s LIKE '%' || region_name || '%' OR
                          region_name LIKE '%' || %s || '%'
                      )
                    LIMIT 1
                """, (
                    region_data.region_level,
                    f"%{region_data.region_name}%",
                    region_data.region_name,
                    region_data.region_name
                ))
                
                similarity_match = cursor.fetchone()
                if similarity_match:
                    return {
                        'is_duplicate': True,
                        'region_id': similarity_match['region_id'],
                        'match_type': 'partial_match',
                        'similarity': 0.8
                    }
                
                # 3. 좌표 기반 검사 (반경 5km 이내)
                if region_data.latitude and region_data.longitude:
                    cursor.execute("""
                        SELECT region_id, region_name,
                               ST_Distance(
                                   ST_Point(%s, %s)::geography,
                                   ST_Point(center_longitude, center_latitude)::geography
                               ) as distance_meters
                        FROM regions
                        WHERE region_level = %s
                          AND center_latitude IS NOT NULL
                          AND center_longitude IS NOT NULL
                          AND ST_DWithin(
                              ST_Point(%s, %s)::geography,
                              ST_Point(center_longitude, center_latitude)::geography,
                              5000
                          )
                        ORDER BY distance_meters
                        LIMIT 1
                    """, (
                        region_data.longitude, region_data.latitude,
                        region_data.region_level,
                        region_data.longitude, region_data.latitude
                    ))
                    
                    location_match = cursor.fetchone()
                    if location_match:
                        return {
                            'is_duplicate': True,
                            'region_id': location_match['region_id'],
                            'match_type': 'location',
                            'distance_meters': location_match['distance_meters']
                        }
                
                return {'is_duplicate': False}
    
    def _update_existing_region(self, region_id: str, region_data: RegionData) -> bool:
        """기존 지역 정보 업데이트"""
        
        with self.connection_pool.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # 현재 정보 조회
                cursor.execute("""
                    SELECT region_name_full, region_name_en, 
                           center_latitude, center_longitude,
                           administrative_code, updated_at
                    FROM regions
                    WHERE region_id = %s
                """, (region_id,))
                
                current_data = cursor.fetchone()
                if not current_data:
                    return False
                
                # 업데이트할 필드 확인
                updates = []
                values = []
                
                # 전체 이름이 더 구체적인 경우
                if (region_data.region_name_full and 
                    (not current_data[0] or len(region_data.region_name_full) > len(current_data[0]))):
                    updates.append("region_name_full = %s")
                    values.append(region_data.region_name_full)
                
                # 영문명이 없거나 더 나은 경우
                if region_data.region_name_en and not current_data[1]:
                    updates.append("region_name_en = %s")
                    values.append(region_data.region_name_en)
                
                # 좌표 정보가 없거나 더 정확한 경우
                if (region_data.latitude and region_data.longitude and 
                    (not current_data[2] or not current_data[3])):
                    updates.append("center_latitude = %s, center_longitude = %s")
                    values.extend([region_data.latitude, region_data.longitude])
                
                # 행정구역 코드가 없는 경우
                if region_data.administrative_code and not current_data[4]:
                    updates.append("administrative_code = %s")
                    values.append(region_data.administrative_code)
                
                if updates:
                    updates.append("updated_at = CURRENT_TIMESTAMP")
                    values.append(region_id)
                    
                    query = f"""
                        UPDATE unified_regions 
                        SET {', '.join(updates)}
                        WHERE region_id = %s
                    """
                    
                    cursor.execute(query, values)
                    conn.commit()
                    return True
                
                return False
    
    def _insert_new_region(self, region_data: RegionData) -> str:
        """새 지역 정보 삽입"""
        
        with self.connection_pool.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO unified_regions (
                        region_code, region_name, region_name_full, region_name_en,
                        region_level, center_latitude, center_longitude,
                        administrative_code, is_active, created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    ) RETURNING region_id
                """, (
                    region_data.region_code,
                    region_data.region_name,
                    region_data.region_name_full,
                    region_data.region_name_en,
                    region_data.region_level,
                    region_data.latitude,
                    region_data.longitude,
                    region_data.administrative_code,
                    True
                ))
                
                region_id = cursor.fetchone()[0]
                conn.commit()
                return region_id
    
    def _add_api_mapping(self, region_id: str, region_data: RegionData):
        """API 매핑 정보 추가"""
        
        with self.connection_pool.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # 중복 체크
                cursor.execute("""
                    SELECT mapping_id FROM region_api_mappings
                    WHERE region_id = %s AND api_provider = %s 
                      AND api_region_code = %s
                """, (region_id, region_data.api_provider, region_data.api_region_code))
                
                if cursor.fetchone():
                    return  # 이미 존재
                
                # 새 매핑 추가
                cursor.execute("""
                    INSERT INTO region_api_mappings (
                        region_id, api_provider, api_region_code, api_region_name,
                        additional_codes, mapping_confidence, is_active,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """, (
                    region_id,
                    region_data.api_provider,
                    region_data.api_region_code,
                    region_data.region_name,
                    region_data.additional_data,
                    1.0,  # 새로 추가되는 매핑은 신뢰도 1.0
                    True
                ))
                
                # 트랜잭션은 컨텍스트 매니저에서 자동 처리됨
    
    def _add_coordinate_info(self, region_id: str, region_data: RegionData):
        """좌표 정보 추가"""
        
        with self.connection_pool.get_sync_connection() as conn:
            with conn.cursor() as cursor:
                # 중복 체크
                cursor.execute("""
                    SELECT transform_id FROM coordinate_transformations
                    WHERE region_id = %s 
                      AND ABS(wgs84_latitude - %s) < 0.0001
                      AND ABS(wgs84_longitude - %s) < 0.0001
                """, (region_id, region_data.latitude, region_data.longitude))
                
                if cursor.fetchone():
                    return  # 이미 존재
                
                # 새 좌표 정보 추가
                cursor.execute("""
                    INSERT INTO coordinate_transformations (
                        region_id, wgs84_latitude, wgs84_longitude,
                        transform_accuracy, calculation_method, is_verified,
                        created_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                """, (
                    region_id,
                    region_data.latitude,
                    region_data.longitude,
                    10.0,  # 10km 정확도로 설정
                    'api_provided',
                    False  # 검증 필요
                ))
                
                # 트랜잭션은 컨텍스트 매니저에서 자동 처리됨
    
    def cleanup_duplicates(self, dry_run: bool = True) -> Dict[str, int]:
        """
        중복 데이터 정리 수행
        
        Args:
            dry_run: True면 실제 삭제하지 않고 결과만 반환
            
        Returns:
            Dict[str, int]: 정리 결과 통계
        """
        
        with self.connection_pool.get_sync_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                result = {
                    'duplicate_regions': 0,
                    'duplicate_mappings': 0,
                    'duplicate_coordinates': 0
                }
                
                # 1. 중복 지역 찾기
                cursor.execute("""
                    SELECT region_name, region_level, COUNT(*) as cnt
                    FROM regions
                    GROUP BY region_name, region_level
                    HAVING COUNT(*) > 1
                """)
                
                duplicate_regions = cursor.fetchall()
                result['duplicate_regions'] = len(duplicate_regions)
                
                if not dry_run and duplicate_regions:
                    # 실제 중복 제거 수행 (비활성화)
                    cursor.execute("""
                        UPDATE regions ur1
                        SET is_active = false
                        WHERE ur1.region_code NOT IN (
                            SELECT MIN(ur2.region_code)
                            FROM regions ur2
                            WHERE ur2.region_name = ur1.region_name 
                              AND ur2.region_level = ur1.region_level
                              AND ur2.is_active = true
                        )
                        AND ur1.is_active = true
                    """)
                
                # 2. 중복 매핑 찾기
                cursor.execute("""
                    SELECT api_provider, api_region_code, COUNT(*) as cnt
                    FROM region_api_mappings
                    GROUP BY api_provider, api_region_code
                    HAVING COUNT(*) > 1
                """)
                
                duplicate_mappings = cursor.fetchall()
                result['duplicate_mappings'] = len(duplicate_mappings)
                
                if not dry_run and duplicate_mappings:
                    cursor.execute("""
                        DELETE FROM region_api_mappings ram1
                        WHERE ram1.mapping_id NOT IN (
                            SELECT MIN(ram2.mapping_id)
                            FROM region_api_mappings ram2
                            WHERE ram2.api_provider = ram1.api_provider
                              AND ram2.api_region_code = ram1.api_region_code
                        )
                    """)
                
                if not dry_run:
                    # 정리 로그 기록
                    cursor.execute("""
                        INSERT INTO region_sync_logs (
                            sync_type, sync_batch_id, processed_count,
                            sync_status, started_at, completed_at
                        ) VALUES (
                            'cleanup_duplicates',
                            %s,
                            %s,
                            'success',
                            CURRENT_TIMESTAMP,
                            CURRENT_TIMESTAMP
                        )
                    """, (
                        f"auto_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        result['duplicate_regions'] + result['duplicate_mappings']
                    ))
                
                return result
    
    def get_statistics(self) -> Dict[str, Any]:
        """통계 정보 반환"""
        
        with self.connection_pool.get_sync_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT 
                        (SELECT COUNT(*) FROM regions) as total_regions,
                        (SELECT COUNT(*) FROM region_api_mappings) as total_mappings,
                        (SELECT COUNT(*) FROM coordinate_transformations) as total_coordinates,
                        (SELECT COUNT(DISTINCT api_provider) FROM region_api_mappings) as api_providers,
                        (SELECT COUNT(*) FROM regions WHERE region_level = 1) as provinces,
                        (SELECT COUNT(*) FROM regions WHERE region_level = 2) as cities,
                        (SELECT COUNT(*) FROM regions WHERE region_level = 3) as districts
                """)
                
                return dict(cursor.fetchone())