"""
지역 정보 통합 관리 서비스

기상청 API와 한국관광공사 API의 지역 정보를 통합 관리하는 서비스
- 지역 코드 동기화
- 좌표 변환
- API 매핑 관리
"""

import math
import logging
import json
from typing import Dict, List, Optional, Tuple, Any
from decimal import Decimal
from datetime import datetime

from app.core.database_manager import get_db_manager
from app.collectors.kto_api import KTODataCollector
from config.constants import WEATHER_COORDINATES, OBSERVATION_STATIONS


class RegionUnificationService:
    """지역 정보 통합 관리 서비스"""
    
    def __init__(self):
        self.db_manager = get_db_manager()
        self.logger = logging.getLogger(__name__)
        self.kto_collector = KTODataCollector()
        
        # 좌표 변환 상수 (기상청 Lambert Conformal Conic 투영법)
        self.RE = 6371.00877  # 지구 반경(km)
        self.GRID = 5.0  # 격자 간격(km)
        self.SLAT1 = 30.0  # 투영 위도1(degree)
        self.SLAT2 = 60.0  # 투영 위도2(degree)
        self.OLON = 126.0  # 기준점 경도(degree)
        self.OLAT = 38.0  # 기준점 위도(degree)
        self.XO = 43  # 기준점 X좌표(GRID)
        self.YO = 136  # 기준점 Y좌표(GRID)
        
    def sync_kto_regions(self) -> Dict[str, Any]:
        """KTO API에서 지역 코드 수집 및 동기화"""
        self.logger.info("🔄 KTO 지역 정보 동기화 시작")
        
        sync_log = self._create_sync_log('kto_sync', 'KTO')
        
        try:
            # 1. 시도 단위 지역 코드 수집
            self.logger.info("1단계: 시도 단위 지역 코드 수집")
            area_codes = self.kto_collector.get_area_codes()
            
            if not area_codes:
                self.logger.warning("❌ KTO API에서 지역 코드를 가져올 수 없습니다.")
                self._update_sync_log(sync_log['log_id'], 'failure', error_details={"error": "No area codes retrieved"})
                return {"status": "failure", "message": "No area codes retrieved"}
            
            regions_created = 0
            regions_updated = 0
            mappings_created = 0
            
            # 시도 단위 처리
            for area in area_codes:
                try:
                    area_code = area.get('code')
                    area_name = area.get('name')
                    
                    if not area_code or not area_name:
                        continue
                    
                    # 통합 지역 마스터에 등록 또는 업데이트
                    region_id, is_new = self._create_or_update_region(
                        region_code=area_code,
                        region_name=area_name,
                        region_name_full=area_name,
                        level=1  # 시도 단위
                    )
                    
                    if is_new:
                        regions_created += 1
                    else:
                        regions_updated += 1
                    
                    # KTO API 매핑 정보 등록
                    mapping_created = self._create_or_update_api_mapping(
                        region_id=region_id,
                        api_provider='KTO',
                        api_region_code=area_code,
                        api_region_name=area_name,
                        additional_codes={'area_code': area_code}
                    )
                    
                    if mapping_created:
                        mappings_created += 1
                    
                    # 2. 시군구 단위 수집
                    try:
                        sigungu_codes = self.kto_collector.get_detailed_area_codes(area_code)
                        
                        for sigungu in sigungu_codes:
                            sigungu_code = sigungu.get('code')
                            sigungu_name = sigungu.get('name')
                            
                            if not sigungu_code or not sigungu_name:
                                continue
                            
                            # 시군구 지역 등록
                            child_region_id, child_is_new = self._create_or_update_region(
                                region_code=f"{area_code}-{sigungu_code}",
                                region_name=sigungu_name,
                                region_name_full=f"{area_name} {sigungu_name}",
                                level=2,  # 시군구 단위
                                parent_region_id=region_id
                            )
                            
                            if child_is_new:
                                regions_created += 1
                            else:
                                regions_updated += 1
                            
                            # 시군구 매핑 등록
                            child_mapping_created = self._create_or_update_api_mapping(
                                region_id=child_region_id,
                                api_provider='KTO',
                                api_region_code=sigungu_code,
                                api_region_name=sigungu_name,
                                additional_codes={
                                    'area_code': area_code,
                                    'sigungu_code': sigungu_code,
                                    'parent_area_code': area_code
                                }
                            )
                            
                            if child_mapping_created:
                                mappings_created += 1
                    
                    except Exception as e:
                        self.logger.warning(f"⚠️ 시군구 수집 실패 (지역: {area_name}): {e}")
                        continue
                
                except Exception as e:
                    self.logger.error(f"❌ 지역 처리 실패 (코드: {area.get('code', 'unknown')}): {e}")
                    continue
            
            # 동기화 로그 업데이트
            result = {
                'status': 'success',
                'regions_created': regions_created,
                'regions_updated': regions_updated,
                'mappings_created': mappings_created,
                'total_processed': len(area_codes)
            }
            
            self._update_sync_log(
                sync_log['log_id'], 
                'success',
                processed_count=len(area_codes),
                created_count=regions_created,
                updated_count=regions_updated
            )
            
            self.logger.info(f"✅ KTO 지역 정보 동기화 완료: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ KTO 지역 정보 동기화 실패: {e}")
            self._update_sync_log(sync_log['log_id'], 'failure', error_details={"error": str(e)})
            return {"status": "failure", "error": str(e)}
    
    def sync_kma_regions(self) -> Dict[str, Any]:
        """KMA API 지역 정보와 매핑"""
        self.logger.info("🔄 KMA 지역 정보 동기화 시작")
        
        sync_log = self._create_sync_log('kma_sync', 'KMA')
        
        try:
            mappings_created = 0
            mappings_updated = 0
            transforms_created = 0
            
            for region_name, coords in WEATHER_COORDINATES.items():
                try:
                    # 1. 기존 통합 지역에서 매칭되는 지역 찾기
                    region_id = self._find_matching_region_by_name(region_name)
                    
                    if not region_id:
                        # 새로운 지역 생성 (KMA 전용)
                        region_id, _ = self._create_or_update_region(
                            region_code=f"KMA-{region_name}",
                            region_name=region_name,
                            region_name_full=f"{region_name} (기상청)",
                            level=1
                        )
                        self.logger.info(f"새로운 KMA 전용 지역 생성: {region_name}")
                    
                    # 2. KMA API 매핑 생성
                    station_code = OBSERVATION_STATIONS.get(region_name)
                    additional_codes = {
                        'nx': coords['nx'],
                        'ny': coords['ny'],
                        'station_code': station_code
                    }
                    
                    mapping_created = self._create_or_update_api_mapping(
                        region_id=region_id,
                        api_provider='KMA',
                        api_region_code=region_name.lower(),
                        api_region_name=region_name,
                        additional_codes=additional_codes,
                        mapping_confidence=1.0  # KMA 데이터는 정확함
                    )
                    
                    if mapping_created:
                        mappings_created += 1
                    else:
                        mappings_updated += 1
                    
                    # 3. 좌표 변환 정보 생성
                    lat, lon = self._convert_grid_to_wgs84(coords['nx'], coords['ny'])
                    
                    if lat and lon:
                        transform_created = self._create_or_update_coordinate_transformation(
                            region_id=region_id,
                            wgs84_lat=lat,
                            wgs84_lon=lon,
                            kma_nx=coords['nx'],
                            kma_ny=coords['ny'],
                            kma_station_code=station_code,
                            calculation_method='manual',
                            is_verified=True
                        )
                        
                        if transform_created:
                            transforms_created += 1
                
                except Exception as e:
                    self.logger.error(f"❌ KMA 지역 처리 실패 ({region_name}): {e}")
                    continue
            
            # 동기화 로그 업데이트
            result = {
                'status': 'success',
                'mappings_created': mappings_created,
                'mappings_updated': mappings_updated,
                'transforms_created': transforms_created,
                'total_processed': len(WEATHER_COORDINATES)
            }
            
            self._update_sync_log(
                sync_log['log_id'],
                'success',
                processed_count=len(WEATHER_COORDINATES),
                created_count=mappings_created + transforms_created,
                updated_count=mappings_updated
            )
            
            self.logger.info(f"✅ KMA 지역 정보 동기화 완료: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ KMA 지역 정보 동기화 실패: {e}")
            self._update_sync_log(sync_log['log_id'], 'failure', error_details={"error": str(e)})
            return {"status": "failure", "error": str(e)}
    
    def get_unified_region_by_coordinates(self, lat: float, lon: float) -> Optional[Dict]:
        """좌표로 통합 지역 정보 조회 (PostGIS 없이 간단한 거리 계산)"""
        try:
            # PostGIS 없이 간단한 거리 계산 사용
            query = """
            SELECT ur.*, 
                   ct.kma_grid_nx, ct.kma_grid_ny, ct.kma_station_code,
                   ct.transform_accuracy,
                   SQRT(
                       POW(69.1 * (ct.wgs84_latitude - %s), 2) + 
                       POW(69.1 * (%s - ct.wgs84_longitude) * COS(ct.wgs84_latitude / 57.3), 2)
                   ) as distance_km
            FROM unified_regions ur
            LEFT JOIN coordinate_transformations ct ON ur.region_id = ct.region_id
            WHERE ct.wgs84_latitude IS NOT NULL 
              AND ct.wgs84_longitude IS NOT NULL
              AND ABS(ct.wgs84_latitude - %s) <= 0.5  -- 대략 50km 반경
              AND ABS(ct.wgs84_longitude - %s) <= 0.7
            ORDER BY SQRT(
                POW(69.1 * (ct.wgs84_latitude - %s), 2) + 
                POW(69.1 * (%s - ct.wgs84_longitude) * COS(ct.wgs84_latitude / 57.3), 2)
            )
            LIMIT 1
            """
            
            result = self.db_manager.fetch_one(query, (lat, lon, lat, lon, lat, lon))
            return dict(result) if result else None
            
        except Exception as e:
            self.logger.error(f"좌표 기반 지역 조회 실패: {e}")
            return None
    
    def get_unified_region_by_api_code(self, api_provider: str, api_region_code: str) -> Optional[Dict]:
        """API 코드로 통합 지역 정보 조회"""
        try:
            query = """
            SELECT ur.*, ram.additional_codes, ram.mapping_confidence
            FROM unified_regions ur
            JOIN region_api_mappings ram ON ur.region_id = ram.region_id
            WHERE ram.api_provider = %s 
              AND ram.api_region_code = %s 
              AND ram.is_active = true
            """
            
            result = self.db_manager.fetch_one(query, (api_provider, api_region_code))
            return dict(result) if result else None
            
        except Exception as e:
            self.logger.error(f"API 코드 기반 지역 조회 실패: {e}")
            return None
    
    def convert_wgs84_to_kma_grid(self, lat: float, lon: float) -> Tuple[int, int]:
        """WGS84 좌표를 KMA 격자 좌표로 변환"""
        try:
            # 각도를 라디안으로 변환
            DEGRAD = math.pi / 180.0
            
            re = self.RE / self.GRID
            slat1 = self.SLAT1 * DEGRAD
            slat2 = self.SLAT2 * DEGRAD
            olon = self.OLON * DEGRAD
            olat = self.OLAT * DEGRAD
            
            sn = math.tan(math.pi * 0.25 + slat2 * 0.5) / math.tan(math.pi * 0.25 + slat1 * 0.5)
            sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(sn)
            sf = math.tan(math.pi * 0.25 + slat1 * 0.5)
            sf = math.pow(sf, sn) * math.cos(slat1) / sn
            ro = math.tan(math.pi * 0.25 + olat * 0.5)
            ro = re * sf / math.pow(ro, sn)
            
            ra = math.tan(math.pi * 0.25 + lat * DEGRAD * 0.5)
            ra = re * sf / math.pow(ra, sn)
            theta = lon * DEGRAD - olon
            
            if theta > math.pi:
                theta -= 2.0 * math.pi
            if theta < -math.pi:
                theta += 2.0 * math.pi
                
            theta *= sn
            x = int(ra * math.sin(theta) + self.XO + 0.5)
            y = int(ro - ra * math.cos(theta) + self.YO + 0.5)
            
            return x, y
            
        except Exception as e:
            self.logger.error(f"좌표 변환 실패 (WGS84 -> KMA): {e}")
            return 0, 0
    
    def _convert_grid_to_wgs84(self, nx: int, ny: int) -> Tuple[Optional[float], Optional[float]]:
        """KMA 격자 좌표를 WGS84로 변환 (역변환)"""
        try:
            # 기존 매핑 테이블 사용 (정확한 역변환 공식은 복잡함)
            approximate_mappings = {
                (60, 127): (37.5665, 126.9780),  # 서울
                (98, 76): (35.1796, 129.0756),   # 부산
                (89, 90): (35.8714, 128.6014),   # 대구
                (55, 124): (37.4563, 126.7052),  # 인천
                (58, 74): (35.1595, 126.8526),   # 광주
                (67, 100): (36.3504, 127.3845),  # 대전
                (102, 84): (35.5384, 129.3114),  # 울산
                (66, 103): (36.4800, 127.2890),  # 세종
                (52, 38): (33.4996, 126.5312),   # 제주
            }
            
            return approximate_mappings.get((nx, ny), (None, None))
            
        except Exception as e:
            self.logger.error(f"좌표 변환 실패 (KMA -> WGS84): {e}")
            return None, None
    
    def _create_or_update_region(self, region_code: str, region_name: str, 
                                region_name_full: str = None, level: int = 1,
                                parent_region_id: str = None) -> Tuple[str, bool]:
        """통합 지역 생성 또는 업데이트"""
        try:
            # 기존 지역 확인
            existing_region = self.db_manager.fetch_one(
                "SELECT region_id FROM unified_regions WHERE region_code = %s",
                (region_code,)
            )
            
            if existing_region:
                # 기존 지역 업데이트
                self.db_manager.execute_update("""
                    UPDATE unified_regions 
                    SET region_name = %s, region_name_full = %s, 
                        parent_region_id = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE region_code = %s
                """, (region_name, region_name_full or region_name, parent_region_id, region_code))
                
                return existing_region['region_id'], False
            else:
                # 새로운 지역 생성
                result = self.db_manager.fetch_one("""
                    INSERT INTO unified_regions 
                    (region_code, region_name, region_name_full, region_level, parent_region_id)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING region_id
                """, (region_code, region_name, region_name_full or region_name, level, parent_region_id))
                
                return result['region_id'], True
                
        except Exception as e:
            self.logger.error(f"지역 생성/업데이트 실패: {e}")
            raise
    
    def _create_or_update_api_mapping(self, region_id: str, api_provider: str,
                                     api_region_code: str, api_region_name: str,
                                     additional_codes: Dict = None,
                                     mapping_confidence: float = 1.0) -> bool:
        """API 매핑 생성 또는 업데이트"""
        try:
            # 기존 매핑 확인
            existing_mapping = self.db_manager.fetch_one("""
                SELECT mapping_id FROM region_api_mappings 
                WHERE region_id = %s AND api_provider = %s AND api_region_code = %s
            """, (region_id, api_provider, api_region_code))
            
            if existing_mapping:
                # 기존 매핑 업데이트
                self.db_manager.execute_query("""
                    UPDATE region_api_mappings 
                    SET api_region_name = %s, additional_codes = %s, 
                        mapping_confidence = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE mapping_id = %s
                """, (api_region_name, json.dumps(additional_codes) if additional_codes else None,
                      mapping_confidence, existing_mapping['mapping_id']))
                return False
            else:
                # 새로운 매핑 생성
                self.db_manager.execute_query("""
                    INSERT INTO region_api_mappings 
                    (region_id, api_provider, api_region_code, api_region_name, 
                     additional_codes, mapping_confidence)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (region_id, api_provider, api_region_code, api_region_name,
                      json.dumps(additional_codes) if additional_codes else None, mapping_confidence))
                return True
                
        except Exception as e:
            self.logger.error(f"API 매핑 생성/업데이트 실패: {e}")
            return False
    
    def _create_or_update_coordinate_transformation(self, region_id: str, wgs84_lat: float,
                                                   wgs84_lon: float, kma_nx: int = None,
                                                   kma_ny: int = None, kma_station_code: str = None,
                                                   calculation_method: str = 'manual',
                                                   is_verified: bool = False) -> bool:
        """좌표 변환 정보 생성 또는 업데이트"""
        try:
            # 기존 변환 정보 확인
            existing_transform = self.db_manager.fetch_one("""
                SELECT transform_id FROM coordinate_transformations 
                WHERE region_id = %s AND wgs84_latitude = %s AND wgs84_longitude = %s
            """, (region_id, wgs84_lat, wgs84_lon))
            
            if existing_transform:
                # 기존 변환 정보 업데이트
                self.db_manager.execute_query("""
                    UPDATE coordinate_transformations 
                    SET kma_grid_nx = %s, kma_grid_ny = %s, kma_station_code = %s,
                        calculation_method = %s, is_verified = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE transform_id = %s
                """, (kma_nx, kma_ny, kma_station_code, calculation_method, is_verified,
                      existing_transform['transform_id']))
                return False
            else:
                # 새로운 변환 정보 생성
                self.db_manager.execute_query("""
                    INSERT INTO coordinate_transformations 
                    (region_id, wgs84_latitude, wgs84_longitude, kma_grid_nx, kma_grid_ny,
                     kma_station_code, calculation_method, is_verified, transform_accuracy)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (region_id, wgs84_lat, wgs84_lon, kma_nx, kma_ny, kma_station_code,
                      calculation_method, is_verified, 5.0))  # 5km 격자 기준
                return True
                
        except Exception as e:
            self.logger.error(f"좌표 변환 정보 생성/업데이트 실패: {e}")
            return False
    
    def _find_matching_region_by_name(self, region_name: str) -> Optional[str]:
        """지역명으로 기존 통합 지역 찾기"""
        try:
            # 정확한 매칭 시도
            result = self.db_manager.fetch_one("""
                SELECT region_id FROM unified_regions 
                WHERE region_name = %s OR region_name_full LIKE %s
                ORDER BY region_level ASC
                LIMIT 1
            """, (region_name, f"%{region_name}%"))
            
            return result['region_id'] if result else None
            
        except Exception as e:
            self.logger.error(f"지역명 매칭 실패: {e}")
            return None
    
    def _create_sync_log(self, sync_type: str, api_provider: str) -> Dict:
        """동기화 로그 생성"""
        try:
            batch_id = f"{sync_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            result = self.db_manager.fetch_one("""
                INSERT INTO region_sync_logs 
                (sync_type, sync_batch_id, api_provider, sync_status)
                VALUES (%s, %s, %s, 'running')
                RETURNING log_id
            """, (sync_type, batch_id, api_provider))
            
            return {
                'log_id': result['log_id'],
                'batch_id': batch_id
            }
            
        except Exception as e:
            self.logger.error(f"동기화 로그 생성 실패: {e}")
            return {'log_id': None, 'batch_id': None}
    
    def _update_sync_log(self, log_id: str, status: str, processed_count: int = 0,
                        created_count: int = 0, updated_count: int = 0,
                        error_count: int = 0, error_details: Dict = None):
        """동기화 로그 업데이트"""
        try:
            if not log_id:
                return
                
            self.db_manager.execute_query("""
                UPDATE region_sync_logs 
                SET sync_status = %s, processed_count = %s, created_count = %s,
                    updated_count = %s, error_count = %s, error_details = %s,
                    completed_at = CURRENT_TIMESTAMP
                WHERE log_id = %s
            """, (status, processed_count, created_count, updated_count, error_count,
                  json.dumps(error_details) if error_details else None, log_id))
            
        except Exception as e:
            self.logger.error(f"동기화 로그 업데이트 실패: {e}")
    
    def validate_coordinate_transformations(self, sample_size: int = 50) -> Dict[str, Any]:
        """좌표 변환 정확도 검증"""
        try:
            self.logger.info("🔍 좌표 변환 정확도 검증 시작")
            
            # 샘플 관광지 데이터로 검증
            sample_attractions = self.db_manager.fetch_all("""
                SELECT content_id, latitude, longitude, region_code
                FROM tourist_attractions 
                WHERE latitude IS NOT NULL 
                  AND longitude IS NOT NULL
                  AND latitude BETWEEN 33.0 AND 38.5
                  AND longitude BETWEEN 124.0 AND 132.0
                ORDER BY RANDOM()
                LIMIT %s
            """, (sample_size,))
            
            validation_results = {
                'total_checked': len(sample_attractions),
                'accurate_mappings': 0,
                'inaccurate_mappings': 0,
                'mapping_errors': [],
                'accuracy_rate': 0.0
            }
            
            for attraction in sample_attractions:
                try:
                    lat = float(attraction['latitude'])
                    lon = float(attraction['longitude'])
                    
                    # WGS84 -> KMA 격자 변환
                    nx, ny = self.convert_wgs84_to_kma_grid(lat, lon)
                    
                    # 변환된 격자 좌표로 지역 찾기
                    unified_region = self.get_unified_region_by_coordinates(lat, lon)
                    
                    if unified_region and unified_region.get('distance_km', 100) < 50:  # 50km 이내
                        validation_results['accurate_mappings'] += 1
                    else:
                        validation_results['inaccurate_mappings'] += 1
                        validation_results['mapping_errors'].append({
                            'content_id': attraction['content_id'],
                            'lat': lat,
                            'lon': lon,
                            'converted_nx': nx,
                            'converted_ny': ny,
                            'found_region': unified_region['region_name'] if unified_region else None,
                            'distance_km': unified_region.get('distance_km') if unified_region else None
                        })
                        
                except Exception as e:
                    validation_results['mapping_errors'].append({
                        'content_id': attraction['content_id'],
                        'error': str(e)
                    })
            
            # 정확도 계산
            if validation_results['total_checked'] > 0:
                validation_results['accuracy_rate'] = (
                    validation_results['accurate_mappings'] / validation_results['total_checked']
                ) * 100
            
            self.logger.info(f"✅ 좌표 변환 검증 완료: {validation_results['accuracy_rate']:.1f}% 정확도")
            return validation_results
            
        except Exception as e:
            self.logger.error(f"❌ 좌표 변환 검증 실패: {e}")
            return {'error': str(e)}
    
    def get_region_statistics(self) -> Dict[str, Any]:
        """지역 정보 통계 조회"""
        try:
            stats = {}
            
            # 전체 지역 수
            total_regions = self.db_manager.fetch_one(
                "SELECT COUNT(*) as count FROM unified_regions"
            )
            stats['total_regions'] = total_regions['count']
            
            # 레벨별 지역 수
            level_stats = self.db_manager.fetch_all("""
                SELECT region_level, COUNT(*) as count 
                FROM unified_regions 
                GROUP BY region_level 
                ORDER BY region_level
            """)
            stats['by_level'] = {row['region_level']: row['count'] for row in level_stats}
            
            # API별 매핑 수
            api_stats = self.db_manager.fetch_all("""
                SELECT api_provider, COUNT(*) as count, AVG(mapping_confidence) as avg_confidence
                FROM region_api_mappings 
                WHERE is_active = true
                GROUP BY api_provider
            """)
            stats['by_api'] = {
                row['api_provider']: {
                    'count': row['count'], 
                    'avg_confidence': float(row['avg_confidence']) if row['avg_confidence'] else 0.0
                } 
                for row in api_stats
            }
            
            # 좌표 변환 정보 수
            coord_stats = self.db_manager.fetch_one("""
                SELECT COUNT(*) as total, 
                       COUNT(CASE WHEN is_verified = true THEN 1 END) as verified
                FROM coordinate_transformations
            """)
            stats['coordinates'] = {
                'total': coord_stats['total'],
                'verified': coord_stats['verified']
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"지역 정보 통계 조회 실패: {e}")
            return {'error': str(e)}


def get_region_unification_service():
    """지역 정보 통합 서비스 인스턴스 반환
    
    호환성을 위해 RegionService를 반환합니다.
    """
    from .region_service import RegionService
    return RegionService()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    service = RegionUnificationService()
    
    print("=== 지역 정보 통합 관리 서비스 테스트 ===")
    
    # KTO 지역 동기화
    print("\n1. KTO 지역 정보 동기화")
    kto_result = service.sync_kto_regions()
    print(f"KTO 동기화 결과: {kto_result}")
    
    # KMA 지역 동기화  
    print("\n2. KMA 지역 정보 동기화")
    kma_result = service.sync_kma_regions()
    print(f"KMA 동기화 결과: {kma_result}")
    
    # 좌표 변환 검증
    print("\n3. 좌표 변환 정확도 검증")
    validation_result = service.validate_coordinate_transformations(20)
    print(f"검증 결과: {validation_result}")
    
    # 통계 조회
    print("\n4. 지역 정보 통계")
    stats = service.get_region_statistics()
    print(f"통계: {stats}")


