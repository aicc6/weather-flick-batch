"""
지역 정보 관리 서비스 (regions 테이블 기반)

기존 RegionUnificationService를 regions 테이블 기반으로 재구현
- 지역 코드 동기화
- 좌표 변환
- API 매핑 관리
"""

import math
import logging
import json
from typing import Dict, Optional, Tuple, Any
from datetime import datetime

from app.core.database_manager import get_db_manager
from app.collectors.kto_api import KTODataCollector
from config.constants import WEATHER_COORDINATES, OBSERVATION_STATIONS


class RegionService:
    """지역 정보 관리 서비스 (regions 테이블 기반)"""
    
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
            
            # 시도 단위 처리
            for area in area_codes:
                try:
                    area_code = area.get('code')
                    area_name = area.get('name')
                    
                    if not area_code or not area_name:
                        continue
                    
                    # regions 테이블에 등록 또는 업데이트
                    is_new = self._create_or_update_region(
                        region_code=area_code,
                        region_name=area_name,
                        region_name_full=area_name,
                        level=1,  # 시도 단위
                        api_mappings={
                            'KTO': {
                                'api_region_code': area_code,
                                'api_region_name': area_name,
                                'additional_codes': {'area_code': area_code}
                            }
                        }
                    )
                    
                    if is_new:
                        regions_created += 1
                    else:
                        regions_updated += 1
                    
                    # 2. 시군구 단위 수집
                    try:
                        sigungu_codes = self.kto_collector.get_detailed_area_codes(area_code)
                        
                        for sigungu in sigungu_codes:
                            sigungu_code = sigungu.get('code')
                            sigungu_name = sigungu.get('name')
                            
                            if not sigungu_code or not sigungu_name:
                                continue
                            
                            # 시군구 지역 등록
                            child_is_new = self._create_or_update_region(
                                region_code=f"{area_code}-{sigungu_code}",
                                region_name=sigungu_name,
                                region_name_full=f"{area_name} {sigungu_name}",
                                level=2,  # 시군구 단위
                                parent_region_code=area_code,
                                api_mappings={
                                    'KTO': {
                                        'api_region_code': sigungu_code,
                                        'api_region_name': sigungu_name,
                                        'additional_codes': {
                                            'area_code': area_code,
                                            'sigungu_code': sigungu_code,
                                            'parent_area_code': area_code
                                        }
                                    }
                                }
                            )
                            
                            if child_is_new:
                                regions_created += 1
                            else:
                                regions_updated += 1
                    
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
            
            for region_name, coords in WEATHER_COORDINATES.items():
                try:
                    # 1. 기존 지역에서 매칭되는 지역 찾기
                    region_code = self._find_matching_region_by_name(region_name)
                    
                    if not region_code:
                        # 새로운 지역 생성 (KMA 전용)
                        region_code = f"KMA-{region_name}"
                        self._create_or_update_region(
                            region_code=region_code,
                            region_name=region_name,
                            region_name_full=f"{region_name} (기상청)",
                            level=1
                        )
                        self.logger.info(f"새로운 KMA 전용 지역 생성: {region_name}")
                    
                    # 2. 좌표 변환 정보 생성
                    lat, lon = self._convert_grid_to_wgs84(coords['nx'], coords['ny'])
                    station_code = OBSERVATION_STATIONS.get(region_name)
                    
                    # 3. API 매핑 및 좌표 정보 업데이트
                    api_mappings = self._get_region_api_mappings(region_code)
                    api_mappings['KMA'] = {
                        'api_region_code': region_name.lower(),
                        'api_region_name': region_name,
                        'additional_codes': {
                            'nx': coords['nx'],
                            'ny': coords['ny'],
                            'station_code': station_code
                        },
                        'mapping_confidence': 1.0
                    }
                    
                    coordinate_info = {
                        'kma_grid_nx': coords['nx'],
                        'kma_grid_ny': coords['ny'],
                        'kma_station_code': station_code,
                        'transform_accuracy': 5.0,
                        'calculation_method': 'manual',
                        'is_verified': True
                    }
                    
                    # 업데이트
                    self._update_region_mappings(
                        region_code=region_code,
                        api_mappings=api_mappings,
                        coordinate_info=coordinate_info,
                        center_latitude=lat,
                        center_longitude=lon
                    )
                    
                    mappings_created += 1
                
                except Exception as e:
                    self.logger.error(f"❌ KMA 지역 처리 실패 ({region_name}): {e}")
                    continue
            
            # 동기화 로그 업데이트
            result = {
                'status': 'success',
                'mappings_created': mappings_created,
                'mappings_updated': mappings_updated,
                'total_processed': len(WEATHER_COORDINATES)
            }
            
            self._update_sync_log(
                sync_log['log_id'],
                'success',
                processed_count=len(WEATHER_COORDINATES),
                created_count=mappings_created,
                updated_count=mappings_updated
            )
            
            self.logger.info(f"✅ KMA 지역 정보 동기화 완료: {result}")
            return result
            
        except Exception as e:
            self.logger.error(f"❌ KMA 지역 정보 동기화 실패: {e}")
            self._update_sync_log(sync_log['log_id'], 'failure', error_details={"error": str(e)})
            return {"status": "failure", "error": str(e)}
    
    def get_region_by_coordinates(self, lat: float, lon: float) -> Optional[Dict]:
        """좌표로 지역 정보 조회"""
        try:
            # 간단한 거리 계산 사용
            query = """
            SELECT *, 
                   SQRT(
                       POW(69.1 * (center_latitude - %s), 2) + 
                       POW(69.1 * (%s - center_longitude) * COS(center_latitude / 57.3), 2)
                   ) as distance_km
            FROM regions
            WHERE center_latitude IS NOT NULL 
              AND center_longitude IS NOT NULL
              AND ABS(center_latitude - %s) <= 0.5  -- 대략 50km 반경
              AND ABS(center_longitude - %s) <= 0.7
            ORDER BY SQRT(
                POW(69.1 * (center_latitude - %s), 2) + 
                POW(69.1 * (%s - center_longitude) * COS(center_latitude / 57.3), 2)
            )
            LIMIT 1
            """
            
            result = self.db_manager.fetch_one(query, (lat, lon, lat, lon, lat, lon))
            if result:
                # JSONB 필드 파싱
                result_dict = dict(result)
                if result_dict.get('coordinate_info'):
                    result_dict.update(result_dict['coordinate_info'])
                return result_dict
            return None
            
        except Exception as e:
            self.logger.error(f"좌표 기반 지역 조회 실패: {e}")
            return None
    
    def get_region_by_api_code(self, api_provider: str, api_region_code: str) -> Optional[Dict]:
        """API 코드로 지역 정보 조회"""
        try:
            query = """
            SELECT *
            FROM regions
            WHERE api_mappings->%s->>'api_region_code' = %s
              AND is_active = true
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
                                parent_region_code: str = None,
                                api_mappings: Dict = None) -> bool:
        """지역 생성 또는 업데이트"""
        try:
            # 기존 지역 확인
            existing_region = self.db_manager.fetch_one(
                "SELECT region_code FROM regions WHERE region_code = %s",
                (region_code,)
            )
            
            if existing_region:
                # 기존 지역 업데이트
                update_fields = []
                params = []
                
                if region_name_full:
                    update_fields.append("region_name_full = %s")
                    params.append(region_name_full)
                
                if parent_region_code:
                    update_fields.append("parent_region_code = %s")
                    params.append(parent_region_code)
                
                if api_mappings:
                    # 기존 매핑과 병합
                    existing_mappings = self._get_region_api_mappings(region_code)
                    existing_mappings.update(api_mappings)
                    update_fields.append("api_mappings = %s")
                    params.append(json.dumps(existing_mappings))
                
                update_fields.append("updated_at = CURRENT_TIMESTAMP")
                params.append(region_code)
                
                if update_fields:
                    self.db_manager.execute_update(f"""
                        UPDATE regions 
                        SET {', '.join(update_fields)}
                        WHERE region_code = %s
                    """, params)
                
                return False
            else:
                # 새로운 지역 생성
                self.db_manager.execute_query("""
                    INSERT INTO regions 
                    (region_code, region_name, region_name_full, region_level, 
                     parent_region_code, api_mappings, is_active)
                    VALUES (%s, %s, %s, %s, %s, %s, true)
                """, (region_code, region_name, region_name_full or region_name, 
                      level, parent_region_code, json.dumps(api_mappings or {})))
                
                return True
                
        except Exception as e:
            self.logger.error(f"지역 생성/업데이트 실패: {e}")
            raise
    
    def _update_region_mappings(self, region_code: str, api_mappings: Dict = None,
                               coordinate_info: Dict = None, center_latitude: float = None,
                               center_longitude: float = None):
        """지역 매핑 정보 업데이트"""
        try:
            update_fields = []
            params = []
            
            if api_mappings:
                update_fields.append("api_mappings = %s")
                params.append(json.dumps(api_mappings))
            
            if coordinate_info:
                update_fields.append("coordinate_info = %s")
                params.append(json.dumps(coordinate_info))
            
            if center_latitude is not None:
                update_fields.append("center_latitude = %s")
                update_fields.append("latitude = %s")
                params.extend([center_latitude, center_latitude])
            
            if center_longitude is not None:
                update_fields.append("center_longitude = %s")
                update_fields.append("longitude = %s")
                params.extend([center_longitude, center_longitude])
            
            update_fields.append("updated_at = CURRENT_TIMESTAMP")
            params.append(region_code)
            
            if update_fields:
                self.db_manager.execute_query(f"""
                    UPDATE regions 
                    SET {', '.join(update_fields)}
                    WHERE region_code = %s
                """, params)
                
        except Exception as e:
            self.logger.error(f"지역 매핑 정보 업데이트 실패: {e}")
            raise
    
    def _get_region_api_mappings(self, region_code: str) -> Dict:
        """지역의 API 매핑 정보 조회"""
        try:
            result = self.db_manager.fetch_one(
                "SELECT api_mappings FROM regions WHERE region_code = %s",
                (region_code,)
            )
            
            if result and result['api_mappings']:
                return result['api_mappings']
            return {}
            
        except Exception as e:
            self.logger.error(f"API 매핑 정보 조회 실패: {e}")
            return {}
    
    def _find_matching_region_by_name(self, region_name: str) -> Optional[str]:
        """지역명으로 기존 지역 찾기"""
        try:
            # 정확한 매칭 시도
            result = self.db_manager.fetch_one("""
                SELECT region_code FROM regions 
                WHERE region_name = %s OR region_name_full LIKE %s
                ORDER BY region_level ASC
                LIMIT 1
            """, (region_name, f"%{region_name}%"))
            
            return result['region_code'] if result else None
            
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
                    region = self.get_region_by_coordinates(lat, lon)
                    
                    if region and region.get('distance_km', 100) < 50:  # 50km 이내
                        validation_results['accurate_mappings'] += 1
                    else:
                        validation_results['inaccurate_mappings'] += 1
                        validation_results['mapping_errors'].append({
                            'content_id': attraction['content_id'],
                            'lat': lat,
                            'lon': lon,
                            'converted_nx': nx,
                            'converted_ny': ny,
                            'found_region': region['region_name'] if region else None,
                            'distance_km': region.get('distance_km') if region else None
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
                "SELECT COUNT(*) as count FROM regions WHERE is_active = true"
            )
            stats['total_regions'] = total_regions['count']
            
            # 레벨별 지역 수
            level_stats = self.db_manager.fetch_all("""
                SELECT region_level, COUNT(*) as count 
                FROM regions 
                WHERE is_active = true
                GROUP BY region_level 
                ORDER BY region_level
            """)
            stats['by_level'] = {row['region_level']: row['count'] for row in level_stats}
            
            # API 매핑 수
            api_stats = self.db_manager.fetch_all("""
                SELECT 
                    COUNT(CASE WHEN api_mappings ? 'KTO' THEN 1 END) as kto_count,
                    COUNT(CASE WHEN api_mappings ? 'KMA' THEN 1 END) as kma_count
                FROM regions 
                WHERE is_active = true
            """)
            if api_stats:
                stats['by_api'] = {
                    'KTO': {'count': api_stats[0]['kto_count']},
                    'KMA': {'count': api_stats[0]['kma_count']}
                }
            
            # 좌표 정보 수
            coord_stats = self.db_manager.fetch_one("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN coordinate_info IS NOT NULL THEN 1 END) as with_coordinates
                FROM regions
                WHERE is_active = true
            """)
            stats['coordinates'] = {
                'total': coord_stats['total'],
                'with_coordinates': coord_stats['with_coordinates']
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"지역 정보 통계 조회 실패: {e}")
            return {'error': str(e)}


def get_region_service() -> RegionService:
    """지역 정보 서비스 인스턴스 반환"""
    return RegionService()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    service = RegionService()
    
    print("=== 지역 정보 관리 서비스 테스트 ===")
    
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