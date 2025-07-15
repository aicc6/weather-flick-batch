"""
지역 정보 통합 서비스 (regions 테이블 호환 버전)

기존 regions 테이블 구조를 유지하면서 메모리 캐시를 사용하여
API 매핑과 좌표 변환 정보를 관리합니다.
"""

import logging
from typing import Dict, Any, Tuple
from datetime import datetime

from app.core.unified_api_client import get_unified_api_client, APIProvider
from app.core.database_manager import get_db_manager


class RegionServiceCompatible:
    """지역 정보 관리 서비스 (호환 버전)"""
    
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.api_client = get_unified_api_client()
        self.db_manager = get_db_manager()
        
        # 메모리 캐시 (JSONB 대신 사용)
        self._api_mappings_cache = {}  # {region_code: {provider: mapping_info}}
        self._coordinate_info_cache = {}  # {region_code: coordinate_info}
        
        # 캐시 초기화
        self._load_cache_from_db()
    
    def _load_cache_from_db(self):
        """데이터베이스에서 캐시 로드 (기존 테이블 구조 사용)"""
        try:
            # 기존 API 매핑 테이블에서 로드 시도
            try:
                mappings = self.db_manager.fetch_all("""
                    SELECT region_id, api_provider, api_region_code, api_region_name, mapping_metadata
                    FROM region_api_mappings
                """)
                
                for mapping in mappings:
                    region_id = mapping['region_id']
                    if region_id not in self._api_mappings_cache:
                        self._api_mappings_cache[region_id] = {}
                    
                    self._api_mappings_cache[region_id][mapping['api_provider']] = {
                        'api_region_code': mapping['api_region_code'],
                        'api_region_name': mapping['api_region_name'],
                        'metadata': mapping.get('mapping_metadata', {})
                    }
            except Exception as e:
                self.logger.warning(f"API 매핑 테이블 로드 실패 (테이블이 없을 수 있음): {e}")
            
            # 기존 좌표 변환 테이블에서 로드 시도
            try:
                coordinates = self.db_manager.fetch_all("""
                    SELECT region_id, lambert_x, lambert_y, wgs84_latitude, wgs84_longitude,
                           conversion_accuracy, is_verified
                    FROM coordinate_transformations
                """)
                
                for coord in coordinates:
                    self._coordinate_info_cache[coord['region_id']] = {
                        'lambert_x': coord['lambert_x'],
                        'lambert_y': coord['lambert_y'],
                        'wgs84_latitude': coord['wgs84_latitude'],
                        'wgs84_longitude': coord['wgs84_longitude'],
                        'conversion_accuracy': coord.get('conversion_accuracy'),
                        'is_verified': coord.get('is_verified', False)
                    }
            except Exception as e:
                self.logger.warning(f"좌표 변환 테이블 로드 실패 (테이블이 없을 수 있음): {e}")
                
        except Exception as e:
            self.logger.error(f"캐시 초기화 실패: {e}")
    
    async def sync_kto_regions(self) -> Dict[str, Any]:
        """KTO (한국관광공사) 지역 정보 동기화"""
        self.logger.info("KTO 지역 정보 동기화 시작")
        result = {
            'status': 'running',
            'regions_created': 0,
            'regions_updated': 0,
            'total_processed': 0,
            'errors': []
        }
        
        try:
            # API 클라이언트 컨텍스트 내에서 실행
            async with self.api_client:
                # KTO API에서 지역 정보 조회
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint='areaCode1',
                    params={
                        'numOfRows': 100,
                        'pageNo': 1,
                        'MobileOS': 'ETC',
                        'MobileApp': 'WeatherFlick',
                        'serviceKey': None,  # API 클라이언트가 자동으로 처리
                        '_type': 'json'
                    }
                )
                
                if not response.success:
                    raise Exception(f"KTO API 호출 실패: {response.error}")
                
                # 지역 정보 처리
                items = response.data.get('response', {}).get('body', {}).get('items', {}).get('item', [])
                
                for item in items:
                    try:
                        created = self._create_or_update_region(
                            region_code=item.get('code'),
                            region_name=item.get('name'),
                            region_name_full=item.get('name'),
                            level=1,
                            api_mappings={
                                'KTO': {
                                    'api_region_code': item.get('code'),
                                    'api_region_name': item.get('name')
                                }
                            }
                        )
                        
                        if created:
                            result['regions_created'] += 1
                        else:
                            result['regions_updated'] += 1
                        
                        result['total_processed'] += 1
                        
                    except Exception as e:
                        self.logger.warning(f"지역 처리 실패 [{item.get('name')}]: {e}")
                        result['errors'].append(str(e))
            
            result['status'] = 'success'
            self.logger.info(f"KTO 동기화 완료: 생성 {result['regions_created']}, 업데이트 {result['regions_updated']}")
            
        except Exception as e:
            result['status'] = 'failure'
            result['error'] = str(e)
            self.logger.error(f"KTO 동기화 실패: {e}")
        
        return result
    
    async def sync_kma_regions(self) -> Dict[str, Any]:
        """KMA (기상청) 지역 정보 동기화"""
        self.logger.info("KMA 지역 정보 동기화 시작")
        result = {
            'status': 'running',
            'mappings_created': 0,
            'total_processed': 0,
            'errors': []
        }
        
        try:
            # 기존 지역 정보 조회
            regions = self.db_manager.fetch_all("""
                SELECT region_code, region_name
                FROM regions
                WHERE region_level = 1
            """)
            
            # KMA 지역 매핑 정보 (하드코딩)
            kma_mappings = {
                '11': {'nx': 60, 'ny': 127, 'name': '서울'},
                '26': {'nx': 98, 'ny': 76, 'name': '부산'},
                '27': {'nx': 89, 'ny': 90, 'name': '대구'},
                '28': {'nx': 55, 'ny': 124, 'name': '인천'},
                '29': {'nx': 58, 'ny': 74, 'name': '광주'},
                '30': {'nx': 67, 'ny': 100, 'name': '대전'},
                '31': {'nx': 102, 'ny': 84, 'name': '울산'},
                '36': {'nx': 66, 'ny': 103, 'name': '세종'},
                '50': {'nx': 52, 'ny': 38, 'name': '제주'}
            }
            
            for region in regions:
                region_code = region['region_code']
                
                if region_code in kma_mappings:
                    mapping = kma_mappings[region_code]
                    
                    # 메모리 캐시에 저장
                    if region_code not in self._api_mappings_cache:
                        self._api_mappings_cache[region_code] = {}
                    
                    self._api_mappings_cache[region_code]['KMA'] = {
                        'api_region_code': region_code,
                        'api_region_name': mapping['name'],
                        'nx': mapping['nx'],
                        'ny': mapping['ny']
                    }
                    
                    # 좌표 정보도 함께 저장
                    self._coordinate_info_cache[region_code] = {
                        'lambert_x': mapping['nx'],
                        'lambert_y': mapping['ny'],
                        'wgs84_latitude': self._lambert_to_wgs84(mapping['nx'], mapping['ny'])[0],
                        'wgs84_longitude': self._lambert_to_wgs84(mapping['nx'], mapping['ny'])[1],
                        'is_verified': True
                    }
                    
                    result['mappings_created'] += 1
                    result['total_processed'] += 1
            
            result['status'] = 'success'
            self.logger.info(f"KMA 동기화 완료: 매핑 생성 {result['mappings_created']}")
            
        except Exception as e:
            result['status'] = 'failure'  
            result['error'] = str(e)
            self.logger.error(f"KMA 동기화 실패: {e}")
        
        return result
    
    async def validate_coordinate_transformations(self, sample_size: int = 100) -> Dict[str, Any]:
        """좌표 변환 정확도 검증"""
        self.logger.info(f"좌표 변환 검증 시작 (샘플 크기: {sample_size})")
        
        result = {
            'status': 'running',
            'total_checked': 0,
            'accurate_mappings': 0,
            'inaccurate_mappings': 0,
            'accuracy_rate': 0.0
        }
        
        try:
            # 메모리 캐시에서 검증
            checked = 0
            accurate = 0
            
            for region_code, coord_info in self._coordinate_info_cache.items():
                if checked >= sample_size:
                    break
                
                # 간단한 유효성 검사
                lat = coord_info.get('wgs84_latitude', 0)
                lon = coord_info.get('wgs84_longitude', 0)
                
                if 33.0 <= lat <= 38.5 and 124.0 <= lon <= 132.0:
                    accurate += 1
                
                checked += 1
            
            result['total_checked'] = checked
            result['accurate_mappings'] = accurate
            result['inaccurate_mappings'] = checked - accurate
            result['accuracy_rate'] = (accurate / checked * 100) if checked > 0 else 0
            result['status'] = 'success'
            
            self.logger.info(f"좌표 검증 완료: {result['accuracy_rate']:.1f}% 정확도")
            
        except Exception as e:
            result['status'] = 'failure'
            result['error'] = str(e)
            self.logger.error(f"좌표 검증 실패: {e}")
        
        return result
    
    def _create_or_update_region(self, region_code: str, region_name: str, 
                                region_name_full: str = None, level: int = 1,
                                parent_region_code: str = None,
                                api_mappings: Dict = None) -> bool:
        """지역 정보 생성 또는 업데이트"""
        try:
            # 기존 지역 확인
            existing = self.db_manager.fetch_one(
                "SELECT region_code FROM regions WHERE region_code = %s",
                (region_code,)
            )
            
            if existing:
                # 업데이트
                self.db_manager.execute_query("""
                    UPDATE regions 
                    SET region_name = %s, 
                        parent_region_code = %s,
                        updated_at = %s
                    WHERE region_code = %s
                """, (region_name, parent_region_code, datetime.now(), region_code))
                
                # 캐시 업데이트
                if api_mappings:
                    self._api_mappings_cache[region_code] = api_mappings
                
                return False
            else:
                # 생성
                self.db_manager.execute_query("""
                    INSERT INTO regions (region_code, region_name, region_level, parent_region_code)
                    VALUES (%s, %s, %s, %s)
                """, (region_code, region_name, level, parent_region_code))
                
                # 캐시 업데이트
                if api_mappings:
                    self._api_mappings_cache[region_code] = api_mappings
                
                return True
                
        except Exception as e:
            self.logger.error(f"지역 생성/업데이트 실패 [{region_code}]: {e}")
            raise
    
    def _lambert_to_wgs84(self, x: float, y: float) -> Tuple[float, float]:
        """Lambert Conformal Conic 좌표를 WGS84로 변환 (간단한 근사)"""
        # 기상청 Lambert Conformal Conic 투영 기준점
        base_lat = 38.0
        base_lon = 126.0
        
        # 간단한 선형 변환 (실제로는 더 복잡한 수식 필요)
        lat = base_lat + (y - 90) * 0.01
        lon = base_lon + (x - 60) * 0.01
        
        return lat, lon
    
    def get_region_statistics(self) -> Dict[str, Any]:
        """지역 정보 통계 조회"""
        try:
            stats = self.db_manager.fetch_one("""
                SELECT 
                    COUNT(*) as total_regions,
                    COUNT(DISTINCT CASE WHEN region_level = 1 THEN region_code END) as level1_regions,
                    COUNT(DISTINCT parent_region_code) as parent_regions
                FROM regions
            """)
            
            return {
                'total_regions': stats['total_regions'],
                'level1_regions': stats['level1_regions'],
                'parent_regions': stats['parent_regions'],
                'api_mappings_cached': len(self._api_mappings_cache),
                'coordinate_info_cached': len(self._coordinate_info_cache)
            }
        except Exception as e:
            self.logger.error(f"통계 조회 실패: {e}")
            return {}


# 싱글톤 인스턴스
_region_service_compatible = None


def get_region_service_compatible() -> RegionServiceCompatible:
    """RegionServiceCompatible 싱글톤 인스턴스 반환"""
    global _region_service_compatible
    if _region_service_compatible is None:
        _region_service_compatible = RegionServiceCompatible()
    return _region_service_compatible