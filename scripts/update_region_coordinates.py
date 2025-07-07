"""
기상청 API 지역별 좌표 정보 업데이트 스크립트

확장된 기상청 지역 정보를 데이터베이스에 업데이트합니다.
- 전국 17개 시도 중심점 좌표 업데이트
- 기상청 격자 좌표 및 관측소 코드 업데이트
- 추가 주요 도시 좌표 정보 삽입
"""

import json
import logging
from datetime import datetime
import sys
import os

# 프로젝트 루트를 Python 경로에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import get_db_manager
from app.services.kma_region_coordinates import (
    get_all_kma_regions, 
    get_additional_stations,
    KMA_REGION_COORDINATES
)

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RegionCoordinateUpdater:
    """지역 좌표 정보 업데이트 클래스"""
    
    def __init__(self):
        self.db = get_db_manager()
        self.logger = logger
    
    def update_main_regions(self):
        """주요 시도 지역 좌표 정보 업데이트"""
        self.logger.info("🔧 주요 시도 지역 좌표 정보 업데이트 시작")
        
        kma_regions = get_all_kma_regions()
        updated_count = 0
        mapping_updated = 0
        coordinate_updated = 0
        
        for region_name, region_data in kma_regions.items():
            try:
                area_code = region_data["area_code"]
                
                # 1. unified_regions 테이블 좌표 업데이트
                self.logger.info(f"📍 {region_name} 좌표 정보 업데이트 중...")
                
                update_result = self.db.execute_update("""
                    UPDATE unified_regions 
                    SET center_latitude = %s, 
                        center_longitude = %s,
                        region_name_full = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE region_code = %s
                """, (
                    region_data["latitude"], 
                    region_data["longitude"],
                    region_data["region_name_full"],
                    area_code
                ))
                
                if update_result > 0:
                    updated_count += 1
                    self.logger.info(f"✅ {region_name} 좌표 업데이트 완료: ({region_data['latitude']}, {region_data['longitude']})")
                else:
                    self.logger.warning(f"⚠️ {region_name} (코드: {area_code}) 지역을 찾을 수 없습니다.")
                    continue
                
                # 2. 지역 ID 조회
                region = self.db.fetch_one(
                    "SELECT region_id FROM unified_regions WHERE region_code = %s", 
                    (area_code,)
                )
                
                if not region:
                    self.logger.warning(f"⚠️ {region_name} 지역 ID를 찾을 수 없습니다.")
                    continue
                
                region_id = region['region_id']
                
                # 3. KMA API 매핑 업데이트
                additional_codes = json.dumps({
                    'nx': region_data['nx'],
                    'ny': region_data['ny'],
                    'station_code': region_data['station_code'],
                    'region_center': region_data['region_center']
                })
                
                mapping_result = self.db.execute_update("""
                    INSERT INTO region_api_mappings 
                    (region_id, api_provider, api_region_code, api_region_name, 
                     additional_codes, mapping_confidence)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (api_provider, api_region_code, region_id) DO UPDATE SET
                        api_region_name = EXCLUDED.api_region_name,
                        additional_codes = EXCLUDED.additional_codes,
                        mapping_confidence = EXCLUDED.mapping_confidence,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    region_id, 'KMA', region_name.lower(), region_name,
                    additional_codes, 1.0
                ))
                
                if mapping_result > 0:
                    mapping_updated += 1
                
                # 4. 좌표 변환 정보 업데이트
                coord_result = self.db.execute_update("""
                    INSERT INTO coordinate_transformations 
                    (region_id, wgs84_latitude, wgs84_longitude, kma_grid_nx, kma_grid_ny,
                     kma_station_code, transform_accuracy, calculation_method, is_verified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (region_id, wgs84_latitude, wgs84_longitude) DO UPDATE SET
                        kma_grid_nx = EXCLUDED.kma_grid_nx,
                        kma_grid_ny = EXCLUDED.kma_grid_ny,
                        kma_station_code = EXCLUDED.kma_station_code,
                        is_verified = EXCLUDED.is_verified,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    region_id, region_data["latitude"], region_data["longitude"],
                    region_data["nx"], region_data["ny"], region_data["station_code"],
                    5.0, 'official_kma', True
                ))
                
                if coord_result > 0:
                    coordinate_updated += 1
                
            except Exception as e:
                self.logger.error(f"❌ {region_name} 처리 실패: {e}")
                continue
        
        self.logger.info(f"✅ 주요 지역 업데이트 완료: {updated_count}개 지역, {mapping_updated}개 매핑, {coordinate_updated}개 좌표 변환")
        return updated_count, mapping_updated, coordinate_updated
    
    def add_additional_cities(self):
        """추가 주요 도시 정보 삽입"""
        self.logger.info("🏙️ 추가 주요 도시 정보 삽입 시작")
        
        additional_stations = get_additional_stations()
        added_regions = 0
        added_mappings = 0
        added_coordinates = 0
        
        for city_name, city_data in additional_stations.items():
            try:
                # 1. 새로운 지역으로 추가 (레벨 3: 시군구)
                region_code = f"CITY-{city_name}"
                
                # 부모 지역 찾기 (경기도 도시들은 경기도를 부모로 설정)
                parent_region_id = None
                if city_name in ["고양", "성남", "용인", "부천", "안산", "안양", "남양주"]:
                    parent_region = self.db.fetch_one(
                        "SELECT region_id FROM unified_regions WHERE region_code = %s", ("31",)
                    )
                    if parent_region:
                        parent_region_id = parent_region['region_id']
                
                # 지역이 이미 존재하는지 확인
                existing_region = self.db.fetch_one(
                    "SELECT region_id FROM unified_regions WHERE region_code = %s", 
                    (region_code,)
                )
                
                if existing_region:
                    region_id = existing_region['region_id']
                    self.logger.info(f"📍 {city_name} 이미 존재, 업데이트 진행")
                else:
                    # 새로운 지역 추가
                    result = self.db.fetch_one("""
                        INSERT INTO unified_regions 
                        (region_code, region_name, region_name_full, region_level, 
                         parent_region_id, center_latitude, center_longitude)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING region_id
                    """, (
                        region_code, city_name, f"{city_name}시",
                        3, parent_region_id, city_data["latitude"], city_data["longitude"]
                    ))
                    
                    region_id = result['region_id']
                    added_regions += 1
                    self.logger.info(f"✅ {city_name} 새로운 지역 추가 완료")
                
                # 2. KMA API 매핑 추가
                additional_codes = json.dumps({
                    'nx': city_data['nx'],
                    'ny': city_data['ny'],
                    'station_code': city_data['station_code'],
                    'city_type': 'additional'
                })
                
                self.db.execute_update("""
                    INSERT INTO region_api_mappings 
                    (region_id, api_provider, api_region_code, api_region_name, 
                     additional_codes, mapping_confidence)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (api_provider, api_region_code, region_id) DO UPDATE SET
                        additional_codes = EXCLUDED.additional_codes,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    region_id, 'KMA', city_name.lower(), city_name,
                    additional_codes, 0.9
                ))
                
                added_mappings += 1
                
                # 3. 좌표 변환 정보 추가
                self.db.execute_update("""
                    INSERT INTO coordinate_transformations 
                    (region_id, wgs84_latitude, wgs84_longitude, kma_grid_nx, kma_grid_ny,
                     kma_station_code, transform_accuracy, calculation_method, is_verified)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (region_id, wgs84_latitude, wgs84_longitude) DO UPDATE SET
                        kma_grid_nx = EXCLUDED.kma_grid_nx,
                        kma_grid_ny = EXCLUDED.kma_grid_ny,
                        kma_station_code = EXCLUDED.kma_station_code,
                        updated_at = CURRENT_TIMESTAMP
                """, (
                    region_id, city_data["latitude"], city_data["longitude"],
                    city_data["nx"], city_data["ny"], city_data["station_code"],
                    7.0, 'official_kma', True
                ))
                
                added_coordinates += 1
                
            except Exception as e:
                self.logger.error(f"❌ {city_name} 추가 실패: {e}")
                continue
        
        self.logger.info(f"✅ 추가 도시 정보 추가 완료: {added_regions}개 지역, {added_mappings}개 매핑, {added_coordinates}개 좌표 변환")
        return added_regions, added_mappings, added_coordinates
    
    def validate_coordinates(self):
        """좌표 정보 검증"""
        self.logger.info("🔍 좌표 정보 검증 시작")
        
        # 통계 수집
        stats = {}
        
        # 전체 지역 수
        total_regions = self.db.fetch_one("SELECT COUNT(*) as count FROM unified_regions")
        stats['total_regions'] = total_regions['count']
        
        # 좌표가 있는 지역 수
        regions_with_coords = self.db.fetch_one("""
            SELECT COUNT(*) as count FROM unified_regions 
            WHERE center_latitude IS NOT NULL AND center_longitude IS NOT NULL
        """)
        stats['regions_with_coordinates'] = regions_with_coords['count']
        
        # KMA 매핑된 지역 수
        kma_mappings = self.db.fetch_one("""
            SELECT COUNT(*) as count FROM region_api_mappings 
            WHERE api_provider = %s
        """, ('KMA',))
        stats['kma_mappings'] = kma_mappings['count']
        
        # 좌표 변환 정보 수
        coordinate_transforms = self.db.fetch_one("""
            SELECT COUNT(*) as count FROM coordinate_transformations
        """)
        stats['coordinate_transformations'] = coordinate_transforms['count']
        
        # 검증된 좌표 수
        verified_coords = self.db.fetch_one("""
            SELECT COUNT(*) as count FROM coordinate_transformations 
            WHERE is_verified = true
        """)
        stats['verified_coordinates'] = verified_coords['count']
        
        self.logger.info(f"📊 좌표 정보 검증 결과:")
        self.logger.info(f"  - 전체 지역: {stats['total_regions']}개")
        self.logger.info(f"  - 좌표 보유 지역: {stats['regions_with_coordinates']}개")
        self.logger.info(f"  - KMA 매핑: {stats['kma_mappings']}개")
        self.logger.info(f"  - 좌표 변환 정보: {stats['coordinate_transformations']}개")
        self.logger.info(f"  - 검증된 좌표: {stats['verified_coordinates']}개")
        
        return stats
    
    def run_full_update(self):
        """전체 업데이트 실행"""
        self.logger.info("🚀 기상청 지역 좌표 정보 전체 업데이트 시작")
        
        start_time = datetime.now()
        
        try:
            # 1. 주요 시도 지역 업데이트
            main_results = self.update_main_regions()
            
            # 2. 추가 주요 도시 추가
            additional_results = self.add_additional_cities()
            
            # 3. 좌표 정보 검증
            validation_stats = self.validate_coordinates()
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 결과 요약
            total_regions = main_results[0] + additional_results[0]
            total_mappings = main_results[1] + additional_results[1]
            total_coordinates = main_results[2] + additional_results[2]
            
            self.logger.info("✅ 기상청 지역 좌표 정보 업데이트 완료!")
            self.logger.info(f"📈 업데이트 결과:")
            self.logger.info(f"  - 업데이트된 지역: {total_regions}개")
            self.logger.info(f"  - 업데이트된 매핑: {total_mappings}개")
            self.logger.info(f"  - 업데이트된 좌표: {total_coordinates}개")
            self.logger.info(f"⏱️ 소요 시간: {duration:.2f}초")
            
            return {
                'success': True,
                'updated_regions': total_regions,
                'updated_mappings': total_mappings,
                'updated_coordinates': total_coordinates,
                'validation_stats': validation_stats,
                'duration_seconds': duration
            }
            
        except Exception as e:
            self.logger.error(f"❌ 전체 업데이트 실패: {e}")
            return {
                'success': False,
                'error': str(e)
            }


def main():
    """메인 실행 함수"""
    print("=== 기상청 API 지역별 좌표 정보 업데이트 ===")
    
    updater = RegionCoordinateUpdater()
    result = updater.run_full_update()
    
    if result['success']:
        print(f"\\n✅ 업데이트 성공!")
        print(f"업데이트된 지역: {result['updated_regions']}개")
        print(f"업데이트된 매핑: {result['updated_mappings']}개")
        print(f"업데이트된 좌표: {result['updated_coordinates']}개")
        print(f"소요 시간: {result['duration_seconds']:.2f}초")
    else:
        print(f"\\n❌ 업데이트 실패: {result['error']}")


if __name__ == "__main__":
    main()