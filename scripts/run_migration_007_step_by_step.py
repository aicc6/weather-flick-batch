#!/usr/bin/env python3
"""
마이그레이션 007 단계별 실행 스크립트
UnifiedRegions를 Regions로 마이그레이션
"""

import sys
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.core.database_manager import get_db_manager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_migration():
    """마이그레이션 단계별 실행"""
    db_manager = get_db_manager()
    
    try:
        logger.info("🚀 마이그레이션 시작: UnifiedRegions -> Regions")
        
        # 1단계: regions 테이블에 컬럼 추가
        logger.info("\n1단계: regions 테이블에 필요한 컬럼 추가")
        
        columns_to_add = [
            ("region_name_full", "VARCHAR(150)"),
            ("region_name_en", "VARCHAR(100)"),
            ("center_latitude", "DECIMAL(10, 8)"),
            ("center_longitude", "DECIMAL(11, 8)"),
            ("administrative_code", "VARCHAR(20)"),
            ("boundary_data", "JSONB"),
            ("api_mappings", "JSONB"),
            ("coordinate_info", "JSONB"),
            ("is_active", "BOOLEAN DEFAULT TRUE")
        ]
        
        for column_name, column_type in columns_to_add:
            try:
                query = f"ALTER TABLE regions ADD COLUMN IF NOT EXISTS {column_name} {column_type}"
                db_manager.execute_query(query)
                logger.info(f"   ✅ {column_name} 컬럼 추가 완료")
            except Exception as e:
                logger.warning(f"   ⚠️ {column_name} 컬럼 추가 실패: {e}")
        
        # 2단계: 인덱스 생성
        logger.info("\n2단계: 인덱스 생성")
        
        indexes = [
            ("idx_regions_name_full", "regions(region_name_full)"),
            ("idx_regions_administrative_code", "regions(administrative_code)"),
            ("idx_regions_is_active", "regions(is_active)"),
            ("idx_regions_api_mappings", "regions USING GIN(api_mappings)")
        ]
        
        for index_name, index_def in indexes:
            try:
                query = f"CREATE INDEX IF NOT EXISTS {index_name} ON {index_def}"
                db_manager.execute_query(query)
                logger.info(f"   ✅ {index_name} 인덱스 생성 완료")
            except Exception as e:
                logger.warning(f"   ⚠️ {index_name} 인덱스 생성 실패: {e}")
        
        # 3단계: unified_regions 테이블 확인
        logger.info("\n3단계: unified_regions 테이블 확인")
        
        # unified_regions 테이블이 있는지 확인
        table_check = db_manager.fetch_one("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'unified_regions'
            )
        """)
        
        if not table_check['exists']:
            logger.warning("   ⚠️ unified_regions 테이블이 없습니다. 마이그레이션을 건너뜁니다.")
            return True
        
        logger.info("   ✅ unified_regions 테이블 확인 완료")
        
        # 4단계: 데이터 마이그레이션
        logger.info("\n4단계: unified_regions 데이터를 regions로 마이그레이션")
        
        try:
            # 먼저 unified_regions의 데이터 수 확인
            count_result = db_manager.fetch_one("SELECT COUNT(*) as count FROM unified_regions")
            unified_count = count_result['count']
            logger.info(f"   - unified_regions 레코드 수: {unified_count}")
            
            if unified_count > 0:
                # 데이터 마이그레이션
                db_manager.execute_query("""
                    INSERT INTO regions (
                        region_code,
                        region_name,
                        region_name_full,
                        region_name_en,
                        parent_region_code,
                        region_level,
                        latitude,
                        longitude,
                        center_latitude,
                        center_longitude,
                        administrative_code,
                        boundary_data,
                        is_active,
                        created_at,
                        updated_at
                    )
                    SELECT 
                        ur.region_code,
                        ur.region_name,
                        ur.region_name_full,
                        ur.region_name_en,
                        parent_ur.region_code as parent_region_code,
                        ur.region_level,
                        ur.center_latitude as latitude,
                        ur.center_longitude as longitude,
                        ur.center_latitude,
                        ur.center_longitude,
                        ur.administrative_code,
                        ur.boundary_data,
                        ur.is_active,
                        ur.created_at,
                        CURRENT_TIMESTAMP
                    FROM unified_regions ur
                    LEFT JOIN unified_regions parent_ur ON ur.parent_region_id = parent_ur.region_id
                    WHERE NOT EXISTS (
                        SELECT 1 FROM regions r WHERE r.region_code = ur.region_code
                    )
                """)
                
                logger.info("   ✅ 데이터 마이그레이션 완료")
            
        except Exception as e:
            logger.warning(f"   ⚠️ 데이터 마이그레이션 실패: {e}")
        
        # 5단계: API 매핑 정보 통합
        logger.info("\n5단계: API 매핑 정보 통합")
        
        try:
            # region_api_mappings 테이블 확인
            mapping_check = db_manager.fetch_one("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'region_api_mappings'
                )
            """)
            
            if mapping_check['exists']:
                db_manager.execute_query("""
                    UPDATE regions r
                    SET api_mappings = subquery.mappings,
                        updated_at = CURRENT_TIMESTAMP
                    FROM (
                        SELECT 
                            ur.region_code,
                            jsonb_object_agg(
                                ram.api_provider,
                                jsonb_build_object(
                                    'api_region_code', ram.api_region_code,
                                    'api_region_name', ram.api_region_name,
                                    'additional_codes', ram.additional_codes,
                                    'mapping_confidence', ram.mapping_confidence
                                )
                            ) as mappings
                        FROM unified_regions ur
                        JOIN region_api_mappings ram ON ur.region_id = ram.region_id
                        WHERE ram.is_active = true
                        GROUP BY ur.region_code
                    ) as subquery
                    WHERE r.region_code = subquery.region_code
                """)
                
                logger.info("   ✅ API 매핑 정보 통합 완료")
            else:
                logger.warning("   ⚠️ region_api_mappings 테이블이 없습니다.")
                
        except Exception as e:
            logger.warning(f"   ⚠️ API 매핑 정보 통합 실패: {e}")
        
        # 6단계: 좌표 변환 정보 통합
        logger.info("\n6단계: 좌표 변환 정보 통합")
        
        try:
            # coordinate_transformations 테이블 확인
            coord_check = db_manager.fetch_one("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'coordinate_transformations'
                )
            """)
            
            if coord_check['exists']:
                db_manager.execute_query("""
                    UPDATE regions r
                    SET coordinate_info = subquery.coord_info,
                        updated_at = CURRENT_TIMESTAMP
                    FROM (
                        SELECT 
                            ur.region_code,
                            jsonb_build_object(
                                'kma_grid_nx', ct.kma_grid_nx,
                                'kma_grid_ny', ct.kma_grid_ny,
                                'kma_station_code', ct.kma_station_code,
                                'transform_accuracy', ct.transform_accuracy,
                                'calculation_method', ct.calculation_method,
                                'is_verified', ct.is_verified
                            ) as coord_info
                        FROM unified_regions ur
                        JOIN coordinate_transformations ct ON ur.region_id = ct.region_id
                        WHERE ct.is_verified = true
                    ) as subquery
                    WHERE r.region_code = subquery.region_code
                """)
                
                logger.info("   ✅ 좌표 변환 정보 통합 완료")
            else:
                logger.warning("   ⚠️ coordinate_transformations 테이블이 없습니다.")
                
        except Exception as e:
            logger.warning(f"   ⚠️ 좌표 변환 정보 통합 실패: {e}")
        
        # 7단계: 결과 확인
        logger.info("\n7단계: 마이그레이션 결과 확인")
        
        # regions 테이블 확인
        regions_count = db_manager.fetch_one("SELECT COUNT(*) as count FROM regions")
        logger.info(f"   - regions 테이블 레코드 수: {regions_count['count']}")
        
        # API 매핑 확인
        mapping_count = db_manager.fetch_one("SELECT COUNT(*) as count FROM regions WHERE api_mappings IS NOT NULL")
        logger.info(f"   - API 매핑 정보가 있는 지역 수: {mapping_count['count']}")
        
        # 좌표 정보 확인
        coord_count = db_manager.fetch_one("SELECT COUNT(*) as count FROM regions WHERE coordinate_info IS NOT NULL")
        logger.info(f"   - 좌표 변환 정보가 있는 지역 수: {coord_count['count']}")
        
        logger.info("\n✅ 마이그레이션 완료!")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 마이그레이션 실패: {e}")
        return False


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("UnifiedRegions -> Regions 단계별 마이그레이션")
    logger.info("=" * 50)
    
    # 확인 메시지
    print("\n⚠️  주의사항:")
    print("1. 이 스크립트는 unified_regions 데이터를 regions로 마이그레이션합니다.")
    print("2. 단계별로 실행되므로 오류가 발생해도 계속 진행됩니다.")
    print("3. 기존 테이블은 삭제되지 않습니다.")
    print("\n계속하시겠습니까? (y/N): ", end="")
    
    response = input().strip().lower()
    if response != 'y':
        print("마이그레이션을 취소했습니다.")
        sys.exit(0)
    
    if run_migration():
        print("\n✅ 마이그레이션이 성공적으로 완료되었습니다!")
        print("\n다음 단계:")
        print("1. RegionService 테스트: python scripts/test_region_service.py")
        print("2. 애플리케이션 테스트")
        print("3. 문제가 없으면 기존 테이블 삭제")
    else:
        print("\n❌ 마이그레이션이 실패했습니다. 로그를 확인하세요.")