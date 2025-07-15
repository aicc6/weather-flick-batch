"""
regions 테이블로의 마이그레이션 검증 스크립트

이 스크립트는 다음을 확인합니다:
1. regions 테이블에 모든 데이터가 올바르게 마이그레이션되었는지
2. 새로운 서비스들이 정상 작동하는지
3. 백업 테이블을 정리할 준비가 되었는지
"""

import logging
from datetime import datetime
import os
import sys

# 경로 설정
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.services.region_service import RegionService
from app.core.database_manager import get_db_manager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MigrationVerifier:
    """마이그레이션 검증 클래스"""
    
    def __init__(self):
        self.db_manager = get_db_manager()
        self.region_service = RegionService()
        self.results = {
            'data_integrity': {},
            'service_tests': {},
            'cleanup_readiness': {},
            'errors': []
        }
    
    def verify_data_integrity(self):
        """데이터 무결성 검증"""
        logger.info("=== 데이터 무결성 검증 시작 ===")
        
        try:
            # 1. regions 테이블 총 개수
            regions_count = self.db_manager.fetch_one(
                "SELECT COUNT(*) as count FROM regions"
            )
            self.results['data_integrity']['regions_count'] = regions_count['count']
            
            # 2. api_mappings JSONB 데이터 검증
            invalid_mappings = self.db_manager.fetch_all("""
                SELECT region_code 
                FROM regions 
                WHERE api_mappings IS NOT NULL 
                  AND jsonb_typeof(api_mappings) != 'object'
            """)
            self.results['data_integrity']['invalid_mappings'] = len(invalid_mappings)
            
            # 3. coordinate_info JSONB 데이터 검증
            invalid_coords = self.db_manager.fetch_all("""
                SELECT region_code 
                FROM regions 
                WHERE coordinate_info IS NOT NULL 
                  AND jsonb_typeof(coordinate_info) != 'object'
            """)
            self.results['data_integrity']['invalid_coordinates'] = len(invalid_coords)
            
            # 4. KTO 매핑 확인
            kto_mappings = self.db_manager.fetch_one("""
                SELECT COUNT(*) as count 
                FROM regions 
                WHERE api_mappings ? 'KTO'
            """)
            self.results['data_integrity']['kto_mappings'] = kto_mappings['count']
            
            # 5. KMA 매핑 확인
            kma_mappings = self.db_manager.fetch_one("""
                SELECT COUNT(*) as count 
                FROM regions 
                WHERE api_mappings ? 'KMA'
            """)
            self.results['data_integrity']['kma_mappings'] = kma_mappings['count']
            
            # 6. 좌표 정보 확인
            coords_count = self.db_manager.fetch_one("""
                SELECT COUNT(*) as count 
                FROM regions 
                WHERE coordinate_info IS NOT NULL
            """)
            self.results['data_integrity']['coordinate_info_count'] = coords_count['count']
            
            logger.info(f"✅ 데이터 무결성 검증 완료: {self.results['data_integrity']}")
            
        except Exception as e:
            logger.error(f"❌ 데이터 무결성 검증 실패: {e}")
            self.results['errors'].append(f"Data integrity check failed: {str(e)}")
    
    def test_region_service(self):
        """RegionService 기능 테스트"""
        logger.info("=== RegionService 기능 테스트 시작 ===")
        
        try:
            # 1. 좌표로 지역 조회 테스트
            test_coords = [(37.5665, 126.9780), (35.1796, 129.0756)]  # 서울, 부산
            coord_test_results = []
            
            for lat, lon in test_coords:
                region = self.region_service.get_region_by_coordinates(lat, lon)
                if region:
                    coord_test_results.append({
                        'coords': (lat, lon),
                        'found': True,
                        'region_name': region.get('region_name')
                    })
                else:
                    coord_test_results.append({
                        'coords': (lat, lon),
                        'found': False
                    })
            
            self.results['service_tests']['coordinate_lookup'] = coord_test_results
            
            # 2. API 코드로 지역 조회 테스트
            api_test_results = []
            
            # KTO 테스트
            kto_region = self.region_service.get_region_by_api_code('KTO', '1')
            api_test_results.append({
                'api': 'KTO',
                'code': '1',
                'found': kto_region is not None,
                'region_name': kto_region.get('region_name') if kto_region else None
            })
            
            # KMA 테스트
            kma_region = self.region_service.get_region_by_api_code('KMA', 'seoul')
            api_test_results.append({
                'api': 'KMA',
                'code': 'seoul',
                'found': kma_region is not None,
                'region_name': kma_region.get('region_name') if kma_region else None
            })
            
            self.results['service_tests']['api_lookup'] = api_test_results
            
            # 3. 좌표 변환 테스트
            nx, ny = self.region_service.convert_wgs84_to_kma_grid(37.5665, 126.9780)
            self.results['service_tests']['coordinate_transform'] = {
                'input': (37.5665, 126.9780),
                'output': (nx, ny),
                'expected': (60, 127),
                'match': nx == 60 and ny == 127
            }
            
            # 4. 통계 조회 테스트
            stats = self.region_service.get_region_statistics()
            self.results['service_tests']['statistics'] = stats
            
            logger.info("✅ RegionService 기능 테스트 완료")
            
        except Exception as e:
            logger.error(f"❌ RegionService 기능 테스트 실패: {e}")
            self.results['errors'].append(f"Service test failed: {str(e)}")
    
    def check_cleanup_readiness(self):
        """백업 테이블 정리 준비 상태 확인"""
        logger.info("=== 백업 테이블 정리 준비 상태 확인 ===")
        
        try:
            # 1. 기존 테이블 존재 여부
            old_tables = ['unified_regions', 'region_api_mappings', 'coordinate_transformations']
            table_status = {}
            
            for table in old_tables:
                exists = self.db_manager.fetch_one("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (table,))
                table_status[table] = exists['exists']
            
            self.results['cleanup_readiness']['old_tables'] = table_status
            
            # 2. 백업 테이블 존재 여부
            backup_tables = [
                'unified_regions_backup',
                'region_api_mappings_backup',
                'coordinate_transformations_backup'
            ]
            backup_status = {}
            
            for table in backup_tables:
                exists = self.db_manager.fetch_one("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    )
                """, (table,))
                backup_status[table] = exists['exists']
            
            self.results['cleanup_readiness']['backup_tables'] = backup_status
            
            # 3. 호환성 뷰 존재 여부
            view_exists = self.db_manager.fetch_one("""
                SELECT EXISTS (
                    SELECT FROM information_schema.views 
                    WHERE table_schema = 'public' 
                    AND table_name = 'unified_regions_view'
                )
            """)
            self.results['cleanup_readiness']['compatibility_view'] = view_exists['exists']
            
            # 4. 정리 준비 완료 여부
            ready_for_cleanup = (
                self.results['data_integrity'].get('regions_count', 0) > 0 and
                self.results['data_integrity'].get('invalid_mappings', 1) == 0 and
                self.results['data_integrity'].get('invalid_coordinates', 1) == 0 and
                len(self.results['errors']) == 0
            )
            
            self.results['cleanup_readiness']['ready'] = ready_for_cleanup
            
            logger.info("✅ 백업 테이블 정리 준비 상태 확인 완료")
            
        except Exception as e:
            logger.error(f"❌ 백업 테이블 정리 준비 상태 확인 실패: {e}")
            self.results['errors'].append(f"Cleanup readiness check failed: {str(e)}")
    
    def generate_cleanup_script(self):
        """정리 스크립트 생성"""
        if not self.results['cleanup_readiness'].get('ready', False):
            logger.warning("⚠️ 아직 정리할 준비가 되지 않았습니다.")
            return
        
        cleanup_sql = """
-- =============================================
-- regions 마이그레이션 정리 스크립트
-- 생성일: {timestamp}
-- =============================================

-- 1. 기존 테이블 삭제 (CASCADE로 관련 객체도 함께 삭제)
DROP TABLE IF EXISTS unified_regions CASCADE;
DROP TABLE IF EXISTS region_api_mappings CASCADE;
DROP TABLE IF EXISTS coordinate_transformations CASCADE;

-- 2. 호환성 뷰 삭제
DROP VIEW IF EXISTS unified_regions_view;

-- 3. 백업 테이블 삭제 (필요시 주석 해제)
-- DROP TABLE IF EXISTS unified_regions_backup;
-- DROP TABLE IF EXISTS region_api_mappings_backup;
-- DROP TABLE IF EXISTS coordinate_transformations_backup;

-- 4. 확인
SELECT 'Cleanup completed' as status;
""".format(timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        
        cleanup_file = 'cleanup_old_region_tables.sql'
        with open(cleanup_file, 'w') as f:
            f.write(cleanup_sql)
        
        logger.info(f"✅ 정리 스크립트 생성 완료: {cleanup_file}")
    
    def run_verification(self):
        """전체 검증 실행"""
        logger.info("=== regions 마이그레이션 검증 시작 ===")
        
        # 1. 데이터 무결성 검증
        self.verify_data_integrity()
        
        # 2. 서비스 기능 테스트
        self.test_region_service()
        
        # 3. 정리 준비 상태 확인
        self.check_cleanup_readiness()
        
        # 4. 결과 출력
        self.print_results()
        
        # 5. 정리 준비가 되었으면 스크립트 생성
        if self.results['cleanup_readiness'].get('ready', False):
            self.generate_cleanup_script()
    
    def print_results(self):
        """검증 결과 출력"""
        print("\n" + "="*50)
        print("regions 마이그레이션 검증 결과")
        print("="*50)
        
        # 데이터 무결성
        print("\n[데이터 무결성]")
        for key, value in self.results['data_integrity'].items():
            print(f"  - {key}: {value}")
        
        # 서비스 테스트
        print("\n[서비스 테스트]")
        if 'coordinate_lookup' in self.results['service_tests']:
            print("  - 좌표 조회:")
            for test in self.results['service_tests']['coordinate_lookup']:
                status = "✅" if test['found'] else "❌"
                print(f"    {status} {test['coords']} -> {test.get('region_name', 'Not found')}")
        
        if 'api_lookup' in self.results['service_tests']:
            print("  - API 코드 조회:")
            for test in self.results['service_tests']['api_lookup']:
                status = "✅" if test['found'] else "❌"
                print(f"    {status} {test['api']} {test['code']} -> {test.get('region_name', 'Not found')}")
        
        if 'coordinate_transform' in self.results['service_tests']:
            ct = self.results['service_tests']['coordinate_transform']
            status = "✅" if ct['match'] else "❌"
            print(f"  - 좌표 변환: {status} {ct['input']} -> {ct['output']}")
        
        # 정리 준비 상태
        print("\n[정리 준비 상태]")
        ready = self.results['cleanup_readiness'].get('ready', False)
        print(f"  - 정리 준비 완료: {'✅ 예' if ready else '❌ 아니오'}")
        
        # 오류
        if self.results['errors']:
            print("\n[오류]")
            for error in self.results['errors']:
                print(f"  ❌ {error}")
        
        print("\n" + "="*50)


if __name__ == "__main__":
    verifier = MigrationVerifier()
    verifier.run_verification()