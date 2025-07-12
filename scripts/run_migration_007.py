#!/usr/bin/env python3
"""
마이그레이션 007 실행 스크립트
UnifiedRegions를 Regions로 마이그레이션
"""

import os
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
    """마이그레이션 실행"""
    db_manager = get_db_manager()
    
    # 마이그레이션 파일 경로
    migration_file = Path(__file__).parent.parent / "database" / "migrations" / "007_migrate_unified_regions_to_regions.sql"
    
    if not migration_file.exists():
        logger.error(f"마이그레이션 파일이 없습니다: {migration_file}")
        return False
    
    try:
        logger.info("🚀 마이그레이션 시작: UnifiedRegions -> Regions")
        
        # SQL 파일 읽기
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # SQL 문장들을 분리 (세미콜론으로 구분)
        sql_statements = [stmt.strip() for stmt in sql_content.split(';') if stmt.strip()]
        
        # 각 문장 실행
        for i, statement in enumerate(sql_statements, 1):
            if statement:
                try:
                    logger.info(f"실행 중... ({i}/{len(sql_statements)})")
                    db_manager.execute_query(statement)
                except Exception as e:
                    logger.warning(f"문장 {i} 실행 중 오류 (계속 진행): {e}")
        
        logger.info("✅ 마이그레이션 완료!")
        
        # 결과 확인
        logger.info("\n📊 마이그레이션 결과:")
        
        # regions 테이블 확인
        regions_count = db_manager.fetch_one("SELECT COUNT(*) as count FROM regions WHERE is_active = true")
        logger.info(f"- regions 테이블 레코드 수: {regions_count['count']}")
        
        # API 매핑 확인
        mapping_count = db_manager.fetch_one("SELECT COUNT(*) as count FROM regions WHERE api_mappings IS NOT NULL")
        logger.info(f"- API 매핑 정보가 있는 지역 수: {mapping_count['count']}")
        
        # 좌표 정보 확인
        coord_count = db_manager.fetch_one("SELECT COUNT(*) as count FROM regions WHERE coordinate_info IS NOT NULL")
        logger.info(f"- 좌표 변환 정보가 있는 지역 수: {coord_count['count']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 마이그레이션 실패: {e}")
        return False


if __name__ == "__main__":
    logger.info("=" * 50)
    logger.info("UnifiedRegions -> Regions 마이그레이션")
    logger.info("=" * 50)
    
    # 확인 메시지
    print("\n⚠️  주의사항:")
    print("1. 이 스크립트는 unified_regions 데이터를 regions로 마이그레이션합니다.")
    print("2. 백업 테이블이 자동으로 생성됩니다.")
    print("3. 기존 테이블은 삭제되지 않습니다.")
    print("\n계속하시겠습니까? (y/N): ", end="")
    
    response = input().strip().lower()
    if response != 'y':
        print("마이그레이션을 취소했습니다.")
        sys.exit(0)
    
    if run_migration():
        print("\n✅ 마이그레이션이 성공적으로 완료되었습니다!")
        print("\n다음 단계:")
        print("1. 애플리케이션을 테스트하여 정상 작동 확인")
        print("2. 문제가 없으면 기존 테이블 삭제")
        print("   - DROP TABLE unified_regions CASCADE;")
        print("   - DROP TABLE region_api_mappings CASCADE;")
        print("   - DROP TABLE coordinate_transformations CASCADE;")
    else:
        print("\n❌ 마이그레이션이 실패했습니다. 로그를 확인하세요.")