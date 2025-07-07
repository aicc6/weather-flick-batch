#!/usr/bin/env python3
"""
weather_regions 테이블 외래키 제약조건 수정 스크립트

기상청 예보구역 코드와 기존 지역 코드 형식이 다르므로
외래키 제약조건을 제거하여 독립적으로 저장할 수 있도록 합니다.
"""

import sys
import os
import logging

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import DatabaseManager
from app.core.database_manager_extension import extend_database_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fix_weather_regions_constraints():
    """weather_regions 테이블 제약조건 수정"""
    
    db_manager = extend_database_manager(DatabaseManager().sync_manager)
    
    try:
        logger.info("weather_regions 테이블 외래키 제약조건 수정 시작")
        
        # 1. 기존 외래키 제약조건 제거
        logger.info("기존 외래키 제약조건 제거...")
        drop_fk_query = """
        ALTER TABLE weather_regions 
        DROP CONSTRAINT IF EXISTS weather_regions_region_code_fkey
        """
        
        db_manager.execute_update(drop_fk_query)
        logger.info("외래키 제약조건 제거 완료")
        
        # 2. region_code 컬럼을 NULL 허용으로 변경 (선택사항)
        logger.info("region_code 컬럼 제약조건 완화...")
        alter_column_query = """
        ALTER TABLE weather_regions 
        ALTER COLUMN region_code DROP NOT NULL
        """
        
        try:
            db_manager.execute_update(alter_column_query)
            logger.info("region_code 컬럼 NULL 허용으로 변경 완료")
        except Exception as e:
            logger.warning(f"컬럼 제약조건 변경 실패 (무시하고 계속): {e}")
        
        # 3. 새로운 인덱스 추가 (성능 향상)
        logger.info("성능 향상을 위한 인덱스 추가...")
        index_queries = [
            """
            CREATE INDEX IF NOT EXISTS idx_weather_regions_region_code 
            ON weather_regions(region_code)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_weather_regions_coordinates 
            ON weather_regions(latitude, longitude)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_weather_regions_grid 
            ON weather_regions(grid_x, grid_y)
            """,
            """
            CREATE INDEX IF NOT EXISTS idx_weather_regions_active 
            ON weather_regions(is_active) WHERE is_active = true
            """
        ]
        
        for query in index_queries:
            try:
                db_manager.execute_update(query)
                logger.info("인덱스 추가 완료")
            except Exception as e:
                logger.warning(f"인덱스 추가 실패 (무시하고 계속): {e}")
        
        # 4. 테이블 구조 확인
        logger.info("수정된 테이블 구조 확인...")
        check_query = """
        SELECT 
            column_name, 
            data_type, 
            is_nullable, 
            column_default
        FROM information_schema.columns 
        WHERE table_name = 'weather_regions' 
        ORDER BY ordinal_position
        """
        
        columns = db_manager.fetch_all(check_query)
        
        print("\n✅ weather_regions 테이블 수정 완료")
        print("📋 현재 테이블 구조:")
        for col in columns:
            print(f"  - {col.get('column_name')} | {col.get('data_type')} | "
                  f"NULL: {col.get('is_nullable')} | Default: {col.get('column_default')}")
        
        # 5. 외래키 제약조건 확인
        fk_query = """
        SELECT constraint_name
        FROM information_schema.table_constraints 
        WHERE table_name = 'weather_regions' 
            AND constraint_type = 'FOREIGN KEY'
        """
        
        fk_constraints = db_manager.fetch_all(fk_query)
        
        if fk_constraints:
            print("\n⚠️  남은 외래키 제약조건:")
            for fk in fk_constraints:
                print(f"  - {fk.get('constraint_name')}")
        else:
            print("\n✅ 외래키 제약조건이 모두 제거되었습니다.")
        
        logger.info("weather_regions 테이블 수정 작업 완료")
        
    except Exception as e:
        logger.error(f"테이블 수정 실패: {e}")
        raise


if __name__ == "__main__":
    fix_weather_regions_constraints()