#!/usr/bin/env python3
"""
데이터베이스 스키마 확인 스크립트

테이블 구조와 제약조건을 확인합니다.
"""

import sys
import os

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import DatabaseManager
from app.core.database_manager_extension import extend_database_manager


def check_database_schema():
    """데이터베이스 스키마 확인"""
    
    db_manager = extend_database_manager(DatabaseManager().sync_manager)
    
    print("🔍 데이터베이스 스키마 확인\n")
    
    # 테이블 목록 조회
    tables_query = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
    ORDER BY table_name
    """
    
    tables = db_manager.fetch_all(tables_query)
    print(f"📋 전체 테이블 목록 ({len(tables)}개):")
    for table in tables:
        print(f"  - {table.get('table_name')}")
    
    print()
    
    # 지역 관련 테이블들 상세 확인
    region_tables = ['regions', 'weather_regions', 'unified_regions', 'legal_dong_codes']
    
    for table_name in region_tables:
        try:
            # 테이블 스키마 확인
            schema_query = f"""
            SELECT 
                column_name, 
                data_type, 
                is_nullable, 
                column_default
            FROM information_schema.columns 
            WHERE table_name = '{table_name}' 
            ORDER BY ordinal_position
            """
            
            columns = db_manager.fetch_all(schema_query)
            
            if columns:
                print(f"🏗️  {table_name} 테이블 스키마:")
                for col in columns:
                    print(f"  - {col.get('column_name')} | {col.get('data_type')} | "
                          f"NULL: {col.get('is_nullable')} | Default: {col.get('column_default')}")
                
                # 데이터 개수 확인
                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                count_result = db_manager.fetch_one(count_query)
                count = count_result.get('count', 0) if count_result else 0
                print(f"  📊 데이터 개수: {count}개")
                
                # 샘플 데이터 5개 확인
                if count > 0:
                    sample_query = f"SELECT * FROM {table_name} LIMIT 5"
                    samples = db_manager.fetch_all(sample_query)
                    print(f"  🔖 샘플 데이터:")
                    for i, sample in enumerate(samples, 1):
                        print(f"    {i}. {dict(sample)}")
                
                print()
            else:
                print(f"❌ {table_name} 테이블이 존재하지 않습니다.\n")
                
        except Exception as e:
            print(f"❌ {table_name} 테이블 조회 실패: {e}\n")
    
    # 외래키 제약조건 확인
    print("🔗 외래키 제약조건 확인:")
    fk_query = """
    SELECT 
        tc.table_name,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name,
        tc.constraint_name
    FROM information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
        ON ccu.constraint_name = tc.constraint_name
        AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name IN ('regions', 'weather_regions', 'unified_regions')
    ORDER BY tc.table_name, kcu.column_name
    """
    
    try:
        fk_constraints = db_manager.fetch_all(fk_query)
        
        if fk_constraints:
            for fk in fk_constraints:
                print(f"  - {fk.get('table_name')}.{fk.get('column_name')} → "
                      f"{fk.get('foreign_table_name')}.{fk.get('foreign_column_name')} "
                      f"({fk.get('constraint_name')})")
        else:
            print("  외래키 제약조건이 없습니다.")
            
    except Exception as e:
        print(f"  외래키 조회 실패: {e}")


if __name__ == "__main__":
    check_database_schema()