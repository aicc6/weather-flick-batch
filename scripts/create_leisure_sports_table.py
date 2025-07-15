#!/usr/bin/env python3
"""
leisure_sports 테이블 생성 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import DatabaseManager
from app.core.logger import get_logger

logger = get_logger(__name__)

def create_leisure_sports_table():
    """leisure_sports 테이블 생성"""
    
    db_manager = DatabaseManager()
    
    # SQL 파일 읽기
    sql_file_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'database/migrations/012_create_leisure_sports_table.sql'
    )
    
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    try:
        # SQL 실행
        db_manager.execute_update(sql_script, ())
        print("✅ leisure_sports 테이블이 성공적으로 생성되었습니다.")
        
        # 테이블 확인
        check_query = """
        SELECT 
            column_name, 
            data_type, 
            is_nullable 
        FROM information_schema.columns 
        WHERE table_name = 'leisure_sports'
        ORDER BY ordinal_position
        LIMIT 10;
        """
        
        columns = db_manager.execute_query(check_query, ())
        print("\n생성된 테이블 컬럼 (처음 10개):")
        for col in columns:
            print(f"  - {col['column_name']}: {col['data_type']} ({'NULL 가능' if col['is_nullable'] == 'YES' else 'NOT NULL'})")
            
    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_leisure_sports_table()