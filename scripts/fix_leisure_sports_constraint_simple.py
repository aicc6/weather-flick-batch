#!/usr/bin/env python3
"""
레저 스포츠 테이블 유니크 제약조건 추가 스크립트 (단순 버전)
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import SyncDatabaseManager

def main():
    db_manager = SyncDatabaseManager()
    
    try:
        # DDL 명령어 실행
        print("=== 레저 스포츠 테이블 유니크 제약조건 추가 ===")
        
        # 1. 중복 데이터 제거
        print("1. 중복 데이터 제거...")
        remove_duplicates_sql = """
        DELETE FROM leisure_sports 
        WHERE id NOT IN (
            SELECT MAX(id) 
            FROM leisure_sports 
            GROUP BY content_id
        );
        """
        
        db_manager.execute_ddl(remove_duplicates_sql)
        print("중복 데이터 제거 완료")
        
        # 2. 유니크 제약조건 추가
        print("2. 유니크 제약조건 추가...")
        constraint_sql = """
        ALTER TABLE leisure_sports 
        ADD CONSTRAINT leisure_sports_content_id_unique 
        UNIQUE (content_id);
        """
        
        db_manager.execute_ddl(constraint_sql)
        print("유니크 제약조건 추가 완료")
        
    except Exception as e:
        if "already exists" in str(e):
            print("유니크 제약조건이 이미 존재합니다.")
        else:
            print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()