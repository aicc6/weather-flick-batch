#!/usr/bin/env python3
"""
레저 스포츠 테이블 유니크 제약조건 추가 스크립트
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import SyncDatabaseManager

async def main():
    db_manager = SyncDatabaseManager()
    
    try:
        # 유니크 제약조건 추가
        print("=== 레저 스포츠 테이블 유니크 제약조건 추가 ===")
        
        # 1. 기존 중복 데이터 확인
        duplicate_query = """
        SELECT content_id, COUNT(*) as count 
        FROM leisure_sports 
        GROUP BY content_id 
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10;
        """
        
        duplicates = db_manager.execute_query(duplicate_query)
        print(f"중복 데이터 개수: {len(duplicates)}")
        
        if duplicates:
            print("중복 데이터 샘플:")
            for dup in duplicates:
                print(f"  - content_id: {dup['content_id']}, count: {dup['count']}")
        
        # 2. 중복 데이터 제거 (최신 데이터만 남김)
        if duplicates:
            print("\n=== 중복 데이터 제거 ===")
            remove_duplicates_query = """
            DELETE FROM leisure_sports 
            WHERE id NOT IN (
                SELECT MAX(id) 
                FROM leisure_sports 
                GROUP BY content_id
            );
            """
            
            result = db_manager.execute_query(remove_duplicates_query)
            print(f"중복 데이터 제거 완료")
        
        # 3. 유니크 제약조건 추가
        print("\n=== 유니크 제약조건 추가 ===")
        constraint_query = """
        ALTER TABLE leisure_sports 
        ADD CONSTRAINT leisure_sports_content_id_unique 
        UNIQUE (content_id);
        """
        
        try:
            db_manager.execute_query(constraint_query)
            print("유니크 제약조건 추가 완료")
        except Exception as e:
            if "already exists" in str(e):
                print("유니크 제약조건이 이미 존재합니다.")
            else:
                print(f"유니크 제약조건 추가 실패: {e}")
        
        # 4. 제약조건 확인
        print("\n=== 제약조건 확인 ===")
        constraint_check_query = """
        SELECT constraint_name, constraint_type 
        FROM information_schema.table_constraints 
        WHERE table_name = 'leisure_sports' 
        AND constraint_type = 'UNIQUE';
        """
        
        constraints = db_manager.execute_query(constraint_check_query)
        print(f"유니크 제약조건: {len(constraints)}개")
        for constraint in constraints:
            print(f"  - {constraint['constraint_name']}: {constraint['constraint_type']}")
        
    except Exception as e:
        print(f"오류 발생: {e}")
    finally:
        db_manager.close()

if __name__ == "__main__":
    asyncio.run(main())