#!/usr/bin/env python3
"""
레저 스포츠 데이터 삭제 및 재수집 준비 스크립트
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import SyncDatabaseManager

def main():
    db_manager = SyncDatabaseManager()
    
    try:
        print("=== 레저 스포츠 데이터 삭제 및 재수집 준비 ===")
        
        # 1. 기존 레저 스포츠 데이터 개수 확인
        print("1. 기존 데이터 개수 확인...")
        count_query = "SELECT COUNT(*) as count FROM leisure_sports;"
        result = db_manager.fetch_one(count_query)
        print(f"삭제 전 레저 스포츠 데이터: {result['count']}건")
        
        # 2. 레저 스포츠 데이터 삭제
        print("2. 기존 레저 스포츠 데이터 삭제...")
        delete_query = "DELETE FROM leisure_sports;"
        db_manager.execute_update(delete_query)
        print("레저 스포츠 데이터 삭제 완료")
        
        # 3. 삭제 후 확인
        print("3. 삭제 후 확인...")
        result = db_manager.fetch_one(count_query)
        print(f"삭제 후 레저 스포츠 데이터: {result['count']}건")
        
        # 4. 관련 raw_data 정리 (선택사항)
        print("4. 관련 API raw_data 정리...")
        raw_data_query = """
        DELETE FROM api_raw_data 
        WHERE endpoint = 'areaBasedList2' 
        AND request_params->>'contentTypeId' = '28'
        AND created_at < NOW() - INTERVAL '1 day';
        """
        db_manager.execute_update(raw_data_query)
        print("이전 raw_data 정리 완료")
        
        print("\n=== 레저 스포츠 데이터 재수집 준비 완료 ===")
        print("이제 collect_leisure_sports.py를 실행하여 상세정보와 함께 재수집하세요.")
        
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()