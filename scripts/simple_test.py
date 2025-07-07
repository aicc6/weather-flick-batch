#!/usr/bin/env python3
"""
간단한 테스트 스크립트
"""

import os
import sys
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(override=True)

from app.core.database_manager_extension import get_extended_database_manager

def test_database():
    """데이터베이스 연결 및 테이블 확인"""
    print("🔍 데이터베이스 테스트")
    
    try:
        db_manager = get_extended_database_manager()
        print(f"데이터베이스 매니저 타입: {type(db_manager)}")
        print(f"속성: {dir(db_manager)}")
        
        # restaurants 테이블 확인
        if hasattr(db_manager, 'fetch_one'):
            query = "SELECT COUNT(*) as count FROM restaurants"
            result = db_manager.fetch_one(query)
            print(f"restaurants 테이블 레코드 수: {result['count'] if result else 'N/A'}")
        else:
            print("fetch_one 메서드가 없습니다")
        
        # upsert_restaurant 메서드 확인
        if hasattr(db_manager, 'upsert_restaurant'):
            print("✅ upsert_restaurant 메서드 존재")
        else:
            print("❌ upsert_restaurant 메서드 없음")
            
    except Exception as e:
        print(f"오류: {e}")
        import traceback
        traceback.print_exc()

def test_api_keys():
    """API 키 확인"""
    print("\n🔑 API 키 테스트")
    
    kto_key = os.getenv("KTO_API_KEY")
    if kto_key:
        print(f"KTO_API_KEY: {kto_key[:20]}... (길이: {len(kto_key)})")
        
        # 쉼표로 구분된 여러 키인지 확인
        if "," in kto_key:
            keys = [k.strip() for k in kto_key.split(",")]
            print(f"다중 키 개수: {len(keys)}")
            for i, key in enumerate(keys):
                print(f"  키 #{i}: {key[:20]}...")
        else:
            print("단일 키")
    else:
        print("❌ KTO_API_KEY 환경 변수가 설정되지 않음")

if __name__ == "__main__":
    test_api_keys()
    test_database()