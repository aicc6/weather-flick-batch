#!/usr/bin/env python3
"""
음식점 UPSERT 문제 테스트 스크립트
"""

import os
import sys
import logging

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv(override=True)

from app.core.database_manager_extension import get_extended_database_manager

# 로깅 레벨 설정
logging.basicConfig(level=logging.DEBUG)

def test_restaurant_upsert():
    """음식점 UPSERT 테스트"""
    print("🍽️ 음식점 UPSERT 테스트 시작")
    
    db_manager = get_extended_database_manager()
    
    # 테스트 데이터
    test_data = {
        "content_id": "test_001",
        "region_code": "1",
        "restaurant_name": "테스트 음식점",
        "address": "서울시 테스트구",
        "addr2": "테스트동 123-456",
        "latitude": 37.5665,
        "longitude": 126.9780,
        "first_image": "http://test.com/image1.jpg",
        "thumbnail_url": "http://test.com/thumb1.jpg",
        "phone_number": "02-123-4567",
        "category_large_code": "A05",
        "category_medium_code": "A0502",
        "sigungu_code": "1",
        "description": "테스트 음식점입니다",
        "homepage_url": "http://test.com",
        "booktour": "N",
        "createdtime": "20250101120000",
        "modifiedtime": "20250101120000",
        "telname": "대표번호",
        "faxno": "02-123-4568",
        "zipcode": "12345",
        "mlevel": 6,
        "detail_intro_info": {"test": "intro"},
        "detail_additional_info": {"test": "additional"},
        "raw_data_id": None,  # NULL로 설정하여 외래 키 제약조건 우회
        "last_sync_at": "2025-01-01 12:00:00",
        "data_quality_score": 95.5
    }
    
    try:
        result = db_manager.upsert_restaurant(test_data)
        print(f"✅ UPSERT 결과: {result}")
    except Exception as e:
        print(f"❌ UPSERT 실패: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_restaurant_upsert()