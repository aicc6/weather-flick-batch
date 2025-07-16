#!/usr/bin/env python3
"""
문화시설 및 숙박시설 데이터 수집 스크립트
"""

import asyncio
import os
import sys
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collectors.unified_kto_client import get_unified_kto_client
from app.processors.data_transformation_pipeline import get_transformation_pipeline
from app.core.database_manager_extension import get_extended_database_manager

async def collect_cultural_and_accommodations():
    """문화시설 및 숙박시설 데이터 수집"""
    
    print("문화시설 및 숙박시설 데이터 수집 시작...")
    
    # 통합 구조 초기화
    unified_client = get_unified_kto_client()
    transformation_pipeline = get_transformation_pipeline()
    db_manager = get_extended_database_manager()
    
    try:
        # 1. 문화시설 수집 (contentTypeId: 14)
        print("\n=== 문화시설 데이터 수집 시작 ===")
        cultural_result = await unified_client.collect_all_data(
            content_types=["14"],  # 문화시설
            area_codes=None,  # 모든 지역
            store_raw=True,
            auto_transform=True,
            include_new_apis=False,
            include_hierarchical_regions=False
        )
        
        print(f"문화시설 수집 완료: 원본 {cultural_result['total_raw_records']}건, 처리 {cultural_result['total_processed_records']}건")
        
        # 2. 숙박시설 수집 (contentTypeId: 32)
        print("\n=== 숙박시설 데이터 수집 시작 ===")
        accommodation_result = await unified_client.collect_all_data(
            content_types=["32"],  # 숙박시설
            area_codes=None,  # 모든 지역
            store_raw=True,
            auto_transform=True,
            include_new_apis=False,
            include_hierarchical_regions=False
        )
        
        print(f"숙박시설 수집 완료: 원본 {accommodation_result['total_raw_records']}건, 처리 {accommodation_result['total_processed_records']}건")
        
        # 3. 전체 통계
        print("\n=== 수집 완료 ===")
        print(f"문화시설: {cultural_result['total_processed_records']}건")
        print(f"숙박시설: {accommodation_result['total_processed_records']}건")
        print(f"총 {cultural_result['total_processed_records'] + accommodation_result['total_processed_records']}건 수집 완료")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(collect_cultural_and_accommodations())