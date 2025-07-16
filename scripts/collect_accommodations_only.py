#!/usr/bin/env python3
"""
숙박시설 데이터만 수집하는 스크립트
"""

import asyncio
import sys
import os

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from app.collectors.unified_kto_client import get_unified_kto_client
from app.core.logger import get_logger

logger = get_logger(__name__)

async def collect_accommodations():
    """숙박시설 데이터만 수집"""
    try:
        # 통합 KTO 클라이언트 초기화
        client = get_unified_kto_client()
        
        logger.info("숙박시설 데이터 수집 시작")
        
        # 숙박시설 데이터만 수집 (contenttypeid=32)
        result = await client.collect_all_data(
            content_types=["32"],  # 숙박시설만
            area_codes=None,  # 모든 지역
            store_raw=True,
            auto_transform=True,
            include_new_apis=False,  # 기본 API만 사용
            include_hierarchical_regions=False
        )
        
        logger.info(f"숙박시설 데이터 수집 완료: 원본 {result.get('total_raw_records', 0)}건, 처리 {result.get('total_processed_records', 0)}건")
        
        return True
        
    except Exception as e:
        logger.error(f"숙박시설 데이터 수집 중 오류: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """메인 실행 함수"""
    success = await collect_accommodations()
    
    if success:
        print("숙박시설 데이터 수집이 성공적으로 완료되었습니다.")
    else:
        print("숙박시설 데이터 수집 중 오류가 발생했습니다.")

if __name__ == "__main__":
    asyncio.run(main())