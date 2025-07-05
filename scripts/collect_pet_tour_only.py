#!/usr/bin/env python3
"""
반려동물 동반여행 정보 수집 전용 스크립트

KTO API의 detailPetTour2 엔드포인트를 사용하여 반려동물 동반여행 정보를 수집합니다.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(override=True)

from app.collectors.unified_kto_client import UnifiedKTOClient
from app.core.multi_api_key_manager import get_api_key_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def collect_pet_tour_data():
    """반려동물 동반여행 정보 수집"""
    
    print("\n" + "🐕" + " 반려동물 동반여행 정보 수집 시작 " + "🐕")
    print("시작 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    # API 키 상태 확인
    key_manager = get_api_key_manager()
    try:
        active_key = key_manager.get_active_key("KTO")
        print(f"✅ 사용 가능한 KTO API 키 확인됨")
    except Exception as e:
        print(f"⚠️ API 키 상태 확인 실패: {e}")
    
    client = UnifiedKTOClient()
    
    try:
        # 반려동물 동반여행 정보 수집 실행
        result = await client.collect_pet_tour_data(
            content_ids=None,  # 전체 조회
            store_raw=True,    # 원본 데이터 저장
            auto_transform=True # 자동 변환
        )
        
        print("\n📊 수집 결과:")
        print(f"  - 배치 ID: {result.get('sync_batch_id')}")
        print(f"  - API 엔드포인트: {result.get('api_endpoint')}")
        print(f"  - 원본 레코드 수: {result.get('total_raw_records', 0):,}개")
        print(f"  - 처리된 레코드 수: {result.get('total_processed_records', 0):,}개")
        print(f"  - 상태: {result.get('status')}")
        
        if result.get('errors'):
            print(f"  - 오류 수: {len(result['errors'])}개")
            for error in result['errors'][:3]:  # 처음 3개 오류만 표시
                print(f"    * {error}")
        
        return result
        
    except Exception as e:
        logger.error(f"반려동물 동반여행 정보 수집 실패: {e}")
        print(f"❌ 수집 실패: {e}")
        return None


async def verify_data_collection():
    """수집된 데이터 확인"""
    
    print("\n🔍 수집된 데이터 확인")
    
    try:
        from app.core.database_manager_extension import get_extended_database_manager
        db_manager = get_extended_database_manager()
        
        # pet_tour_info 테이블 조회
        query = "SELECT COUNT(*) as count FROM pet_tour_info"
        result = db_manager.fetch_one(query)
        count = result["count"] if result else 0
        
        print(f"📊 pet_tour_info 테이블 총 레코드 수: {count:,}개")
        
        if count > 0:
            # 최근 데이터 조회
            recent_query = """
            SELECT content_id, title, address, pet_acpt_abl, created_at 
            FROM pet_tour_info 
            ORDER BY created_at DESC 
            LIMIT 5
            """
            recent_results = db_manager.fetch_all(recent_query)
            
            print("📋 최근 저장된 반려동물 동반여행 정보:")
            for row in recent_results:
                print(f"  - {row['title']} (ID: {row['content_id']}, 반려동물 가능: {row['pet_acpt_abl']}, 생성: {row['created_at']})")
        
        return count
        
    except Exception as e:
        logger.error(f"데이터 확인 실패: {e}")
        print(f"❌ 데이터 확인 실패: {e}")
        return 0


async def main():
    """메인 실행 함수"""
    
    start_time = datetime.now()
    
    # 1. 반려동물 동반여행 정보 수집
    collection_result = await collect_pet_tour_data()
    
    if collection_result:
        # 2. 수집된 데이터 확인
        await asyncio.sleep(2)  # 잠시 대기
        final_count = await verify_data_collection()
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        print(f"\n✅ 반려동물 동반여행 정보 수집 완료")
        print(f"소요 시간: {duration}")
        print(f"최종 데이터 수: {final_count:,}개")
    
    else:
        print(f"\n❌ 반려동물 동반여행 정보 수집 실패")


if __name__ == "__main__":
    asyncio.run(main())