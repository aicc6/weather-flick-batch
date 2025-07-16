import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collectors.unified_kto_client import UnifiedKTOClient

async def main():
    client = UnifiedKTOClient()
    
    # 기본 데이터 수집 (기존 방식)
    print("=== 기본 레저 스포츠 데이터 수집 ===")
    result = await client.collect_all_data(content_types=["28"], include_new_apis=True)
    print("레저스포츠 수집 결과:", result)
    
    # 상세정보 수집 (추가 실행) - 전체 데이터 대상
    print("\n=== 레저 스포츠 상세정보 수집 ===")
    detail_result = await client.collect_detailed_information(
        content_types=["28"],
        max_content_ids=5000,  # 전체 데이터 수집 (충분한 수량)
        store_raw=True,
        auto_transform=True
    )
    print("상세정보 수집 결과:", detail_result)

if __name__ == "__main__":
    asyncio.run(main())
