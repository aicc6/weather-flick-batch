#!/usr/bin/env python3
"""
레저 스포츠 상세정보만 수집하는 스크립트
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collectors.unified_kto_client import UnifiedKTOClient
from app.core.database_manager import SyncDatabaseManager

async def main():
    # 1. 데이터베이스에서 기존 content_id 조회
    db_manager = SyncDatabaseManager()
    
    query = "SELECT content_id FROM leisure_sports WHERE facility_name = '미상' ORDER BY content_id;"
    content_ids = db_manager.fetch_all(query)
    
    print(f"=== 레저 스포츠 상세정보 수집 ({len(content_ids)}개 전체) ===")
    
    # 2. 상세정보 수집 클라이언트 생성
    client = UnifiedKTOClient()
    
    # 3. 각 content_id에 대해 상세정보 수집
    success_count = 0
    fail_count = 0
    
    for i, row in enumerate(content_ids):
        content_id = row['content_id']
        
        # 100건마다 진행상황 출력
        if i % 100 == 0:
            print(f"\n진행상황: {i+1}/{len(content_ids)} ({success_count}건 성공, {fail_count}건 실패)")
        
        # detailCommon2 API 호출
        detail_info = await client.collect_detail_common(content_id, '28', store_raw=False)
        
        if detail_info:
            title = detail_info.get('title', '정보 없음')
            
            # 데이터베이스 업데이트
            if title and title != '정보 없음':
                update_query = """
                UPDATE leisure_sports 
                SET facility_name = %s, 
                    tel = %s, 
                    homepage = %s,
                    overview = %s
                WHERE content_id = %s
                """
                
                params = [
                    title,
                    detail_info.get('tel', ''),
                    detail_info.get('homepage', ''),
                    detail_info.get('overview', ''),
                    content_id
                ]
                
                try:
                    db_manager.execute_update(update_query, params)
                    success_count += 1
                except Exception as e:
                    fail_count += 1
                    if i % 100 == 0:  # 오류는 100건마다만 출력
                        print(f"   업데이트 실패 ({content_id}): {e}")
            else:
                fail_count += 1
        else:
            fail_count += 1
    
    print(f"\n=== 상세정보 수집 완료 ===")
    print(f"전체: {len(content_ids)}건")
    print(f"성공: {success_count}건")
    print(f"실패: {fail_count}건")
    print(f"성공률: {success_count/len(content_ids)*100:.1f}%")

if __name__ == "__main__":
    asyncio.run(main())