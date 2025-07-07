#!/usr/bin/env python3
"""
우선순위 시스템 테스트 스크립트
"""

import os
import sys
import asyncio

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv(override=True)

from app.core.data_priority_manager import get_priority_manager


def test_priority_manager():
    """우선순위 매니저 테스트"""
    print("🧪 우선순위 시스템 테스트")
    print("=" * 50)
    
    priority_manager = get_priority_manager()
    
    try:
        # 1. 현재 데이터 수 조회 테스트
        print("1. 현재 데이터 수 조회 테스트:")
        data_counts = priority_manager.get_current_data_counts()
        for content_type, count in data_counts.items():
            name = priority_manager.content_type_names.get(content_type, f"타입{content_type}")
            print(f"   {name} (타입 {content_type}): {count:,}개")
        
        # 2. 우선순위 정렬 테스트
        print(f"\n2. 우선순위 정렬 테스트 (데이터 부족 순):")
        priority_list = priority_manager.get_priority_sorted_content_types()
        for rank, (content_type, count, name) in enumerate(priority_list, 1):
            urgency = "🔥" if count == 0 else "⚠️" if count < 1000 else "✅"
            print(f"   {rank}. {name}: {count:,}개 {urgency}")
        
        # 3. 지역별 우선순위 테스트 (음식점)
        print(f"\n3. 음식점 지역별 우선순위 테스트:")
        area_priorities = priority_manager.get_area_priority_by_content_type("39")
        if area_priorities:
            print(f"   지역별 음식점 데이터 (상위 5개):")
            for area_code, count in area_priorities[:5]:
                area_name = priority_manager._get_area_name(area_code)
                print(f"     - {area_name}: {count:,}개")
        else:
            print(f"   ⚠️ 음식점 지역별 데이터 없음")
        
        # 4. 수집 계획 제안 테스트
        print(f"\n4. 수집 계획 제안 테스트:")
        collection_plan = priority_manager.get_recommended_collection_order(max_per_type=3)
        print(f"   총 컨텐츠 타입: {collection_plan['total_content_types']}개")
        print(f"   상위 3개 우선순위:")
        for item in collection_plan['priority_order'][:3]:
            print(f"     {item['rank']}. {item['name']}: {item['current_count']:,}개 ({item['priority_reason']})")
        
        print(f"\n✅ 우선순위 시스템 테스트 완료")
        
    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")
        import traceback
        print(f"스택 트레이스:\n{traceback.format_exc()}")


if __name__ == "__main__":
    test_priority_manager()