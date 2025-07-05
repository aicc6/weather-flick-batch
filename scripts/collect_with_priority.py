#!/usr/bin/env python3
"""
데이터 부족 순 우선순위 기반 수집 스크립트

현재 데이터베이스의 수집 현황을 분석하여 
데이터가 적은 컨텐츠 타입부터 우선적으로 수집합니다.
"""

import os
import sys
import asyncio
from datetime import datetime

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv(override=True)

# API 키 매니저 리셋
from app.core.multi_api_key_manager import reset_api_key_manager
from app.core.unified_api_client import reset_unified_api_client

reset_api_key_manager()
reset_unified_api_client()

from app.collectors.unified_kto_client import get_unified_kto_client
from app.core.data_priority_manager import get_priority_manager


async def analyze_current_priority():
    """현재 데이터 우선순위 분석"""
    print("📊 현재 데이터 수집 우선순위 분석")
    print("=" * 60)
    
    priority_manager = get_priority_manager()
    
    # 우선순위 분석 출력
    priority_manager.print_priority_analysis()
    
    # 수집 계획 제안
    collection_plan = priority_manager.get_recommended_collection_order(max_per_type=3)
    
    print(f"\n💡 권장 수집 계획:")
    print(f"분석 시간: {collection_plan['analysis_time']}")
    print(f"대상 컨텐츠 타입: {collection_plan['total_content_types']}개")
    
    print(f"\n🎯 상위 5개 우선순위:")
    for item in collection_plan['priority_order'][:5]:
        rank = item['rank']
        name = item['name']
        content_type = item['content_type']
        current_count = item['current_count']
        reason = item['priority_reason']
        area_count = item['total_recommended_areas']
        
        urgency_icon = "🔥" if current_count == 0 else "⚠️" if current_count < 1000 else "✅"
        print(f"  {rank}. {name} (타입 {content_type}): {current_count:,}개 {urgency_icon}")
        print(f"     이유: {reason}, 권장 지역: {area_count}개")
    
    return collection_plan


async def collect_priority_based_data(max_content_types: int = 3, 
                                    max_areas_per_type: int = 2):
    """우선순위 기반 데이터 수집"""
    print(f"\n🚀 우선순위 기반 데이터 수집 시작")
    print(f"최대 컨텐츠 타입: {max_content_types}개")
    print(f"타입당 최대 지역: {max_areas_per_type}개")
    print("=" * 60)
    
    priority_manager = get_priority_manager()
    client = get_unified_kto_client()
    
    # 우선순위 목록 조회
    priority_list = priority_manager.get_priority_sorted_content_types()
    
    # 상위 N개 컨텐츠 타입만 선택
    target_content_types = [item[0] for item in priority_list[:max_content_types]]
    
    print(f"🎯 수집 대상 컨텐츠 타입:")
    for i, (content_type, count, name) in enumerate(priority_list[:max_content_types], 1):
        urgency = "🔥 긴급" if count == 0 else "⚠️ 부족" if count < 1000 else "✅ 보통"
        print(f"  {i}. {name} (타입 {content_type}): {count:,}개 - {urgency}")
    
    # 각 컨텐츠 타입별로 순차 수집
    total_results = {}
    
    for content_type, current_count, name in priority_list[:max_content_types]:
        print(f"\n📋 {name} (타입 {content_type}) 수집 시작...")
        
        # 해당 컨텐츠 타입의 지역별 우선순위 조회
        area_priorities = priority_manager.get_area_priority_by_content_type(content_type)
        
        if area_priorities:
            # 상위 N개 지역만 선택 (데이터가 적은 지역 우선)
            target_areas = [str(area_code) for area_code, count in area_priorities[:max_areas_per_type]]
            print(f"  대상 지역: {target_areas}")
        else:
            # 지역 정보가 없으면 주요 지역만 수집
            target_areas = ["1", "6", "31", "39"]  # 서울, 부산, 경기, 제주
            print(f"  기본 지역 사용: {target_areas}")
        
        try:
            # 단일 컨텐츠 타입으로 수집
            result = await client.collect_all_data(
                content_types=[content_type],
                area_codes=target_areas,
                store_raw=True,
                auto_transform=True,
                include_new_apis=False,
                include_hierarchical_regions=False,
                use_priority_sorting=False  # 이미 우선순위가 적용됨
            )
            
            total_results[content_type] = result
            
            # 결과 요약
            if result.get("status") == "completed":
                content_data = result.get('content_types_collected', {}).get(content_type, {})
                raw_count = content_data.get('total_raw_records', 0)
                processed_count = content_data.get('total_processed_records', 0)
                
                print(f"  ✅ {name} 수집 완료: 원본 {raw_count}개, 처리 {processed_count}개")
            elif result.get("status") == "skipped":
                print(f"  ⏭️ {name} 수집 건너뜀: {result.get('reason', 'Unknown')}")
            else:
                print(f"  ❌ {name} 수집 실패")
                
        except Exception as e:
            print(f"  ❌ {name} 수집 중 예외 발생: {e}")
            total_results[content_type] = {"status": "error", "error": str(e)}
        
        # 다음 컨텐츠 타입 수집 전 잠시 대기 (API 제한 방지)
        await asyncio.sleep(2)
    
    return total_results


async def collect_single_content_type_priority():
    """단일 컨텐츠 타입 우선순위 수집"""
    print(f"\n🎯 단일 컨텐츠 타입 우선순위 수집")
    print("=" * 60)
    
    priority_manager = get_priority_manager()
    
    # 우선순위 목록 조회
    priority_list = priority_manager.get_priority_sorted_content_types()
    
    print(f"최우선 컨텐츠 타입 선택:")
    for i, (content_type, count, name) in enumerate(priority_list[:5], 1):
        urgency = "🔥" if count == 0 else "⚠️" if count < 1000 else "✅"
        print(f"  {i}. {name} (타입 {content_type}): {count:,}개 {urgency}")
    
    try:
        choice = int(input(f"\n선택 (1-5): ").strip())
        if 1 <= choice <= 5:
            selected = priority_list[choice - 1]
            content_type, current_count, name = selected
            
            print(f"\n📋 선택된 컨텐츠: {name} (타입 {content_type})")
            print(f"현재 데이터: {current_count:,}개")
            
            # 지역 선택
            print(f"\n지역 선택:")
            print(f"1. 전국 모든 지역 (17개)")
            print(f"2. 주요 지역만 (서울, 부산, 경기, 제주)")
            print(f"3. 데이터 부족 지역 우선 (상위 5개)")
            
            area_choice = input(f"선택 (1-3): ").strip()
            
            if area_choice == "1":
                area_codes = None  # 전국
                print(f"전국 모든 지역에서 수집")
            elif area_choice == "2":
                area_codes = ["1", "6", "31", "39"]
                print(f"주요 지역에서 수집: 서울, 부산, 경기, 제주")
            elif area_choice == "3":
                area_priorities = priority_manager.get_area_priority_by_content_type(content_type)
                if area_priorities:
                    area_codes = [str(area_code) for area_code, count in area_priorities[:5]]
                    print(f"데이터 부족 지역 우선 수집: {area_codes}")
                else:
                    area_codes = ["1", "6", "31", "39"]
                    print(f"지역 정보 없음, 주요 지역 사용: {area_codes}")
            else:
                print(f"잘못된 선택, 주요 지역 사용")
                area_codes = ["1", "6", "31", "39"]
            
            # 수집 실행
            client = get_unified_kto_client()
            
            result = await client.collect_all_data(
                content_types=[content_type],
                area_codes=area_codes,
                store_raw=True,
                auto_transform=True,
                include_new_apis=False,
                include_hierarchical_regions=False,
                use_priority_sorting=False
            )
            
            # 결과 출력
            print_collection_result(result, name)
            
        else:
            print(f"잘못된 선택입니다.")
            
    except ValueError:
        print(f"숫자를 입력해주세요.")
    except KeyboardInterrupt:
        print(f"\n사용자가 취소했습니다.")


def print_collection_result(result: dict, content_name: str):
    """수집 결과 출력"""
    print(f"\n📊 {content_name} 수집 결과:")
    print("=" * 40)
    
    if result.get("status") == "completed":
        print(f"✅ 수집 성공")
        print(f"배치 ID: {result.get('sync_batch_id', 'N/A')}")
        print(f"시작 시간: {result.get('started_at', 'N/A')}")
        print(f"완료 시간: {result.get('completed_at', 'N/A')}")
        
        content_data = list(result.get('content_types_collected', {}).values())
        if content_data:
            data = content_data[0]
            print(f"원본 레코드: {data.get('total_raw_records', 0):,}개")
            print(f"처리된 레코드: {data.get('total_processed_records', 0):,}개")
            
            area_results = data.get('area_results', {})
            if area_results:
                print(f"\n지역별 수집 현황:")
                for area_code, area_data in area_results.items():
                    area_name = get_area_name(area_code)
                    raw_count = area_data.get('total_raw_records', 0)
                    processed_count = area_data.get('total_processed_records', 0)
                    print(f"  - {area_name}: 원본 {raw_count}개, 처리 {processed_count}개")
        
    elif result.get("status") == "skipped":
        print(f"⏭️ 수집 건너뜀")
        print(f"이유: {result.get('reason', 'Unknown')}")
        if result.get('next_retry_time'):
            print(f"다음 재시도: {result.get('next_retry_time')}")
            
    else:
        print(f"❌ 수집 실패")
        errors = result.get('errors', [])
        if errors:
            print(f"오류:")
            for error in errors[:3]:
                print(f"  - {error}")


def get_area_name(area_code: str) -> str:
    """지역 코드에서 지역명 반환"""
    area_names = {
        "1": "서울특별시", "2": "인천광역시", "3": "대전광역시", "4": "대구광역시",
        "5": "광주광역시", "6": "부산광역시", "7": "울산광역시", "8": "세종특별자치시",
        "31": "경기도", "32": "강원특별자치도", "33": "충청북도", "34": "충청남도", 
        "35": "경상북도", "36": "경상남도", "37": "전북특별자치도", "38": "전라남도", "39": "제주도"
    }
    return area_names.get(str(area_code), f"지역{area_code}")


async def main():
    """메인 함수"""
    print("🎯 데이터 우선순위 기반 수집 도구")
    print("=" * 60)
    print(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    while True:
        print("\n수집 옵션을 선택하세요:")
        print("1. 현재 데이터 우선순위 분석만 수행")
        print("2. 우선순위 기반 자동 수집 (상위 3개 타입)")
        print("3. 단일 컨텐츠 타입 선택 수집")
        print("4. 우선순위 적용한 전체 수집")
        print("0. 종료")
        
        choice = input("\n선택: ").strip()
        
        if choice == "0":
            print("👋 프로그램을 종료합니다.")
            break
            
        elif choice == "1":
            await analyze_current_priority()
            
        elif choice == "2":
            await analyze_current_priority()
            confirm = input("\n위 분석을 바탕으로 수집을 시작하시겠습니까? (y/N): ").strip().lower()
            if confirm == 'y':
                await collect_priority_based_data(max_content_types=3, max_areas_per_type=2)
            
        elif choice == "3":
            await collect_single_content_type_priority()
            
        elif choice == "4":
            await analyze_current_priority()
            confirm = input("\n우선순위를 적용한 전체 수집을 시작하시겠습니까? (y/N): ").strip().lower()
            if confirm == 'y':
                client = get_unified_kto_client()
                result = await client.collect_all_data(
                    content_types=None,  # 전체
                    area_codes=None,     # 전국
                    store_raw=True,
                    auto_transform=True,
                    include_new_apis=False,
                    include_hierarchical_regions=False,
                    use_priority_sorting=True  # 우선순위 정렬 활성화
                )
                print_collection_result(result, "전체 컨텐츠")
            
        else:
            print("⚠️ 올바른 선택지를 입력해주세요.")


if __name__ == "__main__":
    asyncio.run(main())