#!/usr/bin/env python3
"""
단일 API 호출 테스트 스크립트
음식점 데이터 수집 시 발생하는 정확한 오류를 확인합니다.
"""

import os
import sys
import asyncio
import json
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

from app.core.unified_api_client import get_unified_api_client, APIProvider
from app.core.multi_api_key_manager import get_api_key_manager


async def test_single_restaurant_call():
    """단일 음식점 API 호출 테스트"""
    print("🧪 단일 음식점 API 호출 테스트")
    print("=" * 50)
    
    # 키 매니저 초기화
    key_manager = get_api_key_manager()
    
    # 기본 파라미터 설정
    params = {
        "MobileOS": "ETC",
        "MobileApp": "WeatherFlick", 
        "_type": "json",
        "contentTypeId": "39",  # 음식점
        "areaCode": "1",        # 서울
        "numOfRows": "10",      # 적은 수로 테스트
        "pageNo": "1"
    }
    
    print(f"테스트 파라미터:")
    for key, value in params.items():
        print(f"  {key}: {value}")
    
    try:
        print(f"\n🔑 API 키 상태 확인:")
        active_key = key_manager.get_active_key(APIProvider.KTO)
        if active_key:
            print(f"  사용할 키: {active_key.key[:10]}...")
            print(f"  현재 사용량: {active_key.current_usage}/{active_key.daily_limit}")
            print(f"  활성 상태: {active_key.is_active}")
        else:
            print(f"  ❌ 사용 가능한 키가 없습니다!")
            return
        
        print(f"\n📞 API 호출 시작...")
        
        # async with 구문으로 API 클라이언트 사용
        api_client = get_unified_api_client()
        async with api_client:
            response = await api_client.call_api(
                api_provider=APIProvider.KTO,
                endpoint="areaBasedList2",
                params=params,
                store_raw=False  # 테스트이므로 저장하지 않음
            )
        
        print(f"\n📊 응답 결과:")
        print(f"  성공 여부: {response.success}")
        print(f"  응답 상태: {response.response_status}")
        print(f"  오류 메시지: {response.error}")
        print(f"  처리 시간: {response.duration_ms}ms")
        
        if response.data:
            print(f"  응답 데이터 구조:")
            if isinstance(response.data, dict):
                print(f"    - 키 목록: {list(response.data.keys())}")
                
                # response 구조 확인
                if 'response' in response.data:
                    resp = response.data['response']
                    print(f"    - response 키: {list(resp.keys()) if isinstance(resp, dict) else type(resp)}")
                    
                    if isinstance(resp, dict) and 'body' in resp:
                        body = resp['body']
                        print(f"    - body 키: {list(body.keys()) if isinstance(body, dict) else type(body)}")
                        
                        if isinstance(body, dict) and 'items' in body:
                            items = body['items']
                            print(f"    - items 타입: {type(items)}")
                            
                            if isinstance(items, dict) and 'item' in items:
                                item_list = items['item']
                                print(f"    - item 타입: {type(item_list)}")
                                if isinstance(item_list, list):
                                    print(f"    - 아이템 개수: {len(item_list)}")
                                    if len(item_list) > 0:
                                        print(f"    - 첫 번째 아이템 키: {list(item_list[0].keys()) if isinstance(item_list[0], dict) else 'Not dict'}")
            else:
                print(f"    - 데이터 타입: {type(response.data)}")
                print(f"    - 데이터 미리보기: {str(response.data)[:200]}...")
        else:
            print(f"  응답 데이터: None")
        
        # raw_response 속성은 없으므로 제거
            
        return response.success
        
    except Exception as e:
        print(f"❌ API 호출 중 예외 발생: {e}")
        print(f"예외 타입: {type(e)}")
        import traceback
        print(f"스택 트레이스:\n{traceback.format_exc()}")
        return False


async def test_multiple_regions():
    """여러 지역 순차 테스트"""
    print(f"\n🗺️ 여러 지역 순차 테스트")
    print("=" * 50)
    
    regions = {
        "1": "서울특별시",
        "6": "부산광역시", 
        "31": "경기도",
        "39": "제주도"
    }
    
    for area_code, area_name in regions.items():
        print(f"\n📍 {area_name} ({area_code}) 테스트:")
        
        params = {
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json", 
            "contentTypeId": "39",  # 음식점
            "areaCode": area_code,
            "numOfRows": "5",       # 매우 적은 수
            "pageNo": "1"
        }
        
        try:
            api_client = get_unified_api_client()
            async with api_client:
                response = await api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="areaBasedList2",
                    params=params,
                    store_raw=False
                )
            
            success_icon = "✅" if response.success else "❌"
            print(f"  {success_icon} 결과: {response.success}")
            
            if not response.success:
                print(f"    오류: {response.error}")
                print(f"    응답 상태: {response.response_status}")
            else:
                # 데이터 개수 확인
                if response.data and isinstance(response.data, dict):
                    items = response.data.get('response', {}).get('body', {}).get('items', {})
                    if isinstance(items, dict) and 'item' in items:
                        item_list = items['item']
                        if isinstance(item_list, list):
                            print(f"    음식점 개수: {len(item_list)}개")
                        else:
                            print(f"    음식점 개수: 1개 (단일 객체)")
                    else:
                        print(f"    아이템 없음")
            
        except Exception as e:
            print(f"  ❌ 예외 발생: {e}")
        
        # 잠시 대기 (API 제한 방지)
        await asyncio.sleep(1)


async def main():
    """메인 함수"""
    print("🔍 음식점 API 호출 디버깅")
    print("=" * 60)
    print(f"테스트 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. 단일 API 호출 테스트
    success = await test_single_restaurant_call()
    
    if success:
        # 2. 여러 지역 테스트 (성공한 경우에만)
        await test_multiple_regions()
    else:
        print(f"\n⚠️ 단일 API 호출이 실패했으므로 다중 지역 테스트를 건너뜁니다.")
    
    print(f"\n" + "=" * 60)
    print(f"🏁 테스트 완료")


if __name__ == "__main__":
    asyncio.run(main())