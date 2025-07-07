#!/usr/bin/env python3
"""
기상청 예보구역 API 테스트 스크립트

실제 API 응답을 확인하여 데이터 형식을 파악합니다.
"""

import sys
import os
import asyncio
import aiohttp
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_api_config


async def test_forecast_api():
    """기상청 예보구역 API 테스트"""
    
    api_config = get_api_config()
    api_key = api_config.kma_api_key.split(',')[0]  # 첫 번째 키 사용
    
    # 테스트할 API 엔드포인트들
    test_apis = [
        {
            "name": "기상청 예보구역 API (헬프 없이)",
            "url": "https://apihub.kma.go.kr/api/typ01/url/fct_shrt_reg.php",
            "params": {
                "authKey": api_key,
                "reg": "11B00000",  # 서울
                "tmfc": datetime.now().strftime("%Y%m%d0500"),
                "help": "0"
            }
        },
        {
            "name": "기상청 예보구역 API (전체 구역)",
            "url": "https://apihub.kma.go.kr/api/typ01/url/fct_shrt_reg.php",
            "params": {
                "authKey": api_key,
                "tmfc": datetime.now().strftime("%Y%m%d0500"),
                "help": "0"
            }
        },
        {
            "name": "기상청 예보구역 API (최신 발표시간)",
            "url": "https://apihub.kma.go.kr/api/typ01/url/fct_shrt_reg.php",
            "params": {
                "authKey": api_key,
                "reg": "11B00000",
                "tmfc": "202507060500",  # 어제 날짜
                "help": "0"
            }
        },
        {
            "name": "공공데이터포털 단기예보 API",
            "url": "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst",
            "params": {
                "serviceKey": api_key,
                "pageNo": "1",
                "numOfRows": "1",
                "dataType": "JSON",
                "base_date": datetime.now().strftime("%Y%m%d"),
                "base_time": "0500",
                "nx": "60",  # 서울 격자 X
                "ny": "127"  # 서울 격자 Y
            }
        }
    ]
    
    async with aiohttp.ClientSession() as session:
        for api_test in test_apis:
            print(f"\n{'='*60}")
            print(f"테스트: {api_test['name']}")
            print(f"URL: {api_test['url']}")
            print(f"파라미터: {api_test['params']}")
            print(f"{'='*60}")
            
            try:
                async with session.get(api_test['url'], params=api_test['params']) as response:
                    print(f"상태 코드: {response.status}")
                    print(f"응답 헤더: {dict(response.headers)}")
                    
                    response_text = await response.text()
                    print(f"응답 길이: {len(response_text)} 문자")
                    print(f"응답 내용 (처음 500자):")
                    print("-" * 40)
                    print(response_text[:500])
                    print("-" * 40)
                    
                    if len(response_text) > 500:
                        print(f"응답 내용 (마지막 200자):")
                        print("-" * 40)
                        print(response_text[-200:])
                        print("-" * 40)
                    
            except Exception as e:
                print(f"오류 발생: {e}")


if __name__ == "__main__":
    asyncio.run(test_forecast_api())