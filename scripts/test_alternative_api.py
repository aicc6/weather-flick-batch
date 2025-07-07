#!/usr/bin/env python3
"""
기상청 대체 API 테스트 스크립트

다른 기상청 API 엔드포인트들을 테스트합니다.
"""

import sys
import os
import asyncio
import aiohttp
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_api_config


async def test_alternative_apis():
    """대체 기상청 API 테스트"""
    
    api_config = get_api_config()
    api_key = api_config.kma_api_key.split(',')[0]  # 첫 번째 키 사용
    
    # 테스트할 대체 API들
    test_apis = [
        {
            "name": "기상청 지역 코드 조회 API",
            "url": "https://apihub.kma.go.kr/api/typ01/url/kma_regist.php", 
            "params": {
                "authKey": api_key,
                "help": "1"
            }
        },
        {
            "name": "기상청 관측지점 조회 API",
            "url": "https://apihub.kma.go.kr/api/typ01/url/fct_asos_rltm.php",
            "params": {
                "authKey": api_key,
                "help": "1"
            }
        },
        {
            "name": "기상청 AWS 관측지점 API",
            "url": "https://apihub.kma.go.kr/api/typ01/url/fct_aws_rltm.php",
            "params": {
                "authKey": api_key,
                "help": "1"
            }
        },
        {
            "name": "기상청 동네예보 API (공공데이터)",
            "url": "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst",
            "params": {
                "serviceKey": api_key,
                "dataType": "JSON",
                "base_date": datetime.now().strftime("%Y%m%d"),
                "base_time": "0500",
                "nx": "60",
                "ny": "127"
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
                    print(f"응답 내용 (처음 800자):")
                    print("-" * 40)
                    print(response_text[:800])
                    print("-" * 40)
                    
                    if len(response_text) > 800:
                        print(f"응답 내용 (마지막 200자):")
                        print("-" * 40)
                        print(response_text[-200:])
                        print("-" * 40)
                    
            except Exception as e:
                print(f"오류 발생: {e}")


if __name__ == "__main__":
    asyncio.run(test_alternative_apis())