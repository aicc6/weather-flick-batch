"""
한국관광공사 API - areaCode2 지역 코드 조회 모듈

이 모듈은 한국관광공사의 관광정보 서비스 API 중 areaCode2 엔드포인트를 사용하여
지역 코드 정보를 조회하는 기능을 제공합니다.

API 정보:
- 서비스명: 한국관광공사_국문 관광정보 서비스_GW
- 엔드포인트: areaCode2
- 기능: 지역 코드 정보 조회
- 데이터 형식: JSON/XML
- 비용: 무료

참고 링크:
http://www.data.go.kr/tcs/dss/selectApiDataDetailView.do?publicDataPk=15101578#/API%20%EB%AA%A9%EB%A1%9D/areaCode2
"""

import os
import requests
from dotenv import load_dotenv


# 환경 변수 로드
load_dotenv(override=True)

# API 설정
KTO_API_KEY = os.getenv("KTO_API_KEY")
KTO_API_BASE_URL = os.getenv("KTO_API_BASE_URL")

def get_local_codes():
  url = f"{KTO_API_BASE_URL}"
  params = {
    "serviceKey": KTO_API_KEY,
    "_type": "json",
    "numOfRows": 10,
    "pageNo": 1,
  }

  try:
    response = requests.get(url, params=params, verify=False)
    response.raise_for_status()  # HTTP 오류 체크

    # 응답 내용 확인
    print(f"Status Code: {response.status_code}")
    print(f"Response Length: {len(response.text)}")

    # 응답이 비어있지 않은지 확인
    if not response.text.strip():
      print("Empty response received")
      return None

    # JSON 파싱 시도
    json_data = response.json()
    print("JSON parsing successful!")
    return json_data

  except requests.exceptions.RequestException as e:
    print(f"Request error: {e}")
    return None
  except ValueError as e:
    print(f"JSON decode error: {e}")
    print(f"Response content: {response.text}")
    return None

# 테스트 실행
result = get_local_codes()
if result:
    print("\n=== API Response ===")
    print(f"Response type: {type(result)}")
    if isinstance(result, dict):
        print("Response keys:", list(result.keys()))
        if 'response' in result:
            print("Response structure:", list(result['response'].keys()))
    print(result)
else:
    print("Failed to get data from API")

