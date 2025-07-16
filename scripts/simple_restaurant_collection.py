#!/usr/bin/env python3
"""
간단한 음식점 데이터 수집 스크립트
"""

import requests
import os
import psycopg2
from urllib.parse import quote
from dotenv import load_dotenv
import json
import time

# .env 파일 로드
load_dotenv()

# 환경 변수
KTO_API_KEYS = os.getenv('KTO_API_KEY', '').split(',')
DATABASE_URL = os.getenv('DATABASE_URL')

def get_restaurants_from_api(api_key, area_code=1, page=1):
    """KTO API에서 음식점 데이터 조회"""
    
    base_url = "http://apis.data.go.kr/B551011/KorService2/areaBasedList2"
    
    params = {
        'serviceKey': api_key,
        'MobileOS': 'ETC',
        'MobileApp': 'WeatherFlick',
        'arrange': 'A',
        'contentTypeId': '39',  # 음식점
        'areaCode': area_code,
        'numOfRows': 10,
        'pageNo': page,
        '_type': 'json'
    }
    
    # URL 생성 (serviceKey는 별도 처리)
    url_params = []
    for k, v in params.items():
        if k == 'serviceKey':
            url_params.append(f"{k}={quote(v, safe='')}")
        else:
            url_params.append(f"{k}={v}")
    
    url = f"{base_url}?{'&'.join(url_params)}"
    
    print(f"API 호출: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # 응답 구조 확인
        if 'response' in data and 'body' in data['response'] and 'items' in data['response']['body']:
            items = data['response']['body']['items'].get('item', [])
            return items if isinstance(items, list) else [items]
        else:
            print(f"예상치 못한 응답 구조: {json.dumps(data, indent=2, ensure_ascii=False)}")
            return []
            
    except Exception as e:
        print(f"API 호출 오류: {e}")
        return []

def save_to_database(restaurants):
    """데이터베이스에 음식점 정보 저장"""
    
    if not restaurants:
        print("저장할 음식점 데이터가 없습니다.")
        return
    
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    
    insert_count = 0
    
    for restaurant in restaurants:
        try:
            # 데이터 준비
            content_id = restaurant.get('contentid')
            title = restaurant.get('title', '')
            addr1 = restaurant.get('addr1', '')
            addr2 = restaurant.get('addr2', '')
            latitude = float(restaurant.get('mapy', 0))
            longitude = float(restaurant.get('mapx', 0))
            tel = restaurant.get('tel', '')
            areacode = restaurant.get('areacode', '')
            sigungucode = restaurant.get('sigungucode', '')
            zipcode = restaurant.get('zipcode', '')
            firstimage = restaurant.get('firstimage', '')
            firstimage2 = restaurant.get('firstimage2', '')
            cat1 = restaurant.get('cat1', '')
            cat2 = restaurant.get('cat2', '')
            cat3 = restaurant.get('cat3', '')
            
            # INSERT 쿼리
            insert_query = """
            INSERT INTO restaurants (
                content_id, restaurant_name, region_code, sigungu_code,
                address, detail_address, zipcode,
                latitude, longitude, tel,
                first_image, first_image_small,
                category_code, sub_category_code,
                created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT (content_id) DO UPDATE SET
                restaurant_name = EXCLUDED.restaurant_name,
                updated_at = CURRENT_TIMESTAMP;
            """
            
            cursor.execute(insert_query, (
                content_id, title, areacode, sigungucode,
                addr1, addr2, zipcode,
                latitude, longitude, tel,
                firstimage, firstimage2,
                cat1, cat2
            ))
            
            insert_count += 1
            print(f"저장됨: {title} ({content_id})")
            
        except Exception as e:
            print(f"저장 오류: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"총 {insert_count}개 음식점 정보 저장 완료")

def main():
    """메인 실행 함수"""
    
    print("음식점 데이터 수집 시작...")
    
    # 서울 지역(1) 음식점 데이터 수집
    area_code = 1
    total_collected = 0
    
    for api_key in KTO_API_KEYS:
        if not api_key:
            continue
            
        print(f"\nAPI 키 사용: {api_key[:10]}...")
        
        for page in range(1, 3):  # 테스트로 2페이지만
            print(f"\n페이지 {page} 수집 중...")
            
            restaurants = get_restaurants_from_api(api_key, area_code, page)
            
            if restaurants:
                print(f"{len(restaurants)}개 음식점 발견")
                save_to_database(restaurants)
                total_collected += len(restaurants)
                time.sleep(1)  # API 제한 고려
            else:
                print("더 이상 데이터가 없습니다.")
                break
        
        # 첫 번째 유효한 키로만 테스트
        if total_collected > 0:
            break
    
    print(f"\n수집 완료! 총 {total_collected}개 음식점 정보 수집")

if __name__ == "__main__":
    main()