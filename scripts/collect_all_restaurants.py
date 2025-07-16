#!/usr/bin/env python3
"""
전체 음식점 데이터 수집 스크립트
- 모든 지역의 음식점 데이터를 수집
"""

import requests
import os
import psycopg2
from urllib.parse import quote
from dotenv import load_dotenv
import json
import time
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# .env 파일 로드
load_dotenv()

# 환경 변수
KTO_API_KEYS = os.getenv('KTO_API_KEY', '').split(',')
DATABASE_URL = os.getenv('DATABASE_URL')

# 지역 코드 (전국)
AREA_CODES = {
    1: "서울", 2: "인천", 3: "대전", 4: "대구", 5: "광주",
    6: "부산", 7: "울산", 8: "세종", 31: "경기", 32: "강원",
    33: "충북", 34: "충남", 35: "전북", 36: "전남",
    37: "경북", 38: "경남", 39: "제주"
}

def get_restaurants_from_api(api_key, area_code=1, page=1, num_rows=100):
    """KTO API에서 음식점 데이터 조회"""
    
    base_url = "http://apis.data.go.kr/B551011/KorService2/areaBasedList2"
    
    params = {
        'serviceKey': api_key,
        'MobileOS': 'ETC',
        'MobileApp': 'WeatherFlick',
        'arrange': 'A',
        'contentTypeId': '39',  # 음식점
        'areaCode': area_code,
        'numOfRows': num_rows,
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
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # 응답 구조 확인
        if 'response' in data and 'body' in data['response'] and 'items' in data['response']['body']:
            items = data['response']['body']['items'].get('item', [])
            total_count = data['response']['body'].get('totalCount', 0)
            return (items if isinstance(items, list) else [items]), total_count
        else:
            logger.error(f"예상치 못한 응답 구조: {json.dumps(data, indent=2, ensure_ascii=False)}")
            return [], 0
            
    except Exception as e:
        logger.error(f"API 호출 오류: {e}")
        return [], 0

def save_to_database(restaurants):
    """데이터베이스에 음식점 정보 저장"""
    
    if not restaurants:
        return 0
    
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
            
        except Exception as e:
            logger.error(f"저장 오류 ({content_id}): {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return insert_count

def collect_area_restaurants(api_key, area_code, area_name):
    """특정 지역의 모든 음식점 수집"""
    
    logger.info(f"\n{'='*50}")
    logger.info(f"{area_name}({area_code}) 지역 음식점 수집 시작")
    
    total_collected = 0
    page = 1
    num_rows = 100
    
    while True:
        restaurants, total_count = get_restaurants_from_api(api_key, area_code, page, num_rows)
        
        if not restaurants:
            break
        
        saved_count = save_to_database(restaurants)
        total_collected += saved_count
        
        logger.info(f"  페이지 {page}: {len(restaurants)}개 중 {saved_count}개 저장 (전체: {total_count}개)")
        
        # 다음 페이지 확인
        if page * num_rows >= total_count:
            break
        
        page += 1
        time.sleep(0.5)  # API 제한 고려
    
    logger.info(f"{area_name} 수집 완료: 총 {total_collected}개 저장")
    return total_collected

def main():
    """메인 실행 함수"""
    
    logger.info("전체 음식점 데이터 수집 시작...")
    
    # 사용 가능한 API 키 선택
    api_key = None
    for key in KTO_API_KEYS:
        if key and len(key) > 10:
            api_key = key
            break
    
    if not api_key:
        logger.error("사용 가능한 API 키가 없습니다.")
        return
    
    logger.info(f"API 키 사용: {api_key[:10]}...")
    
    # 전체 통계
    grand_total = 0
    
    # 각 지역별로 수집
    for area_code, area_name in AREA_CODES.items():
        try:
            collected = collect_area_restaurants(api_key, area_code, area_name)
            grand_total += collected
        except Exception as e:
            logger.error(f"{area_name} 수집 중 오류: {e}")
            continue
    
    logger.info(f"\n{'='*50}")
    logger.info(f"전체 수집 완료! 총 {grand_total}개 음식점 정보 저장")
    
    # 최종 통계
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM restaurants")
    total_in_db = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    
    logger.info(f"데이터베이스 전체 음식점 수: {total_in_db}개")

if __name__ == "__main__":
    main()