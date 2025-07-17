"""
한국관광공사 API에서 반려동물 동반 여행지 데이터 수집
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import requests
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import time
import json

# .env 파일 로드
load_dotenv()

# 설정
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/weather_flick')
TOUR_API_KEY = os.getenv('TOUR_API_KEY')

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 데이터베이스 연결
engine = create_engine(DATABASE_URL)

def get_pet_tour_from_api(area_code=None):
    """한국관광공사 API에서 반려동물 동반 여행지 조회"""
    base_url = "http://apis.data.go.kr/B551011/KorService1/detailPetTour1"
    
    all_items = []
    page_no = 1
    
    while True:
        params = {
            'serviceKey': TOUR_API_KEY,
            'MobileOS': 'ETC',
            'MobileApp': 'WeatherFlick',
            '_type': 'json',
            'numOfRows': 100,
            'pageNo': page_no,
            'arrange': 'A'  # 제목순 정렬
        }
        
        if area_code:
            params['areaCode'] = area_code
        
        try:
            logger.info(f"API 호출 - 페이지 {page_no}, 지역코드: {area_code}")
            response = requests.get(base_url, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'response' in data and data['response']['header']['resultCode'] == '0000':
                body = data['response']['body']
                total_count = body.get('totalCount', 0)
                items = body.get('items', {})
                
                if items and 'item' in items:
                    item_list = items['item']
                    if not isinstance(item_list, list):
                        item_list = [item_list]
                    
                    all_items.extend(item_list)
                    logger.info(f"  수집된 항목: {len(item_list)}개 (총 {len(all_items)}/{total_count})")
                    
                    # 모든 데이터를 수집했으면 종료
                    if len(all_items) >= total_count:
                        break
                else:
                    logger.info("  더 이상 데이터가 없습니다.")
                    break
                    
                page_no += 1
                time.sleep(0.5)  # API 호출 제한 방지
            else:
                logger.error(f"API 오류: {data}")
                break
                
        except Exception as e:
            logger.error(f"API 호출 실패: {e}")
            break
    
    return all_items

def insert_pet_tour_data(items):
    """수집한 데이터를 pet_tour_info 테이블에 삽입"""
    inserted = 0
    updated = 0
    
    with engine.connect() as conn:
        for item in items:
            try:
                # 데이터 준비
                data = {
                    'content_id': item.get('contentid'),
                    'content_type_id': item.get('contenttypeid'),
                    'title': item.get('title'),
                    'address': item.get('addr1', ''),
                    'latitude': float(item.get('mapy')) if item.get('mapy') else None,
                    'longitude': float(item.get('mapx')) if item.get('mapx') else None,
                    'area_code': item.get('areacode'),
                    'sigungu_code': item.get('sigungucode'),
                    'tel': item.get('tel'),
                    'homepage': item.get('homepage'),
                    'overview': item.get('overview'),
                    'cat1': item.get('cat1'),
                    'cat2': item.get('cat2'),
                    'cat3': item.get('cat3'),
                    'first_image': item.get('firstimage'),
                    'first_image2': item.get('firstimage2'),
                    'pet_acpt_abl': item.get('petacptabl') or item.get('petAcptAbl'),
                    'pet_info': item.get('petinfo') or item.get('petInfo'),
                    'created_time': item.get('createdtime'),
                    'modified_time': item.get('modifiedtime')
                }
                
                # 중복 확인
                exists = conn.execute(text(
                    "SELECT 1 FROM pet_tour_info WHERE content_id = :content_id"
                ), {'content_id': data['content_id']}).scalar()
                
                if exists:
                    # 업데이트
                    conn.execute(text("""
                        UPDATE pet_tour_info SET
                            title = :title,
                            address = :address,
                            pet_acpt_abl = :pet_acpt_abl,
                            pet_info = :pet_info,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE content_id = :content_id
                    """), data)
                    updated += 1
                else:
                    # 삽입
                    conn.execute(text("""
                        INSERT INTO pet_tour_info (
                            content_id, content_type_id, title, address,
                            latitude, longitude, area_code, sigungu_code,
                            tel, homepage, overview, cat1, cat2, cat3,
                            first_image, first_image2, pet_acpt_abl, pet_info
                        ) VALUES (
                            :content_id, :content_type_id, :title, :address,
                            :latitude, :longitude, :area_code, :sigungu_code,
                            :tel, :homepage, :overview, :cat1, :cat2, :cat3,
                            :first_image, :first_image2, :pet_acpt_abl, :pet_info
                        )
                    """), data)
                    inserted += 1
                    
            except Exception as e:
                logger.error(f"데이터 처리 오류 [{item.get('title')}]: {e}")
                continue
        
        conn.commit()
    
    return inserted, updated

def main():
    if not TOUR_API_KEY:
        logger.error("TOUR_API_KEY가 설정되지 않았습니다.")
        return
    
    logger.info("=== 반려동물 동반 여행지 데이터 수집 시작 ===")
    
    # 주요 지역 코드
    area_codes = [
        ('1', '서울'),
        ('6', '부산'),
        ('39', '제주'),
        ('31', '경기'),
        ('2', '인천'),
        ('4', '대구'),
        ('5', '광주'),
        ('3', '대전')
    ]
    
    total_inserted = 0
    total_updated = 0
    
    for area_code, area_name in area_codes:
        logger.info(f"\n{area_name} 지역 데이터 수집 중...")
        items = get_pet_tour_from_api(area_code)
        
        if items:
            inserted, updated = insert_pet_tour_data(items)
            total_inserted += inserted
            total_updated += updated
            logger.info(f"{area_name}: 신규 {inserted}건, 업데이트 {updated}건")
        else:
            logger.info(f"{area_name}: 데이터 없음")
    
    # 전체 데이터도 한 번 조회 (지역코드 없이)
    logger.info(f"\n전체 데이터 수집 중...")
    items = get_pet_tour_from_api()
    if items:
        inserted, updated = insert_pet_tour_data(items)
        total_inserted += inserted
        total_updated += updated
        logger.info(f"전체: 신규 {inserted}건, 업데이트 {updated}건")
    
    logger.info(f"\n=== 수집 완료 ===")
    logger.info(f"총 신규: {total_inserted}건")
    logger.info(f"총 업데이트: {total_updated}건")
    
    # 최종 통계
    with engine.connect() as conn:
        total = conn.execute(text("SELECT COUNT(*) FROM pet_tour_info")).scalar()
        logger.info(f"pet_tour_info 테이블 총 데이터: {total}건")

if __name__ == "__main__":
    main()