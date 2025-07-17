"""
반려동물 동반 여행지 데이터 확인 및 추가 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import json
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 데이터베이스 URL 직접 설정
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5432/weather_flick')

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 데이터베이스 연결
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def check_pet_tour_table():
    """pet_tour_info 테이블 존재 여부 확인"""
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'pet_tour_info'
            );
        """))
        exists = result.scalar()
        logger.info(f"pet_tour_info 테이블 존재: {exists}")
        return exists

def check_existing_pet_data():
    """기존 pet_tour_info 데이터 확인"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM pet_tour_info"))
        count = result.scalar()
        logger.info(f"기존 pet_tour_info 데이터 수: {count}")
        return count

def find_pet_tour_api_data():
    """api_raw_data에서 반려동물 관련 데이터 찾기"""
    with engine.connect() as conn:
        # 먼저 테이블 구조 확인
        columns = conn.execute(text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'api_raw_data'
            AND table_schema = 'public'
        """))
        
        logger.info("api_raw_data 테이블 컬럼:")
        col_list = []
        for col in columns:
            col_list.append(col[0])
            logger.info(f"  - {col[0]}")
        
        # response_body 컬럼이 있는지 확인
        if 'response_body' in col_list:
            # 먼저 어떤 endpoint들이 있는지 확인
            endpoints = conn.execute(text("""
                SELECT DISTINCT endpoint, COUNT(*) as cnt 
                FROM api_raw_data 
                WHERE endpoint LIKE '%pet%' 
                   OR endpoint LIKE '%Pet%'
                   OR response_body::text LIKE '%petTour%'
                   OR response_body::text LIKE '%반려동물%'
                GROUP BY endpoint
                ORDER BY cnt DESC
                LIMIT 10
            """))
            
            logger.info("반려동물 관련 API 엔드포인트:")
            for row in endpoints:
                logger.info(f"  - {row[0]}: {row[1]}건")
            
            # 실제 데이터 찾기
            result = conn.execute(text("""
                SELECT id, endpoint, response_body, created_at
                FROM api_raw_data
                WHERE (endpoint LIKE '%pet%' 
                   OR endpoint LIKE '%Pet%'
                   OR response_body::text LIKE '%petTour%'
                   OR response_body::text LIKE '%petAcptAbl%'
                   OR response_body::text LIKE '%반려동물%')
                AND response_status = 200
                ORDER BY created_at DESC
                LIMIT 100
            """))
            
            pet_data = []
            for row in result:
                pet_data.append({
                    'id': row[0],
                    'endpoint': row[1],
                    'raw_data': row[2],  # response_body를 raw_data로 매핑
                    'created_at': row[3]
                })
            
            logger.info(f"발견된 반려동물 관련 API 데이터: {len(pet_data)}건")
            return pet_data
        else:
            logger.warning("api_raw_data 테이블에 response_body 컬럼이 없습니다.")
            return []

def parse_and_insert_pet_data(api_data_list):
    """API 데이터를 파싱하여 pet_tour_info 테이블에 삽입"""
    inserted_count = 0
    
    with engine.connect() as conn:
        for api_data in api_data_list:
            try:
                raw_data = api_data['raw_data']
                
                # API 응답 구조 확인
                if isinstance(raw_data, dict) and 'response' in raw_data:
                    response = raw_data['response']
                    if 'body' in response and 'items' in response['body']:
                        items = response['body']['items']
                        if isinstance(items, dict) and 'item' in items:
                            item_list = items['item']
                            if not isinstance(item_list, list):
                                item_list = [item_list]
                            
                            for item in item_list:
                                # 반려동물 관련 필드가 있는지 확인
                                if any(key in item for key in ['petacptabl', 'petAcptAbl', 'petinfo', 'petInfo']):
                                    # 데이터 매핑
                                    insert_data = {
                                        'content_id': item.get('contentid'),
                                        'content_type_id': item.get('contenttypeid'),
                                        'title': item.get('title'),
                                        'address': item.get('addr1', '') + ' ' + item.get('addr2', ''),
                                        'latitude': item.get('mapy'),
                                        'longitude': item.get('mapx'),
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
                                        'raw_data_id': api_data['id']
                                    }
                                    
                                    # NULL 값 처리
                                    insert_data = {k: (v if v != '' else None) for k, v in insert_data.items()}
                                    
                                    # 중복 확인
                                    if insert_data['content_id']:
                                        exists = conn.execute(text(
                                            "SELECT 1 FROM pet_tour_info WHERE content_id = :content_id"
                                        ), {'content_id': insert_data['content_id']}).scalar()
                                        
                                        if not exists:
                                            # 데이터 삽입
                                            conn.execute(text("""
                                                INSERT INTO pet_tour_info (
                                                    content_id, content_type_id, title, address,
                                                    latitude, longitude, area_code, sigungu_code,
                                                    tel, homepage, overview, cat1, cat2, cat3,
                                                    first_image, first_image2, pet_acpt_abl, pet_info,
                                                    raw_data_id
                                                ) VALUES (
                                                    :content_id, :content_type_id, :title, :address,
                                                    :latitude, :longitude, :area_code, :sigungu_code,
                                                    :tel, :homepage, :overview, :cat1, :cat2, :cat3,
                                                    :first_image, :first_image2, :pet_acpt_abl, :pet_info,
                                                    :raw_data_id
                                                )
                                            """), insert_data)
                                            inserted_count += 1
                                            logger.info(f"추가됨: {insert_data['title']}")
                                        else:
                                            logger.debug(f"이미 존재: {insert_data['content_id']}")
                
            except Exception as e:
                logger.error(f"데이터 처리 중 오류: {e}")
                continue
        
        conn.commit()
    
    return inserted_count

def check_detail_info_for_pet_data():
    """관광지 상세 정보에서 반려동물 정보 확인"""
    with engine.connect() as conn:
        # detail_intro_info 또는 detail_additional_info에 반려동물 정보가 있는지 확인
        result = conn.execute(text("""
            SELECT content_id, attraction_name, detail_intro_info, detail_additional_info
            FROM tourist_attractions
            WHERE detail_intro_info::text LIKE '%pet%' 
               OR detail_intro_info::text LIKE '%반려%'
               OR detail_additional_info::text LIKE '%pet%'
               OR detail_additional_info::text LIKE '%반려%'
            LIMIT 10
        """))
        
        pet_attractions = []
        for row in result:
            pet_info = {}
            if row[2]:  # detail_intro_info
                intro = row[2]
                if isinstance(intro, dict):
                    for key, value in intro.items():
                        if 'pet' in key.lower() or '반려' in str(value):
                            pet_info['intro'] = {key: value}
            
            if row[3]:  # detail_additional_info
                additional = row[3]
                if isinstance(additional, dict):
                    for key, value in additional.items():
                        if 'pet' in key.lower() or '반려' in str(value):
                            pet_info['additional'] = {key: value}
            
            if pet_info:
                pet_attractions.append({
                    'content_id': row[0],
                    'name': row[1],
                    'pet_info': pet_info
                })
        
        logger.info(f"관광지 상세정보에서 발견된 반려동물 정보: {len(pet_attractions)}건")
        for attr in pet_attractions:
            logger.info(f"  - {attr['name']}: {attr['pet_info']}")
        
        return pet_attractions

def main():
    logger.info("=== 반려동물 동반 여행지 데이터 확인 및 추가 시작 ===")
    
    # 1. 테이블 확인
    if not check_pet_tour_table():
        logger.error("pet_tour_info 테이블이 없습니다. 마이그레이션을 먼저 실행하세요.")
        return
    
    # 2. 기존 데이터 확인
    existing_count = check_existing_pet_data()
    
    # 3. API 원본 데이터에서 반려동물 관련 데이터 찾기
    pet_api_data = find_pet_tour_api_data()
    
    # 4. 데이터 파싱 및 삽입
    if pet_api_data:
        inserted = parse_and_insert_pet_data(pet_api_data)
        logger.info(f"새로 추가된 반려동물 동반 여행지: {inserted}건")
    
    # 5. 관광지 상세정보에서 반려동물 정보 확인
    pet_in_details = check_detail_info_for_pet_data()
    
    # 6. 최종 결과
    final_count = check_existing_pet_data()
    logger.info(f"=== 완료 ===")
    logger.info(f"기존 데이터: {existing_count}건")
    logger.info(f"최종 데이터: {final_count}건")
    logger.info(f"추가된 데이터: {final_count - existing_count}건")

if __name__ == "__main__":
    main()