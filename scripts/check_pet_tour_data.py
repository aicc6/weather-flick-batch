"""
반려동물 동반 여행지 데이터 확인 및 활용 가능성 점검
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from sqlalchemy import create_engine, text
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

def check_pet_tour_data():
    """반려동물 동반 여행지 데이터 현황 확인"""
    with engine.connect() as conn:
        # 1. 총 데이터 수 확인
        total = conn.execute(text("SELECT COUNT(*) FROM pet_tour_info")).scalar()
        logger.info(f"\n=== 반려동물 동반 여행지 현황 ===")
        logger.info(f"총 데이터 수: {total}건")
        
        # 2. 지역별 분포
        regions = conn.execute(text("""
            SELECT 
                COALESCE(r.region_name, p.area_code) as region,
                COUNT(*) as cnt
            FROM pet_tour_info p
            LEFT JOIN regions r ON p.area_code = r.tour_api_area_code
            GROUP BY r.region_name, p.area_code
            ORDER BY cnt DESC
        """))
        
        logger.info("\n지역별 분포:")
        for row in regions:
            logger.info(f"  - {row[0]}: {row[1]}건")
        
        # 3. 콘텐츠 타입별 분포
        types = conn.execute(text("""
            SELECT content_type_id, COUNT(*) as cnt
            FROM pet_tour_info
            GROUP BY content_type_id
            ORDER BY cnt DESC
        """))
        
        content_types = {
            '12': '관광지',
            '14': '문화시설',
            '15': '축제/공연/행사',
            '25': '여행코스',
            '28': '레포츠',
            '32': '숙박',
            '38': '쇼핑',
            '39': '음식점'
        }
        
        logger.info("\n콘텐츠 타입별 분포:")
        for row in types:
            type_name = content_types.get(str(row[0]), f"기타({row[0]})")
            logger.info(f"  - {type_name}: {row[1]}건")
        
        # 4. 샘플 데이터 확인
        samples = conn.execute(text("""
            SELECT 
                p.content_id,
                p.title,
                p.address,
                p.pet_acpt_abl,
                p.pet_info,
                r.region_name
            FROM pet_tour_info p
            LEFT JOIN regions r ON p.area_code = r.tour_api_area_code
            WHERE p.pet_acpt_abl IS NOT NULL OR p.pet_info IS NOT NULL
            LIMIT 10
        """))
        
        logger.info("\n샘플 데이터:")
        for row in samples:
            logger.info(f"\n  [{row[5]}] {row[1]}")
            logger.info(f"    주소: {row[2]}")
            logger.info(f"    반려동물 동반: {row[3]}")
            if row[4]:
                logger.info(f"    상세정보: {row[4][:100]}...")

def check_integration_possibility():
    """기존 관광지 데이터와의 통합 가능성 확인"""
    with engine.connect() as conn:
        # tourist_attractions 테이블과 pet_tour_info의 연결 가능성 확인
        overlap = conn.execute(text("""
            SELECT COUNT(DISTINCT p.content_id)
            FROM pet_tour_info p
            INNER JOIN tourist_attractions t ON p.content_id = t.content_id
        """)).scalar()
        
        logger.info(f"\n=== 통합 가능성 분석 ===")
        logger.info(f"tourist_attractions와 중복되는 content_id: {overlap}건")
        
        # 지역별 매칭 확인
        region_match = conn.execute(text("""
            SELECT 
                r.region_name,
                COUNT(DISTINCT t.content_id) as tourist_cnt,
                COUNT(DISTINCT p.content_id) as pet_cnt
            FROM regions r
            LEFT JOIN tourist_attractions t ON r.tour_api_area_code = t.region_code
            LEFT JOIN pet_tour_info p ON r.tour_api_area_code = p.area_code
            WHERE r.region_name IN ('서울', '부산', '제주')
            GROUP BY r.region_name
        """))
        
        logger.info("\n주요 지역별 데이터 현황:")
        for row in region_match:
            logger.info(f"  - {row[0]}: 관광지 {row[1]}건, 반려동물 동반 {row[2]}건")

def suggest_implementation():
    """구현 방안 제안"""
    logger.info("\n=== 구현 방안 제안 ===")
    logger.info("1. 즉시 활용 가능한 방법:")
    logger.info("   - pet_tour_info 테이블을 맞춤 일정 추천 시 추가 데이터소스로 활용")
    logger.info("   - 여행 스타일에 'pet'이 포함된 경우 pet_tour_info 우선 검색")
    
    logger.info("\n2. 데이터 통합 방법:")
    logger.info("   - tourist_attractions의 detail_intro_info에 반려동물 정보 추가")
    logger.info("   - 별도 태그 필드를 만들어 '반려동물동반가능' 태그 추가")
    
    logger.info("\n3. API 개선사항:")
    logger.info("   - /custom-travel/recommendations에서 pet_tour_info 조회 추가")
    logger.info("   - 반려동물 동반 가능 여부를 별도 필터로 제공")

def main():
    logger.info("=== 반려동물 동반 여행지 데이터 분석 시작 ===")
    
    # 1. 현재 데이터 확인
    check_pet_tour_data()
    
    # 2. 통합 가능성 확인
    check_integration_possibility()
    
    # 3. 구현 방안 제안
    suggest_implementation()
    
    logger.info("\n=== 분석 완료 ===")

if __name__ == "__main__":
    main()