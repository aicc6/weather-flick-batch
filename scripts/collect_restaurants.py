#!/usr/bin/env python3
"""
음식점 데이터 수집 스크립트
- KTO API에서 음식점 정보를 수집하여 데이터베이스에 저장
"""

import sys
import os
import asyncio
import logging
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.collectors.unified_kto_client_extended import ExtendedUnifiedKTOClient
from app.core.database_manager_extension import DatabaseManagerExtension
from app.core.database_connection_pool import DatabaseConnectionPool

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def collect_restaurants():
    """음식점 데이터 수집"""
    
    # 데이터베이스 초기화
    db_pool = DatabaseConnectionPool()
    await db_pool.initialize_async_pool()
    
    # 데이터베이스 매니저
    db_manager = DatabaseManagerExtension(db_pool)
    
    # KTO 클라이언트 초기화
    # KTO API 키 가져오기
    kto_api_keys = os.getenv('KTO_API_KEY', '').split(',')
    if not kto_api_keys or not kto_api_keys[0]:
        raise ValueError("KTO_API_KEY 환경변수가 설정되지 않았습니다.")
    
    kto_client = ExtendedUnifiedKTOClient()
    # API 키 설정
    kto_client.api_keys = kto_api_keys
    kto_client.db_manager = db_manager
    
    try:
        # 지역 코드 목록 조회 (서울, 경기 우선 테스트)
        area_codes = [1, 31]  # 서울, 경기
        
        total_collected = 0
        
        for area_code in area_codes:
            logger.info(f"지역 코드 {area_code} 음식점 데이터 수집 시작")
            
            # 음식점 타입 코드 (FoodPlace39)
            content_type_id = "39"
            
            # 데이터 수집
            page = 1
            page_size = 100
            
            while True:
                try:
                    result = await kto_client.get_list_by_type(
                        content_type_id=content_type_id,
                        area_code=area_code,
                        page=page,
                        page_size=page_size
                    )
                    
                    if not result or not result.get('items'):
                        logger.info(f"지역 {area_code}: 더 이상 데이터가 없습니다.")
                        break
                    
                    items = result['items']
                    total_collected += len(items)
                    
                    logger.info(f"지역 {area_code}: 페이지 {page} - {len(items)}개 항목 수집")
                    
                    # 다음 페이지로
                    page += 1
                    
                    # API 제한 고려
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"데이터 수집 중 오류 발생: {e}")
                    break
        
        logger.info(f"총 {total_collected}개 음식점 데이터 수집 완료")
        
        # 데이터베이스 확인
        query = "SELECT COUNT(*) FROM restaurants"
        result = db_manager.db_manager.fetch_one(query)
        if result:
            logger.info(f"데이터베이스에 저장된 음식점 수: {result[0]}")
        
    except Exception as e:
        logger.error(f"음식점 데이터 수집 실패: {e}")
        raise
    
    finally:
        # 정리
        await db_pool.close_all()

if __name__ == "__main__":
    asyncio.run(collect_restaurants())