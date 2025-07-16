#!/usr/bin/env python3
"""
날씨 데이터 수집 스크립트
주요 도시들의 날씨 데이터를 수집하여 DB에 저장
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import asyncio
from datetime import datetime
from database import get_db
from config import settings
from jobs.weather_job import WeatherUpdateJob
from app.models import WeatherForecast, WeatherSummary
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def main():
    """메인 실행 함수"""
    try:
        logger.info("날씨 데이터 수집 시작...")
        
        # WeatherUpdateJob 인스턴스 생성
        job = WeatherUpdateJob()
        
        # 날씨 업데이트 실행
        result = await job.run()
        
        if result['status'] == 'success':
            logger.info(f"날씨 데이터 수집 성공: {result['message']}")
            
            # DB에서 저장된 데이터 확인
            with next(get_db()) as db:
                # 날씨 예보 수
                forecast_count = db.query(WeatherForecast).count()
                logger.info(f"저장된 날씨 예보 수: {forecast_count}")
                
                # 날씨 요약 수
                summary_count = db.query(WeatherSummary).count()
                logger.info(f"저장된 날씨 요약 수: {summary_count}")
                
                # 최근 업데이트된 도시들
                recent_cities = db.query(WeatherForecast.city_name).distinct().limit(10).all()
                logger.info(f"최근 업데이트된 도시: {[city[0] for city in recent_cities]}")
                
        else:
            logger.error(f"날씨 데이터 수집 실패: {result.get('error', 'Unknown error')}")
            
    except Exception as e:
        logger.error(f"스크립트 실행 중 오류 발생: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())