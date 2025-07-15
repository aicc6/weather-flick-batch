#!/usr/bin/env python
"""Simple data collection script for initial data"""

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from app.core.database_manager import SyncDatabaseManager
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_sample_attractions():
    """Create sample tourist attractions"""
    db = SyncDatabaseManager()
    
    sample_attractions = [
        {
            'content_id': 'TOUR_001',
            'title': '경복궁',
            'attraction_name': '경복궁',
            'address': '서울특별시 종로구 사직로 161',
            'latitude': 37.5796,
            'longitude': 126.9770,
            'region_code': '1',
            'image_url': 'https://example.com/gyeongbokgung.jpg',
            'description': '조선 왕조의 대표적인 궁궐'
        },
        {
            'content_id': 'TOUR_002',
            'title': 'N서울타워',
            'address': '서울특별시 용산구 남산공원길 105',
            'latitude': 37.5512,
            'longitude': 126.9882,
            'region_code': '1',
            'image_url': 'https://example.com/namsan.jpg',
            'description': '서울의 랜드마크 타워'
        },
        {
            'content_id': 'TOUR_003',
            'title': '해운대해수욕장',
            'address': '부산광역시 해운대구 해운대해변로 264',
            'latitude': 35.1587,
            'longitude': 129.1604,
            'region_code': '3',
            'image_url': 'https://example.com/haeundae.jpg',
            'description': '한국의 대표적인 해수욕장'
        },
        {
            'content_id': 'TOUR_004',
            'title': '성산일출봉',
            'address': '제주특별자치도 서귀포시 성산읍',
            'latitude': 33.4580,
            'longitude': 126.9425,
            'region_code': '39',
            'image_url': 'https://example.com/seongsan.jpg',
            'description': 'UNESCO 세계자연유산'
        },
        {
            'content_id': 'TOUR_005',
            'title': '인천차이나타운',
            'address': '인천광역시 중구 차이나타운로',
            'latitude': 37.4759,
            'longitude': 126.6169,
            'region_code': '2',
            'image_url': 'https://example.com/chinatown.jpg',
            'description': '한국 최초의 차이나타운'
        }
    ]
    
    for attraction in sample_attractions:
        try:
            # Add attraction_name field
            attraction['attraction_name'] = attraction['title']
            
            insert_query = """
            INSERT INTO tourist_attractions (
                content_id, attraction_name, address, 
                latitude, longitude, 
                region_code, image_url,
                description
            ) VALUES (
                %(content_id)s, %(attraction_name)s, %(address)s,
                %(latitude)s, %(longitude)s,
                %(region_code)s, %(image_url)s,
                %(description)s
            ) ON CONFLICT (content_id) DO UPDATE SET
                attraction_name = EXCLUDED.attraction_name,
                address = EXCLUDED.address
            """
            
            db.execute_query(insert_query, attraction)
            logger.info(f"Inserted attraction: {attraction['title']}")
            
        except Exception as e:
            logger.error(f"Error inserting {attraction['title']}: {e}")

def create_sample_weather():
    """Create sample weather data"""
    db = SyncDatabaseManager()
    
    # Insert current weather (using the actual schema)
    from datetime import date
    today = date.today()
    
    weather_data = [
        {
            'region_code': '1',
            'region_name': '서울특별시',
            'weather_date': today,
            'year': today.year,
            'month': today.month,
            'day': today.day,
            'avg_temp': 25.5,
            'max_temp': 28.0,
            'min_temp': 22.0
        },
        {
            'region_code': '3',
            'region_name': '부산광역시',
            'weather_date': today,
            'year': today.year,
            'month': today.month,
            'day': today.day,
            'avg_temp': 27.0,
            'max_temp': 30.0,
            'min_temp': 24.0
        },
        {
            'region_code': '39',
            'region_name': '제주특별자치도',
            'weather_date': today,
            'year': today.year,
            'month': today.month,
            'day': today.day,
            'avg_temp': 26.5,
            'max_temp': 29.0,
            'min_temp': 23.0
        }
    ]
    
    for weather in weather_data:
        try:
            weather_query = """
            INSERT INTO weather_current (
                region_code, region_name, weather_date,
                year, month, day,
                avg_temp, max_temp, min_temp
            ) VALUES (
                %(region_code)s, %(region_name)s, %(weather_date)s,
                %(year)s, %(month)s, %(day)s,
                %(avg_temp)s, %(max_temp)s, %(min_temp)s
            ) ON CONFLICT (region_code, weather_date) DO UPDATE SET
                avg_temp = EXCLUDED.avg_temp,
                max_temp = EXCLUDED.max_temp,
                min_temp = EXCLUDED.min_temp
            """
            
            db.execute_query(weather_query, weather)
            logger.info(f"Inserted weather for {weather['region_name']}")
            
        except Exception as e:
            logger.error(f"Error inserting weather for {weather['region_name']}: {e}")

def main():
    logger.info("Starting simple data collection...")
    
    # Create sample attractions
    create_sample_attractions()
    
    # Create sample weather
    create_sample_weather()
    
    # Verify data
    db = SyncDatabaseManager()
    
    count_query = "SELECT COUNT(*) as count FROM tourist_attractions"
    result = db.execute_query(count_query)
    logger.info(f"Total attractions: {result[0]['count']}")
    
    weather_count = "SELECT COUNT(*) as count FROM weather_current"
    result = db.execute_query(weather_count)
    logger.info(f"Total weather records: {result[0]['count']}")
    
    logger.info("Data collection completed!")

if __name__ == "__main__":
    main()