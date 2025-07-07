#!/usr/bin/env python3
"""
모의 예보구역 데이터 생성 스크립트

API 접근 권한 문제로 실제 데이터를 가져올 수 없을 때 
모의 데이터를 생성하여 시스템을 테스트합니다.
"""

import sys
import os
import logging
from datetime import datetime
from typing import List, Dict, Any

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_app_settings
from app.core.database_manager import DatabaseManager
from app.core.database_manager_extension import extend_database_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockForecastDataGenerator:
    """모의 예보구역 데이터 생성기"""
    
    def __init__(self):
        self.db_manager = extend_database_manager(DatabaseManager().sync_manager)
    
    def generate_mock_forecast_regions(self) -> List[Dict[str, Any]]:
        """모의 예보구역 데이터 생성"""
        
        # 실제 기상청 예보구역 코드와 좌표 정보 (샘플)
        mock_regions = [
            {
                "region_code": "11B00000",
                "region_name": "서울특별시",
                "latitude": 37.5665,
                "longitude": 126.9780,
                "grid_x": 60,
                "grid_y": 127,
                "administrative_code": "11"
            },
            {
                "region_code": "11B10101", 
                "region_name": "종로구",
                "latitude": 37.5735,
                "longitude": 126.9788,
                "grid_x": 60,
                "grid_y": 127,
                "administrative_code": "11110"
            },
            {
                "region_code": "11B10102",
                "region_name": "중구",
                "latitude": 37.5641,
                "longitude": 126.9979,
                "grid_x": 60,
                "grid_y": 127,
                "administrative_code": "11140"
            },
            {
                "region_code": "26000000",
                "region_name": "부산광역시",
                "latitude": 35.1796,
                "longitude": 129.0756,
                "grid_x": 98,
                "grid_y": 76,
                "administrative_code": "26"
            },
            {
                "region_code": "27000000",
                "region_name": "대구광역시",
                "latitude": 35.8714,
                "longitude": 128.6014,
                "grid_x": 89,
                "grid_y": 90,
                "administrative_code": "27"
            },
            {
                "region_code": "28000000",
                "region_name": "인천광역시",
                "latitude": 37.4563,
                "longitude": 126.7052,
                "grid_x": 55,
                "grid_y": 124,
                "administrative_code": "28"
            },
            {
                "region_code": "29000000",
                "region_name": "광주광역시",
                "latitude": 35.1595,
                "longitude": 126.8526,
                "grid_x": 58,
                "grid_y": 74,
                "administrative_code": "29"
            },
            {
                "region_code": "30000000",
                "region_name": "대전광역시",
                "latitude": 36.3504,
                "longitude": 127.3845,
                "grid_x": 67,
                "grid_y": 100,
                "administrative_code": "30"
            },
            {
                "region_code": "31000000",
                "region_name": "울산광역시",
                "latitude": 35.5384,
                "longitude": 129.3114,
                "grid_x": 102,
                "grid_y": 84,
                "administrative_code": "31"
            },
            {
                "region_code": "36000000",
                "region_name": "세종특별자치시",
                "latitude": 36.4801,
                "longitude": 127.2892,
                "grid_x": 66,
                "grid_y": 103,
                "administrative_code": "36"
            },
            {
                "region_code": "41000000",
                "region_name": "경기도",
                "latitude": 37.4138,
                "longitude": 127.5183,
                "grid_x": 60,
                "grid_y": 120,
                "administrative_code": "41"
            },
            {
                "region_code": "42000000",
                "region_name": "강원특별자치도",
                "latitude": 37.8228,
                "longitude": 128.1555,
                "grid_x": 73,
                "grid_y": 134,
                "administrative_code": "42"
            },
            {
                "region_code": "43000000",
                "region_name": "충청북도",
                "latitude": 36.8000,
                "longitude": 127.7000,
                "grid_x": 69,
                "grid_y": 107,
                "administrative_code": "43"
            },
            {
                "region_code": "44000000",
                "region_name": "충청남도",
                "latitude": 36.5184,
                "longitude": 126.8000,
                "grid_x": 68,
                "grid_y": 100,
                "administrative_code": "44"
            },
            {
                "region_code": "45000000",
                "region_name": "전북특별자치도",
                "latitude": 35.7175,
                "longitude": 127.1530,
                "grid_x": 63,
                "grid_y": 89,
                "administrative_code": "45"
            },
            {
                "region_code": "46000000",
                "region_name": "전라남도",
                "latitude": 34.8679,
                "longitude": 126.9910,
                "grid_x": 51,
                "grid_y": 67,
                "administrative_code": "46"
            },
            {
                "region_code": "47000000",
                "region_name": "경상북도",
                "latitude": 36.4919,
                "longitude": 128.8889,
                "grid_x": 87,
                "grid_y": 106,
                "administrative_code": "47"
            },
            {
                "region_code": "48000000",
                "region_name": "경상남도",
                "latitude": 35.4606,
                "longitude": 128.2132,
                "grid_x": 91,
                "grid_y": 77,
                "administrative_code": "48"
            },
            {
                "region_code": "49000000",
                "region_name": "제주특별자치도",
                "latitude": 33.4996,
                "longitude": 126.5312,
                "grid_x": 52,
                "grid_y": 38,
                "administrative_code": "50"
            }
        ]
        
        # 현재 시간 추가
        current_time = datetime.now()
        
        for region in mock_regions:
            region.update({
                "is_active": True,
                "forecast_region_type": "short_term",
                "created_at": current_time,
                "updated_at": current_time
            })
        
        logger.info(f"모의 예보구역 데이터 생성 완료: {len(mock_regions)}개")
        return mock_regions
    
    def save_mock_data(self, regions: List[Dict[str, Any]]) -> int:
        """모의 데이터를 데이터베이스에 저장"""
        
        try:
            saved_count = 0
            
            for region in regions:
                try:
                    success = self._upsert_forecast_region(region)
                    if success:
                        saved_count += 1
                        
                except Exception as e:
                    logger.error(f"모의 데이터 저장 실패 {region.get('region_code')}: {e}")
                    continue
            
            logger.info(f"모의 예보구역 데이터 저장 완료: {saved_count}/{len(regions)}")
            return saved_count
            
        except Exception as e:
            logger.error(f"모의 데이터 배치 저장 실패: {e}")
            return 0
    
    def _upsert_forecast_region(self, region_data: Dict[str, Any]) -> bool:
        """예보구역 데이터 UPSERT"""
        try:
            query = """
            INSERT INTO weather_regions (
                region_code, region_name, latitude, longitude, 
                grid_x, grid_y, is_active, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT (region_code) DO UPDATE SET
                region_name = EXCLUDED.region_name,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                grid_x = EXCLUDED.grid_x,
                grid_y = EXCLUDED.grid_y,
                is_active = EXCLUDED.is_active,
                updated_at = CURRENT_TIMESTAMP
            """
            
            params = (
                region_data.get("region_code"),
                region_data.get("region_name"),
                float(region_data.get("latitude")),
                float(region_data.get("longitude")),
                region_data.get("grid_x"),
                region_data.get("grid_y"),
                region_data.get("is_active", True)
            )
            
            self.db_manager.execute_update(query, params)
            logger.info(f"모의 예보구역 저장 성공: {region_data.get('region_code')} - {region_data.get('region_name')}")
            return True
            
        except Exception as e:
            logger.error(f"모의 예보구역 UPSERT 실패: {e}")
            return False


def main():
    """메인 실행 함수"""
    try:
        logger.info("모의 예보구역 데이터 생성 시작")
        
        generator = MockForecastDataGenerator()
        
        # 모의 데이터 생성
        mock_regions = generator.generate_mock_forecast_regions()
        
        if mock_regions:
            # 데이터베이스에 저장
            saved_count = generator.save_mock_data(mock_regions)
            logger.info(f"모의 데이터 생성 완료: {saved_count}개 저장")
            
            print(f"\n✅ 모의 예보구역 데이터 생성 완료")
            print(f"📊 총 {saved_count}개 지역 저장")
            print(f"💾 weather_regions 테이블에 저장됨")
            
        else:
            logger.warning("생성된 모의 데이터가 없습니다")
            
    except Exception as e:
        logger.error(f"모의 데이터 생성 작업 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()