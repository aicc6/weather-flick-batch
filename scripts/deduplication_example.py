"""
중복 방지 시스템 사용 예제
외부 API에서 수집한 지역 정보를 중복 검사 후 저장
"""

import os
import sys
import logging
from typing import List

# 프로젝트 루트 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_connection_pool import get_connection_pool, PoolConfig
from app.core.deduplication_manager import DeduplicationManager, RegionData

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def example_kto_api_integration():
    """KTO API 통합 예제"""
    
    # 데이터베이스 연결 설정
    pool_config = PoolConfig(sync_min_connections=2, sync_max_connections=5)
    connection_pool = get_connection_pool(pool_config)
    
    # 중복 방지 관리자 생성
    dedup_manager = DeduplicationManager(connection_pool)
    
    # KTO API에서 가져온 가상의 지역 데이터
    kto_regions = [
        {
            "areaCode": "1",
            "areaName": "서울",
            "rnum": 1
        },
        {
            "areaCode": "2", 
            "areaName": "인천",
            "rnum": 2
        },
        {
            "areaCode": "1",  # 중복 데이터
            "areaName": "서울특별시",  # 조금 다른 이름
            "rnum": 3
        }
    ]
    
    success_count = 0
    skip_count = 0
    error_count = 0
    
    for region_info in kto_regions:
        try:
            # KTO 데이터를 RegionData 구조로 변환
            region_data = RegionData(
                region_code=region_info["areaCode"],
                region_name=region_info["areaName"],
                region_level=1,  # 시도 레벨
                api_provider="KTO",
                api_region_code=region_info["areaCode"],
                additional_data={
                    "area_code": region_info["areaCode"],
                    "rnum": region_info["rnum"]
                }
            )
            
            # 중복 검사 후 추가
            success, message = dedup_manager.add_region_data(region_data)
            
            if success:
                if "새 지역 추가" in message:
                    success_count += 1
                    logger.info(f"✅ {message}")
                else:
                    skip_count += 1
                    logger.info(f"⚠️ {message}")
            else:
                error_count += 1
                logger.error(f"❌ {message}")
                
        except Exception as e:
            error_count += 1
            logger.error(f"❌ 처리 실패: {region_info['areaName']} - {e}")
    
    # 결과 요약
    logger.info(f"\n=== 처리 결과 ===")
    logger.info(f"신규 추가: {success_count}개")
    logger.info(f"중복 스킵: {skip_count}개") 
    logger.info(f"오류 발생: {error_count}개")
    
    # 통계 정보 출력
    stats = dedup_manager.get_statistics()
    logger.info(f"\n=== 현재 통계 ===")
    logger.info(f"전체 지역: {stats['total_regions']}개")
    logger.info(f"API 매핑: {stats['total_mappings']}개")
    logger.info(f"API 제공자: {stats['api_providers']}개")

def example_weather_api_integration():
    """기상청 API 통합 예제"""
    
    pool_config = PoolConfig(sync_min_connections=2, sync_max_connections=5)
    connection_pool = get_connection_pool(pool_config)
    dedup_manager = DeduplicationManager(connection_pool)
    
    # 기상청 API에서 가져온 가상의 관측소 데이터
    weather_stations = [
        {
            "station_code": "108",
            "station_name": "서울",
            "latitude": 37.5665,
            "longitude": 126.9780,
            "grid_nx": 60,
            "grid_ny": 127
        },
        {
            "station_code": "159",
            "station_name": "부산",
            "latitude": 35.1796,
            "longitude": 129.0756,
            "grid_nx": 98,
            "grid_ny": 76
        }
    ]
    
    for station in weather_stations:
        try:
            region_data = RegionData(
                region_code=station["station_code"],
                region_name=station["station_name"],
                region_level=1,
                latitude=station["latitude"],
                longitude=station["longitude"],
                api_provider="KMA",
                api_region_code=station["station_code"],
                additional_data={
                    "station_code": station["station_code"],
                    "nx": station["grid_nx"],
                    "ny": station["grid_ny"]
                }
            )
            
            success, message = dedup_manager.add_region_data(region_data)
            logger.info(f"기상청 데이터: {message}")
            
        except Exception as e:
            logger.error(f"기상청 데이터 처리 실패: {station['station_name']} - {e}")

def example_duplicate_cleanup():
    """중복 데이터 정리 예제"""
    
    pool_config = PoolConfig(sync_min_connections=2, sync_max_connections=5)
    connection_pool = get_connection_pool(pool_config)
    dedup_manager = DeduplicationManager(connection_pool)
    
    # 먼저 드라이런으로 중복 상황 확인
    logger.info("=== 중복 검사 (드라이런) ===")
    dry_run_result = dedup_manager.cleanup_duplicates(dry_run=True)
    
    logger.info(f"중복 지역: {dry_run_result['duplicate_regions']}개")
    logger.info(f"중복 매핑: {dry_run_result['duplicate_mappings']}개")
    logger.info(f"중복 좌표: {dry_run_result['duplicate_coordinates']}개")
    
    # 사용자 확인 후 실제 정리 (주석 해제 시)
    # logger.info("=== 실제 중복 정리 ===")
    # actual_result = dedup_manager.cleanup_duplicates(dry_run=False)
    # logger.info(f"정리 완료: {actual_result}")

def main():
    """메인 실행 함수"""
    try:
        logger.info("=== 중복 방지 시스템 예제 실행 ===")
        
        # 1. KTO API 데이터 통합
        logger.info("\n1. KTO API 데이터 통합")
        example_kto_api_integration()
        
        # 2. 기상청 API 데이터 통합
        logger.info("\n2. 기상청 API 데이터 통합")
        example_weather_api_integration()
        
        # 3. 중복 데이터 정리
        logger.info("\n3. 중복 데이터 정리")
        example_duplicate_cleanup()
        
        logger.info("\n=== 예제 실행 완료 ===")
        
    except Exception as e:
        logger.error(f"예제 실행 중 오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()