#!/usr/bin/env python3
"""
api_raw_data 테이블 구조 및 데이터 확인 스크립트
"""

import sys
import os

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import DatabaseManager
from app.core.database_manager_extension import extend_database_manager


def check_api_raw_data_table():
    """api_raw_data 테이블 구조 및 데이터 확인"""
    
    db_manager = extend_database_manager(DatabaseManager().sync_manager)
    
    print("🔍 api_raw_data 테이블 분석\n")
    
    # 테이블 스키마 확인
    schema_query = """
    SELECT 
        column_name, 
        data_type, 
        is_nullable, 
        column_default,
        character_maximum_length
    FROM information_schema.columns 
    WHERE table_name = 'api_raw_data' 
    ORDER BY ordinal_position
    """
    
    try:
        columns = db_manager.fetch_all(schema_query)
        
        if columns:
            print("🏗️  api_raw_data 테이블 스키마:")
            for col in columns:
                max_len = f" ({col.get('character_maximum_length')})" if col.get('character_maximum_length') else ""
                print(f"  - {col.get('column_name')} | {col.get('data_type')}{max_len} | "
                      f"NULL: {col.get('is_nullable')} | Default: {col.get('column_default')}")
            
            # 데이터 개수 확인
            count_query = "SELECT COUNT(*) as count FROM api_raw_data"
            count_result = db_manager.fetch_one(count_query)
            count = count_result.get('count', 0) if count_result else 0
            print(f"\n📊 총 데이터 개수: {count}개")
            
            if count > 0:
                # API 제공자별 분포 확인
                provider_query = """
                SELECT 
                    api_provider,
                    COUNT(*) as count,
                    MIN(created_at) as first_call,
                    MAX(created_at) as last_call
                FROM api_raw_data
                GROUP BY api_provider
                ORDER BY count DESC
                """
                
                providers = db_manager.fetch_all(provider_query)
                print("\n📡 API 제공자별 분포:")
                for provider in providers:
                    print(f"  - {provider['api_provider']}: {provider['count']}개 "
                          f"(최초: {provider['first_call']}, 최근: {provider['last_call']})")
                
                # 엔드포인트별 분포 확인
                endpoint_query = """
                SELECT 
                    endpoint,
                    COUNT(*) as count
                FROM api_raw_data
                GROUP BY endpoint
                ORDER BY count DESC
                LIMIT 10
                """
                
                endpoints = db_manager.fetch_all(endpoint_query)
                print("\n🔗 API 엔드포인트별 분포 (상위 10개):")
                for endpoint in endpoints:
                    print(f"  - {endpoint['endpoint']}: {endpoint['count']}개")
                
                # 최근 데이터 샘플 확인
                sample_query = """
                SELECT 
                    api_provider,
                    endpoint,
                    request_params,
                    response_status,
                    created_at
                FROM api_raw_data
                ORDER BY created_at DESC
                LIMIT 5
                """
                
                samples = db_manager.fetch_all(sample_query)
                print("\n🔖 최근 API 호출 샘플 (5개):")
                for i, sample in enumerate(samples, 1):
                    print(f"  {i}. {sample['api_provider']} - {sample['endpoint']}")
                    print(f"     상태: {sample['response_status']} | 시간: {sample['created_at']}")
                    if sample['request_params']:
                        print(f"     파라미터: {str(sample['request_params'])[:100]}...")
                
                # KMA/기상청 관련 데이터 확인
                kma_query = """
                SELECT COUNT(*) as count
                FROM api_raw_data
                WHERE api_provider ILIKE '%kma%' OR api_provider ILIKE '%기상청%' OR api_provider ILIKE '%weather%'
                   OR endpoint ILIKE '%weather%' OR endpoint ILIKE '%기상%'
                """
                
                kma_result = db_manager.fetch_one(kma_query)
                kma_count = kma_result.get('count', 0) if kma_result else 0
                print(f"\n🌤️  기상청/날씨 관련 데이터: {kma_count}개")
                
        else:
            print("❌ api_raw_data 테이블이 존재하지 않거나 컬럼 정보를 가져올 수 없습니다.")
            
    except Exception as e:
        print(f"❌ 테이블 조회 실패: {e}")
    
    # 기상청 예보구역 관련 원본 데이터 저장 여부 확인
    print("\n🔍 기상청 예보구역 관련 원본 데이터 저장 여부:")
    
    forecast_query = """
    SELECT COUNT(*) as count
    FROM api_raw_data
    WHERE (endpoint ILIKE '%forecast%' OR endpoint ILIKE '%fct_shrt_reg%')
       OR (request_params::text ILIKE '%forecast%' OR request_params::text ILIKE '%reg%')
    """
    
    try:
        forecast_result = db_manager.fetch_one(forecast_query)
        forecast_count = forecast_result.get('count', 0) if forecast_result else 0
        
        if forecast_count > 0:
            print(f"✅ 예보구역 관련 원본 데이터 {forecast_count}개 발견")
            
            # 상세 내용 조회
            detail_query = """
            SELECT 
                api_provider,
                endpoint,
                request_params,
                response_status,
                created_at
            FROM api_raw_data
            WHERE (endpoint ILIKE '%forecast%' OR endpoint ILIKE '%fct_shrt_reg%')
               OR (request_params::text ILIKE '%forecast%' OR request_params::text ILIKE '%reg%')
            ORDER BY created_at DESC
            LIMIT 3
            """
            
            details = db_manager.fetch_all(detail_query)
            print("  📄 상세 내용:")
            for detail in details:
                print(f"    - {detail['api_provider']} | {detail['endpoint']} | {detail['created_at']}")
        else:
            print("❌ 예보구역 관련 원본 데이터가 저장되지 않았습니다.")
            
    except Exception as e:
        print(f"❌ 예보구역 데이터 조회 실패: {e}")


if __name__ == "__main__":
    check_api_raw_data_table()