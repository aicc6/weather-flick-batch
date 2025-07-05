#!/usr/bin/env python3
"""
음식점 데이터 수집 문제 진단 스크립트

데이터가 어디서 누락되고 있는지 단계별로 확인합니다.
"""

import os
import sys
import asyncio
import logging
import uuid
from datetime import datetime
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(override=True)

from app.core.multi_api_key_manager import get_api_key_manager, APIProvider, reset_api_key_manager
from app.core.unified_api_client import get_unified_api_client, reset_unified_api_client
from app.processors.data_transformation_pipeline import get_transformation_pipeline
from app.core.database_manager_extension import get_extended_database_manager

# 매니저 리셋
reset_api_key_manager()
reset_unified_api_client()

# 로깅 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


async def test_raw_api_call():
    """1단계: 원본 API 호출 테스트"""
    print("\n" + "="*60)
    print("🔬 1단계: 원본 API 호출 테스트")
    print("="*60)
    
    api_client = get_unified_api_client()
    
    async with api_client:
        # 서울 음식점 1개만 조회
        response = await api_client.call_api(
            api_provider=APIProvider.KTO,
            endpoint="areaBasedList2",
            params={
                "MobileOS": "ETC",
                "MobileApp": "WeatherFlick",
                "_type": "json",
                "contentTypeId": "39",  # 음식점
                "areaCode": "1",        # 서울
                "numOfRows": 1,
                "pageNo": 1
            },
            store_raw=True,
            use_cache=False
        )
    
    if response.success:
        print(f"✅ API 호출 성공")
        print(f"   Raw Data ID: {response.raw_data_id}")
        print(f"   응답 크기: {len(str(response.data))} 문자")
        
        # 응답 구조 확인
        if "items" in response.data:
            items = response.data["items"]
            if "item" in items:
                item_list = items["item"]
                if isinstance(item_list, list):
                    item_count = len(item_list)
                else:
                    item_count = 1
                    item_list = [item_list]
                
                print(f"   아이템 수: {item_count}개")
                
                if item_count > 0:
                    first_item = item_list[0]
                    print(f"   첫 번째 아이템 키: {list(first_item.keys())}")
                    print(f"   contentid: {first_item.get('contentid')}")
                    print(f"   title: {first_item.get('title')}")
                    print(f"   contenttypeid: {first_item.get('contenttypeid')}")
                
                return response.raw_data_id, item_list
        
        print("❌ 응답에 items가 없습니다")
        print(f"   응답 키: {list(response.data.keys())}")
        return response.raw_data_id, []
    
    else:
        print(f"❌ API 호출 실패: {response.error}")
        return None, []


async def test_data_transformation(raw_data_id, expected_items):
    """2단계: 데이터 변환 테스트"""
    print("\n" + "="*60)
    print("🔄 2단계: 데이터 변환 테스트")
    print("="*60)
    
    if not raw_data_id:
        print("❌ Raw Data ID가 없어 변환 테스트를 건너뜁니다")
        return []
    
    pipeline = get_transformation_pipeline()
    
    try:
        result = await pipeline.transform_raw_data(raw_data_id)
        
        if result.success:
            print(f"✅ 데이터 변환 성공")
            print(f"   입력 데이터 수: {result.input_count}개")
            print(f"   출력 데이터 수: {result.output_count}개")
            print(f"   품질 점수: {result.quality_score:.1f}")
            
            if result.processed_data and len(result.processed_data) > 0:
                first_item = result.processed_data[0]
                print(f"   변환된 필드: {list(first_item.keys())}")
                print(f"   content_id: {first_item.get('content_id')}")
                print(f"   restaurant_name: {first_item.get('restaurant_name')}")
                print(f"   data_source: {first_item.get('data_source')}")
            
            return result.processed_data
        else:
            print(f"❌ 데이터 변환 실패")
            if result.errors:
                for error in result.errors:
                    print(f"   오류: {error}")
            return []
    
    except Exception as e:
        print(f"❌ 변환 중 예외 발생: {e}")
        return []


async def test_database_save(processed_data, raw_data_id=None):
    """3단계: 데이터베이스 저장 테스트"""
    print("\n" + "="*60)
    print("💾 3단계: 데이터베이스 저장 테스트")
    print("="*60)
    
    if not processed_data:
        print("❌ 변환된 데이터가 없어 저장 테스트를 건너뜁니다")
        return False
    
    db_manager = get_extended_database_manager()
    
    try:
        saved_count = 0
        for item in processed_data:
            # 메타데이터 추가 (실제 raw_data_id 사용)
            item["raw_data_id"] = raw_data_id if raw_data_id else str(uuid.uuid4())
            item["data_quality_score"] = 85.0
            item["last_sync_at"] = datetime.utcnow()
            
            # 음식점 데이터 저장 시도
            success = db_manager.upsert_restaurant(item)
            if success:
                saved_count += 1
                print(f"✅ 음식점 데이터 저장 성공: {item.get('restaurant_name')}")
            else:
                print(f"❌ 음식점 데이터 저장 실패: {item.get('restaurant_name')}")
        
        print(f"📊 총 {saved_count}/{len(processed_data)}개 저장 완료")
        return saved_count > 0
    
    except Exception as e:
        print(f"❌ 저장 중 예외 발생: {e}")
        return False


async def test_database_query():
    """4단계: 데이터베이스 조회 테스트"""
    print("\n" + "="*60)
    print("🔍 4단계: 데이터베이스 조회 테스트")
    print("="*60)
    
    db_manager = get_extended_database_manager()
    
    try:
        # restaurants 테이블 조회
        query = "SELECT COUNT(*) as count FROM restaurants"
        result = db_manager.fetch_one(query)
        
        if result:
            count = result["count"]
            print(f"📊 restaurants 테이블 총 레코드 수: {count:,}개")
            
            if count > 0:
                # 최근 데이터 조회
                recent_query = """
                SELECT content_id, restaurant_name, address, created_at 
                FROM restaurants 
                ORDER BY created_at DESC 
                LIMIT 5
                """
                recent_results = db_manager.fetch_all(recent_query)
                
                print(f"📋 최근 저장된 음식점 데이터:")
                for row in recent_results:
                    print(f"   - {row['restaurant_name']} (ID: {row['content_id']}, 생성: {row['created_at']})")
            
            return count > 0
        else:
            print("❌ 조회 결과가 없습니다")
            return False
    
    except Exception as e:
        print(f"❌ 조회 중 예외 발생: {e}")
        
        # 테이블 존재 확인
        try:
            table_check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'restaurants'
            )
            """
            exists_result = db_manager.fetch_one(table_check_query)
            
            if exists_result and exists_result["exists"]:
                print("✅ restaurants 테이블은 존재합니다")
                
                # 테이블 구조 확인
                structure_query = """
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'restaurants' 
                ORDER BY ordinal_position
                """
                columns = db_manager.fetch_all(structure_query)
                print("📋 테이블 구조:")
                for col in columns:
                    print(f"   - {col['column_name']}: {col['data_type']}")
            else:
                print("❌ restaurants 테이블이 존재하지 않습니다!")
        except Exception as table_error:
            print(f"❌ 테이블 확인 중 오류: {table_error}")
        
        return False


async def run_comprehensive_diagnosis():
    """종합 진단 실행"""
    print("\n" + "🔬" + " 음식점 데이터 수집 문제 종합 진단 " + "🔬")
    print("시작 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    test_results = {
        "api_call": False,
        "data_transformation": False,
        "database_save": False,
        "database_query": False
    }
    
    try:
        # 1단계: API 호출
        raw_data_id, expected_items = await test_raw_api_call()
        test_results["api_call"] = raw_data_id is not None and len(expected_items) > 0
        
        # 2단계: 데이터 변환
        processed_data = await test_data_transformation(raw_data_id, expected_items)
        test_results["data_transformation"] = len(processed_data) > 0
        
        # 3단계: 데이터베이스 저장
        save_success = await test_database_save(processed_data, raw_data_id)
        test_results["database_save"] = save_success
        
        # 4단계: 데이터베이스 조회
        query_success = await test_database_query()
        test_results["database_query"] = query_success
        
    except Exception as e:
        print(f"\n❌ 진단 중 예외 발생: {e}")
        import traceback
        traceback.print_exc()
    
    # 결과 요약
    print("\n" + "="*60)
    print("📊 진단 결과 요약")
    print("="*60)
    
    test_names = {
        "api_call": "1️⃣ API 호출",
        "data_transformation": "2️⃣ 데이터 변환", 
        "database_save": "3️⃣ 데이터베이스 저장",
        "database_query": "4️⃣ 데이터베이스 조회"
    }
    
    passed_tests = 0
    for test_key, test_name in test_names.items():
        status = "✅ 성공" if test_results[test_key] else "❌ 실패"
        print(f"{test_name}: {status}")
        if test_results[test_key]:
            passed_tests += 1
    
    print(f"\n📈 전체 결과: {passed_tests}/{len(test_results)} 단계 성공")
    
    # 문제 진단 및 해결책 제시
    print("\n💡 문제 진단 및 해결책:")
    
    if not test_results["api_call"]:
        print("🔴 API 호출 실패")
        print("   - KTO_API_KEY 환경 변수 확인")
        print("   - API 키 유효성 확인")
        print("   - 네트워크 연결 확인")
    
    elif not test_results["data_transformation"]:
        print("🟠 데이터 변환 실패")
        print("   - 음식점 데이터 변환 로직 확인")
        print("   - contentTypeId=39 처리 확인")
        print("   - 필드 매핑 확인")
    
    elif not test_results["database_save"]:
        print("🟡 데이터베이스 저장 실패")
        print("   - restaurants 테이블 존재 확인")
        print("   - upsert_restaurant 메서드 확인")
        print("   - 데이터베이스 연결 확인")
    
    elif not test_results["database_query"]:
        print("🔵 데이터베이스 조회 실패")
        print("   - 테이블 권한 확인")
        print("   - SQL 쿼리 문법 확인")
    
    else:
        print("🟢 모든 단계가 성공했습니다!")
        print("   - 음식점 데이터 수집 파이프라인이 정상 작동합니다")
    
    print("완료 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    return test_results


if __name__ == "__main__":
    asyncio.run(run_comprehensive_diagnosis())