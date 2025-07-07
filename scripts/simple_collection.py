#!/usr/bin/env python3
"""
간단한 관광지 데이터 수집 스크립트
작성일: 2025-07-07
목적: API 키 리셋 후 직접 KTO 클라이언트로 데이터 수집
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# API 키 매니저 리셋
from app.core.multi_api_key_manager import reset_api_key_manager, get_api_key_manager, APIProvider
from app.core.unified_api_client import reset_unified_api_client
reset_api_key_manager()
reset_unified_api_client()

from app.collectors.unified_kto_client import get_unified_kto_client
from app.core.logger import get_logger

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = get_logger(__name__)


async def reset_api_keys():
    """API 키 상태 리셋"""
    print("🔧 API 키 상태 리셋 중...")
    
    try:
        manager = get_api_key_manager()
        
        # 모든 키 강제 활성화
        reset_count = 0
        for provider in [APIProvider.KTO, APIProvider.KMA]:
            if provider in manager.api_keys:
                for key_info in manager.api_keys[provider]:
                    if not key_info.is_active or key_info.error_count >= 5:
                        key_info.is_active = True
                        key_info.error_count = 0
                        key_info.rate_limit_reset_time = None
                        key_info.last_error_time = None
                        key_info.current_usage = 0
                        reset_count += 1
                        print(f"  ✅ {provider.value} 키 {key_info.key[:10]}... 활성화됨")
        
        # 캐시 업데이트
        try:
            manager._save_to_cache()
            print(f"  ✅ {reset_count}개 키 상태 저장 완료")
        except Exception as e:
            print(f"  ⚠️ 캐시 저장 실패: {e}")
        
        return reset_count > 0
        
    except Exception as e:
        print(f"❌ API 키 리셋 실패: {e}")
        return False


async def simple_collection():
    """간단한 관광지 데이터 수집"""
    print("\n" + "="*60)
    print("🏛️ 간단한 관광지 데이터 수집")
    print("="*60)
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. API 키 리셋
    if not await reset_api_keys():
        print("❌ API 키 리셋에 실패했습니다.")
        return False
    
    # 2. 수집 설정
    print("\n📋 수집 설정:")
    print("• 컨텐츠 타입: 관광지(12), 숙박(32), 음식점(39)")
    print("• 지역: 서울, 부산, 제주, 경기 (주요 4개 지역)")
    print("• 상세 정보: 기본정보만")
    print("• 예상 시간: 30분-1시간")
    print("• 예상 데이터: 5,000-10,000건")
    
    # 사용자 확인
    response = input("\n🚀 수집을 시작하시겠습니까? (y/n): ").lower()
    if response not in ['y', 'yes']:
        print("❌ 수집을 취소했습니다.")
        return False
    
    # 3. 데이터 수집 실행
    print("\n🚀 데이터 수집 시작...")
    start_time = datetime.now()
    
    try:
        client = get_unified_kto_client()
        
        # 주요 컨텐츠 타입과 지역으로 제한된 수집
        result = await client.collect_all_data(
            content_types=["12", "32", "39"],  # 관광지, 숙박, 음식점
            area_codes=["1", "6", "39", "31"],  # 서울, 부산, 제주, 경기
            store_raw=True,
            auto_transform=True,
            include_new_apis=False,  # 신규 API 제외 (안정성)
            include_hierarchical_regions=False,  # 계층적 지역코드 제외
            use_priority_sorting=False
        )
        
        execution_time = datetime.now() - start_time
        
        print("\n" + "="*60)
        if result.get('status') == 'completed':
            print("🎉 관광지 데이터 수집 완료!")
            print("="*60)
            
            # 결과 상세 정보
            total_raw = result.get('total_raw_records', 0)
            total_processed = result.get('total_processed_records', 0)
            print(f"📥 원본 데이터: {total_raw:,}건")
            print(f"📊 처리된 데이터: {total_processed:,}건")
            print(f"⏱️ 실행 시간: {execution_time}")
            print(f"🔄 배치 ID: {result.get('sync_batch_id', 'N/A')}")
            
            # 컨텐츠 타입별 결과
            content_results = result.get('content_types_collected', {})
            if content_results:
                print(f"\n📋 컨텐츠 타입별 수집 결과:")
                content_names = {
                    "12": "관광지", "32": "숙박", "39": "음식점"
                }
                
                for content_type, data in content_results.items():
                    name = content_names.get(content_type, f"타입{content_type}")
                    raw_count = data.get('total_raw_records', 0)
                    processed_count = data.get('total_processed_records', 0)
                    print(f"  • {name}: 원본 {raw_count:,}건 → 처리 {processed_count:,}건")
                    
                    # 지역별 결과
                    area_results = data.get('area_results', {})
                    if area_results:
                        area_names = {"1": "서울", "6": "부산", "39": "제주", "31": "경기"}
                        for area_code, area_data in area_results.items():
                            area_name = area_names.get(area_code, f"지역{area_code}")
                            area_count = area_data.get('total_processed_records', 0)
                            if area_count > 0:
                                print(f"    - {area_name}: {area_count:,}건")
            
            print(f"\n📈 다음 단계:")
            print("1. 전체 수집: python scripts/dev_quick_collection.py")
            print("2. 데이터 현황: python scripts/analyze_database_direct.py")
            print("3. 우선순위 수집: python scripts/collect_with_priority.py")
            
            return True
            
        else:
            print("❌ 관광지 데이터 수집 실패")
            print("="*60)
            print(f"상태: {result.get('status', 'unknown')}")
            print(f"이유: {result.get('reason', 'Unknown error')}")
            
            errors = result.get('errors', [])
            if errors:
                print(f"\n상세 오류:")
                for error in errors[:3]:  # 최대 3개만 표시
                    print(f"  - {error}")
            
            return False
        
    except Exception as e:
        print(f"\n❌ 수집 실행 중 오류 발생: {e}")
        logger.error(f"Collection execution error: {e}", exc_info=True)
        return False
    
    finally:
        print("="*60)


async def main():
    """메인 실행 함수"""
    success = await simple_collection()
    
    if success:
        print("\n🎉 간단 수집이 성공적으로 완료되었습니다!")
        print("이제 전체 대량 수집을 실행하거나 현재 데이터를 확인할 수 있습니다.")
    else:
        print("\n❌ 간단 수집에 실패했습니다.")
        print("API 키 설정이나 네트워크 연결을 확인해주세요.")


if __name__ == "__main__":
    asyncio.run(main())