#!/usr/bin/env python3
"""
단일 컨텐츠에 대한 상세 API 호출 테스트
작성일: 2025-07-06
목적: KTO API detailCommon2, detailIntro2, detailInfo2, detailImage2 실제 호출 확인
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.collectors.unified_kto_client import UnifiedKTOClient
from app.core.database_manager_extension import get_extended_database_manager

# 로깅 설정 - DEBUG 레벨로 설정
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_single_content_api_calls():
    """단일 컨텐츠에 대한 상세 API 호출 테스트"""
    
    print("=== 단일 컨텐츠 상세 API 호출 테스트 ===")
    print()
    
    # 테스트할 content_id와 content_type_id
    test_content_id = "128776"  # 남산 케이블카
    test_content_type = "12"    # 관광지
    
    kto_client = UnifiedKTOClient()
    
    print(f"🔍 테스트 대상: content_id={test_content_id}, content_type={test_content_type}")
    print()
    
    # 1. detailCommon2 테스트
    print("1. detailCommon2 API 호출 테스트")
    try:
        result = await kto_client.collect_detail_common(test_content_id, test_content_type)
        if result:
            print(f"   ✅ 성공: {len(str(result))}자 데이터 반환")
            print(f"   📄 반환 데이터 키들: {list(result.keys()) if isinstance(result, dict) else 'dict가 아님'}")
        else:
            print("   ❌ 데이터 없음 또는 실패")
    except Exception as e:
        print(f"   💥 오류: {e}")
    
    await asyncio.sleep(1.0)
    
    # 2. detailIntro2 테스트
    print("\n2. detailIntro2 API 호출 테스트")
    try:
        result = await kto_client.collect_detail_intro(test_content_id, test_content_type)
        if result:
            print(f"   ✅ 성공: {len(str(result))}자 데이터 반환")
            print(f"   📄 반환 데이터 키들: {list(result.keys()) if isinstance(result, dict) else 'dict가 아님'}")
        else:
            print("   ❌ 데이터 없음 또는 실패")
    except Exception as e:
        print(f"   💥 오류: {e}")
    
    await asyncio.sleep(1.0)
    
    # 3. detailInfo2 테스트  
    print("\n3. detailInfo2 API 호출 테스트")
    try:
        result = await kto_client.collect_detail_info(test_content_id, test_content_type)
        if result:
            print(f"   ✅ 성공: {len(result)}건 데이터 반환")
            if len(result) > 0:
                print(f"   📄 첫 번째 데이터 키들: {list(result[0].keys()) if isinstance(result[0], dict) else 'dict가 아님'}")
        else:
            print("   ❌ 데이터 없음 또는 실패")
    except Exception as e:
        print(f"   💥 오류: {e}")
    
    await asyncio.sleep(1.0)
    
    # 4. detailImage2 테스트
    print("\n4. detailImage2 API 호출 테스트")
    try:
        result = await kto_client.collect_detail_images(test_content_id)
        if result:
            print(f"   ✅ 성공: {len(result)}건 이미지 데이터 반환")
            if len(result) > 0:
                print(f"   📄 첫 번째 이미지 데이터 키들: {list(result[0].keys()) if isinstance(result[0], dict) else 'dict가 아님'}")
        else:
            print("   ❌ 데이터 없음 또는 실패")
    except Exception as e:
        print(f"   💥 오류: {e}")
    
    print()
    print("=== 테스트 완료 ===")


async def main():
    """메인 실행 함수"""
    await test_single_content_api_calls()


if __name__ == "__main__":
    asyncio.run(main())