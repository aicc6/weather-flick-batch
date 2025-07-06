#!/usr/bin/env python3
"""
단일 컨텐츠 타입 상세 정보 처리 테스트
작성일: 2025-07-06
목적: 오류 수정 후 단일 컨텐츠 타입 테스트
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

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_single_content():
    """단일 컨텐츠 테스트"""
    
    kto_client = UnifiedKTOClient()
    db_manager = get_extended_database_manager()
    
    # 테스트할 컨텐츠 정보 (관광지)
    content_id = "141105"
    content_type = "12"
    
    logger.info(f"🔍 컨텐츠 {content_id} (타입 {content_type}) 테스트 시작")
    
    results = {}
    
    # detailCommon2 테스트
    try:
        logger.info("  📋 detailCommon2 테스트...")
        result = await kto_client.collect_detail_common(content_id, content_type)
        results['detailCommon2'] = {'success': result is not None, 'data': result}
        logger.info(f"    {'✅ 성공' if result else '⚠️  데이터 없음'}")
    except Exception as e:
        logger.error(f"    ❌ 실패: {e}")
        results['detailCommon2'] = {'success': False, 'error': str(e)}
    
    await asyncio.sleep(0.5)
    
    # detailIntro2 테스트
    try:
        logger.info("  📋 detailIntro2 테스트...")
        result = await kto_client.collect_detail_intro(content_id, content_type)
        results['detailIntro2'] = {'success': result is not None, 'data': result}
        logger.info(f"    {'✅ 성공' if result else '⚠️  데이터 없음'}")
    except Exception as e:
        logger.error(f"    ❌ 실패: {e}")
        results['detailIntro2'] = {'success': False, 'error': str(e)}
    
    await asyncio.sleep(0.5)
    
    # detailInfo2 테스트
    try:
        logger.info("  📋 detailInfo2 테스트...")
        result = await kto_client.collect_detail_info(content_id, content_type)
        results['detailInfo2'] = {'success': result is not None, 'data': result}
        logger.info(f"    {'✅ 성공' if result else '⚠️  데이터 없음'}: {len(result) if result else 0}건")
    except Exception as e:
        logger.error(f"    ❌ 실패: {e}")
        results['detailInfo2'] = {'success': False, 'error': str(e)}
    
    await asyncio.sleep(0.5)
    
    # detailImage2 테스트
    try:
        logger.info("  📋 detailImage2 테스트...")
        result = await kto_client.collect_detail_images(content_id)
        results['detailImage2'] = {'success': result is not None, 'data': result}
        logger.info(f"    {'✅ 성공' if result else '⚠️  데이터 없음'}: {len(result) if result else 0}건")
    except Exception as e:
        logger.error(f"    ❌ 실패: {e}")
        results['detailImage2'] = {'success': False, 'error': str(e)}
    
    # 결과 요약
    success_count = sum(1 for r in results.values() if r['success'])
    logger.info(f"📊 결과: {success_count}/4 API 성공")
    
    # 상세 결과 출력
    for api_name, result in results.items():
        if result['success']:
            data = result['data']
            if isinstance(data, list):
                logger.info(f"  {api_name}: ✅ {len(data)}건")
            elif data:
                logger.info(f"  {api_name}: ✅ 1건")
            else:
                logger.info(f"  {api_name}: ⚠️  데이터 없음")
        else:
            error = result.get('error', '알 수 없는 오류')
            logger.info(f"  {api_name}: ❌ {error}")
    
    return results


async def main():
    """메인 실행 함수"""
    
    print("=== Weather Flick 단일 컨텐츠 타입 테스트 ===")
    print()
    
    results = await test_single_content()
    
    print()
    print("단일 컨텐츠 테스트가 완료되었습니다.")


if __name__ == "__main__":
    asyncio.run(main())