#!/usr/bin/env python3
"""
소규모 병렬 처리 성능 테스트
API 키 한도 제한을 고려한 축소 테스트
"""

import os
import sys
import asyncio
import time
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
from app.core.concurrent_api_manager import ConcurrencyConfig

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_small_parallel():
    """소규모 병렬 처리 테스트"""
    
    print("=== 소규모 병렬 처리 테스트 ===")
    
    # 테스트용 샘플 컨텐츠 ID (실제 존재하는 ID)
    sample_content_ids = ["141105", "2733099"]  # 2개만 테스트
    content_type = "12"  # 관광지
    
    print(f"테스트 컨텐츠 ID: {sample_content_ids}")
    
    # 1. 순차 처리 테스트
    print("\n--- 순차 처리 테스트 ---")
    sequential_client = UnifiedKTOClient(enable_parallel=False)
    
    start_time = time.time()
    sequential_result = await sequential_client._collect_detailed_info_sequential(
        content_ids=sample_content_ids,
        content_type_id=content_type,
        store_raw=True
    )
    sequential_duration = time.time() - start_time
    
    print(f"순차 처리 결과:")
    print(f"  소요 시간: {sequential_duration:.2f}초")
    print(f"  성공 API: {sequential_result['detail_common'] + sequential_result['detail_intro'] + sequential_result['detail_info'] + sequential_result['detail_images']}")
    print(f"  성공 컨텐츠: {len(sequential_result['successful_content_ids'])}")
    print(f"  오류 수: {len(sequential_result['errors'])}")
    
    # 잠깐 대기 (API 키 회복)
    print("\n⏳ API 키 회복을 위해 30초 대기...")
    await asyncio.sleep(30)
    
    # 2. 병렬 처리 테스트
    print("\n--- 병렬 처리 테스트 ---")
    
    concurrency_config = ConcurrencyConfig(
        max_concurrent_kto=2,      # 적은 동시 호출 수
        max_concurrent_total=3,    
        min_delay_between_calls=0.5,  # 더 긴 지연
        adaptive_delay=True,
        batch_size=2
    )
    
    parallel_client = UnifiedKTOClient(enable_parallel=True, concurrency_config=concurrency_config)
    
    start_time = time.time()
    parallel_result = await parallel_client.collect_detailed_info_parallel(
        content_ids=sample_content_ids,
        content_type_id=content_type,
        store_raw=True,
        batch_size=2
    )
    parallel_duration = time.time() - start_time
    
    print(f"병렬 처리 결과:")
    print(f"  소요 시간: {parallel_duration:.2f}초")
    print(f"  성공 API: {parallel_result['detail_common'] + parallel_result['detail_intro'] + parallel_result['detail_info'] + parallel_result['detail_images']}")
    print(f"  성공 컨텐츠: {len(parallel_result['successful_content_ids'])}")
    print(f"  오류 수: {len(parallel_result['errors'])}")
    
    # 성능 통계
    if parallel_client.concurrent_manager:
        stats = parallel_client.concurrent_manager.get_performance_stats()
        print(f"  평균 응답시간: {stats.get('average_response_time', 0):.3f}초")
        print(f"  동시 처리 피크: {stats.get('concurrent_peaks', {})}")
    
    # 성능 비교
    if sequential_duration > 0 and parallel_duration > 0:
        improvement = (sequential_duration - parallel_duration) / sequential_duration * 100
        speedup = sequential_duration / parallel_duration
        
        print(f"\n🎯 성능 비교:")
        print(f"  처리 시간 차이: {sequential_duration - parallel_duration:.2f}초")
        print(f"  성능 개선: {improvement:.1f}%")
        print(f"  속도 향상: {speedup:.1f}배")
    
    print("\n✅ 소규모 테스트 완료")


if __name__ == "__main__":
    asyncio.run(test_small_parallel())