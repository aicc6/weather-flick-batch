#!/usr/bin/env python3
"""
메모리 최적화 테스트 스크립트

메모리 효율성 개선 효과를 측정합니다.
"""

import os
import sys
import asyncio
import time
import random
from pathlib import Path
from typing import List, Dict, Any

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.core.memory_optimizer import (
    get_memory_optimizer, 
    MemoryConfig, 
    reset_memory_optimizer
)
from app.collectors.unified_kto_client import UnifiedKTOClient
from app.core.concurrent_api_manager import ConcurrencyConfig
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def generate_large_dataset(size: int = 1000) -> List[Dict[str, Any]]:
    """메모리 테스트용 대용량 데이터셋 생성"""
    
    dataset = []
    
    for i in range(size):
        # 메모리를 많이 사용하는 더미 데이터
        data = {
            'id': f'test_{i:06d}',
            'content': 'x' * 1000,  # 1KB 문자열
            'metadata': {
                'numbers': list(range(100)),  # 숫자 배열
                'nested': {
                    'data': ['item'] * 50,
                    'values': [random.random() for _ in range(50)]
                }
            },
            'large_text': 'Lorem ipsum ' * 200  # 약 2KB 텍스트
        }
        dataset.append(data)
    
    return dataset


def memory_intensive_processor(item: Dict[str, Any]) -> Dict[str, Any]:
    """메모리를 많이 사용하는 처리 함수"""
    
    # 추가 메모리 할당
    temp_data = []
    for i in range(100):
        temp_data.append({
            'index': i,
            'data': item['content'] * 2,
            'processed': True
        })
    
    # 처리 결과 (원본보다 작음)
    result = {
        'id': item['id'],
        'processed_content': item['content'][:100],
        'summary': f"Processed {len(temp_data)} items"
    }
    
    # temp_data는 자동으로 가비지 컬렉션됨
    return result


async def test_memory_optimization():
    """메모리 최적화 테스트"""
    
    print("=== 메모리 최적화 테스트 ===")
    
    # 메모리 최적화 설정
    memory_config = MemoryConfig(
        warning_threshold_mb=100,
        critical_threshold_mb=200,
        default_chunk_size=50,
        adaptive_chunking=True,
        gc_frequency=50,
        auto_gc=True,
        monitor_interval=5,
        enable_monitoring=True
    )
    
    # 메모리 최적화기 초기화
    optimizer = get_memory_optimizer(memory_config)
    optimizer.start_monitoring()
    
    try:
        # 1. 기본 메모리 상태 확인
        print("\n--- 초기 메모리 상태 ---")
        initial_stats = optimizer.get_memory_report()
        print(f"초기 메모리: {initial_stats['memory_stats']['current_memory_mb']:.1f}MB")
        
        # 2. 대용량 데이터셋 생성
        print("\n--- 대용량 데이터셋 생성 ---")
        dataset_size = 5000
        
        with optimizer.memory_context("dataset_generation"):
            dataset = generate_large_dataset(dataset_size)
        
        generation_stats = optimizer.get_memory_report()
        print(f"데이터셋 생성 후: {generation_stats['memory_stats']['current_memory_mb']:.1f}MB")
        print(f"데이터셋 크기: {len(dataset)}개 항목")
        
        # 3. 메모리 최적화 없이 처리 (비교군)
        print("\n--- 메모리 최적화 없이 처리 ---")
        start_time = time.time()
        
        unoptimized_results = []
        with optimizer.memory_context("unoptimized_processing"):
            for item in dataset:
                result = memory_intensive_processor(item)
                unoptimized_results.append(result)
        
        unoptimized_duration = time.time() - start_time
        unoptimized_stats = optimizer.get_memory_report()
        
        print(f"비최적화 처리 완료: {unoptimized_duration:.2f}초")
        print(f"최대 메모리: {unoptimized_stats['memory_stats']['peak_memory_mb']:.1f}MB")
        print(f"GC 횟수: {unoptimized_stats['gc_stats']['collections']}")
        
        # 메모리 정리
        del unoptimized_results
        optimizer._force_garbage_collection()
        
        # 잠깐 대기
        await asyncio.sleep(2)
        
        # 4. 메모리 최적화 청크 처리
        print("\n--- 메모리 최적화 청크 처리 ---")
        start_time = time.time()
        
        optimized_results = []
        with optimizer.memory_context("optimized_processing"):
            for chunk in optimizer.chunk_iterator(dataset, chunk_size=100):
                chunk_results = []
                for item in chunk:
                    result = memory_intensive_processor(item)
                    chunk_results.append(result)
                optimized_results.extend(chunk_results)
                
                # 청크 처리 후 메모리 정리
                del chunk_results
        
        optimized_duration = time.time() - start_time
        optimized_stats = optimizer.get_memory_report()
        
        print(f"최적화 처리 완료: {optimized_duration:.2f}초")
        print(f"최대 메모리: {optimized_stats['memory_stats']['peak_memory_mb']:.1f}MB")
        print(f"GC 횟수: {optimized_stats['gc_stats']['collections']}")
        
        # 5. 스트리밍 처리 테스트
        print("\n--- 스트리밍 처리 테스트 ---")
        start_time = time.time()
        
        streaming_count = 0
        with optimizer.memory_context("streaming_processing"):
            for result in optimizer.streaming_processor(
                iter(dataset), 
                memory_intensive_processor, 
                batch_size=50
            ):
                streaming_count += 1
                # 결과를 저장하지 않고 즉시 처리
        
        streaming_duration = time.time() - start_time
        streaming_stats = optimizer.get_memory_report()
        
        print(f"스트리밍 처리 완료: {streaming_duration:.2f}초")
        print(f"처리된 항목: {streaming_count}개")
        print(f"최대 메모리: {streaming_stats['memory_stats']['peak_memory_mb']:.1f}MB")
        print(f"GC 횟수: {streaming_stats['gc_stats']['collections']}")
        
        # 6. 결과 비교 및 분석
        print("\n--- 성능 비교 결과 ---")
        
        memory_savings = (
            unoptimized_stats['memory_stats']['peak_memory_mb'] - 
            optimized_stats['memory_stats']['peak_memory_mb']
        )
        
        print(f"메모리 절약량: {memory_savings:.1f}MB")
        print(f"메모리 효율성: {memory_savings / unoptimized_stats['memory_stats']['peak_memory_mb'] * 100:.1f}% 개선")
        
        # 처리 시간 비교
        time_difference = optimized_duration - unoptimized_duration
        if time_difference < 0:
            print(f"처리 시간: {abs(time_difference):.2f}초 단축")
        else:
            print(f"처리 시간: {time_difference:.2f}초 증가 (메모리 최적화 오버헤드)")
        
        # 7. 최종 메모리 보고서
        print("\n--- 최종 메모리 보고서 ---")
        final_report = optimizer.get_memory_report()
        
        print(f"현재 메모리: {final_report['memory_stats']['current_memory_mb']:.1f}MB")
        print(f"피크 메모리: {final_report['memory_stats']['peak_memory_mb']:.1f}MB")
        print(f"총 GC 횟수: {final_report['gc_stats']['collections']}")
        print(f"경고 발생: {final_report['memory_stats']['warning_count']}회")
        print(f"위험 발생: {final_report['memory_stats']['critical_count']}회")
        
        print("\n권장사항:")
        for recommendation in final_report['recommendations']:
            print(f"  - {recommendation}")
        
    finally:
        # 정리
        optimizer.stop_monitoring()
        reset_memory_optimizer()
        
        print("\n✅ 메모리 최적화 테스트 완료")


async def test_real_api_memory():
    """실제 API 호출 메모리 테스트"""
    
    print("\n=== 실제 API 메모리 테스트 ===")
    
    # 메모리 최적화기 설정
    memory_config = MemoryConfig(
        warning_threshold_mb=150,
        critical_threshold_mb=300,
        default_chunk_size=10,
        adaptive_chunking=True
    )
    
    optimizer = get_memory_optimizer(memory_config)
    optimizer.start_monitoring()
    
    try:
        # 샘플 컨텐츠 ID (적은 수로 테스트)
        sample_content_ids = ["141105", "2733099", "3112040"]
        
        print(f"API 테스트 컨텐츠: {len(sample_content_ids)}개")
        
        # 병렬 처리 + 메모리 최적화
        concurrency_config = ConcurrencyConfig(
            max_concurrent_kto=2,
            max_concurrent_total=3,
            min_delay_between_calls=1.0,
            batch_size=2
        )
        
        kto_client = UnifiedKTOClient(
            enable_parallel=True, 
            concurrency_config=concurrency_config
        )
        
        with optimizer.memory_context("api_processing"):
            result = await kto_client.collect_detailed_info_parallel(
                content_ids=sample_content_ids,
                content_type_id="12",
                store_raw=True,
                batch_size=2
            )
        
        # 메모리 보고서
        api_report = optimizer.get_memory_report()
        
        print(f"API 처리 완료:")
        print(f"  성공 API: {result['detail_common'] + result['detail_intro'] + result['detail_info'] + result['detail_images']}")
        print(f"  메모리 사용: {api_report['memory_stats']['current_memory_mb']:.1f}MB")
        print(f"  피크 메모리: {api_report['memory_stats']['peak_memory_mb']:.1f}MB")
        
    except Exception as e:
        print(f"API 테스트 오류: {e}")
    
    finally:
        optimizer.stop_monitoring()
        reset_memory_optimizer()


async def main():
    """메인 실행 함수"""
    
    print("=== Weather Flick 메모리 최적화 테스트 ===")
    print()
    
    try:
        # 메모리 최적화 테스트
        await test_memory_optimization()
        
        # 실제 API 메모리 테스트 (API 키가 있는 경우)
        if os.getenv('KTO_API_KEY'):
            await test_real_api_memory()
        else:
            print("⚠️ KTO_API_KEY가 없어 실제 API 테스트를 건너뜁니다.")
        
        print("\n✅ 모든 테스트가 완료되었습니다.")
        
    except Exception as e:
        print(f"❌ 테스트 실행 오류: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())