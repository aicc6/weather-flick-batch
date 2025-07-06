#!/usr/bin/env python3
"""
병렬 처리 성능 테스트 스크립트
작성일: 2025-07-07
목적: 순차 처리 vs 병렬 처리 성능 비교
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
from app.core.database_manager_extension import get_extended_database_manager
from app.core.concurrent_api_manager import ConcurrencyConfig

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PerformanceTester:
    """성능 테스트 클래스"""
    
    def __init__(self):
        self.db_manager = get_extended_database_manager()
    
    async def get_sample_content_ids(self, content_type: str, table_name: str, limit: int = 20) -> list:
        """샘플 컨텐츠 ID 조회"""
        
        try:
            query = f"""
            SELECT content_id 
            FROM {table_name} 
            WHERE content_id IS NOT NULL
            ORDER BY created_at DESC
            LIMIT {limit}
            """
            
            results = self.db_manager.fetch_all(query)
            if results:
                return [row['content_id'] for row in results]
            else:
                logger.warning(f"⚠️ {table_name}에서 샘플 데이터를 찾을 수 없습니다.")
                return []
                
        except Exception as e:
            logger.error(f"❌ {table_name} 샘플 데이터 조회 실패: {e}")
            return []
    
    async def test_sequential_processing(self, content_ids: list, content_type: str):
        """순차 처리 테스트"""
        
        logger.info(f"🔄 순차 처리 테스트 시작: {len(content_ids)}개 컨텐츠")
        
        # 순차 처리용 클라이언트 (병렬 처리 비활성화)
        kto_client = UnifiedKTOClient(enable_parallel=False)
        
        start_time = time.time()
        
        result = await kto_client._collect_detailed_info_sequential(
            content_ids=content_ids,
            content_type_id=content_type,
            store_raw=True
        )
        
        duration = time.time() - start_time
        
        return {
            'method': 'sequential',
            'duration': duration,
            'content_count': len(content_ids),
            'successful_apis': result['detail_common'] + result['detail_intro'] + result['detail_info'] + result['detail_images'],
            'total_possible_apis': len(content_ids) * 4,
            'success_rate': len(result['successful_content_ids']) / len(content_ids) * 100 if content_ids else 0,
            'errors': len(result['errors']),
            'api_per_second': (len(content_ids) * 4) / duration if duration > 0 else 0
        }
    
    async def test_parallel_processing(self, content_ids: list, content_type: str, batch_size: int = 10):
        """병렬 처리 테스트"""
        
        logger.info(f"⚡ 병렬 처리 테스트 시작: {len(content_ids)}개 컨텐츠 (배치: {batch_size})")
        
        # 병렬 처리용 클라이언트 설정
        concurrency_config = ConcurrencyConfig(
            max_concurrent_kto=5,      # KTO API 동시 호출 수
            max_concurrent_total=8,    # 전체 동시 호출 수
            min_delay_between_calls=0.1,
            adaptive_delay=True,
            batch_size=batch_size
        )
        
        kto_client = UnifiedKTOClient(enable_parallel=True, concurrency_config=concurrency_config)
        
        start_time = time.time()
        
        result = await kto_client.collect_detailed_info_parallel(
            content_ids=content_ids,
            content_type_id=content_type,
            store_raw=True,
            batch_size=batch_size
        )
        
        duration = time.time() - start_time
        
        # 성능 통계 조회
        performance_stats = {}
        if kto_client.concurrent_manager:
            performance_stats = kto_client.concurrent_manager.get_performance_stats()
        
        return {
            'method': 'parallel',
            'duration': duration,
            'content_count': len(content_ids),
            'batch_size': batch_size,
            'successful_apis': result['detail_common'] + result['detail_intro'] + result['detail_info'] + result['detail_images'],
            'total_possible_apis': len(content_ids) * 4,
            'success_rate': len(result['successful_content_ids']) / len(content_ids) * 100 if content_ids else 0,
            'errors': len(result['errors']),
            'api_per_second': (len(content_ids) * 4) / duration if duration > 0 else 0,
            'concurrent_peaks': performance_stats.get('concurrent_peaks', {}),
            'average_response_time': performance_stats.get('average_response_time', 0),
            'circuit_breaker_trips': performance_stats.get('circuit_breaker_trips', 0)
        }
    
    async def run_performance_comparison(self, content_type: str = "12", table_name: str = "tourist_attractions"):
        """성능 비교 테스트 실행"""
        
        logger.info(f"=== 성능 비교 테스트 시작: {table_name} ===")
        
        # 샘플 데이터 조회
        sample_content_ids = await self.get_sample_content_ids(content_type, table_name, limit=20)
        
        if len(sample_content_ids) < 5:
            logger.error(f"❌ 충분한 샘플 데이터가 없습니다: {len(sample_content_ids)}개")
            return
        
        # 작은 샘플로 테스트 (5개)
        small_sample = sample_content_ids[:5]
        
        # 큰 샘플로 테스트 (20개)
        large_sample = sample_content_ids[:20]
        
        results = []
        
        logger.info("\n--- 소규모 테스트 (5개 컨텐츠) ---")
        
        # 순차 처리 (소규모)
        sequential_small = await self.test_sequential_processing(small_sample, content_type)
        results.append(sequential_small)
        
        # 병렬 처리 (소규모, 배치 5)
        parallel_small = await self.test_parallel_processing(small_sample, content_type, batch_size=5)
        results.append(parallel_small)
        
        logger.info("\n--- 대규모 테스트 (20개 컨텐츠) ---")
        
        # 순차 처리 (대규모)
        sequential_large = await self.test_sequential_processing(large_sample, content_type)
        results.append(sequential_large)
        
        # 병렬 처리 (대규모, 배치 10)
        parallel_large = await self.test_parallel_processing(large_sample, content_type, batch_size=10)
        results.append(parallel_large)
        
        # 결과 분석 및 출력
        self.print_performance_report(results)
        
        return results
    
    def print_performance_report(self, results):
        """성능 리포트 출력"""
        
        logger.info("\n" + "="*80)
        logger.info("🏆 성능 테스트 결과 요약")
        logger.info("="*80)
        
        for result in results:
            method = result['method']
            content_count = result['content_count']
            duration = result['duration']
            success_rate = result['success_rate']
            api_per_second = result['api_per_second']
            
            logger.info(f"\n📊 {method.upper()} 처리 ({content_count}개 컨텐츠):")
            logger.info(f"  ⏱️  처리 시간: {duration:.2f}초")
            logger.info(f"  ✅ 성공률: {success_rate:.1f}%")
            logger.info(f"  🚀 처리 속도: {api_per_second:.2f} API/초")
            logger.info(f"  🎯 성공한 API: {result['successful_apis']}/{result['total_possible_apis']}")
            logger.info(f"  ❌ 오류 수: {result['errors']}")
            
            if method == 'parallel':
                logger.info(f"  🔧 배치 크기: {result['batch_size']}")
                logger.info(f"  📈 동시 처리 피크: {result.get('concurrent_peaks', {})}")
                logger.info(f"  ⚡ 평균 응답시간: {result.get('average_response_time', 0):.3f}초")
        
        # 성능 개선 계산
        if len(results) >= 2:
            sequential_time = next(r['duration'] for r in results if r['method'] == 'sequential')
            parallel_time = next(r['duration'] for r in results if r['method'] == 'parallel')
            
            if parallel_time > 0:
                improvement = (sequential_time - parallel_time) / sequential_time * 100
                speedup = sequential_time / parallel_time
                
                logger.info(f"\n🎯 성능 개선 효과:")
                logger.info(f"  ⚡ 처리 시간 단축: {improvement:.1f}%")
                logger.info(f"  🚀 속도 향상: {speedup:.1f}배")
        
        logger.info("\n" + "="*80)


async def main():
    """메인 실행 함수"""
    
    print("=== Weather Flick 병렬 처리 성능 테스트 ===")
    print()
    
    tester = PerformanceTester()
    
    # 관광지 데이터로 성능 테스트
    await tester.run_performance_comparison(
        content_type="12",
        table_name="tourist_attractions"
    )
    
    print()
    print("성능 테스트가 완료되었습니다.")


if __name__ == "__main__":
    asyncio.run(main())