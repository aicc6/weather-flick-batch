#!/usr/bin/env python3
"""
선택적 저장 시스템 데모

Phase 2.1에서 구현한 선택적 저장 시스템을 실제로 테스트하고 
성능을 측정하는 데모 스크립트입니다.
"""

import sys
import os
import time
import asyncio
import random
from datetime import datetime, timedelta
from typing import Dict, Any, List

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.selective_storage_manager import StorageRequest, get_storage_manager
from app.core.async_storage_queue import initialize_async_storage, shutdown_async_storage, get_async_storage_queue
from app.core.api_client_extension import APIClientStorageExtension, StorageEnabledAPIClient


class SelectiveStorageDemo:
    """선택적 저장 시스템 데모"""
    
    def __init__(self):
        """데모 초기화"""
        self.storage_manager = get_storage_manager()
        self.async_queue = None
        self.demo_results = {}
        
        print("🚀 선택적 저장 시스템 데모 초기화")
    
    def run_demo(self):
        """전체 데모 실행"""
        print("\n" + "="*80)
        print("📊 선택적 저장 시스템 데모 시작")
        print("="*80)
        
        try:
            # 1. 저장 정책 엔진 테스트
            self.demo_storage_policy_engine()
            
            # 2. 동기 저장 성능 테스트
            self.demo_sync_storage_performance()
            
            # 3. 비동기 저장 시스템 테스트
            self.demo_async_storage_system()
            
            # 4. API 클라이언트 통합 테스트
            self.demo_api_client_integration()
            
            # 5. 대용량 배치 처리 테스트
            self.demo_batch_processing()
            
            # 6. 종합 성능 분석
            self.analyze_overall_performance()
            
        except Exception as e:
            print(f"❌ 데모 실행 중 오류 발생: {e}")
            raise
        finally:
            # 정리
            if self.async_queue:
                shutdown_async_storage()
    
    def demo_storage_policy_engine(self):
        """저장 정책 엔진 데모"""
        print("\n🎯 1. 저장 정책 엔진 테스트")
        print("-" * 50)
        
        test_cases = [
            {
                "name": "KMA 예보구역 (정상)",
                "provider": "KMA",
                "endpoint": "fct_shrt_reg",
                "size_bytes": 500_000,
                "status_code": 200
            },
            {
                "name": "KMA 예보구역 (대용량)",
                "provider": "KMA", 
                "endpoint": "fct_shrt_reg",
                "size_bytes": 10_000_000,  # 10MB
                "status_code": 200
            },
            {
                "name": "KTO 관광지 (정상)",
                "provider": "KTO",
                "endpoint": "areaBasedList2",
                "size_bytes": 2_000_000,  # 2MB
                "status_code": 200
            },
            {
                "name": "KMA 오류 응답",
                "provider": "KMA",
                "endpoint": "fct_shrt_reg",
                "size_bytes": 1000,
                "status_code": 500
            },
            {
                "name": "비활성화 엔드포인트",
                "provider": "KMA",
                "endpoint": "health",
                "size_bytes": 100,
                "status_code": 200
            }
        ]
        
        policy_results = []
        
        for test_case in test_cases:
            storage_request = StorageRequest(
                provider=test_case["provider"],
                endpoint=test_case["endpoint"],
                request_url="http://demo.com/api",
                request_params={"demo": "test"},
                response_data={"test": "data"},
                response_size_bytes=test_case["size_bytes"],
                status_code=test_case["status_code"],
                execution_time_ms=100,
                created_at=datetime.now()
            )
            
            start_time = time.time()
            should_store, reason, metadata = self.storage_manager.should_store_response(storage_request)
            decision_time_ms = (time.time() - start_time) * 1000
            
            result = {
                "test_case": test_case["name"],
                "should_store": should_store,
                "reason": reason,
                "decision_time_ms": decision_time_ms,
                "ttl_days": metadata.get("ttl_days", "N/A"),
                "priority": metadata.get("priority", "N/A")
            }
            
            policy_results.append(result)
            
            status_icon = "✅" if should_store else "❌"
            print(f"{status_icon} {test_case['name']:<25} | "
                  f"저장: {should_store:<5} | "
                  f"TTL: {str(metadata.get('ttl_days', 'N/A')):<4} | "
                  f"우선순위: {metadata.get('priority', 'N/A'):<3} | "
                  f"{decision_time_ms:.2f}ms")
        
        self.demo_results["policy_engine"] = policy_results
        
        # 정책 엔진 통계
        engine_stats = self.storage_manager.policy_engine.get_statistics()
        print(f"\n📈 정책 엔진 통계:")
        print(f"   • 결정 횟수: {engine_stats['decisions_made']}")
        print(f"   • 승인률: {engine_stats.get('approval_rate', 0):.1f}%")
        print(f"   • 거부율: {engine_stats.get('rejection_rate', 0):.1f}%")
    
    def demo_sync_storage_performance(self):
        """동기 저장 성능 데모"""
        print("\n⚡ 2. 동기 저장 성능 테스트")
        print("-" * 50)
        
        test_requests = []
        for i in range(50):  # 50개 요청 생성
            storage_request = StorageRequest(
                provider="KMA",
                endpoint="fct_shrt_reg",
                request_url=f"http://demo.com/api/request_{i}",
                request_params={"demo": f"test_{i}"},
                response_data={"result": f"data_{i}", "items": list(range(10))},
                response_size_bytes=random.randint(1000, 100_000),
                status_code=200 if random.random() > 0.1 else 500,  # 10% 오류율
                execution_time_ms=random.uniform(50, 200),
                created_at=datetime.now(),
                request_id=f"demo_sync_{i}"
            )
            test_requests.append(storage_request)
        
        print(f"📦 {len(test_requests)}개 동기 저장 요청 처리 중...")
        
        start_time = time.time()
        sync_results = []
        
        for request in test_requests:
            result = self.storage_manager.process_storage_request(request)
            sync_results.append(result)
        
        total_time = time.time() - start_time
        
        # 결과 분석
        stored_count = sum(1 for r in sync_results if r.get("storage_success", False))
        decision_count = sum(1 for r in sync_results if r["should_store"])
        avg_process_time = sum(r["process_time_ms"] for r in sync_results) / len(sync_results)
        
        print(f"✅ 동기 저장 완료:")
        print(f"   • 전체 처리 시간: {total_time:.2f}초")
        print(f"   • 저장 결정: {decision_count}/{len(test_requests)}개")
        print(f"   • 실제 저장: {stored_count}/{decision_count}개")
        print(f"   • 평균 처리 시간: {avg_process_time:.2f}ms/요청")
        print(f"   • 초당 처리량: {len(test_requests)/total_time:.1f} 요청/초")
        
        self.demo_results["sync_storage"] = {
            "total_requests": len(test_requests),
            "stored_count": stored_count,
            "decision_count": decision_count,
            "total_time_sec": total_time,
            "avg_process_time_ms": avg_process_time,
            "throughput_rps": len(test_requests)/total_time
        }
    
    def demo_async_storage_system(self):
        """비동기 저장 시스템 데모"""
        print("\n🔄 3. 비동기 저장 시스템 테스트")
        print("-" * 50)
        
        # 비동기 큐 초기화
        self.async_queue = initialize_async_storage(
            queue_size=500,
            worker_count=3,
            batch_size=20,
            flush_interval_seconds=2
        )
        
        print("📡 비동기 저장 큐 초기화 완료")
        
        # 비동기 저장 요청 생성 및 전송
        async_requests = []
        for i in range(100):  # 100개 요청
            storage_request = StorageRequest(
                provider=random.choice(["KMA", "KTO", "WEATHER"]),
                endpoint=random.choice(["fct_shrt_reg", "areaBasedList2", "forecast"]),
                request_url=f"http://demo.com/async/request_{i}",
                request_params={"demo": f"async_test_{i}"},
                response_data={"async_result": f"data_{i}", "timestamp": datetime.now().isoformat()},
                response_size_bytes=random.randint(5000, 500_000),
                status_code=200 if random.random() > 0.05 else 500,  # 5% 오류율
                execution_time_ms=random.uniform(30, 150),
                created_at=datetime.now(),
                request_id=f"demo_async_{i}"
            )
            async_requests.append(storage_request)
        
        print(f"📤 {len(async_requests)}개 비동기 저장 요청 큐에 추가 중...")
        
        # 큐에 요청 추가
        start_time = time.time()
        queued_count = 0
        
        for request in async_requests:
            priority = random.choice([1, 1, 2, 2, 2, 3])  # 가중 우선순위
            if self.async_queue.enqueue(request, priority=priority):
                queued_count += 1
        
        enqueue_time = time.time() - start_time
        
        print(f"✅ 큐 추가 완료: {queued_count}/{len(async_requests)}개 ({enqueue_time:.2f}초)")
        
        # 처리 대기
        print("⏳ 비동기 처리 대기 중... (10초)")
        
        for i in range(10):
            time.sleep(1)
            stats = self.async_queue.get_statistics()
            remaining = stats["queue_sizes"]["total"]
            print(f"   📊 진행률: {i+1}/10초, 대기열: {remaining}개", end="\r")
        
        print("\n")
        
        # 최종 통계
        final_stats = self.async_queue.get_statistics()
        health = self.async_queue.health_check()
        
        print(f"📈 비동기 저장 통계:")
        print(f"   • 큐 추가: {final_stats['queued_requests']}개")
        print(f"   • 처리 완료: {final_stats['processed_requests']}개")
        print(f"   • 성공률: {final_stats['success_rate']:.1f}%")
        print(f"   • 큐 사용률: {final_stats['queue_utilization']:.1f}%")
        print(f"   • 시스템 상태: {'✅ 정상' if health['healthy'] else '⚠️ 경고'}")
        
        self.demo_results["async_storage"] = final_stats
    
    def demo_api_client_integration(self):
        """API 클라이언트 통합 데모"""
        print("\n🔗 4. API 클라이언트 통합 테스트")
        print("-" * 50)
        
        # 저장 기능 통합 클라이언트 생성
        demo_client = StorageEnabledAPIClient("DEMO_API", enable_async_storage=True)
        
        # 가상 API 함수들
        def successful_api_call():
            time.sleep(random.uniform(0.05, 0.15))  # 50-150ms 지연
            return {
                "status": "success",
                "data": [{"id": i, "value": f"item_{i}"} for i in range(10)],
                "timestamp": datetime.now().isoformat()
            }
        
        def failing_api_call():
            time.sleep(random.uniform(0.02, 0.08))  # 20-80ms 지연
            raise ConnectionError("Demo connection error")
        
        print("🧪 다양한 API 호출 시나리오 테스트:")
        
        # 성공적인 API 호출들
        success_results = []
        for i in range(20):
            try:
                api_result, storage_result = demo_client.execute_api_call_with_storage(
                    endpoint="demo_endpoint",
                    api_call_func=successful_api_call,
                    request_params={"test_id": i},
                    priority=random.choice([1, 2, 3])
                )
                success_results.append(storage_result)
                print(f"   ✅ 성공 호출 {i+1}: 저장={storage_result.get('storage_attempted', False)}")
            except Exception as e:
                print(f"   ❌ 예상치 못한 오류: {e}")
        
        # 실패하는 API 호출들  
        error_results = []
        for i in range(5):
            try:
                api_result, storage_result = demo_client.execute_api_call_with_storage(
                    endpoint="demo_endpoint",
                    api_call_func=failing_api_call,
                    request_params={"error_test_id": i},
                    priority=1,  # 오류는 높은 우선순위
                    force_sync=True  # 동기 저장으로 테스트
                )
            except ConnectionError:
                # 예상된 오류
                print(f"   ⚠️ 예상된 오류 {i+1}: 오류 데이터 저장됨")
        
        # 클라이언트 통계
        client_stats = demo_client.get_storage_statistics()
        
        print(f"\n📊 API 클라이언트 통계:")
        print(f"   • API 호출 횟수: {client_stats['api_calls']}")
        print(f"   • 저장 시도: {client_stats['storage_attempts']}")
        print(f"   • 저장 성공: {client_stats['storage_successes']}")
        print(f"   • 저장 시도율: {client_stats['storage_attempt_rate']:.1f}%")
        print(f"   • 비동기 사용률: {client_stats['async_usage_rate']:.1f}%")
        
        self.demo_results["api_client"] = client_stats
    
    def demo_batch_processing(self):
        """배치 처리 데모"""
        print("\n📦 5. 대용량 배치 처리 테스트")
        print("-" * 50)
        
        # 대용량 배치 요청 생성
        batch_size = 200
        batch_requests = []
        
        for i in range(batch_size):
            provider = random.choice(["KMA", "KTO", "WEATHER"])
            endpoint_map = {
                "KMA": ["fct_shrt_reg", "getUltraSrtFcst", "getVilageFcst"],
                "KTO": ["areaBasedList2", "detailCommon2", "areaCode2"],
                "WEATHER": ["forecast", "weather"]
            }
            
            storage_request = StorageRequest(
                provider=provider,
                endpoint=random.choice(endpoint_map[provider]),
                request_url=f"http://demo.com/batch/request_{i}",
                request_params={"batch_id": f"batch_{i}", "provider": provider},
                response_data={
                    "batch_data": [{"id": j, "value": random.randint(1, 1000)} for j in range(5)],
                    "metadata": {"provider": provider, "batch_index": i}
                },
                response_size_bytes=random.randint(10_000, 1_000_000),
                status_code=200 if random.random() > 0.03 else random.choice([400, 500, 503]),
                execution_time_ms=random.uniform(20, 300),
                created_at=datetime.now(),
                request_id=f"batch_{i}"
            )
            batch_requests.append(storage_request)
        
        print(f"🔄 {batch_size}개 배치 요청 처리 중...")
        
        # 배치 처리 실행
        start_time = time.time()
        batch_results = self.storage_manager.bulk_process_requests(batch_requests)
        batch_time = time.time() - start_time
        
        # 결과 분석
        successful_stores = sum(1 for r in batch_results if r.get("storage_success", False))
        decisions_to_store = sum(1 for r in batch_results if r.get("should_store", False))
        
        # 제공자별 통계
        provider_stats = {}
        for i, request in enumerate(batch_requests):
            provider = request.provider
            if provider not in provider_stats:
                provider_stats[provider] = {"total": 0, "stored": 0}
            provider_stats[provider]["total"] += 1
            if batch_results[i].get("storage_success", False):
                provider_stats[provider]["stored"] += 1
        
        print(f"✅ 배치 처리 완료:")
        print(f"   • 처리 시간: {batch_time:.2f}초")
        print(f"   • 저장 결정: {decisions_to_store}/{batch_size}개")
        print(f"   • 실제 저장: {successful_stores}/{decisions_to_store}개")
        print(f"   • 처리량: {batch_size/batch_time:.1f} 요청/초")
        
        print(f"\n📈 제공자별 저장 통계:")
        for provider, stats in provider_stats.items():
            rate = (stats["stored"] / stats["total"]) * 100 if stats["total"] > 0 else 0
            print(f"   • {provider}: {stats['stored']}/{stats['total']} ({rate:.1f}%)")
        
        self.demo_results["batch_processing"] = {
            "batch_size": batch_size,
            "processing_time_sec": batch_time,
            "decisions_to_store": decisions_to_store,
            "successful_stores": successful_stores,
            "throughput_rps": batch_size/batch_time,
            "provider_stats": provider_stats
        }
    
    def analyze_overall_performance(self):
        """종합 성능 분석"""
        print("\n📊 6. 종합 성능 분석")
        print("-" * 50)
        
        # 전체 통계 수집
        storage_stats = self.storage_manager.get_statistics()
        policy_stats = self.storage_manager.policy_engine.get_statistics()
        
        if self.async_queue:
            async_stats = self.async_queue.get_statistics()
        else:
            async_stats = {}
        
        print("🎯 핵심 성능 지표:")
        print(f"   • 평균 저장 결정 시간: {storage_stats.get('avg_decision_time_ms', 0):.2f}ms")
        print(f"   • 평균 저장 실행 시간: {storage_stats.get('avg_storage_time_ms', 0):.2f}ms")
        print(f"   • 전체 저장 성공률: {storage_stats.get('storage_success_rate', 0):.1f}%")
        print(f"   • 정책 승인률: {policy_stats.get('approval_rate', 0):.1f}%")
        
        if async_stats:
            print(f"   • 비동기 큐 성공률: {async_stats.get('success_rate', 0):.1f}%")
            print(f"   • 평균 큐 처리 시간: {async_stats.get('avg_processing_time_ms', 0):.2f}ms")
        
        print("\n📈 데모 전체 결과 요약:")
        
        # 동기 vs 비동기 성능 비교
        if "sync_storage" in self.demo_results and "async_storage" in self.demo_results:
            sync_throughput = self.demo_results["sync_storage"]["throughput_rps"]
            
            print(f"   • 동기 저장 처리량: {sync_throughput:.1f} 요청/초")
            print(f"   • 비동기 저장 큐 성공률: {self.demo_results['async_storage']['success_rate']:.1f}%")
        
        # 배치 처리 성능
        if "batch_processing" in self.demo_results:
            batch_throughput = self.demo_results["batch_processing"]["throughput_rps"]
            print(f"   • 배치 처리 처리량: {batch_throughput:.1f} 요청/초")
        
        # API 클라이언트 통합 효과
        if "api_client" in self.demo_results:
            api_stats = self.demo_results["api_client"]
            print(f"   • API 저장 통합률: {api_stats['storage_attempt_rate']:.1f}%")
        
        print("\n✅ 성능 목표 달성 여부:")
        
        # Phase 2.1 목표 검증
        decision_time = storage_stats.get('avg_decision_time_ms', 0)
        success_rate = storage_stats.get('storage_success_rate', 0)
        
        print(f"   • API 응답 지연 5ms 이하: {'✅ 달성' if decision_time <= 5 else '❌ 미달성'} ({decision_time:.2f}ms)")
        print(f"   • 저장 실패율 1% 이하: {'✅ 달성' if success_rate >= 99 else '❌ 미달성'} ({100-success_rate:.1f}% 실패율)")
        
        if async_stats:
            queue_util = async_stats.get('queue_utilization', 0)
            print(f"   • 큐 안정성 (90% 미만): {'✅ 달성' if queue_util < 90 else '❌ 미달성'} ({queue_util:.1f}% 사용률)")
        
        print("\n🎉 선택적 저장 시스템 데모 완료!")


def main():
    """메인 실행 함수"""
    try:
        print("🚀 선택적 저장 시스템 데모 시작\n")
        
        demo = SelectiveStorageDemo()
        demo.run_demo()
        
        print("\n" + "="*80)
        print("🎯 데모 요약")
        print("="*80)
        print("✅ 모든 데모가 성공적으로 완료되었습니다!")
        print("📊 Phase 2.1 목표 달성 상태를 확인하세요.")
        print("🔗 이제 Phase 2.2 (TTL 기반 자동 정리 시스템)로 진행할 수 있습니다.")
        
    except Exception as e:
        print(f"❌ 데모 실행 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()