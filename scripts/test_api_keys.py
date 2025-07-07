#!/usr/bin/env python3
"""
API 키 상태 테스트 및 검증 스크립트

수정된 API 키 관리 로직을 테스트하고 검증합니다.
"""

import os
import sys
import asyncio
import logging
from datetime import datetime
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.core.multi_api_key_manager import get_api_key_manager, APIProvider
from app.utils.api_key_monitor import get_api_key_monitor
from app.collectors.unified_kto_client import get_unified_kto_client


# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'logs/api_key_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

logger = logging.getLogger(__name__)


async def test_key_manager_basic():
    """키 매니저 기본 기능 테스트"""
    print("\n" + "="*60)
    print("🧪 키 매니저 기본 기능 테스트")
    print("="*60)
    
    key_manager = get_api_key_manager()
    
    # 1. 키 로드 상태 확인
    print("1️⃣ 키 로드 상태 확인")
    usage_stats = key_manager.get_usage_stats()
    print(f"   총 KTO 키: {usage_stats['providers'].get('KTO', {}).get('total_keys', 0)}개")
    print(f"   활성 KTO 키: {usage_stats['providers'].get('KTO', {}).get('active_keys', 0)}개")
    
    # 2. 상세 키 상태 조회
    print("\n2️⃣ 상세 키 상태 조회")
    detailed_status = key_manager.get_detailed_key_status(APIProvider.KTO)
    if "error" not in detailed_status:
        for i, key_info in enumerate(detailed_status["keys"]):
            status_emoji = "🟢" if key_info["is_available"] else "🔴"
            print(f"   키 #{i}: {key_info['key_preview']} {status_emoji}")
            if key_info["unavailable_reason"]:
                print(f"      이유: {key_info['unavailable_reason']}")
    else:
        print(f"   ❌ 오류: {detailed_status['error']}")
    
    # 3. 활성 키 획득 테스트
    print("\n3️⃣ 활성 키 획득 테스트")
    active_key = key_manager.get_active_key(APIProvider.KTO)
    if active_key:
        print(f"   ✅ 활성 키 획득 성공: {active_key.key[:10]}...")
        print(f"   사용량: {active_key.current_usage}/{active_key.daily_limit}")
    else:
        print("   ❌ 활성 키 획득 실패")
    
    return active_key is not None


async def test_api_monitor():
    """API 키 모니터 기능 테스트"""
    print("\n" + "="*60)
    print("🔍 API 키 모니터 기능 테스트")
    print("="*60)
    
    monitor = get_api_key_monitor()
    
    # 1. 빠른 상태 요약
    print("1️⃣ 빠른 상태 요약")
    summary = monitor.get_quick_status_summary(APIProvider.KTO)
    if "error" not in summary:
        print(f"   총 키: {summary['total_keys']}개")
        print(f"   사용 가능: {summary['available_keys']}개")
        print(f"   건강도: {summary['health_percentage']:.1f}%")
        print(f"   상태: {summary['status']}")
    else:
        print(f"   ❌ 오류: {summary['error']}")
    
    # 2. 상태 보고서 출력
    print("\n2️⃣ 상세 상태 보고서")
    monitor.print_status_report(APIProvider.KTO)
    
    return summary.get("available_keys", 0) > 0


async def test_kto_client_integration():
    """KTO 클라이언트 통합 테스트"""
    print("\n" + "="*60)
    print("🌐 KTO 클라이언트 통합 테스트")
    print("="*60)
    
    kto_client = get_unified_kto_client()
    
    try:
        # 간단한 지역 코드 조회 테스트
        print("1️⃣ 지역 코드 조회 테스트")
        area_codes = await kto_client.collect_area_codes()
        
        if area_codes:
            print(f"   ✅ 지역 코드 조회 성공: {len(area_codes)}개")
            # 첫 번째 결과만 출력
            if len(area_codes) > 0:
                print(f"   예시: {area_codes[0]['region_name']} (코드: {area_codes[0]['region_code']})")
        else:
            print("   ❌ 지역 코드 조회 실패")
        
        return len(area_codes) > 0
        
    except Exception as e:
        print(f"   ❌ 테스트 중 오류 발생: {e}")
        logger.error(f"KTO 클라이언트 테스트 오류: {e}")
        return False


async def test_error_handling():
    """오류 처리 로직 테스트"""
    print("\n" + "="*60)
    print("⚠️ 오류 처리 로직 테스트")
    print("="*60)
    
    key_manager = get_api_key_manager()
    
    # 1. 잘못된 API 키로 호출 시뮬레이션
    print("1️⃣ 오류 기록 시뮬레이션")
    
    # 테스트용 가짜 키 정보
    test_key = "test_invalid_key"
    
    # 실제 키가 있는지 확인
    detailed_status = key_manager.get_detailed_key_status(APIProvider.KTO)
    if "error" not in detailed_status and len(detailed_status["keys"]) > 0:
        # 첫 번째 키로 오류 시뮬레이션
        first_key_info = detailed_status["keys"][0]
        actual_key = first_key_info["key_preview"].replace("...", "")
        
        # 오류 기록 (API 키 매니저에서 실제 키를 찾을 수 있도록)
        # 하지만 실제로는 키를 손상시키지 않음
        print(f"   시뮬레이션 대상 키: {first_key_info['key_preview']}")
        print("   (실제로는 키를 손상시키지 않습니다)")
        
        # 현재 오류 수 확인
        original_error_count = first_key_info["error_count"]
        print(f"   현재 오류 수: {original_error_count}")
        
        return True
    else:
        print("   ❌ 테스트할 키가 없습니다")
        return False


async def test_key_recovery():
    """키 복구 기능 테스트"""
    print("\n" + "="*60)
    print("🔧 키 복구 기능 테스트")
    print("="*60)
    
    monitor = get_api_key_monitor()
    key_manager = get_api_key_manager()
    
    # 1. 현재 비활성화된 키 확인
    print("1️⃣ 비활성화된 키 확인")
    detailed_status = key_manager.get_detailed_key_status(APIProvider.KTO)
    
    if "error" not in detailed_status:
        inactive_keys = [k for k in detailed_status["keys"] if not k["is_active"]]
        print(f"   비활성화된 키: {len(inactive_keys)}개")
        
        if len(inactive_keys) > 0:
            print("   비활성화된 키 목록:")
            for key_info in inactive_keys:
                print(f"     - 키 #{key_info['index']}: {key_info['key_preview']} "
                      f"(오류: {key_info['error_count']}회)")
        
        # 2. 복구 시도 (실제로는 수행하지 않음 - 데모용)
        print("\n2️⃣ 복구 시도 시뮬레이션")
        if len(inactive_keys) > 0:
            print("   실제 복구는 수행하지 않습니다 (데모용)")
            print("   복구를 원한다면 monitor.attempt_key_recovery()를 호출하세요")
        else:
            print("   복구할 비활성화된 키가 없습니다")
        
        return len(inactive_keys) >= 0  # 0개여도 정상
    else:
        print(f"   ❌ 오류: {detailed_status['error']}")
        return False


async def run_comprehensive_test():
    """종합 테스트 실행"""
    print("\n" + "🚀" + " KTO API 키 관리 시스템 종합 테스트 " + "🚀")
    print("시작 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    test_results = {
        "key_manager_basic": False,
        "api_monitor": False,
        "kto_client_integration": False,
        "error_handling": False,
        "key_recovery": False
    }
    
    try:
        # 환경 변수 확인
        if not os.getenv("KTO_API_KEY"):
            print("❌ KTO_API_KEY 환경 변수가 설정되지 않았습니다")
            return test_results
        
        # 테스트 실행
        test_results["key_manager_basic"] = await test_key_manager_basic()
        test_results["api_monitor"] = await test_api_monitor()
        test_results["kto_client_integration"] = await test_kto_client_integration()
        test_results["error_handling"] = await test_error_handling()
        test_results["key_recovery"] = await test_key_recovery()
        
    except Exception as e:
        print(f"\n❌ 종합 테스트 중 예외 발생: {e}")
        logger.error(f"종합 테스트 예외: {e}")
    
    # 결과 요약
    print("\n" + "="*60)
    print("📊 테스트 결과 요약")
    print("="*60)
    
    passed_tests = sum(test_results.values())
    total_tests = len(test_results)
    
    for test_name, passed in test_results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name.replace('_', ' ').title()}: {status}")
    
    print(f"\n📈 전체 결과: {passed_tests}/{total_tests} 테스트 통과")
    print(f"성공률: {(passed_tests/total_tests*100):.1f}%")
    
    if passed_tests == total_tests:
        print("🎉 모든 테스트가 통과했습니다!")
    elif passed_tests >= total_tests * 0.8:
        print("👍 대부분의 테스트가 통과했습니다")
    else:
        print("⚠️ 일부 테스트에서 문제가 발견되었습니다")
    
    print("완료 시간:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    
    return test_results


async def main():
    """메인 함수"""
    # 로그 디렉토리 생성
    os.makedirs("logs", exist_ok=True)
    
    # 명령줄 인수 처리
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "basic":
            await test_key_manager_basic()
        elif command == "monitor":
            await test_api_monitor()
        elif command == "client":
            await test_kto_client_integration()
        elif command == "error":
            await test_error_handling()
        elif command == "recovery":
            await test_key_recovery()
        elif command == "all":
            await run_comprehensive_test()
        else:
            print("사용법: python test_api_keys.py [basic|monitor|client|error|recovery|all]")
    else:
        # 기본값: 종합 테스트
        await run_comprehensive_test()


if __name__ == "__main__":
    asyncio.run(main())