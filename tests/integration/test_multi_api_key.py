#!/usr/bin/env python3
"""
다중 API 키 시스템 테스트 스크립트

이 스크립트는 다중 API 키 관리 시스템의 동작을 테스트합니다.
"""

import sys
import os

# tests/integration/ -> project root 경로 설정
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.append(project_root)

from app.core.multi_api_key_manager import get_api_key_manager, APIProvider
from app.core.base_api_client import KTOAPIClient
from typing import Dict, Any


def test_multi_api_key_manager():
    """다중 API 키 매니저 테스트"""
    print("🔧 다중 API 키 매니저 테스트 시작")

    # 키 매니저 인스턴스 생성
    from dotenv import load_dotenv

    load_dotenv()  # 환경 변수 다시 로드
    key_manager = get_api_key_manager()

    # 로드된 키 정보 출력
    stats = key_manager.get_usage_stats()
    print(f"📊 총 키 개수: {stats['total_keys']}")
    print(f"🔑 활성 키 개수: {stats['active_keys']}")

    for provider, provider_stats in stats["providers"].items():
        print(f"\n[{provider}]")
        print(f"  - 총 키: {provider_stats['total_keys']}개")
        print(f"  - 활성 키: {provider_stats['active_keys']}개")
        print(
            f"  - 총 사용량: {provider_stats['total_usage']}/{provider_stats['total_limit']}"
        )

        for key_info in provider_stats["keys"]:
            print(
                f"    키 {key_info['index']}: {key_info['key_preview']} "
                f"({key_info['usage']}/{key_info['limit']}, "
                f"{key_info['usage_percent']:.1f}%)"
            )

    # 활성 키 테스트
    print("\n🔑 활성 키 테스트")
    for provider in [APIProvider.KTO, APIProvider.KMA]:
        active_key = key_manager.get_active_key(provider)
        if active_key:
            print(
                f"{provider.value}: {active_key.key[:10]}... (사용량: {active_key.current_usage}/{active_key.daily_limit})"
            )
        else:
            print(f"{provider.value}: 사용 가능한 키 없음")


class TestKTOAPIClient(KTOAPIClient):
    """테스트용 KTO API 클라이언트"""

    def get_request_stats(self) -> Dict[str, Any]:
        """API 요청 통계 반환"""
        return {
            "total_requests": self.daily_request_count,
            "rate_limit_count": self.rate_limit_count,
            "daily_limit": self.max_daily_requests,
        }


def test_kto_api_client():
    """KTO API 클라이언트 테스트"""
    print("\n🌐 KTO API 클라이언트 테스트")

    # KTO API 클라이언트 생성
    client = TestKTOAPIClient()

    # 현재 사용 중인 키 확인
    current_key = client._get_current_api_key()
    if current_key:
        print(f"현재 사용 중인 키: {current_key[:10]}...")
    else:
        print("⚠️ 사용 가능한 API 키가 없습니다.")
        return

    # 간단한 API 호출 테스트 (지역 코드 조회)
    print("📍 지역 코드 조회 테스트 중...")

    # 서울 지역 코드 조회
    params = {"areaCode": "1"}

    try:
        result = client.make_request("areaCode2", params)
        if result:
            print(
                f"✅ API 호출 성공: {len(result.get('items', {}).get('item', []))}개 항목 조회"
            )
        else:
            print("❌ API 호출 실패")
    except Exception as e:
        print(f"❌ API 호출 중 오류: {e}")


if __name__ == "__main__":
    print("🚀 다중 API 키 시스템 테스트 시작\n")

    try:
        test_multi_api_key_manager()
        test_kto_api_client()

        print("\n✅ 테스트 완료!")

    except Exception as e:
        print(f"\n❌ 테스트 중 오류 발생: {e}")
        import traceback

        traceback.print_exc()
