#!/usr/bin/env python3
"""
수동 배치 실행 간편 도구

프로젝트 루트에서 실행하여 다중 API 키 시스템과 함께 배치 작업을 실행합니다.
"""

import sys
import os
from pathlib import Path

# 프로젝트 루트 확인
project_root = Path(__file__).parent
os.chdir(project_root)

# 환경 변수 로드
from dotenv import load_dotenv

load_dotenv()

# 스크립트 경로 추가
sys.path.insert(0, str(project_root))


def show_usage():
    """사용법 출력"""
    print(
        """
🚀 WeatherFlick 수동 배치 실행 도구

사용 가능한 명령어:
  python run_manual_batch.py list                      # 작업 목록
  python run_manual_batch.py status                    # 작업 상태
  python run_manual_batch.py test                      # 다중 키 시스템 테스트
  python run_manual_batch.py run [작업코드]              # 특정 작업 실행
  python run_manual_batch.py run-all                   # 모든 작업 순차 실행

주요 작업 코드:
  - weather: 날씨 데이터 업데이트
  - tourism: 관광지 데이터 동기화  
  - comprehensive-tourism: 종합 관광정보 수집
  - incremental-tourism: 증분 관광정보 수집
  - health: 시스템 헬스체크
  - backup: 데이터베이스 백업
    """
    )


def test_multi_key_system():
    """다중 API 키 시스템 테스트"""
    print("🔧 다중 API 키 시스템 테스트")

    from app.core.multi_api_key_manager import get_api_key_manager
    from app.core.base_api_client import KTOAPIClient

    # 키 매니저 상태
    manager = get_api_key_manager()
    stats = manager.get_usage_stats()

    print(f"📊 총 키 개수: {stats['total_keys']}")
    print(f"🔑 활성 키 개수: {stats['active_keys']}")

    for provider, data in stats["providers"].items():
        print(f"\n[{provider}]")
        print(f"  총 키: {data['total_keys']}개")
        print(f"  활성 키: {data['active_keys']}개")
        print(f"  총 사용량: {data['total_usage']}/{data['total_limit']}")

    # API 호출 테스트
    class TestClient(KTOAPIClient):
        def get_request_stats(self):
            return {}

    try:
        client = TestClient()
        key = client._get_current_api_key()
        if key:
            print(f"\n🔑 활성 키: {key[:10]}...")
            result = client.make_request("areaCode2", {"areaCode": "1", "numOfRows": 1})
            if result:
                print("✅ API 호출 성공")
            else:
                print("❌ API 호출 실패")
        else:
            print("⚠️ 사용 가능한 API 키가 없습니다")
    except Exception as e:
        print(f"❌ 테스트 오류: {e}")


def main():
    if len(sys.argv) < 2:
        show_usage()
        return

    command = sys.argv[1]

    if command == "test":
        test_multi_key_system()
    elif command == "list":
        os.system("python scripts/run_batch.py list")
    elif command == "status":
        os.system("python scripts/run_batch.py status")
    elif command == "run" and len(sys.argv) >= 3:
        job_code = sys.argv[2]
        os.system(f"python scripts/run_batch.py run {job_code}")
    elif command == "run-all":
        print("🚀 모든 배치 작업을 순차적으로 실행합니다...")
        os.system("python scripts/run_batch.py run-all")
    else:
        show_usage()


if __name__ == "__main__":
    main()
