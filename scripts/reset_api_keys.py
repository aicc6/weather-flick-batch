#!/usr/bin/env python3
"""
API 키 매니저 리셋 스크립트
작성일: 2025-07-06
목적: 비활성화된 API 키들을 다시 활성화
"""

import os
import sys
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.core.multi_api_key_manager import get_api_key_manager, reset_api_key_manager, APIProvider

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def reset_api_keys():
    """API 키 상태 리셋"""
    
    print("=== API 키 매니저 리셋 ===")
    print()
    
    try:
        # 1. 기존 매니저 인스턴스 가져오기
        manager = get_api_key_manager()
        
        # 2. 현재 상태 확인
        print("현재 KTO API 키 상태:")
        for i, key_info in enumerate(manager.api_keys.get(APIProvider.KTO, [])):
            print(f"  키 #{i}: {key_info.key[:10]}... | "
                  f"활성: {key_info.is_active} | "
                  f"오류: {key_info.error_count}회 | "
                  f"사용량: {key_info.current_usage}/{key_info.daily_limit}")
        
        print()
        
        # 3. 모든 키 강제 활성화
        print("모든 API 키 강제 활성화 중...")
        for provider in [APIProvider.KTO, APIProvider.KMA]:
            if provider in manager.api_keys:
                for key_info in manager.api_keys[provider]:
                    key_info.is_active = True
                    key_info.error_count = 0
                    key_info.rate_limit_reset_time = None
                    key_info.last_error_time = None
                    print(f"  ✅ {provider.value} 키 {key_info.key[:10]}... 활성화됨")
        
        # 4. 캐시 업데이트
        print("\n캐시 업데이트 중...")
        try:
            manager._save_to_cache()
            print("  ✅ 캐시 저장 완료")
        except Exception as e:
            print(f"  ⚠️ 캐시 저장 실패: {e}")
        
        # 5. 새로운 상태 확인
        print("\n리셋 후 KTO API 키 상태:")
        for i, key_info in enumerate(manager.api_keys.get(APIProvider.KTO, [])):
            print(f"  키 #{i}: {key_info.key[:10]}... | "
                  f"활성: {key_info.is_active} | "
                  f"오류: {key_info.error_count}회 | "
                  f"사용량: {key_info.current_usage}/{key_info.daily_limit}")
        
        print()
        print("✅ API 키 리셋 완료!")
        
    except Exception as e:
        print(f"❌ API 키 리셋 실패: {e}")
        return False
    
    return True


def main():
    """메인 실행 함수"""
    print("=== Weather Flick API 키 리셋 도구 ===")
    print()
    
    if reset_api_keys():
        print("API 키 리셋이 성공적으로 완료되었습니다.")
        print("이제 상세 API 테스트를 다시 실행할 수 있습니다.")
    else:
        print("API 키 리셋에 실패했습니다.")
        sys.exit(1)


if __name__ == "__main__":
    main()