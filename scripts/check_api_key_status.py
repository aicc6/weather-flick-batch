#!/usr/bin/env python3
"""
API 키 상태 확인 및 제한 해제 시간 조회 스크립트
"""

import os
import sys
from datetime import datetime, timedelta

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv(override=True)

from app.core.multi_api_key_manager import get_api_key_manager, APIProvider


def check_api_key_status():
    """API 키 상태 상세 확인"""
    print("🔑 API 키 상태 확인")
    print("=" * 60)
    print(f"확인 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    key_manager = get_api_key_manager()
    
    # KTO API 키 상태 확인
    print("\n📊 KTO API 키 상태:")
    
    # 전체 사용량 통계
    usage_stats = key_manager.get_usage_stats()
    kto_stats = usage_stats.get('providers', {}).get('KTO', {})
    
    print(f"  총 키 개수: {kto_stats.get('total_keys', 0)}개")
    print(f"  활성 키 개수: {kto_stats.get('active_keys', 0)}개")
    print(f"  총 사용량: {kto_stats.get('total_usage', 0):,}회")
    print(f"  총 한도: {kto_stats.get('total_limit', 0):,}회")
    
    if kto_stats.get('total_limit', 0) > 0:
        usage_percent = (kto_stats.get('total_usage', 0) / kto_stats.get('total_limit', 1)) * 100
        print(f"  사용률: {usage_percent:.1f}%")
    
    # 개별 키 상태
    print("\n🔍 개별 키 상태:")
    for i, key_info in enumerate(kto_stats.get('keys', [])):
        key_preview = key_info.get('key_preview', 'N/A')
        usage = key_info.get('usage', 0)
        limit = key_info.get('limit', 0)
        usage_percent = key_info.get('usage_percent', 0)
        is_active = key_info.get('is_active', False)
        error_count = key_info.get('error_count', 0)
        last_used = key_info.get('last_used', 'Never')
        
        status_icon = "✅" if is_active else "❌"
        status_text = "활성" if is_active else "비활성"
        
        print(f"  키 {i+1} ({key_preview}): {status_icon} {status_text}")
        print(f"    - 사용량: {usage:,}/{limit:,}회 ({usage_percent:.1f}%)")
        print(f"    - 오류 횟수: {error_count}회")
        print(f"    - 마지막 사용: {last_used}")
    
    # 제한 상태 확인
    print("\n⏰ 제한 상태 확인:")
    
    all_limited = key_manager.are_all_keys_rate_limited(APIProvider.KTO)
    print(f"  모든 키 제한 여부: {'예' if all_limited else '아니오'}")
    
    if all_limited:
        next_reset = key_manager.get_next_reset_time(APIProvider.KTO)
        if next_reset:
            time_until_reset = next_reset - datetime.now()
            hours = int(time_until_reset.total_seconds() // 3600)
            minutes = int((time_until_reset.total_seconds() % 3600) // 60)
            print(f"  다음 해제 시간: {next_reset.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"  남은 시간: {hours}시간 {minutes}분")
        else:
            print("  다음 해제 시간: 확인 불가")
    
    # 상세 제한 상태
    rate_limit_status = key_manager.get_rate_limit_status(APIProvider.KTO)
    print("\n📈 상세 제한 상태:")
    print(f"  활성 키: {rate_limit_status.get('active_keys', 0)}개")
    print(f"  제한된 키: {rate_limit_status.get('limited_keys', 0)}개")
    print(f"  전체 키: {rate_limit_status.get('total_keys', 0)}개")
    
    return all_limited, next_reset


def suggest_solutions(all_limited, next_reset):
    """해결 방안 제시"""
    print("\n💡 해결 방안:")
    
    if all_limited:
        print("  📛 모든 API 키가 제한된 상태입니다.")
        
        if next_reset:
            print("  ⏳ 대기 방안:")
            print(f"    - {next_reset.strftime('%H:%M')} 이후에 다시 시도")
            print("    - 일반적으로 1시간 후 자동 해제됩니다")
        
        print("  🔑 추가 API 키 방안:")
        print("    - 한국관광공사에서 추가 API 키 발급 신청")
        print("    - .env 파일의 KTO_API_KEY에 쉼표로 구분하여 추가")
        
        print("  📊 사용량 분산 방안:")
        print("    - 수집 지역을 나누어 시간대별로 실행")
        print("    - 주요 지역만 우선 수집 후 나머지는 나중에")
    
    else:
        print("  ✅ 사용 가능한 API 키가 있습니다.")
        print("  🚀 음식점 데이터 수집을 재시도할 수 있습니다.")
        
        print("  📈 효율적인 수집 방안:")
        print("    - 주요 지역(서울, 부산, 경기, 제주)부터 시작")
        print("    - 지역별로 나누어 순차적으로 수집")
        print("    - API 제한 발생 시 다른 지역으로 전환")


def check_daily_usage_reset():
    """일일 사용량 리셋 확인"""
    print("\n🔄 일일 사용량 리셋 정보:")
    print("  - 한국관광공사 API는 일일 기준으로 사용량이 리셋됩니다")
    print("  - 보통 자정(00:00)에 리셋되지만, 정확한 시간은 API 제공자에 따라 다를 수 있습니다")
    print(f"  - 현재 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 다음 자정까지 시간 계산
    now = datetime.now()
    tomorrow = (now + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    time_until_reset = tomorrow - now
    hours = int(time_until_reset.total_seconds() // 3600)
    minutes = int((time_until_reset.total_seconds() % 3600) // 60)
    
    print(f"  - 다음 자정까지: {hours}시간 {minutes}분")


def main():
    """메인 함수"""
    try:
        all_limited, next_reset = check_api_key_status()
        suggest_solutions(all_limited, next_reset)
        check_daily_usage_reset()
        
        print("\n" + "=" * 60)
        print("✅ API 키 상태 확인 완료")
        
    except Exception as e:
        print(f"❌ API 키 상태 확인 중 오류 발생: {e}")


if __name__ == "__main__":
    main()