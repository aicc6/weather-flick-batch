#!/usr/bin/env python3
"""
음식점 데이터만 수집하는 전용 스크립트
"""

import os
import sys
import asyncio
from datetime import datetime

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv(override=True)

# API 키 매니저 리셋
from app.core.multi_api_key_manager import reset_api_key_manager
from app.core.unified_api_client import reset_unified_api_client

reset_api_key_manager()
reset_unified_api_client()

from app.collectors.unified_kto_client import get_unified_kto_client
from app.utils.api_key_monitor import get_api_key_monitor
from app.core.multi_api_key_manager import APIProvider


async def check_api_keys_before_collection():
    """데이터 수집 전 API 키 상태 확인"""
    print("🔑 API 키 상태 확인 중...")
    
    monitor = get_api_key_monitor()
    summary = monitor.get_quick_status_summary(APIProvider.KTO)
    
    if "error" in summary:
        print(f"❌ API 키 상태 확인 실패: {summary['error']}")
        return False
    
    print(f"📊 KTO API 키 상태:")
    print(f"  - 총 키: {summary['total_keys']}개")
    print(f"  - 사용 가능: {summary['available_keys']}개")
    print(f"  - 건강도: {summary['health_percentage']:.1f}%")
    
    if summary['available_keys'] == 0:
        print("⚠️ 사용 가능한 API 키가 없습니다!")
        monitor.print_status_report(APIProvider.KTO)
        
        # 복구 시도 제안
        print("\n🔧 키 복구를 시도하시겠습니까? (y/n): ", end="")
        try:
            choice = input().strip().lower()
            if choice == 'y':
                print("🔄 키 복구 시도 중...")
                recovery_result = await monitor.attempt_key_recovery(APIProvider.KTO)
                if recovery_result['recovered_keys'] > 0:
                    print(f"✅ {recovery_result['recovered_keys']}개 키 복구 성공!")
                    return True
                else:
                    print("❌ 키 복구 실패")
                    return False
            else:
                return False
        except:
            return False
    
    elif summary['available_keys'] < summary['total_keys'] * 0.5:
        print("⚠️ 절반 이상의 키에 문제가 있습니다. 상세 상태를 확인하세요.")
        monitor.print_status_report(APIProvider.KTO)
    
    return summary['available_keys'] > 0


async def collect_restaurants_all_regions():
    """전국 모든 지역 음식점 데이터 수집"""
    print("🍽️ 전국 모든 지역 음식점 데이터 수집 시작")
    print("=" * 50)
    
    # API 키 상태 확인
    if not await check_api_keys_before_collection():
        print("❌ API 키 문제로 인해 수집을 중단합니다.")
        return {"status": "failed", "reason": "api_key_unavailable"}
    
    client = get_unified_kto_client()
    
    result = await client.collect_all_data(
        content_types=["39"],  # 음식점만
        area_codes=None,       # 전국 모든 지역
        store_raw=True,
        auto_transform=True,
        include_new_apis=False,
        include_hierarchical_regions=False,
    )
    
    return result


async def collect_restaurants_specific_regions(region_codes):
    """특정 지역 음식점 데이터 수집"""
    print(f"🍽️ 지정된 지역 음식점 데이터 수집 시작: {region_codes}")
    print("=" * 50)
    
    # API 키 상태 확인
    if not await check_api_keys_before_collection():
        print("❌ API 키 문제로 인해 수집을 중단합니다.")
        return {"status": "failed", "reason": "api_key_unavailable"}
    
    client = get_unified_kto_client()
    
    result = await client.collect_all_data(
        content_types=["39"],     # 음식점만
        area_codes=region_codes,  # 지정된 지역들
        store_raw=True,
        auto_transform=True,
        include_new_apis=False,
        include_hierarchical_regions=False,
    )
    
    return result


async def collect_restaurants_major_cities():
    """주요 도시 음식점 데이터 수집"""
    major_cities = ["1", "6", "31", "39"]  # 서울, 부산, 경기, 제주
    
    print(f"🍽️ 주요 도시 음식점 데이터 수집 시작")
    print("대상 지역: 서울, 부산, 경기, 제주")
    print("=" * 50)
    
    # API 키 상태 확인
    if not await check_api_keys_before_collection():
        print("❌ API 키 문제로 인해 수집을 중단합니다.")
        return {"status": "failed", "reason": "api_key_unavailable"}
    
    client = get_unified_kto_client()
    
    result = await client.collect_all_data(
        content_types=["39"],     # 음식점만
        area_codes=major_cities,  # 주요 도시들
        store_raw=True,
        auto_transform=True,
        include_new_apis=False,
        include_hierarchical_regions=False,
    )
    
    return result


def print_available_regions():
    """사용 가능한 지역 코드 출력"""
    regions = {
        "1": "서울특별시",
        "2": "인천광역시", 
        "3": "대전광역시",
        "4": "대구광역시",
        "5": "광주광역시",
        "6": "부산광역시",
        "7": "울산광역시",
        "8": "세종특별자치시",
        "31": "경기도",
        "32": "강원특별자치도",
        "33": "충청북도",
        "34": "충청남도", 
        "35": "경상북도",
        "36": "경상남도",
        "37": "전북특별자치도",
        "38": "전라남도",
        "39": "제주도"
    }
    
    print("\n📍 사용 가능한 지역 코드:")
    for code, name in regions.items():
        print(f"  {code}: {name}")
    print()


def print_result_summary(result):
    """수집 결과 요약 출력"""
    print("\n" + "=" * 50)
    print("📊 수집 결과 요약")
    print("=" * 50)
    
    if result.get("status") == "skipped":
        print(f"⚠️ 작업 건너뜀: {result.get('reason', 'Unknown')}")
        if result.get("next_retry_time"):
            print(f"다음 재시도 시간: {result['next_retry_time']}")
        return
    
    print(f"배치 ID: {result.get('sync_batch_id', 'N/A')}")
    print(f"시작 시간: {result.get('started_at', 'N/A')}")
    print(f"완료 시간: {result.get('completed_at', 'N/A')}")
    print(f"상태: {result.get('status', 'N/A')}")
    
    content_types = result.get('content_types_collected', {})
    restaurants_data = content_types.get('39', {})
    
    if restaurants_data:
        print(f"\n🍽️ 음식점 데이터 수집 결과:")
        print(f"  - 총 원본 레코드: {restaurants_data.get('total_raw_records', 0):,}개")
        print(f"  - 처리된 레코드: {restaurants_data.get('total_processed_records', 0):,}개")
        print(f"  - 수집 지역 수: {len(restaurants_data.get('area_results', {})):,}개")
        
        area_results = restaurants_data.get('area_results', {})
        if area_results:
            print(f"\n지역별 수집 현황:")
            for area_code, area_data in area_results.items():
                area_name = get_area_name(area_code)
                raw_count = area_data.get('total_raw_records', 0)
                processed_count = area_data.get('total_processed_records', 0)
                print(f"  - {area_name} ({area_code}): 원본 {raw_count}개, 처리 {processed_count}개")
    
    errors = result.get('errors', [])
    if errors:
        print(f"\n❌ 오류 발생 ({len(errors)}개):")
        for error in errors[:5]:  # 최대 5개만 표시
            print(f"  - {error}")
        if len(errors) > 5:
            print(f"  ... 및 {len(errors) - 5}개 추가 오류")


def get_area_name(area_code):
    """지역 코드에서 지역명 반환"""
    regions = {
        "1": "서울특별시", "2": "인천광역시", "3": "대전광역시", "4": "대구광역시",
        "5": "광주광역시", "6": "부산광역시", "7": "울산광역시", "8": "세종특별자치시",
        "31": "경기도", "32": "강원특별자치도", "33": "충청북도", "34": "충청남도", 
        "35": "경상북도", "36": "경상남도", "37": "전북특별자치도", "38": "전라남도", "39": "제주도"
    }
    return regions.get(area_code, f"지역{area_code}")


async def main():
    """메인 함수"""
    import argparse
    
    parser = argparse.ArgumentParser(description='음식점 데이터 수집 도구')
    parser.add_argument('--mode', choices=['all', 'major', 'regions', 'list'], 
                       default='major', help='수집 모드 선택')
    parser.add_argument('--regions', type=str, 
                       help='특정 지역 코드 (쉼표로 구분, 예: 1,6,31)')
    parser.add_argument('--interactive', action='store_true', 
                       help='대화형 모드로 실행')
    
    args = parser.parse_args()
    
    print("🍽️ 음식점 데이터 수집 도구")
    print("=" * 50)
    print(f"실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 대화형 모드
    if args.interactive:
        await run_interactive_mode()
        return
    
    # 명령행 모드
    try:
        if args.mode == 'all':
            print("\n🍽️ 전국 모든 지역 음식점 데이터 수집")
            result = await collect_restaurants_all_regions()
            print_result_summary(result)
            
        elif args.mode == 'major':
            print("\n🍽️ 주요 지역 음식점 데이터 수집")
            result = await collect_restaurants_major_cities()
            print_result_summary(result)
            
        elif args.mode == 'regions':
            if not args.regions:
                print("❌ --regions 옵션으로 지역 코드를 지정해주세요.")
                print_available_regions()
                return
            
            region_codes = [code.strip() for code in args.regions.split(",")]
            print(f"\n🍽️ 지정된 지역 음식점 데이터 수집: {region_codes}")
            result = await collect_restaurants_specific_regions(region_codes)
            print_result_summary(result)
            
        elif args.mode == 'list':
            print_available_regions()
            
    except Exception as e:
        print(f"❌ 수집 중 오류 발생: {e}")
        import traceback
        traceback.print_exc()


async def run_interactive_mode():
    """대화형 모드 실행"""
    while True:
        print("\n수집 옵션을 선택하세요:")
        print("1. 전국 모든 지역 음식점 데이터 수집")
        print("2. 특정 지역 음식점 데이터 수집")
        print("3. 주요 지역만 수집 (서울, 부산, 경기, 제주)")
        print("4. 사용 가능한 지역 코드 보기")
        print("5. API 키 상태 확인")
        print("6. API 키 건강 상태 점검")
        print("7. 문제 키 복구 시도")
        print("0. 종료")
        
        choice = input("\n선택: ").strip()
        
        if choice == "0":
            print("👋 프로그램을 종료합니다.")
            break
            
        elif choice == "1":
            try:
                result = await collect_restaurants_all_regions()
                print_result_summary(result)
            except Exception as e:
                print(f"❌ 수집 중 오류 발생: {e}")
                
        elif choice == "2":
            print_available_regions()
            region_input = input("수집할 지역 코드를 쉼표로 구분해서 입력하세요 (예: 1,6,31): ").strip()
            
            if region_input:
                region_codes = [code.strip() for code in region_input.split(",")]
                try:
                    result = await collect_restaurants_specific_regions(region_codes)
                    print_result_summary(result)
                except Exception as e:
                    print(f"❌ 수집 중 오류 발생: {e}")
            else:
                print("⚠️ 지역 코드를 입력해주세요.")
                
        elif choice == "3":
            try:
                result = await collect_restaurants_major_cities()
                print_result_summary(result)
            except Exception as e:
                print(f"❌ 수집 중 오류 발생: {e}")
                
        elif choice == "4":
            print_available_regions()
            
        elif choice == "5":
            # API 키 상태 확인
            monitor = get_api_key_monitor()
            monitor.print_status_report(APIProvider.KTO)
            
        elif choice == "6":
            # API 키 건강 상태 점검
            print("🔍 API 키 건강 상태 점검 중...")
            try:
                monitor = get_api_key_monitor()
                health_results = await monitor.check_all_keys_health(APIProvider.KTO)
                
                print(f"\n📊 건강 상태 점검 결과:")
                print(f"  - 총 키: {health_results['total_keys']}개")
                print(f"  - 건강한 키: {health_results['healthy_keys']}개")
                print(f"  - 문제 키: {health_results['unhealthy_keys']}개")
                
                if health_results.get('recommendations'):
                    print(f"\n💡 권장사항:")
                    for rec in health_results['recommendations']:
                        print(f"  {rec}")
                        
            except Exception as e:
                print(f"❌ 건강 상태 점검 실패: {e}")
            
        elif choice == "7":
            # 문제 키 복구 시도
            print("🔧 문제 키 복구 시도 중...")
            try:
                monitor = get_api_key_monitor()
                recovery_results = await monitor.attempt_key_recovery(APIProvider.KTO)
                
                print(f"\n🔄 복구 결과:")
                print(f"  - 시도한 키: {recovery_results['attempted_keys']}개")
                print(f"  - 복구된 키: {recovery_results['recovered_keys']}개")
                print(f"  - 실패한 키: {recovery_results['failed_keys']}개")
                
                if recovery_results['recovered_keys'] > 0:
                    print("✅ 일부 키가 성공적으로 복구되었습니다!")
                else:
                    print("❌ 복구된 키가 없습니다.")
                    
            except Exception as e:
                print(f"❌ 키 복구 실패: {e}")
            
        else:
            print("⚠️ 올바른 선택지를 입력해주세요.")


if __name__ == "__main__":
    asyncio.run(main())