#!/usr/bin/env python3
"""
개발 단계 빠른 대량 수집 스크립트
작성일: 2025-07-07
목적: 즉시 실행 가능한 관광지 데이터 대량 수집
"""

import os
import sys
import asyncio
import logging
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from jobs.tourism.comprehensive_tourism_job import ComprehensiveTourismJob
from app.core.logger import get_logger

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = get_logger(__name__)


async def execute_comprehensive_collection():
    """종합 관광정보 대량 수집 실행"""
    print("\n" + "="*60)
    print("🏛️ 개발 단계 관광지 데이터 대량 수집")
    print("="*60)
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    print("📋 수집 계획:")
    print("• 컨텐츠 타입: 8개 (관광지, 숙박, 음식점, 문화시설, 축제, 여행코스, 레포츠, 쇼핑)")
    print("• 지역 범위: 전국 17개 시도")
    print("• 상세 정보: 기본정보, 소개정보, 상세정보, 이미지")
    print("• 예상 시간: 2-4시간")
    print("• 예상 데이터: 50,000+건")
    print()
    
    # 사용자 확인
    while True:
        response = input("🚀 대량 수집을 시작하시겠습니까? (y/n): ").lower()
        if response in ['y', 'yes']:
            break
        elif response in ['n', 'no']:
            print("❌ 수집을 취소했습니다.")
            return
        else:
            print("⚠️ 'y' 또는 'n'을 입력해주세요.")
    
    # 종합 수집 작업 시작
    print("\n🚀 종합 관광정보 수집 시작...")
    start_time = datetime.now()
    
    try:
        job = ComprehensiveTourismJob()
        result = await job.execute()
        
        execution_time = datetime.now() - start_time
        
        print("\n" + "="*60)
        if result:  # boolean 결과 확인
            print("🎉 관광지 데이터 대량 수집 완료!")
            print("="*60)
            print(f"⏱️ 총 실행 시간: {execution_time}")
            
            # 데이터베이스에서 최근 수집 결과 확인
            try:
                # 최근 배치 로그 확인
                recent_log_query = """
                SELECT processed_records, execution_context 
                FROM batch_job_logs 
                WHERE job_name = 'comprehensive_tourism_sync' 
                AND status = 'success'
                ORDER BY created_at DESC 
                LIMIT 1
                """
                
                log_result = job.db_manager.execute_query(recent_log_query)
                if log_result:
                    processed_records = log_result[0][0] if log_result[0][0] else 0
                    print(f"📊 처리된 데이터: {processed_records:,}건")
                    
                    # 실행 컨텍스트에서 추가 정보 확인
                    execution_context = log_result[0][1] if len(log_result[0]) > 1 else None
                    if execution_context and isinstance(execution_context, dict):
                        sync_batch_id = execution_context.get('sync_batch_id', 'N/A')
                        print(f"🔄 배치 ID: {sync_batch_id}")
                        
                        raw_records = execution_context.get('raw_records_count', 0)
                        if raw_records:
                            print(f"📥 원본 데이터: {raw_records:,}건")
                else:
                    print("📊 수집 데이터 정보를 확인할 수 없습니다.")
                    
            except Exception as e:
                print(f"📊 수집 결과 조회 중 오류: {e}")
            
            print(f"\n📈 다음 단계 추천:")
            print("1. 데이터 현황 확인: python scripts/analyze_database_direct.py")
            print("2. 데이터 품질 검사: python scripts/test_quality_engine.py")
            print("3. 추천 엔진 테스트: python jobs/recommendation/travel_recommendation_engine.py")
            
        else:
            print("❌ 관광지 데이터 수집 실패")
            print("="*60)
            
            # 최근 실패 로그 확인
            try:
                error_log_query = """
                SELECT error_message, created_at 
                FROM batch_job_logs 
                WHERE job_name = 'comprehensive_tourism_sync' 
                AND status = 'failure'
                ORDER BY created_at DESC 
                LIMIT 1
                """
                
                error_result = job.db_manager.execute_query(error_log_query)
                if error_result and error_result[0][0]:
                    print(f"오류: {error_result[0][0]}")
                else:
                    print("오류: 세부 정보를 확인할 수 없습니다.")
                    
            except Exception as e:
                print(f"오류: 로그 조회 실패 - {e}")
        
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ 수집 실행 중 오류 발생: {e}")
        logger.error(f"Collection execution error: {e}", exc_info=True)


async def execute_priority_collection():
    """우선순위 기반 수집 (수동 모드)"""
    print("\n🎯 우선순위 기반 수집을 위해 별도 스크립트를 실행합니다...")
    print("다음 명령어를 실행하세요:")
    print("python scripts/collect_with_priority.py")


async def execute_specific_collection():
    """특정 타입별 수집"""
    print("\n📋 특정 타입별 수집 옵션:")
    print("1. 음식점 데이터: python scripts/collect_restaurants_only.py --mode all")
    print("2. 반려동물 관광: python scripts/collect_pet_tour_only.py")
    print("3. 특정 지역만: python scripts/collect_with_priority.py")


def show_main_menu():
    """메인 메뉴 표시"""
    print("\n" + "="*60)
    print("🎯 개발 단계 관광지 데이터 수집 도구")
    print("="*60)
    print("1. 종합 대량 수집 (추천) - 모든 타입, 전국")
    print("2. 우선순위 기반 수집 - 부족한 데이터 우선")
    print("3. 특정 타입별 수집 - 개별 컨텐츠 타입")
    print("4. 현재 데이터 현황 확인")
    print("0. 종료")


async def show_data_status():
    """현재 데이터 현황 확인"""
    print("\n📊 현재 데이터 현황을 확인하려면 다음 명령어를 실행하세요:")
    print("python scripts/analyze_database_direct.py")
    print("\n또는 우선순위 분석:")
    print("python scripts/collect_with_priority.py")


async def main():
    """메인 실행 함수"""
    while True:
        show_main_menu()
        choice = input("\n선택: ").strip()
        
        if choice == "0":
            print("👋 프로그램을 종료합니다.")
            break
        elif choice == "1":
            await execute_comprehensive_collection()
        elif choice == "2":
            await execute_priority_collection()
        elif choice == "3":
            await execute_specific_collection()
        elif choice == "4":
            await show_data_status()
        else:
            print("⚠️ 올바른 선택지를 입력해주세요.")
        
        if choice in ["1", "2", "3", "4"]:
            input("\n계속하려면 Enter를 누르세요...")


if __name__ == "__main__":
    asyncio.run(main())