#!/usr/bin/env python3
"""
전체 관광 데이터 수집 스크립트
- 모든 컨텐츠 타입의 데이터를 수집
"""

import asyncio
import os
import sys
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobs.tourism.comprehensive_tourism_job import ComprehensiveTourismJob

async def collect_all_tourism_data():
    """전체 관광 데이터 수집"""
    
    print("전체 관광 데이터 수집 시작...")
    
    # Job 인스턴스 생성
    job = ComprehensiveTourismJob()
    
    try:
        # 실행
        success = await job.execute()
        
        if success:
            print("전체 관광 데이터 수집 완료!")
        else:
            print("전체 관광 데이터 수집 실패!")
        
    except Exception as e:
        print(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(collect_all_tourism_data())