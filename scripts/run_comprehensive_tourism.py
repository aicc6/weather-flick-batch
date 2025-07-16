#!/usr/bin/env python3
"""
종합 관광정보 수집 스크립트
"""

import asyncio
import sys
import os

# 상위 디렉토리 경로 추가
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from jobs.tourism.comprehensive_tourism_job import ComprehensiveTourismJob

async def main():
    """메인 실행 함수"""
    try:
        # 종합 관광정보 수집 작업 실행
        job = ComprehensiveTourismJob()
        success = await job.execute()
        
        if success:
            print("종합 관광정보 수집이 성공적으로 완료되었습니다.")
        else:
            print("종합 관광정보 수집 중 오류가 발생했습니다.")
            
    except Exception as e:
        print(f"스크립트 실행 중 오류: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(main())