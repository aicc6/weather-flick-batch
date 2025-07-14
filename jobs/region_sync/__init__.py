"""
지역 정보 동기화 작업 모듈
"""

# 새로운 RegionJob 사용
from .region_job import RegionJob

# 호환성을 위한 별칭 (기존 코드가 RegionUnificationJob을 사용하는 경우)
RegionUnificationJob = RegionJob

# 비동기 실행 함수
async def run_region_job():
    """RegionJob 실행"""
    job = RegionJob()
    return await job.execute()

# 호환성을 위한 별칭
run_region_unification_job = run_region_job

__all__ = ['RegionJob', 'run_region_job', 'RegionUnificationJob', 'run_region_unification_job']