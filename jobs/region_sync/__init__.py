"""
지역 정보 동기화 작업 모듈
"""

# 새로운 버전 (간단한 버전 사용)
from .region_job_simple import RegionJobSimple as RegionJob, run_region_job_simple as run_region_job

# 호환성을 위한 별칭 (기존 코드가 RegionUnificationJob을 사용하는 경우)
RegionUnificationJob = RegionJob
run_region_unification_job = run_region_job

__all__ = ['RegionJob', 'run_region_job', 'RegionUnificationJob', 'run_region_unification_job']