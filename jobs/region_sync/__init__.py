"""
지역 정보 동기화 작업 모듈
"""

from .region_job import RegionJob, run_region_job
# 호환성을 위한 임시 import (추후 제거 예정)
try:
    from .region_unification_job import RegionUnificationJob, run_region_unification_job
    __all__ = ['RegionJob', 'run_region_job', 'RegionUnificationJob', 'run_region_unification_job']
except ImportError:
    __all__ = ['RegionJob', 'run_region_job']