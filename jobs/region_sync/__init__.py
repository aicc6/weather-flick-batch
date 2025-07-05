"""
지역 정보 통합 동기화 작업 모듈
"""

from .region_unification_job import RegionUnificationJob, run_region_unification_job

__all__ = ['RegionUnificationJob', 'run_region_unification_job']