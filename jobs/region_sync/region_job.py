"""
지역 정보 동기화 배치 작업 (regions 테이블 기반)

기상청과 한국관광공사 API의 지역 정보를 통합 관리하는 배치 작업
- KTO 지역 코드 동기화
- KMA 지역 매핑 동기화  
- 좌표 변환 검증
- 데이터 품질 검사
"""

import asyncio
import logging
import json
from typing import Dict, Any
from datetime import datetime

from app.core.base_job import BaseJob
from app.services.region_service import RegionService
from app.core.database_manager import get_db_manager


class RegionJob(BaseJob):
    """지역 정보 동기화 작업 (regions 테이블 기반)"""
    
    def __init__(self):
        super().__init__()
        self.job_name = "region_sync_job"
        self.region_service = RegionService()
        self.db_manager = get_db_manager()
        
    async def execute(self) -> Dict[str, Any]:
        """지역 정보 동기화 실행"""
        
        self.logger.info("🚀 지역 정보 동기화 작업 시작 (regions 테이블 기반)")
        
        # 배치 작업 로그 시작
        job_log_id = self._create_batch_job_log()
        
        results = {
            'job_log_id': job_log_id,
            'kto_sync': None,
            'kma_sync': None,
            'coordinate_validation': None,
            'data_quality_check': None,
            'statistics': None,
            'overall_status': 'running'
        }
        
        try:
            # 1. KTO 지역 정보 동기화
            self.logger.info("📍 1단계: KTO 지역 정보 동기화")
            kto_result = await self._run_with_timeout(
                self.region_service.sync_kto_regions(),
                timeout=600  # 10분
            )
            results['kto_sync'] = kto_result
            
            # 2. KMA 지역 정보 동기화
            self.logger.info("🌤️ 2단계: KMA 지역 정보 동기화")
            kma_result = await self._run_with_timeout(
                self.region_service.sync_kma_regions(),
                timeout=600  # 10분
            )
            results['kma_sync'] = kma_result
            
            # 3. 좌표 변환 검증
            self.logger.info("🔍 3단계: 좌표 변환 정확도 검증")
            validation_result = await self._run_with_timeout(
                self.region_service.validate_coordinate_transformations(
                    sample_size=50
                ),
                timeout=300  # 5분
            )
            results['coordinate_validation'] = validation_result
            
            # 4. 데이터 품질 검사
            self.logger.info("✅ 4단계: 데이터 품질 검사")
            quality_result = await self._run_with_timeout(
                self._check_data_quality(),
                timeout=180  # 3분
            )
            results['data_quality_check'] = quality_result
            
            # 5. 통계 수집
            self.logger.info("📊 5단계: 지역 정보 통계 수집")
            stats = await self._run_with_timeout(
                self._collect_statistics(),
                timeout=60  # 1분
            )
            results['statistics'] = stats
            
            # 전체 상태 결정
            if all([
                results.get('kto_sync', {}).get('status') == 'success',
                results.get('kma_sync', {}).get('status') == 'success',
                results.get('coordinate_validation', {}).get('overall_status') == 'success'
            ]):
                results['overall_status'] = 'success'
            else:
                results['overall_status'] = 'partial_success'
                
            self.logger.info(f"✅ 지역 정보 동기화 완료: {results['overall_status']}")
            
        except Exception as e:
            self.logger.error(f"❌ 지역 정보 동기화 실패: {e}")
            results['overall_status'] = 'failure'
            results['error'] = str(e)
            
        finally:
            # 배치 작업 로그 업데이트
            self._update_batch_job_log(
                job_log_id,
                results['overall_status'],
                results
            )
            
        return results
    
    async def _check_data_quality(self) -> Dict[str, Any]:
        """데이터 품질 검사"""
        try:
            quality_checks = {
                'missing_coordinates': 0,
                'duplicate_codes': 0,
                'invalid_mappings': 0,
                'orphan_regions': 0,
                'total_issues': 0
            }
            
            # 1. 좌표 누락 확인
            query = """
            SELECT COUNT(*) as count
            FROM regions
            WHERE (latitude IS NULL OR longitude IS NULL)
              AND is_active = true
            """
            result = self.db_manager.fetch_one(query)
            quality_checks['missing_coordinates'] = result['count'] if result else 0
            
            # 2. 중복 코드 확인
            query = """
            SELECT COUNT(*) as count
            FROM (
                SELECT region_code, COUNT(*) as cnt
                FROM regions
                WHERE is_active = true
                GROUP BY region_code
                HAVING COUNT(*) > 1
            ) duplicates
            """
            result = self.db_manager.fetch_one(query)
            quality_checks['duplicate_codes'] = result['count'] if result else 0
            
            # 3. 고아 지역 확인 (부모가 없는 하위 지역)
            query = """
            SELECT COUNT(*) as count
            FROM regions r1
            WHERE r1.parent_region_code IS NOT NULL
              AND r1.is_active = true
              AND NOT EXISTS (
                  SELECT 1 FROM regions r2
                  WHERE r2.region_code = r1.parent_region_code
                  AND r2.is_active = true
              )
            """
            result = self.db_manager.fetch_one(query)
            quality_checks['orphan_regions'] = result['count'] if result else 0
            
            # 전체 이슈 수 계산
            quality_checks['total_issues'] = sum([
                quality_checks['missing_coordinates'],
                quality_checks['duplicate_codes'],
                quality_checks['orphan_regions']
            ])
            
            quality_checks['status'] = 'pass' if quality_checks['total_issues'] == 0 else 'warning'
            
            return quality_checks
            
        except Exception as e:
            self.logger.error(f"데이터 품질 검사 실패: {e}")
            return {'status': 'error', 'error': str(e)}
    
    async def _collect_statistics(self) -> Dict[str, Any]:
        """지역 정보 통계 수집"""
        try:
            stats = {}
            
            # 전체 지역 수
            query = "SELECT COUNT(*) as count FROM regions WHERE is_active = true"
            result = self.db_manager.fetch_one(query)
            stats['total_regions'] = result['count'] if result else 0
            
            # 레벨별 지역 수
            query = """
            SELECT region_level, COUNT(*) as count
            FROM regions
            WHERE is_active = true
            GROUP BY region_level
            ORDER BY region_level
            """
            results = self.db_manager.fetch_all(query)
            stats['regions_by_level'] = {
                row['region_level']: row['count'] 
                for row in results
            } if results else {}
            
            # 좌표 정보가 있는 지역 수
            query = """
            SELECT COUNT(*) as count
            FROM regions
            WHERE latitude IS NOT NULL 
              AND longitude IS NOT NULL
              AND is_active = true
            """
            result = self.db_manager.fetch_one(query)
            stats['regions_with_coordinates'] = result['count'] if result else 0
            
            # 그리드 정보가 있는 지역 수
            query = """
            SELECT COUNT(*) as count
            FROM regions
            WHERE grid_x IS NOT NULL 
              AND grid_y IS NOT NULL
              AND is_active = true
            """
            result = self.db_manager.fetch_one(query)
            stats['regions_with_grid'] = result['count'] if result else 0
            
            return stats
            
        except Exception as e:
            self.logger.error(f"통계 수집 실패: {e}")
            return {'error': str(e)}
    
    async def _run_with_timeout(self, coro, timeout: int):
        """타임아웃과 함께 코루틴 실행"""
        try:
            # 코루틴이 아닌 경우 처리
            if not asyncio.iscoroutine(coro):
                return coro
            
            return await asyncio.wait_for(coro, timeout=timeout)
        except asyncio.TimeoutError:
            self.logger.error(f"작업이 {timeout}초 내에 완료되지 않았습니다.")
            return {'status': 'timeout', 'timeout_seconds': timeout}


if __name__ == "__main__":
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 작업 실행
    job = RegionJob()
    result = asyncio.run(job.execute())
    
    print("\n=== 지역 정보 동기화 결과 ===")
    print(json.dumps(result, indent=2, ensure_ascii=False))