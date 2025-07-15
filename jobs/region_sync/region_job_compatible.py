"""
지역 정보 동기화 배치 작업 (regions 테이블 기반 - 호환 버전)

기존 regions 테이블 구조와 호환되는 버전
"""

import asyncio
import logging
from typing import Dict, Any

from app.core.base_job import BaseJob
from app.services.region_service_compatible import get_region_service_compatible
from app.core.database_manager import get_db_manager


class RegionJobCompatible(BaseJob):
    """지역 정보 동기화 작업 (호환 버전)"""
    
    def __init__(self):
        super().__init__()
        self.job_name = "region_job_compatible"
        self.region_service = get_region_service_compatible()
        self.db_manager = get_db_manager()
        
    async def execute(self) -> Dict[str, Any]:
        """지역 정보 동기화 실행"""
        
        self.logger.info("🚀 지역 정보 동기화 작업 시작 (호환 모드)")
        
        results = {
            'kto_sync': None,
            'kma_sync': None,
            'coordinate_validation': None,
            'statistics': None,
            'overall_status': 'running'
        }
        
        try:
            # 1. KTO 지역 정보 동기화
            self.logger.info("📍 1단계: KTO 지역 정보 동기화")
            results['kto_sync'] = await self._run_async_task(
                self.region_service.sync_kto_regions()
            )
            
            if results['kto_sync']['status'] != 'success':
                self.logger.warning("⚠️ KTO 동기화에 문제가 있습니다. 계속 진행합니다.")
            
            # 2. KMA 지역 정보 동기화
            self.logger.info("🌤️ 2단계: KMA 지역 정보 동기화")
            results['kma_sync'] = await self._run_async_task(
                self.region_service.sync_kma_regions()
            )
            
            if results['kma_sync']['status'] != 'success':
                self.logger.warning("⚠️ KMA 동기화에 문제가 있습니다. 계속 진행합니다.")
            
            # 3. 좌표 변환 정확도 검증
            self.logger.info("🔍 3단계: 좌표 변환 정확도 검증")
            results['coordinate_validation'] = await self._run_async_task(
                self.region_service.validate_coordinate_transformations(100)
            )
            
            # 4. 통계 수집
            self.logger.info("📊 4단계: 지역 정보 통계 수집")
            results['statistics'] = await self._run_async_task(
                self.region_service.get_region_statistics()
            )
            
            # 전체 결과 평가
            results['overall_status'] = self._evaluate_overall_status(results)
            
            self.logger.info(f"✅ 지역 정보 동기화 작업 완료: {results['overall_status']}")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 지역 정보 동기화 작업 실패: {e}")
            results['overall_status'] = 'failure'
            results['error'] = str(e)
            
            return results
    
    async def _run_async_task(self, task_result) -> Any:
        """동기 작업을 비동기로 실행"""
        # 이미 완료된 결과인 경우 그대로 반환
        if isinstance(task_result, dict):
            return task_result
        
        # 비동기 작업인 경우 await
        return await task_result
    
    def _evaluate_overall_status(self, results: Dict[str, Any]) -> str:
        """전체 작업 상태 평가"""
        try:
            # 각 단계별 성공 여부 확인
            kto_success = results.get('kto_sync', {}).get('status') == 'success'
            kma_success = results.get('kma_sync', {}).get('status') == 'success'
            
            # 좌표 검증 정확도 확인
            coord_validation = results.get('coordinate_validation', {})
            coord_accuracy = coord_validation.get('accuracy_rate', 0)
            
            # 종합 평가
            if kto_success and kma_success and coord_accuracy >= 80:
                return 'success'
            elif (kto_success or kma_success) and coord_accuracy >= 60:
                return 'partial_success'
            else:
                return 'failure'
                
        except Exception as e:
            self.logger.error(f"전체 상태 평가 실패: {e}")
            return 'failure'


async def run_region_job_compatible():
    """지역 정보 동기화 작업 실행"""
    job = RegionJobCompatible()
    return await job.execute()


if __name__ == "__main__":
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def main():
        print("=== 지역 정보 동기화 배치 작업 (호환 모드) ===")
        
        try:
            result = await run_region_job_compatible()
            
            print("\n✅ 배치 작업 완료")
            print(f"전체 상태: {result['overall_status']}")
            
            # 결과 상세 출력
            if result.get('kto_sync'):
                print(f"\nKTO 동기화: {result['kto_sync']['status']}")
                print(f"- 생성: {result['kto_sync'].get('regions_created', 0)}")
                print(f"- 업데이트: {result['kto_sync'].get('regions_updated', 0)}")
            
            if result.get('kma_sync'):
                print(f"\nKMA 동기화: {result['kma_sync']['status']}")
                print(f"- 매핑 생성: {result['kma_sync'].get('mappings_created', 0)}")
            
            if result.get('coordinate_validation'):
                coord_val = result['coordinate_validation']
                print(f"\n좌표 검증: {coord_val.get('accuracy_rate', 0):.1f}% 정확도")
                print(f"- 검사: {coord_val.get('total_checked', 0)}")
                print(f"- 정확: {coord_val.get('accurate_mappings', 0)}")
            
            if result.get('statistics'):
                stats = result['statistics']
                print("\n지역 통계:")
                print(f"- 전체 지역: {stats.get('total_regions', 0)}")
                print(f"- 레벨별: {stats.get('by_level', {})}")
                print(f"- API 매핑: {stats.get('by_api', {})}")
            
        except Exception as e:
            print(f"❌ 배치 작업 실패: {e}")
    
    asyncio.run(main())