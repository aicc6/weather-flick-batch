"""
지역 정보 동기화 배치 작업 (regions 테이블 기반)

기존 RegionUnificationJob을 regions 테이블 기반으로 재구현
- KTO API 지역 정보 동기화
- KMA API 지역 정보 동기화
- 좌표 변환 검증
"""

import asyncio
import logging
from typing import Dict, Any

from app.services.region_service import RegionService
from jobs.base_job import BaseJob
from config.job_config import JOB_CONFIG


class RegionJob(BaseJob):
    """지역 정보 동기화 배치 작업"""
    
    def __init__(self):
        super().__init__(
            job_name="region_sync",
            description="지역 정보 동기화 작업 (regions 테이블 기반)"
        )
        self.region_service = RegionService()
        
    async def execute(self) -> Dict[str, Any]:
        """배치 작업 실행"""
        self.logger.info("=== 지역 정보 동기화 시작 ===")
        
        results = {
            'status': 'success',
            'kto_sync': None,
            'kma_sync': None,
            'validation': None,
            'statistics': None,
            'errors': []
        }
        
        try:
            # 1. KTO API 지역 정보 동기화
            self.logger.info("1단계: KTO API 지역 정보 동기화")
            try:
                kto_result = await asyncio.to_thread(self.region_service.sync_kto_regions)
                results['kto_sync'] = kto_result
                self.logger.info(f"KTO 동기화 완료: {kto_result}")
            except Exception as e:
                self.logger.error(f"KTO 동기화 실패: {e}")
                results['errors'].append(f"KTO sync error: {str(e)}")
                results['status'] = 'partial_failure'
            
            # 2. KMA API 지역 정보 동기화
            self.logger.info("2단계: KMA API 지역 정보 동기화")
            try:
                kma_result = await asyncio.to_thread(self.region_service.sync_kma_regions)
                results['kma_sync'] = kma_result
                self.logger.info(f"KMA 동기화 완료: {kma_result}")
            except Exception as e:
                self.logger.error(f"KMA 동기화 실패: {e}")
                results['errors'].append(f"KMA sync error: {str(e)}")
                results['status'] = 'partial_failure'
            
            # 3. 좌표 변환 검증 (선택적)
            if JOB_CONFIG.get('region_sync', {}).get('validate_coordinates', True):
                self.logger.info("3단계: 좌표 변환 정확도 검증")
                try:
                    validation_result = await asyncio.to_thread(
                        self.region_service.validate_coordinate_transformations,
                        sample_size=50
                    )
                    results['validation'] = validation_result
                    self.logger.info(f"검증 완료: {validation_result.get('accuracy_rate', 0):.1f}% 정확도")
                    
                    # 정확도가 낮은 경우 경고
                    if validation_result.get('accuracy_rate', 100) < 80:
                        self.logger.warning("⚠️ 좌표 변환 정확도가 80% 미만입니다!")
                        results['errors'].append("Coordinate transformation accuracy below 80%")
                except Exception as e:
                    self.logger.error(f"좌표 검증 실패: {e}")
                    results['errors'].append(f"Validation error: {str(e)}")
            
            # 4. 통계 정보 수집
            self.logger.info("4단계: 지역 정보 통계 수집")
            try:
                statistics = await asyncio.to_thread(self.region_service.get_region_statistics)
                results['statistics'] = statistics
                self.logger.info(f"전체 지역 수: {statistics.get('total_regions', 0)}")
            except Exception as e:
                self.logger.error(f"통계 수집 실패: {e}")
                results['errors'].append(f"Statistics error: {str(e)}")
            
            # 최종 상태 결정
            if results['errors'] and results['status'] != 'partial_failure':
                results['status'] = 'completed_with_errors'
            
            self.logger.info(f"=== 지역 정보 동기화 완료: {results['status']} ===")
            return results
            
        except Exception as e:
            self.logger.error(f"배치 작업 실행 중 예상치 못한 오류: {e}")
            results['status'] = 'failure'
            results['errors'].append(f"Unexpected error: {str(e)}")
            return results
    
    def validate_config(self) -> bool:
        """설정 검증"""
        config = JOB_CONFIG.get('region_sync', {})
        
        # 필수 설정 확인
        if not config.get('enabled', True):
            self.logger.info("지역 동기화 작업이 비활성화되어 있습니다.")
            return False
        
        # 동기화 소스 확인
        sync_sources = config.get('sync_sources', ['KTO', 'KMA'])
        if not sync_sources:
            self.logger.error("동기화 소스가 설정되지 않았습니다.")
            return False
        
        return True
    
    def get_schedule(self) -> str:
        """스케줄 정보 반환"""
        # 기본: 매일 새벽 3시에 실행
        return JOB_CONFIG.get('region_sync', {}).get('schedule', '0 3 * * *')


# 배치 작업 인스턴스 생성 함수
def get_job():
    """배치 프레임워크에서 사용할 작업 인스턴스 반환"""
    return RegionJob()


if __name__ == "__main__":
    # 독립 실행 테스트
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    job = RegionJob()
    
    # 설정 검증
    if not job.validate_config():
        print("❌ 설정 검증 실패")
        exit(1)
    
    # 비동기 실행
    async def run_test():
        result = await job.execute()
        print(f"\n실행 결과: {result}")
    
    asyncio.run(run_test())