"""
지역 정보 동기화 배치 작업 (regions 테이블 기반)

기존 RegionUnificationJob을 regions 테이블 기반으로 재구현
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
from app.services.region_service import get_region_service
from app.core.database_manager import get_db_manager


class RegionJob(BaseJob):
    """지역 정보 동기화 작업 (regions 테이블 기반)"""
    
    def __init__(self):
        super().__init__()
        self.job_name = "region_job"
        self.region_service = get_region_service()
        self.db_manager = get_db_manager()
        
    async def execute(self) -> Dict[str, Any]:
        """지역 정보 동기화 실행"""
        
        self.logger.info("🚀 지역 정보 동기화 작업 시작")
        
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
            
            # 4. 데이터 품질 검사
            self.logger.info("✅ 4단계: 데이터 품질 검사")
            results['data_quality_check'] = await self._run_async_task(
                self._perform_data_quality_check()
            )
            
            # 5. 통계 수집
            self.logger.info("📊 5단계: 지역 정보 통계 수집")
            results['statistics'] = await self._run_async_task(
                self.region_service.get_region_statistics()
            )
            
            # 전체 결과 평가
            results['overall_status'] = self._evaluate_overall_status(results)
            
            # 배치 작업 로그 완료
            self._complete_batch_job_log(
                job_log_id, 
                results['overall_status'],
                self._calculate_processed_records(results)
            )
            
            self.logger.info(f"✅ 지역 정보 동기화 작업 완료: {results['overall_status']}")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ 지역 정보 동기화 작업 실패: {e}")
            results['overall_status'] = 'failure'
            results['error'] = str(e)
            
            # 배치 작업 로그 실패 업데이트
            self._complete_batch_job_log(job_log_id, 'failure', 0, str(e))
            
            return results
    
    async def _run_async_task(self, task_result) -> Any:
        """동기 작업을 비동기로 실행"""
        # 이미 완료된 결과인 경우 그대로 반환
        if isinstance(task_result, dict):
            return task_result
        
        # 비동기 작업인 경우 await
        return await task_result
    
    def _perform_data_quality_check(self) -> Dict[str, Any]:
        """데이터 품질 검사 수행"""
        try:
            quality_check = {
                'region_consistency': self._check_region_consistency(),
                'mapping_completeness': self._check_mapping_completeness(),
                'coordinate_validity': self._check_coordinate_validity(),
                'duplicate_detection': self._check_duplicates(),
                'overall_quality_score': 0.0
            }
            
            # 전체 품질 점수 계산
            scores = []
            for check_name, check_result in quality_check.items():
                if isinstance(check_result, dict) and 'score' in check_result:
                    scores.append(check_result['score'])
            
            if scores:
                quality_check['overall_quality_score'] = sum(scores) / len(scores)
            
            return quality_check
            
        except Exception as e:
            self.logger.error(f"데이터 품질 검사 실패: {e}")
            return {'error': str(e)}
    
    def _check_region_consistency(self) -> Dict[str, Any]:
        """지역 정보 일관성 검사"""
        try:
            # 부모-자식 관계 일관성 검사
            inconsistent_relations = self.db_manager.fetch_all("""
                SELECT r1.region_code, r1.region_name, r2.region_name as parent_name
                FROM regions r1
                LEFT JOIN regions r2 ON r1.parent_region_code = r2.region_code
                WHERE r1.parent_region_code IS NOT NULL 
                  AND r2.region_code IS NULL
            """)
            
            # 중복 지역 코드 검사
            duplicate_codes = self.db_manager.fetch_all("""
                SELECT region_code, COUNT(*) as count
                FROM regions
                GROUP BY region_code
                HAVING COUNT(*) > 1
            """)
            
            consistency_score = 100.0
            issues = []
            
            if inconsistent_relations:
                consistency_score -= len(inconsistent_relations) * 5
                issues.append(f"부모 관계 불일치: {len(inconsistent_relations)}건")
            
            if duplicate_codes:
                consistency_score -= len(duplicate_codes) * 10
                issues.append(f"중복 지역 코드: {len(duplicate_codes)}건")
            
            return {
                'score': max(0, consistency_score),
                'issues': issues,
                'inconsistent_relations': len(inconsistent_relations),
                'duplicate_codes': len(duplicate_codes)
            }
            
        except Exception as e:
            return {'error': str(e), 'score': 0}
    
    def _check_mapping_completeness(self) -> Dict[str, Any]:
        """매핑 완성도 검사"""
        try:
            # 전체 지역 수
            total_regions = self.db_manager.fetch_one(
                "SELECT COUNT(*) as count FROM regions WHERE region_level = 1 AND is_active = true"
            )['count']
            
            # KTO 매핑된 지역 수
            kto_mapped = self.db_manager.fetch_one("""
                SELECT COUNT(*) as count
                FROM regions
                WHERE api_mappings ? 'KTO' 
                  AND region_level = 1 
                  AND is_active = true
            """)['count']
            
            # KMA 매핑된 지역 수
            kma_mapped = self.db_manager.fetch_one("""
                SELECT COUNT(*) as count
                FROM regions
                WHERE api_mappings ? 'KMA' 
                  AND region_level = 1 
                  AND is_active = true
            """)['count']
            
            # 완성도 점수 계산
            kto_completeness = (kto_mapped / total_regions) * 100 if total_regions > 0 else 0
            kma_completeness = (kma_mapped / total_regions) * 100 if total_regions > 0 else 0
            overall_completeness = (kto_completeness + kma_completeness) / 2
            
            return {
                'score': overall_completeness,
                'total_regions': total_regions,
                'kto_mapped': kto_mapped,
                'kma_mapped': kma_mapped,
                'kto_completeness': kto_completeness,
                'kma_completeness': kma_completeness
            }
            
        except Exception as e:
            return {'error': str(e), 'score': 0}
    
    def _check_coordinate_validity(self) -> Dict[str, Any]:
        """좌표 유효성 검사"""
        try:
            # 유효하지 않은 좌표 검사
            invalid_coordinates = self.db_manager.fetch_all("""
                SELECT region_code, center_latitude, center_longitude
                FROM regions
                WHERE coordinate_info IS NOT NULL
                  AND (
                    CAST(center_latitude AS FLOAT) < 33.0 OR 
                    CAST(center_latitude AS FLOAT) > 38.5 OR
                    CAST(center_longitude AS FLOAT) < 124.0 OR 
                    CAST(center_longitude AS FLOAT) > 132.0
                  )
            """)
            
            # 전체 좌표 수
            total_coordinates = self.db_manager.fetch_one(
                "SELECT COUNT(*) as count FROM regions WHERE coordinate_info IS NOT NULL"
            )['count']
            
            # 검증된 좌표 수
            verified_coordinates = self.db_manager.fetch_one("""
                SELECT COUNT(*) as count 
                FROM regions 
                WHERE coordinate_info->>'is_verified' = 'true'
            """)['count']
            
            # 유효성 점수 계산
            invalid_count = len(invalid_coordinates)
            validity_score = 100.0
            
            if total_coordinates > 0:
                invalid_ratio = invalid_count / total_coordinates
                validity_score = max(0, 100 - (invalid_ratio * 100))
            
            return {
                'score': validity_score,
                'total_coordinates': total_coordinates,
                'verified_coordinates': verified_coordinates,
                'invalid_coordinates': invalid_count,
                'verification_rate': (verified_coordinates / total_coordinates * 100) if total_coordinates > 0 else 0
            }
            
        except Exception as e:
            return {'error': str(e), 'score': 0}
    
    def _check_duplicates(self) -> Dict[str, Any]:
        """중복 데이터 검사"""
        try:
            # 중복 지역명 검사
            duplicate_names = self.db_manager.fetch_all("""
                SELECT region_name, COUNT(*) as count
                FROM regions
                GROUP BY region_name, region_level
                HAVING COUNT(*) > 1
            """)
            
            # 중복 API 매핑 검사 (JSONB 필드이므로 간접 검사)
            duplicate_mappings = self.db_manager.fetch_all("""
                SELECT region_code, api_mappings
                FROM regions
                WHERE api_mappings IS NOT NULL
            """)
            
            # 중복 매핑 카운트
            mapping_duplicates = 0
            seen_mappings = {}
            
            for row in duplicate_mappings:
                mappings = row.get('api_mappings', {})
                for provider, mapping in mappings.items():
                    key = f"{provider}:{mapping.get('api_region_code', '')}"
                    if key in seen_mappings:
                        mapping_duplicates += 1
                    else:
                        seen_mappings[key] = row['region_code']
            
            # 중복 점수 계산
            duplicate_score = 100.0
            issues = []
            
            if duplicate_names:
                duplicate_score -= len(duplicate_names) * 5
                issues.append(f"중복 지역명: {len(duplicate_names)}건")
            
            if mapping_duplicates > 0:
                duplicate_score -= mapping_duplicates * 10
                issues.append(f"중복 API 매핑: {mapping_duplicates}건")
            
            return {
                'score': max(0, duplicate_score),
                'issues': issues,
                'duplicate_names': len(duplicate_names),
                'duplicate_mappings': mapping_duplicates
            }
            
        except Exception as e:
            return {'error': str(e), 'score': 0}
    
    def _evaluate_overall_status(self, results: Dict[str, Any]) -> str:
        """전체 작업 상태 평가"""
        try:
            # 각 단계별 성공 여부 확인
            kto_success = results.get('kto_sync', {}).get('status') == 'success'
            kma_success = results.get('kma_sync', {}).get('status') == 'success'
            
            # 좌표 검증 정확도 확인
            coord_validation = results.get('coordinate_validation', {})
            coord_accuracy = coord_validation.get('accuracy_rate', 0)
            
            # 데이터 품질 점수 확인
            quality_check = results.get('data_quality_check', {})
            quality_score = quality_check.get('overall_quality_score', 0)
            
            # 종합 평가
            if kto_success and kma_success and coord_accuracy >= 80 and quality_score >= 85:
                return 'success'
            elif (kto_success or kma_success) and coord_accuracy >= 60 and quality_score >= 70:
                return 'partial_success'
            else:
                return 'failure'
                
        except Exception as e:
            self.logger.error(f"전체 상태 평가 실패: {e}")
            return 'failure'
    
    def _calculate_processed_records(self, results: Dict[str, Any]) -> int:
        """처리된 레코드 수 계산"""
        try:
            total_processed = 0
            
            # KTO 동기화 결과
            kto_sync = results.get('kto_sync', {})
            total_processed += kto_sync.get('total_processed', 0)
            
            # KMA 동기화 결과
            kma_sync = results.get('kma_sync', {})
            total_processed += kma_sync.get('total_processed', 0)
            
            # 좌표 검증 결과
            coord_validation = results.get('coordinate_validation', {})
            total_processed += coord_validation.get('total_checked', 0)
            
            return total_processed
            
        except Exception as e:
            self.logger.error(f"처리된 레코드 수 계산 실패: {e}")
            return 0
    
    def _create_batch_job_log(self) -> int:
        """배치 작업 로그 생성"""
        try:
            result = self.db_manager.fetch_one("""
                INSERT INTO batch_job_logs 
                (job_name, job_type, status, started_at, execution_context)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """, (
                self.job_name,
                'region_sync',
                'running',
                datetime.now(),
                json.dumps({
                    'sync_types': ['kto_sync', 'kma_sync'],
                    'validation_enabled': True,
                    'quality_check_enabled': True,
                    'table': 'regions'  # regions 테이블 사용 명시
                })
            ))
            
            return result['id']
            
        except Exception as e:
            self.logger.error(f"배치 작업 로그 생성 실패: {e}")
            return 0
    
    def _complete_batch_job_log(self, job_log_id: int, status: str, 
                               processed_records: int = 0, error_message: str = None):
        """배치 작업 로그 완료"""
        try:
            if job_log_id == 0:
                return
                
            self.db_manager.execute_query("""
                UPDATE batch_job_logs 
                SET status = %s, completed_at = %s, processed_records = %s, error_message = %s
                WHERE id = %s
            """, (status, datetime.now(), processed_records, error_message, job_log_id))
            
        except Exception as e:
            self.logger.error(f"배치 작업 로그 완료 실패: {e}")


async def run_region_job():
    """지역 정보 동기화 작업 실행"""
    job = RegionJob()
    return await job.execute()


if __name__ == "__main__":
    import json
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    async def main():
        print("=== 지역 정보 동기화 배치 작업 ===")
        
        try:
            result = await run_region_job()
            
            print(f"\n✅ 배치 작업 완료")
            print(f"전체 상태: {result['overall_status']}")
            print(f"처리된 레코드: {result.get('processed_records', 0)}")
            
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
            
            if result.get('data_quality_check'):
                quality = result['data_quality_check']
                print(f"\n데이터 품질: {quality.get('overall_quality_score', 0):.1f}점")
            
        except Exception as e:
            print(f"❌ 배치 작업 실패: {e}")
    
    asyncio.run(main())