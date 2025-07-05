"""
종합 관광정보 수집 배치 작업

한국관광공사 API의 모든 엔드포인트를 사용하여 종합적인 관광정보를 수집하고
데이터베이스에 저장하는 배치 작업입니다.
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict

# 상위 디렉토리 경로 추가 (모듈 import를 위해)
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

# 환경 변수 명시적 로드 (tourism 작업 실행 시 필요)
from dotenv import load_dotenv

load_dotenv(override=True)

# API 키 매니저 및 클라이언트 리셋 (싱글톤 인스턴스 재생성을 위해)
from app.core.multi_api_key_manager import reset_api_key_manager
from app.core.unified_api_client import reset_unified_api_client

reset_api_key_manager()
reset_unified_api_client()

from app.core.base_job import BaseJob, JobConfig
from app.collectors.unified_kto_client import get_unified_kto_client
from app.processors.data_transformation_pipeline import get_transformation_pipeline
from app.core.database_manager_extension import get_extended_database_manager


class ComprehensiveTourismJob(BaseJob):
    """종합 관광정보 수집 작업"""

    def __init__(self, config: JobConfig = None):
        # 기본 설정이 없으면 생성
        if config is None:
            from config.constants import JobType

            config = JobConfig(
                job_name="comprehensive_tourism_sync",
                job_type=JobType.TOURIST_DATA,
                schedule_expression="0 2 * * SUN",
                retry_count=2,
                timeout_minutes=240,  # 4시간
                enabled=True,
            )

        super().__init__(config)
        self.job_name = "comprehensive_tourism_sync"
        self.job_type = "comprehensive_tourism_sync"

        # 통합 구조 초기화
        self.unified_client = get_unified_kto_client()
        self.transformation_pipeline = get_transformation_pipeline()
        self.db_manager = get_extended_database_manager()

        self.logger.info("종합 관광정보 수집 작업 초기화 완료")

    async def execute(self) -> bool:
        """작업 실행 (비동기)"""
        try:
            self.logger.info("=== 종합 관광정보 수집 작업 시작 ===")
            start_time = time.time()

            # 1. 통합 KTO 데이터 수집 (새 구조 사용 + 신규 API 포함)
            self.logger.info("1단계: 통합 KTO API를 통한 종합 데이터 수집 (신규 API 포함)")
            try:
                # 새로운 통합 구조로 모든 데이터 수집 (4개 신규 API 포함)
                collection_result = await self.unified_client.collect_all_data(
                    content_types=None,  # 모든 컨텐츠 타입
                    area_codes=None,  # 모든 지역
                    store_raw=True,  # 원본 데이터 저장
                    auto_transform=True,  # 자동 변환 수행
                    include_new_apis=True,  # 신규 추가된 4개 API 포함
                )

                self.logger.info(
                    f"수집 완료: 원본 {collection_result['total_raw_records']}건, 처리 {collection_result['total_processed_records']}건"
                )

                # 신규 API 수집 결과 로깅
                new_apis_collected = collection_result.get("new_apis_collected", {})
                if new_apis_collected:
                    self.logger.info("=== 신규 API 수집 결과 ===")
                    for api_name, api_result in new_apis_collected.items():
                        raw_count = api_result.get("total_raw_records", 0)
                        processed_count = api_result.get("total_processed_records", 0)
                        self.logger.info(f"  - {api_name}: 원본 {raw_count}건, 처리 {processed_count}건")

                # 수집 결과를 기존 형태로 변환 (호환성)
                comprehensive_data = {
                    "total_raw_records": collection_result["total_raw_records"],
                    "total_processed_records": collection_result[
                        "total_processed_records"
                    ],
                    "content_types_collected": collection_result[
                        "content_types_collected"
                    ],
                    "new_apis_collected": collection_result.get("new_apis_collected", {}),
                    "sync_batch_id": collection_result["sync_batch_id"],
                }

            except Exception as e:
                self.logger.error(f"통합 데이터 수집 중 오류: {e}")
                # 기본 통계 설정
                comprehensive_data = {
                    "total_raw_records": 0,
                    "total_processed_records": 0,
                    "content_types_collected": {},
                    "sync_batch_id": f"fallback_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                }

            # 2. 수집된 데이터 통계 (새 구조)
            total_collected = comprehensive_data.get("total_raw_records", 0)
            total_processed = comprehensive_data.get("total_processed_records", 0)

            self.logger.info(f"원본 데이터 수집: {total_collected:,}개")
            self.logger.info(f"처리된 데이터: {total_processed:,}개")

            # 컨텐츠 타입별 통계
            for content_type, content_result in comprehensive_data.get(
                "content_types_collected", {}
            ).items():
                raw_count = content_result.get("total_raw_records", 0)
                processed_count = content_result.get("total_processed_records", 0)
                content_name = content_result.get(
                    "content_name", f"content_{content_type}"
                )
                self.logger.info(
                    f"  - {content_name}: 원본 {raw_count}개, 처리 {processed_count}개"
                )

            # 3. 데이터 처리 결과 (통합 구조에서 자동 처리됨)
            self.logger.info("2단계: 데이터 처리 완료 (통합 파이프라인에서 자동 처리)")

            # 처리 결과 통계 (이미 처리된 데이터)
            processing_results = {
                "total_processed": total_processed,
                "batch_id": comprehensive_data.get("sync_batch_id"),
            }

            self.logger.info(f"데이터베이스 저장 완료: {total_processed:,}개")

            # 5. 데이터 품질 검사
            self.logger.info("3단계: 데이터 품질 검사")
            quality_results = self._check_data_quality()

            # 6. 실행 시간 계산
            execution_time = time.time() - start_time
            self.logger.info(f"총 실행 시간: {execution_time:.2f}초")

            # 7. 작업 로그 저장 (새 구조)
            self._save_job_log(
                status="success",
                processed_records=total_processed,
                execution_time=execution_time,
                additional_info={
                    "raw_records_count": total_collected,
                    "processed_records_count": total_processed,
                    "processing_results": processing_results,
                    "quality_results": quality_results,
                    "sync_batch_id": comprehensive_data.get("sync_batch_id"),
                },
            )

            self.logger.info("=== 종합 관광정보 수집 작업 완료 ===")
            return True

        except Exception as e:
            self.logger.error(f"종합 관광정보 수집 작업 실패: {e}")
            self._save_job_log(status="failure", error_message=str(e))
            return False

    def _check_data_quality(self) -> Dict:
        """데이터 품질 검사"""
        quality_results = {}

        try:
            # 기존 테이블들과 새로 추가된 신규 API 테이블들의 데이터 품질 검사
            tables_to_check = [
                "cultural_facilities",
                "festivals_events",
                "travel_courses",
                "leisure_sports",
                "accommodations",
                "shopping",
                "restaurants",
                # 신규 API 테이블들
                "pet_tour_info",
                "classification_system_codes",
                "area_based_sync_list",
                "legal_dong_codes",
            ]

            for table in tables_to_check:
                try:
                    # 테이블별 기본 통계 (새로운 DatabaseManager 사용)
                    query = f"""
                        SELECT
                            COUNT(*) as total_records,
                            COUNT(CASE WHEN latitude IS NULL OR longitude IS NULL THEN 1 END) as missing_coordinates,
                            COUNT(CASE WHEN address IS NULL OR address = '' THEN 1 END) as missing_address
                        FROM {table}
                    """

                    result = self.db_manager.fetch_one(query)
                    if result:
                        total = result["total_records"]
                        missing_coords = result["missing_coordinates"]
                        missing_addr = result["missing_address"]

                        # 안전한 품질 점수 계산 (타입 확인 포함)
                        try:
                            total = int(total) if total is not None else 0
                            missing_coords = (
                                int(missing_coords) if missing_coords is not None else 0
                            )
                            missing_addr = (
                                int(missing_addr) if missing_addr is not None else 0
                            )
                            quality_score = (
                                100.0
                                - ((missing_coords + missing_addr) * 100.0 / total)
                                if total > 0
                                else 0
                            )
                        except (ValueError, TypeError):
                            quality_score = 0

                        quality_results[table] = {
                            "total_records": total,
                            "missing_coordinates": missing_coords,
                            "missing_address": missing_addr,
                            "quality_score": round(quality_score, 2),
                        }

                        self.logger.info(
                            f"{table}: {total}개 레코드, 품질점수 {quality_score:.1f}%"
                        )

                except Exception as e:
                    self.logger.warning(f"{table} 품질 검사 오류: {e}")
                    continue

        except Exception as e:
            self.logger.error(f"데이터 품질 검사 실패: {e}")

        return quality_results

    def _save_job_log(
        self,
        status: str,
        processed_records: int = 0,
        execution_time: float = 0,
        error_message: str = None,
        additional_info: Dict = None,
    ):
        """작업 로그 저장"""
        try:
            # 추가 정보를 JSON 문자열로 변환
            info_json = None
            if additional_info:
                import json

                info_json = json.dumps(additional_info, ensure_ascii=False, default=str)

            # 새로운 DatabaseManager의 log_job_result 메서드 사용
            self.db_manager.log_job_result(
                job_name=self.job_name,
                job_type=self.job_type,
                status=status,
                start_time=datetime.now(),
                end_time=datetime.now(),
                processed_records=processed_records,
                error_message=error_message or info_json,
            )

        except Exception as e:
            self.logger.error(f"작업 로그 저장 실패: {e}")

    def get_schedule_info(self) -> Dict:
        """스케줄 정보 반환"""
        return {
            "job_name": self.job_name,
            "schedule": "매주 일요일 새벽 2시",
            "cron": "0 2 * * 0",  # 매주 일요일 새벽 2시
            "description": "한국관광공사 API 종합 관광정보 수집",
            "estimated_duration": "2-4시간",
        }


class IncrementalTourismJob(BaseJob):
    """증분 관광정보 수집 작업 (일일)"""

    def __init__(self, config: JobConfig = None):
        # 기본 설정이 없으면 생성
        if config is None:
            from config.constants import JobType

            config = JobConfig(
                job_name="incremental_tourism_sync",
                job_type=JobType.TOURIST_DATA,
                schedule_expression="0 3 * * *",
                retry_count=3,
                timeout_minutes=60,  # 1시간
                enabled=True,
            )

        super().__init__(config)
        self.job_name = "incremental_tourism_sync"
        self.job_type = "tourist_data"

        # 통합 구조 사용
        self.unified_client = get_unified_kto_client()
        self.transformation_pipeline = get_transformation_pipeline()
        self.db_manager = get_extended_database_manager()

    async def execute(self) -> bool:
        """증분 수집 실행 (주요 데이터만) - 비동기"""
        try:
            self.logger.info("=== 증분 관광정보 수집 작업 시작 ===")

            # 주요 지역만 대상으로 증분 수집
            major_areas = ["1", "6", "31", "39"]  # 서울, 부산, 경기, 제주

            # 1. 통합 축제/행사 정보 수집 (새 구조)
            current_date = datetime.now().strftime("%Y%m%d")
            processed_festivals = 0

            try:
                # 축제/행사 컨텐츠 타입만 수집
                festival_result = await self.unified_client.collect_all_data(
                    content_types=["15"],  # 축제공연행사
                    area_codes=major_areas,
                    store_raw=True,
                    auto_transform=True,
                )

                processed_festivals = festival_result.get("total_processed_records", 0)
                self.logger.info(f"축제/행사 정보 {processed_festivals}개 업데이트")

            except Exception as e:
                self.logger.error(f"축제/행사 정보 수집 오류: {e}")
                processed_festivals = 0

            # 2. 주요 지역 관광지 정보 업데이트 (새 구조)
            total_attractions = 0

            try:
                # 관광지 컨텐츠 타입만 수집
                attraction_result = await self.unified_client.collect_all_data(
                    content_types=["12"],  # 관광지
                    area_codes=major_areas,
                    store_raw=True,
                    auto_transform=True,
                )

                total_attractions = attraction_result.get("total_processed_records", 0)
                self.logger.info(f"관광지 정보 {total_attractions}개 업데이트")

            except Exception as e:
                self.logger.error(f"관광지 정보 수집 오류: {e}")
                total_attractions = 0

            # 3. 작업 로그 저장
            total_processed = processed_festivals + total_attractions
            self._save_job_log("success", total_processed)

            self.logger.info("=== 증분 관광정보 수집 작업 완료 ===")
            return True

        except Exception as e:
            self.logger.error(f"증분 관광정보 수집 작업 실패: {e}")
            self._save_job_log("failure", error_message=str(e))
            return False

    def _save_job_log(
        self, status: str, processed_records: int = 0, error_message: str = None
    ):
        """작업 로그 저장"""
        try:
            # 새로운 DatabaseManager의 log_job_result 메서드 사용
            self.db_manager.log_job_result(
                job_name=self.job_name,
                job_type=self.job_type,
                status=status,
                start_time=datetime.now(),
                end_time=datetime.now(),
                processed_records=processed_records,
                error_message=error_message,
            )

        except Exception as e:
            self.logger.error(f"작업 로그 저장 실패: {e}")

    def get_schedule_info(self) -> Dict:
        """스케줄 정보 반환"""
        return {
            "job_name": self.job_name,
            "schedule": "매일 새벽 3시",
            "cron": "0 3 * * *",  # 매일 새벽 3시
            "description": "주요 관광정보 증분 업데이트",
            "estimated_duration": "30분-1시간",
        }


if __name__ == "__main__":
    import asyncio

    async def test_jobs():
        # 종합 수집 작업 테스트
        print("=== 종합 관광정보 수집 작업 테스트 ===")
        comprehensive_job = ComprehensiveTourismJob()
        success = await comprehensive_job.execute()
        print(f"작업 결과: {'성공' if success else '실패'}")

        print("\n=== 증분 수집 작업 테스트 ===")
        incremental_job = IncrementalTourismJob()
        success = await incremental_job.execute()
        print(f"작업 결과: {'성공' if success else '실패'}")

    # 비동기 실행
    asyncio.run(test_jobs())
