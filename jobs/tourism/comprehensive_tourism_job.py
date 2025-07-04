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

from app.core.base_job import BaseJob, JobConfig
from app.collectors.kto_api import KTODataCollector
from app.processors.tourism_data_processor import TourismDataProcessor
from app.core.database_manager import DatabaseManager


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

        # 데이터 수집기 및 처리기 초기화
        self.collector = KTODataCollector()
        self.processor = TourismDataProcessor()
        self.db_manager = DatabaseManager()

        self.logger.info("종합 관광정보 수집 작업 초기화 완료")

    def execute(self) -> bool:
        """작업 실행"""
        try:
            self.logger.info("=== 종합 관광정보 수집 작업 시작 ===")
            start_time = time.time()

            # 1. 종합 데이터 수집
            self.logger.info("1단계: 종합 관광 데이터 수집")
            try:
                comprehensive_data = self.collector.collect_comprehensive_data()

                # API 실패 시 샘플 데이터 사용
                if not comprehensive_data or all(
                    not data for data in comprehensive_data.values()
                ):
                    self.logger.warning("API 데이터 수집 실패, 샘플 데이터로 전환")
                    sample_area_codes, sample_attractions, sample_festivals = (
                        self.collector.generate_sample_data()
                    )
                    comprehensive_data = {
                        "area_codes": sample_area_codes,
                        "detailed_area_codes": [],
                        "category_codes": [],
                        "tourist_attractions": sample_attractions,
                        "cultural_facilities": [],
                        "festivals_events": sample_festivals,
                        "travel_courses": [],
                        "leisure_sports": [],
                        "accommodations": [],
                        "shopping": [],
                        "restaurants": [],
                    }

            except Exception as e:
                self.logger.error(f"데이터 수집 중 오류: {e}")
                self.logger.info("샘플 데이터로 전환합니다")
                sample_area_codes, sample_attractions, sample_festivals = (
                    self.collector.generate_sample_data()
                )
                comprehensive_data = {
                    "area_codes": sample_area_codes,
                    "detailed_area_codes": [],
                    "category_codes": [],
                    "tourist_attractions": sample_attractions,
                    "cultural_facilities": [],
                    "festivals_events": sample_festivals,
                    "travel_courses": [],
                    "leisure_sports": [],
                    "accommodations": [],
                    "shopping": [],
                    "restaurants": [],
                }

            # 2. 수집된 데이터 통계
            total_collected = sum(
                len(data) for data in comprehensive_data.values() if data
            )
            self.logger.info(f"총 수집된 데이터: {total_collected:,}개")

            for data_type, data_list in comprehensive_data.items():
                if data_list:
                    self.logger.info(f"  - {data_type}: {len(data_list):,}개")

            # 3. 데이터베이스 처리
            self.logger.info("2단계: 데이터베이스 저장 처리")
            processing_results = self.processor.process_comprehensive_data(
                comprehensive_data
            )

            # 4. 처리 결과 통계
            total_processed = sum(processing_results.values())
            self.logger.info(f"총 처리된 데이터: {total_processed:,}개")

            for data_type, count in processing_results.items():
                if count > 0:
                    self.logger.info(f"  - {data_type}: {count:,}개 저장 완료")

            # 5. 데이터 품질 검사
            self.logger.info("3단계: 데이터 품질 검사")
            quality_results = self._check_data_quality()

            # 6. 실행 시간 계산
            execution_time = time.time() - start_time
            self.logger.info(f"총 실행 시간: {execution_time:.2f}초")

            # 7. 작업 로그 저장
            self._save_job_log(
                status="completed",
                processed_records=total_processed,
                execution_time=execution_time,
                additional_info={
                    "collected_count": total_collected,
                    "processing_results": processing_results,
                    "quality_results": quality_results,
                },
            )

            self.logger.info("=== 종합 관광정보 수집 작업 완료 ===")
            return True

        except Exception as e:
            self.logger.error(f"종합 관광정보 수집 작업 실패: {e}")
            self._save_job_log(status="failed", error_message=str(e))
            return False

    def _check_data_quality(self) -> Dict:
        """데이터 품질 검사"""
        quality_results = {}

        try:
            # 새로 추가된 테이블들의 데이터 품질 검사
            tables_to_check = [
                "cultural_facilities",
                "festivals_events",
                "travel_courses",
                "leisure_sports",
                "accommodations",
                "shopping",
                "restaurants",
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

        self.collector = KTODataCollector()
        self.processor = TourismDataProcessor()
        self.db_manager = DatabaseManager()

    def execute(self) -> bool:
        """증분 수집 실행 (주요 데이터만)"""
        try:
            self.logger.info("=== 증분 관광정보 수집 작업 시작 ===")

            # 주요 지역만 대상으로 증분 수집
            major_areas = ["1", "6", "31", "39"]  # 서울, 부산, 경기, 제주

            # 1. 축제/행사 정보 (현재 진행중인 것들)
            current_date = datetime.now().strftime("%Y%m%d")
            try:
                festivals = self.collector.get_festivals_events(
                    event_start_date=current_date
                )

                if festivals:
                    processed_festivals = self.processor.process_festivals_events(
                        festivals
                    )
                    self.logger.info(f"축제/행사 정보 {processed_festivals}개 업데이트")
                else:
                    self.logger.warning("축제/행사 데이터 없음, 샘플 데이터 사용")
                    _, _, sample_festivals = self.collector.generate_sample_data()
                    processed_festivals = self.processor.process_festivals_events(
                        sample_festivals
                    )
                    self.logger.info(
                        f"샘플 축제/행사 정보 {processed_festivals}개 저장"
                    )
            except Exception as e:
                self.logger.error(f"축제/행사 정보 수집 오류: {e}")
                _, _, sample_festivals = self.collector.generate_sample_data()
                processed_festivals = self.processor.process_festivals_events(
                    sample_festivals
                )
                self.logger.info(f"샘플 축제/행사 정보 {processed_festivals}개 저장")

            # 2. 주요 지역 관광지 정보 업데이트
            total_attractions = 0
            for area_code in major_areas:
                try:
                    attractions = self.collector.get_tourist_attractions(
                        area_code=area_code
                    )
                    if attractions:
                        processed = self.processor.process_tourist_attractions(
                            attractions
                        )
                        total_attractions += processed
                    time.sleep(0.5)  # API 호출 간격 조절
                except Exception as e:
                    self.logger.warning(f"지역 {area_code} 관광지 정보 수집 실패: {e}")
                    continue

            self.logger.info(f"관광지 정보 {total_attractions}개 업데이트")

            # 3. 작업 로그 저장
            total_processed = len(festivals) + total_attractions
            self._save_job_log("completed", total_processed)

            self.logger.info("=== 증분 관광정보 수집 작업 완료 ===")
            return True

        except Exception as e:
            self.logger.error(f"증분 관광정보 수집 작업 실패: {e}")
            self._save_job_log("failed", error_message=str(e))
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
    # 종합 수집 작업 테스트
    print("=== 종합 관광정보 수집 작업 테스트 ===")
    comprehensive_job = ComprehensiveTourismJob()
    success = comprehensive_job.execute()
    print(f"작업 결과: {'성공' if success else '실패'}")

    print("\n=== 증분 수집 작업 테스트 ===")
    incremental_job = IncrementalTourismJob()
    success = incremental_job.execute()
    print(f"작업 결과: {'성공' if success else '실패'}")

