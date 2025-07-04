"""
날씨 데이터 수집 작업 통합 테스트
"""

import unittest
from unittest.mock import Mock, patch
from datetime import datetime

import sys
from pathlib import Path

from jobs.weather.weather_data_job import WeatherDataJob
from app.core.base_job import JobConfig
from config.constants import JobType, JobStatus

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class TestWeatherDataJobIntegration(unittest.TestCase):
    """날씨 데이터 수집 작업 통합 테스트"""

    def setUp(self):
        """테스트 설정"""
        self.config = JobConfig(
            job_name="test_weather_job",
            job_type=JobType.WEATHER_DATA,
            schedule_expression="test",
            retry_count=1,
            timeout_minutes=5,
        )
        self.job = WeatherDataJob(self.config)

    @patch("jobs.weather.weather_data_job.get_unified_api_client")
    @patch("jobs.weather.weather_data_job.get_transformation_pipeline")
    @patch("jobs.weather.weather_data_job.get_extended_database_manager")
    def test_execute_success(self, mock_db_manager, mock_pipeline, mock_client):
        """작업 실행 성공 테스트"""
        # Mock 설정
        mock_unified_client = Mock()
        mock_client.return_value = mock_unified_client
        mock_unified_client.__aenter__ = Mock(return_value=mock_unified_client)
        mock_unified_client.__aexit__ = Mock(return_value=None)

        # 변환 파이프라인 mock
        mock_transform_pipeline = Mock()
        mock_pipeline.return_value = mock_transform_pipeline

        # 데이터베이스 매니저 mock
        mock_db_mgr = Mock()
        mock_db_manager.return_value = mock_db_mgr

        # API 응답 mock
        mock_response = Mock()
        mock_response.success = True
        mock_response.raw_data_id = "test_raw_id"
        mock_unified_client.call_api.return_value = mock_response

        # 변환 결과 mock
        mock_transform_result = Mock()
        mock_transform_result.success = True
        mock_transform_result.processed_data = [{"temperature": 25.0, "humidity": 60.0}]
        mock_transform_pipeline.transform_raw_data.return_value = mock_transform_result

        # 데이터베이스 실행 mock
        mock_db_mgr.execute_query.return_value = None

        # 비동기 함수를 동기로 실행하기 위한 patch
        with patch("asyncio.run") as mock_run:
            mock_run.return_value = None

            # 테스트 실행
            import asyncio

            result = asyncio.run(self.job.execute())

            # 검증
            self.assertIsNotNone(result)
            self.assertEqual(result.job_name, "test_weather_job")
            self.assertEqual(result.job_type, JobType.WEATHER_DATA)

    @patch("jobs.weather.weather_data_job.get_unified_api_client")
    def test_execute_with_collector_error(self, mock_client):
        """수집기 오류 시 작업 처리 테스트"""
        # Mock 설정 (오류 발생)
        mock_unified_client = Mock()
        mock_client.return_value = mock_unified_client
        mock_unified_client.__aenter__ = Mock(side_effect=Exception("API 연결 오류"))

        # 테스트 실행 (정상 처리되어야 함 - 예외를 catch하고 continue 처리)
        import asyncio

        # execute는 예외를 발생시키지 않고 결과를 반환해야 함
        result = asyncio.run(self.job.execute())
        self.assertIsNotNone(result)

    def test_run_with_retry(self):
        """재시도 로직 테스트"""
        # 기본적인 job.run() 메서드 테스트
        # 실제 재시도 로직은 BaseJob에서 처리됨

        # pre_execute가 실패하도록 mock
        with patch.object(self.job, "pre_execute", return_value=False):
            result = self.job.run()

            # pre_execute 실패 시 FAILED 상태여야 함
            self.assertEqual(result.status, JobStatus.FAILED)

    def test_pre_execute(self):
        """실행 전 검증 테스트"""
        result = self.job.pre_execute()
        self.assertTrue(result)  # 기본적으로 True 반환

    def test_post_execute(self):
        """실행 후 처리 테스트"""
        # 가짜 JobResult 생성
        from app.core.base_job import JobResult

        result = JobResult(
            job_name="test_job",
            job_type=JobType.WEATHER_DATA,
            status=JobStatus.COMPLETED,
            start_time=datetime.now(),
            end_time=datetime.now(),
        )
        result.processed_records = 10

        # post_execute 호출 (예외가 발생하지 않아야 함)
        try:
            self.job.post_execute(result)
        except Exception as e:
            self.fail(f"post_execute에서 예외 발생: {e}")


class TestWeatherJobConfiguration(unittest.TestCase):
    """날씨 작업 설정 테스트"""

    def test_job_config_creation(self):
        """작업 설정 생성 테스트"""
        config = JobConfig(
            job_name="weather_daily",
            job_type=JobType.WEATHER_DATA,
            schedule_expression="daily",
            retry_count=3,
            timeout_minutes=30,
            dependencies=["dependency_job"],
            enabled=True,
            metadata={"description": "날씨 데이터 수집"},
        )

        self.assertEqual(config.job_name, "weather_daily")
        self.assertEqual(config.job_type, JobType.WEATHER_DATA)
        self.assertEqual(config.schedule_expression, "daily")
        self.assertEqual(config.retry_count, 3)
        self.assertEqual(config.timeout_minutes, 30)
        self.assertEqual(config.dependencies, ["dependency_job"])
        self.assertTrue(config.enabled)
        self.assertEqual(config.metadata["description"], "날씨 데이터 수집")

    def test_job_creation_with_config(self):
        """설정으로 작업 생성 테스트"""
        config = JobConfig(
            job_name="test_job",
            job_type=JobType.WEATHER_DATA,
            schedule_expression="test",
        )

        job = WeatherDataJob(config)

        self.assertEqual(job.config.job_name, "test_job")
        self.assertEqual(job.config.job_type, JobType.WEATHER_DATA)
        self.assertIsNotNone(job.unified_client)
        self.assertIsNotNone(job.transformation_pipeline)
        self.assertIsNotNone(job.db_manager)
        self.assertIsNotNone(job.logger)


if __name__ == "__main__":
    # 통합 테스트 실행
    unittest.main()
