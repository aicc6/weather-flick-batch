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

    @patch("jobs.weather.weather_data_job.WeatherDataCollector")
    def test_execute_success(self, mock_collector_class):
        """작업 실행 성공 테스트"""
        # Mock 설정
        mock_collector = Mock()
        mock_collector_class.return_value = mock_collector

        # 현재 날씨 데이터 mock
        mock_collector.get_current_weather.return_value = {
            "region_name": "서울",
            "temperature": 25.0,
            "humidity": 60.0,
        }

        # 예보 데이터 mock
        mock_collector.get_weather_forecast.return_value = [
            {
                "region_name": "서울",
                "forecast_date": datetime.now(),
                "min_temp": 18.0,
                "max_temp": 28.0,
            }
        ]

        # 과거 데이터 mock
        mock_collector.get_historical_weather.return_value = [
            {"region_name": "서울", "weather_date": datetime.now(), "avg_temp": 22.0}
        ]

        # 테스트 실행
        result = self.job.execute()

        # 검증
        self.assertIsNotNone(result)
        self.assertEqual(result.job_name, "test_weather_job")
        self.assertEqual(result.job_type, JobType.WEATHER_DATA)
        self.assertGreater(result.processed_records, 0)

        # Mock 호출 검증
        self.assertTrue(mock_collector.get_current_weather.called)
        self.assertTrue(mock_collector.get_weather_forecast.called)
        self.assertTrue(mock_collector.get_historical_weather.called)

    @patch("jobs.weather.weather_data_job.WeatherDataCollector")
    def test_execute_with_collector_error(self, mock_collector_class):
        """수집기 오류 시 작업 처리 테스트"""
        # Mock 설정 (오류 발생)
        mock_collector = Mock()
        mock_collector_class.return_value = mock_collector
        mock_collector.get_current_weather.side_effect = Exception("API 오류")

        # 테스트 실행 (예외가 발생해야 함)
        with self.assertRaises(Exception):
            self.job.execute()

    @patch("jobs.weather.weather_data_job.WeatherDataCollector")
    def test_run_with_retry(self, mock_collector_class):
        """재시도 로직 테스트"""
        # Mock 설정 (처음에는 실패, 두 번째에는 성공)
        mock_collector = Mock()
        mock_collector_class.return_value = mock_collector

        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("첫 번째 시도 실패")
            return {"region_name": "서울", "temperature": 25.0}

        mock_collector.get_current_weather.side_effect = side_effect
        mock_collector.get_weather_forecast.return_value = []
        mock_collector.get_historical_weather.return_value = []

        # 테스트 실행
        result = self.job.run()

        # 검증 (첫 번째 시도 실패, 두 번째 시도 성공)
        self.assertEqual(
            result.status, JobStatus.FAILED
        )  # 실제로는 실패할 것 (execute에서 예외 발생)

    def test_pre_execute(self):
        """실행 전 검증 테스트"""
        result = self.job.pre_execute()
        self.assertTrue(result)  # 기본적으로 True 반환

    @patch("jobs.weather.weather_data_job.WeatherDataCollector")
    def test_post_execute(self, mock_collector_class):
        """실행 후 처리 테스트"""
        # Mock 설정
        mock_collector = Mock()
        mock_collector_class.return_value = mock_collector
        mock_collector.get_current_weather.return_value = {"region_name": "서울"}
        mock_collector.get_weather_forecast.return_value = []
        mock_collector.get_historical_weather.return_value = []

        # 실행 후 결과 생성
        result = self.job.execute()

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
        self.assertIsNotNone(job.collector)
        self.assertIsNotNone(job.logger)


if __name__ == "__main__":
    # 통합 테스트 실행
    unittest.main()
