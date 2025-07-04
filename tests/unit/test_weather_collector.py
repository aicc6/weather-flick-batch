"""
날씨 데이터 수집기 단위 테스트
"""

import unittest
from unittest.mock import Mock, patch

import sys
from pathlib import Path

from app.collectors.weather_collector import WeatherDataCollector

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class TestWeatherDataCollector(unittest.TestCase):
    """날씨 데이터 수집기 테스트"""

    def setUp(self):
        """테스트 설정"""
        self.collector = WeatherDataCollector()

    @patch("app.collectors.weather_collector.requests.get")
    def test_get_current_weather_success(self, mock_get):
        """현재 날씨 조회 성공 테스트"""
        # Mock 응답 설정
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {
                        "item": [
                            {"category": "T1H", "obsrValue": "25.0"},
                            {"category": "REH", "obsrValue": "60.0"},
                            {"category": "RN1", "obsrValue": "0.0"},
                            {"category": "WSD", "obsrValue": "2.5"},
                            {"category": "PTY", "obsrValue": "0"},
                        ]
                    }
                },
            }
        }
        mock_get.return_value = mock_response

        # 테스트 실행
        result = self.collector.get_current_weather("서울")

        # 검증
        self.assertIsNotNone(result)
        self.assertEqual(result["region_name"], "서울")
        self.assertEqual(result["temperature"], 25.0)
        self.assertEqual(result["humidity"], 60.0)
        self.assertEqual(result["precipitation"], 0.0)
        self.assertEqual(result["wind_speed"], 2.5)
        self.assertEqual(result["weather_condition"], "맑음")

    @patch("app.collectors.weather_collector.requests.get")
    def test_get_current_weather_api_error(self, mock_get):
        """현재 날씨 조회 API 오류 테스트"""
        # Mock 응답 설정 (API 오류)
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "response": {
                "header": {
                    "resultCode": "99",
                    "resultMsg": "SERVICE_KEY_IS_NOT_REGISTERED_ERROR",
                }
            }
        }
        mock_get.return_value = mock_response

        # 테스트 실행
        result = self.collector.get_current_weather("서울")

        # 검증
        self.assertIsNone(result)

    @patch("app.collectors.weather_collector.requests.get")
    def test_get_current_weather_network_error(self, mock_get):
        """현재 날씨 조회 네트워크 오류 테스트"""
        # Mock 네트워크 오류
        mock_get.side_effect = Exception("Connection timeout")

        # 테스트 실행
        result = self.collector.get_current_weather("서울")

        # 검증
        self.assertIsNone(result)

    def test_get_current_weather_invalid_region(self):
        """잘못된 지역명 테스트"""
        result = self.collector.get_current_weather("존재하지않는지역")
        self.assertIsNone(result)

    @patch("app.collectors.weather_collector.requests.get")
    def test_get_weather_forecast_success(self, mock_get):
        """날씨 예보 조회 성공 테스트"""
        # Mock 응답 설정
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "response": {
                "header": {"resultCode": "00"},
                "body": {
                    "items": {
                        "item": [
                            {
                                "fcstDate": "20250626",
                                "fcstTime": "1200",
                                "category": "TMN",
                                "fcstValue": "18.0",
                            },
                            {
                                "fcstDate": "20250626",
                                "fcstTime": "1200",
                                "category": "TMX",
                                "fcstValue": "28.0",
                            },
                            {
                                "fcstDate": "20250626",
                                "fcstTime": "1200",
                                "category": "POP",
                                "fcstValue": "30",
                            },
                            {
                                "fcstDate": "20250626",
                                "fcstTime": "1200",
                                "category": "PTY",
                                "fcstValue": "0",
                            },
                        ]
                    }
                },
            }
        }
        mock_get.return_value = mock_response

        # 테스트 실행
        result = self.collector.get_weather_forecast("서울", 1)

        # 검증
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 0)

        forecast = result[0]
        self.assertEqual(forecast["region_name"], "서울")
        self.assertEqual(forecast["min_temp"], 18.0)
        self.assertEqual(forecast["max_temp"], 28.0)
        self.assertEqual(forecast["precipitation_prob"], 30)
        self.assertEqual(forecast["weather_condition"], "맑음")

    def test_parse_current_weather(self):
        """현재 날씨 데이터 파싱 테스트"""
        # 테스트 데이터
        items = [
            {"category": "T1H", "obsrValue": "22.5"},
            {"category": "REH", "obsrValue": "65.0"},
            {"category": "RN1", "obsrValue": "1.5"},
            {"category": "WSD", "obsrValue": "3.2"},
            {"category": "PTY", "obsrValue": "1"},
        ]

        # 테스트 실행
        result = self.collector._parse_current_weather(items, "부산")

        # 검증
        self.assertEqual(result["region_name"], "부산")
        self.assertEqual(result["temperature"], 22.5)
        self.assertEqual(result["humidity"], 65.0)
        self.assertEqual(result["precipitation"], 1.5)
        self.assertEqual(result["wind_speed"], 3.2)
        self.assertEqual(result["weather_condition"], "비")

    def test_parse_current_weather_invalid_data(self):
        """잘못된 현재 날씨 데이터 파싱 테스트"""
        # 잘못된 데이터
        items = [
            {"category": "T1H", "obsrValue": "invalid"},
            {"category": "REH", "obsrValue": "65.0"},
        ]

        # 테스트 실행 (예외가 발생하지 않아야 함)
        result = self.collector._parse_current_weather(items, "대구")

        # 검증
        self.assertEqual(result["region_name"], "대구")
        self.assertIsNone(result["temperature"])  # 잘못된 값은 None
        self.assertEqual(result["humidity"], 65.0)  # 올바른 값은 파싱됨


if __name__ == "__main__":
    unittest.main()
