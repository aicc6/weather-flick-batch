"""
날씨 데이터 수집 작업

기상청 API를 통해 날씨 데이터를 수집하는 배치 작업입니다.
"""

from datetime import datetime, timedelta
from typing import Dict, List

from app.core.base_job import BaseJob, JobResult, JobConfig
from app.collectors.weather_collector import WeatherDataCollector
from config.constants import WEATHER_COORDINATES
from app.core.database_manager import DatabaseManager


class WeatherDataJob(BaseJob):
    """날씨 데이터 수집 작업"""

    def __init__(self, config: JobConfig):
        super().__init__(config)
        self.collector = WeatherDataCollector()
        self.db_manager = DatabaseManager()
        self.processed_records = 0

    def execute(self) -> JobResult:
        """날씨 데이터 수집 실행"""
        result = JobResult(
            job_name=self.config.job_name,
            job_type=self.config.job_type,
            status="running",
            start_time=datetime.now(),
        )

        try:
            # 현재 날씨 데이터 수집
            current_weather_data = self._collect_current_weather()
            self.logger.info(f"현재 날씨 데이터 {len(current_weather_data)}건 수집")

            # 예보 데이터 수집
            forecast_data = self._collect_forecast_data()
            self.logger.info(f"예보 데이터 {len(forecast_data)}건 수집")

            # 과거 날씨 데이터 수집 (최근 7일)
            historical_data = self._collect_historical_data()
            self.logger.info(f"과거 날씨 데이터 {len(historical_data)}건 수집")

            # 데이터 저장
            saved_records = self._save_weather_data(
                current_weather_data, forecast_data, historical_data
            )

            result.processed_records = saved_records
            result.metadata = {
                "current_weather_records": len(current_weather_data),
                "forecast_records": len(forecast_data),
                "historical_records": len(historical_data),
                "regions_processed": len(WEATHER_COORDINATES),
            }

            self.logger.info(f"날씨 데이터 수집 완료: 총 {saved_records}건 처리")

        except Exception as e:
            self.logger.error(f"날씨 데이터 수집 실패: {str(e)}")
            raise

        return result

    def _collect_current_weather(self) -> List[Dict]:
        """현재 날씨 데이터 수집"""
        current_data = []

        for region_name in WEATHER_COORDINATES.keys():
            try:
                weather_data = self.collector.get_current_weather(region_name)
                if weather_data:
                    current_data.append(weather_data)
                    self.logger.debug(f"현재 날씨 수집 완료: {region_name}")
            except Exception as e:
                self.logger.warning(f"현재 날씨 수집 실패 [{region_name}]: {str(e)}")

        return current_data

    def _collect_forecast_data(self) -> List[Dict]:
        """예보 데이터 수집"""
        forecast_data = []

        for region_name in WEATHER_COORDINATES.keys():
            try:
                forecasts = self.collector.get_weather_forecast(region_name, days=3)
                forecast_data.extend(forecasts)
                self.logger.debug(f"예보 데이터 수집 완료: {region_name}")
            except Exception as e:
                self.logger.warning(f"예보 데이터 수집 실패 [{region_name}]: {str(e)}")

        return forecast_data

    def _collect_historical_data(self) -> List[Dict]:
        """과거 날씨 데이터 수집"""
        historical_data = []

        # 최근 7일 데이터 수집
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=6)

        for region_name in WEATHER_COORDINATES.keys():
            try:
                historical = self.collector.get_historical_weather(
                    region_name,
                    start_date.strftime("%Y%m%d"),
                    end_date.strftime("%Y%m%d"),
                )
                historical_data.extend(historical)
                self.logger.debug(f"과거 날씨 수집 완료: {region_name}")
            except Exception as e:
                self.logger.warning(f"과거 날씨 수집 실패 [{region_name}]: {str(e)}")

        return historical_data

    def _save_weather_data(
        self,
        current_data: List[Dict],
        forecast_data: List[Dict],
        historical_data: List[Dict],
    ) -> int:
        """날씨 데이터 저장"""
        total_saved = 0

        try:
            # 현재 날씨 데이터를 과거 데이터 형식으로 변환하여 저장
            if current_data:
                current_weather_formatted = self._format_current_weather_for_db(
                    current_data
                )
                if current_weather_formatted:
                    saved_count = self.db_manager.insert_weather_data(
                        current_weather_formatted
                    )
                    total_saved += saved_count
                    self.logger.debug(f"현재 날씨 데이터 {saved_count}건 저장")

            # 예보 데이터 저장
            if forecast_data:
                forecast_formatted = self._format_forecast_data_for_db(forecast_data)
                if forecast_formatted:
                    saved_count = self.db_manager.insert_forecast_data(
                        forecast_formatted
                    )
                    total_saved += saved_count
                    self.logger.debug(f"예보 데이터 {saved_count}건 저장")

            # 과거 날씨 데이터 저장
            if historical_data:
                historical_formatted = self._format_historical_data_for_db(
                    historical_data
                )
                if historical_formatted:
                    saved_count = self.db_manager.insert_weather_data(
                        historical_formatted
                    )
                    total_saved += saved_count
                    self.logger.debug(f"과거 날씨 데이터 {saved_count}건 저장")

        except Exception as e:
            self.logger.error(f"날씨 데이터 저장 중 오류 발생: {str(e)}")
            raise

        return total_saved

    def _format_current_weather_for_db(self, current_data: List[Dict]) -> List[Dict]:
        """현재 날씨 데이터를 DB 저장 형식으로 변환"""
        formatted_data = []
        today = datetime.now().date()

        for data in current_data:
            try:
                formatted_item = {
                    "region_code": self._get_region_code_from_name(
                        data.get("region_name", "")
                    ),
                    "weather_date": today,
                    "year": today.year,
                    "month": today.month,
                    "day": today.day,
                    "avg_temp": data.get("temperature"),
                    "max_temp": data.get("max_temp", data.get("temperature")),
                    "min_temp": data.get("min_temp", data.get("temperature")),
                    "humidity": data.get("humidity"),
                    "precipitation": data.get("precipitation", 0),
                    "wind_speed": data.get("wind_speed"),
                    "weather_condition": data.get("weather_description", ""),
                    "visibility": data.get("visibility"),
                    "uv_index": data.get("uv_index"),
                }
                formatted_data.append(formatted_item)
            except Exception as e:
                self.logger.warning(f"현재 날씨 데이터 변환 실패: {str(e)}")

        return formatted_data

    def _format_forecast_data_for_db(self, forecast_data: List[Dict]) -> List[Dict]:
        """예보 데이터를 DB 저장 형식으로 변환"""
        formatted_data = []

        for data in forecast_data:
            try:
                formatted_item = {
                    "region_code": self._get_region_code_from_name(
                        data.get("region_name", "")
                    ),
                    "forecast_date": datetime.strptime(
                        data.get("forecast_date", ""), "%Y-%m-%d"
                    ).date(),
                    "forecast_type": "short",  # 3일 예보는 단기예보
                    "min_temp": data.get("min_temp"),
                    "max_temp": data.get("max_temp"),
                    "precipitation_prob": data.get("precipitation_prob", 0),
                    "weather_condition": data.get("weather_condition", ""),
                    "forecast_issued_at": datetime.now(),
                }
                formatted_data.append(formatted_item)
            except Exception as e:
                self.logger.warning(f"예보 데이터 변환 실패: {str(e)}")

        return formatted_data

    def _format_historical_data_for_db(self, historical_data: List[Dict]) -> List[Dict]:
        """과거 날씨 데이터를 DB 저장 형식으로 변환"""
        formatted_data = []

        for data in historical_data:
            try:
                weather_date = datetime.strptime(
                    data.get("date", ""), "%Y-%m-%d"
                ).date()
                formatted_item = {
                    "region_code": self._get_region_code_from_name(
                        data.get("region_name", "")
                    ),
                    "weather_date": weather_date,
                    "year": weather_date.year,
                    "month": weather_date.month,
                    "day": weather_date.day,
                    "avg_temp": data.get("avg_temp"),
                    "max_temp": data.get("max_temp"),
                    "min_temp": data.get("min_temp"),
                    "humidity": data.get("humidity"),
                    "precipitation": data.get("precipitation", 0),
                    "wind_speed": data.get("wind_speed"),
                    "weather_condition": data.get("weather_condition", ""),
                    "visibility": data.get("visibility"),
                    "uv_index": data.get("uv_index"),
                }
                formatted_data.append(formatted_item)
            except Exception as e:
                self.logger.warning(f"과거 날씨 데이터 변환 실패: {str(e)}")

        return formatted_data

    def _get_region_code_from_name(self, region_name: str) -> str:
        """지역명으로부터 지역 코드 생성"""
        # 지역명을 간단한 코드로 변환 (추후 regions 테이블과 매핑 필요)
        region_code_mapping = {
            "서울": "11",
            "부산": "26",
            "대구": "27",
            "인천": "28",
            "광주": "29",
            "대전": "30",
            "울산": "31",
            "세종": "36",
            "경기": "41",
            "강원": "42",
            "충북": "43",
            "충남": "44",
            "전북": "45",
            "전남": "46",
            "경북": "47",
            "경남": "48",
            "제주": "50",
        }

        for region, code in region_code_mapping.items():
            if region in region_name:
                return code

        # 기본값 반환
        return "00"

    def pre_execute(self) -> bool:
        """실행 전 검증"""
        # API 키 확인
        if not self.collector.kma_api_key or self.collector.kma_api_key == "test_key":
            self.logger.warning("기상청 API 키가 설정되지 않음. 테스트 모드로 실행")

        # 네트워크 연결 확인 (선택적)
        return True

    def post_execute(self, result: JobResult) -> None:
        """실행 후 처리"""
        super().post_execute(result)

        # 성능 메트릭 로그
        if result.duration_seconds > 0:
            records_per_second = result.processed_records / result.duration_seconds
            self.logger.info(f"처리 성능: {records_per_second:.2f} records/second")

        # 데이터 품질 체크 트리거 (다음 작업에서 실행)
