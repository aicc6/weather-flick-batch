"""
날씨 데이터 수집 작업

기상청 API를 통해 날씨 데이터를 수집하는 배치 작업입니다.
"""

from datetime import datetime, timedelta
from typing import Dict, List

from app.core.base_job import BaseJob, JobResult, JobConfig
from app.core.unified_api_client import get_unified_api_client, APIProvider
from app.processors.data_transformation_pipeline import get_transformation_pipeline
from config.constants import WEATHER_COORDINATES
from app.core.database_manager_extension import get_extended_database_manager


class WeatherDataJob(BaseJob):
    """날씨 데이터 수집 작업"""

    def __init__(self, config: JobConfig):
        super().__init__(config)
        self.unified_client = get_unified_api_client()
        self.transformation_pipeline = get_transformation_pipeline()
        self.db_manager = get_extended_database_manager()
        self.processed_records = 0

    async def execute(self) -> JobResult:
        """날씨 데이터 수집 실행"""
        result = JobResult(
            job_name=self.config.job_name,
            job_type=self.config.job_type,
            status="running",
            start_time=datetime.now(),
        )

        try:
            async with self.unified_client:
                # 통합 기상 데이터 수집 (새 구조)
                weather_collection_result = await self._collect_all_weather_data()

                total_raw_records = weather_collection_result.get(
                    "total_raw_records", 0
                )
                total_processed_records = weather_collection_result.get(
                    "total_processed_records", 0
                )

                self.logger.info(f"원본 날씨 데이터 {total_raw_records}건 수집")
                self.logger.info(f"처리된 날씨 데이터 {total_processed_records}건 저장")

                saved_records = total_processed_records

                result.processed_records = saved_records
                result.metadata = {
                    "raw_weather_records": total_raw_records,
                    "processed_weather_records": total_processed_records,
                    "regions_processed": len(WEATHER_COORDINATES),
                    "batch_id": weather_collection_result.get("batch_id"),
                }

                self.logger.info(f"날씨 데이터 수집 완료: 총 {saved_records}건 처리")

        except Exception as e:
            self.logger.error(f"날씨 데이터 수집 실패: {str(e)}")
            raise

        return result

    async def _collect_all_weather_data(self) -> Dict:
        """통합 기상 데이터 수집 (새 구조)"""
        collection_result = {
            "total_raw_records": 0,
            "total_processed_records": 0,
            "regions_processed": 0,
            "batch_id": f"weather_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        }

        for region_name, coordinates in WEATHER_COORDINATES.items():
            try:
                # 현재 날씨 수집
                current_result = await self._collect_region_current_weather(
                    region_name, coordinates
                )

                # 예보 데이터 수집
                forecast_result = await self._collect_region_forecast(
                    region_name, coordinates
                )

                # 과거 데이터 수집
                historical_result = await self._collect_region_historical(
                    region_name, coordinates
                )

                # 통계 업데이트
                collection_result["total_raw_records"] += (
                    current_result.get("raw_records", 0)
                    + forecast_result.get("raw_records", 0)
                    + historical_result.get("raw_records", 0)
                )

                collection_result["total_processed_records"] += (
                    current_result.get("processed_records", 0)
                    + forecast_result.get("processed_records", 0)
                    + historical_result.get("processed_records", 0)
                )

                collection_result["regions_processed"] += 1

                self.logger.debug(f"지역 날씨 데이터 수집 완료: {region_name}")

            except Exception as e:
                self.logger.warning(
                    f"지역 날씨 데이터 수집 실패 [{region_name}]: {str(e)}"
                )
                continue

        return collection_result

    async def _collect_region_current_weather(
        self, region_name: str, coordinates: Dict
    ) -> Dict:
        """지역 현재 날씨 데이터 수집 (통합 구조)"""
        try:
            params = {
                "lat": coordinates["lat"],
                "lon": coordinates["lon"],
                "units": "metric",
                "lang": "kr",
            }

            response = await self.unified_client.call_api(
                api_provider=APIProvider.WEATHER,
                endpoint="weather",
                params=params,
                store_raw=True,
                cache_ttl=900,  # 15분 캐시
            )

            if response.success:
                # 데이터 변환 및 저장
                transform_result = (
                    await self.transformation_pipeline.transform_raw_data(
                        response.raw_data_id
                    )
                )

                if transform_result.success:
                    # 변환된 데이터를 날씨 테이블에 저장
                    await self._save_weather_current_data(
                        region_name,
                        transform_result.processed_data,
                        response.raw_data_id,
                    )

                    return {
                        "raw_records": 1,
                        "processed_records": len(transform_result.processed_data),
                    }

            return {"raw_records": 0, "processed_records": 0}

        except Exception as e:
            self.logger.error(f"현재 날씨 수집 실패 [{region_name}]: {e}")
            return {"raw_records": 0, "processed_records": 0}

    async def _collect_region_forecast(
        self, region_name: str, coordinates: Dict
    ) -> Dict:
        """지역 예보 데이터 수집 (통합 구조)"""
        try:
            params = {
                "lat": coordinates["lat"],
                "lon": coordinates["lon"],
                "units": "metric",
                "lang": "kr",
                "cnt": 24,  # 3일 예보 (3시간 간격)
            }

            response = await self.unified_client.call_api(
                api_provider=APIProvider.WEATHER,
                endpoint="forecast",
                params=params,
                store_raw=True,
                cache_ttl=1800,  # 30분 캐시
            )

            if response.success:
                # 데이터 변환 및 저장
                transform_result = (
                    await self.transformation_pipeline.transform_raw_data(
                        response.raw_data_id
                    )
                )

                if transform_result.success:
                    # 변환된 데이터를 예보 테이블에 저장
                    await self._save_weather_forecast_data(
                        region_name,
                        transform_result.processed_data,
                        response.raw_data_id,
                    )

                    return {
                        "raw_records": 1,
                        "processed_records": len(transform_result.processed_data),
                    }

            return {"raw_records": 0, "processed_records": 0}

        except Exception as e:
            self.logger.error(f"예보 데이터 수집 실패 [{region_name}]: {e}")
            return {"raw_records": 0, "processed_records": 0}

    async def _collect_region_historical(
        self, region_name: str, coordinates: Dict
    ) -> Dict:
        """지역 과거 날씨 데이터 수집 (통합 구조)"""
        try:
            # 최근 7일 데이터
            end_date = datetime.now() - timedelta(days=1)
            start_date = end_date - timedelta(days=6)

            params = {
                "lat": coordinates["lat"],
                "lon": coordinates["lon"],
                "start_date": start_date.strftime("%Y%m%d"),
                "end_date": end_date.strftime("%Y%m%d"),
            }

            response = await self.unified_client.call_api(
                api_provider=APIProvider.KMA,
                endpoint="getWthrDataList",
                params=params,
                store_raw=True,
                cache_ttl=3600,  # 1시간 캐시
            )

            if response.success:
                # 데이터 변환 및 저장
                transform_result = (
                    await self.transformation_pipeline.transform_raw_data(
                        response.raw_data_id
                    )
                )

                if transform_result.success:
                    # 변환된 데이터를 과거 날씨 테이블에 저장
                    await self._save_weather_historical_data(
                        region_name,
                        transform_result.processed_data,
                        response.raw_data_id,
                    )

                    return {
                        "raw_records": 1,
                        "processed_records": len(transform_result.processed_data),
                    }

            return {"raw_records": 0, "processed_records": 0}

        except Exception as e:
            self.logger.error(f"과거 날씨 수집 실패 [{region_name}]: {e}")
            return {"raw_records": 0, "processed_records": 0}

    async def _save_weather_current_data(
        self, region_name: str, processed_data: List[Dict], raw_data_id: str
    ) -> int:
        """현재 날씨 데이터 저장 (통합 구조)"""
        try:
            saved_count = 0
            for data in processed_data:
                data["raw_data_id"] = raw_data_id
                data["region_name"] = region_name
                # 현재 날씨 테이블에 저장
                await self.db_manager.execute_query(
                    "INSERT INTO current_weather (region_code, temperature, humidity, raw_data_id) VALUES (%s, %s, %s, %s)",
                    (
                        self._get_region_code_from_name(region_name),
                        data.get("temperature"),
                        data.get("humidity"),
                        raw_data_id,
                    ),
                )
                saved_count += 1
            return saved_count
        except Exception as e:
            self.logger.error(f"현재 날씨 데이터 저장 실패: {e}")
            return 0

    async def _save_weather_forecast_data(
        self, region_name: str, processed_data: List[Dict], raw_data_id: str
    ) -> int:
        """예보 날씨 데이터 저장 (통합 구조)"""
        try:
            saved_count = 0
            for data in processed_data:
                data["raw_data_id"] = raw_data_id
                data["region_name"] = region_name
                # 예보 테이블에 저장
                await self.db_manager.execute_query(
                    "INSERT INTO weather_forecast (region_code, forecast_date, min_temp, max_temp, raw_data_id) VALUES (%s, %s, %s, %s, %s)",
                    (
                        self._get_region_code_from_name(region_name),
                        data.get("forecast_date"),
                        data.get("min_temp"),
                        data.get("max_temp"),
                        raw_data_id,
                    ),
                )
                saved_count += 1
            return saved_count
        except Exception as e:
            self.logger.error(f"예보 날씨 데이터 저장 실패: {e}")
            return 0

    async def _save_weather_historical_data(
        self, region_name: str, processed_data: List[Dict], raw_data_id: str
    ) -> int:
        """과거 날씨 데이터 저장 (통합 구조)"""
        try:
            saved_count = 0
            for data in processed_data:
                data["raw_data_id"] = raw_data_id
                data["region_name"] = region_name
                # 과거 날씨 테이블에 저장
                await self.db_manager.execute_query(
                    "INSERT INTO historical_weather_daily (region_code, weather_date, avg_temp, max_temp, min_temp, raw_data_id) VALUES (%s, %s, %s, %s, %s, %s)",
                    (
                        self._get_region_code_from_name(region_name),
                        data.get("weather_date"),
                        data.get("avg_temp"),
                        data.get("max_temp"),
                        data.get("min_temp"),
                        raw_data_id,
                    ),
                )
                saved_count += 1
            return saved_count
        except Exception as e:
            self.logger.error(f"과거 날씨 데이터 저장 실패: {e}")
            return 0

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
        # 통합 클라이언트 및 필수 구성 요소 확인
        if not self.unified_client:
            self.logger.error("통합 API 클라이언트가 초기화되지 않음")
            return False

        if not self.transformation_pipeline:
            self.logger.error("데이터 변환 파이프라인이 초기화되지 않음")
            return False

        if not self.db_manager:
            self.logger.error("데이터베이스 매니저가 초기화되지 않음")
            return False

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
