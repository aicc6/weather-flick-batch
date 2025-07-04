"""
날씨 데이터 업데이트 배치 작업

외부 날씨 API로부터 최신 날씨 정보를 수집하고 업데이트하는 작업
실행 주기: 1시간마다
"""

from datetime import datetime
from typing import Dict, List, Any, Optional

from app.core.logger import get_logger
from config.settings import get_weather_api_settings
from app.core.database_manager_extension import get_extended_database_manager
from app.core.unified_api_client import get_unified_api_client, APIProvider


class WeatherUpdateJob:
    """날씨 데이터 업데이트 작업"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.settings = get_weather_api_settings()
        self.db_manager = get_extended_database_manager()
        self.unified_client = get_unified_api_client()

    async def execute(self) -> Dict[str, Any]:
        """날씨 데이터 업데이트 실행 (통합 API 클라이언트 사용)"""
        self.logger.info("날씨 데이터 업데이트 작업 시작 (통합 구조)")

        async with self.unified_client:
            try:
                # 1. 활성화된 모든 지역 조회
                regions = await self._get_active_regions()
                self.logger.info(f"업데이트 대상 지역 수: {len(regions)}")

                # 2. 각 지역별 날씨 데이터 수집
                total_updated = 0
                failed_regions = []

                for region in regions:
                    try:
                        updated_count = await self._update_region_weather(region)
                        total_updated += updated_count

                    except Exception as e:
                        self.logger.error(
                            f"지역 날씨 업데이트 실패 [{region['region_name']}]: {e}"
                        )
                        failed_regions.append(region["region_name"])
                        continue

                # 3. 캐시 업데이트
                await self._update_weather_cache()

                # 4. 결과 반환
                result = {
                    "processed_records": total_updated,
                    "total_regions": len(regions),
                    "failed_regions": failed_regions,
                    "success_rate": (
                        (len(regions) - len(failed_regions)) / len(regions)
                        if regions
                        else 0
                    ),
                }

                self.logger.info(f"날씨 데이터 업데이트 완료: {total_updated}건 처리")
                return result

            except Exception as e:
                self.logger.error(f"날씨 데이터 업데이트 실패: {e}")
                raise

    async def _get_active_regions(self) -> List[Dict[str, Any]]:
        """활성화된 지역 목록 조회"""
        try:
            query = """
            SELECT region_code, region_name, latitude, longitude
            FROM regions
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            ORDER BY region_name
            """

            # 동기 방식으로 변경
            return self.db_manager.fetch_all(query)

        except Exception as e:
            self.logger.error(f"지역 목록 조회 실패: {e}")
            raise

    async def _update_region_weather(self, region: Dict[str, Any]) -> int:
        """특정 지역의 날씨 데이터 업데이트"""
        try:
            # 현재 날씨 정보 수집
            current_weather = await self._fetch_current_weather(
                region["latitude"], region["longitude"]
            )

            # 7일 예보 정보 수집
            forecast = await self._fetch_weather_forecast(
                region["latitude"], region["longitude"], days=7
            )

            # 데이터베이스 업데이트
            updated_count = 0

            # 현재 날씨 저장
            if current_weather:
                await self._save_current_weather(region["region_code"], current_weather)
                updated_count += 1

            # 예보 정보 저장
            if forecast:
                forecast_count = await self._save_weather_forecast(
                    region["region_code"], forecast
                )
                updated_count += forecast_count

            return updated_count

        except Exception as e:
            self.logger.error(f"지역 날씨 업데이트 실패 [{region['region_name']}]: {e}")
            raise

    async def _fetch_current_weather(
        self, lat: float, lon: float
    ) -> Optional[Dict[str, Any]]:
        """현재 날씨 정보 수집 (통합 API 클라이언트 사용)"""
        try:
            params = {
                "lat": lat,
                "lon": lon,
                "units": "metric",
                "lang": "kr",
            }

            # 통합 API 클라이언트로 호출
            response = await self.unified_client.call_api(
                api_provider=APIProvider.WEATHER,
                endpoint="weather",
                params=params,
                store_raw=True,  # 원본 데이터 저장
                cache_ttl=900,  # 15분 캐시
            )

            if response.success:
                return self._parse_current_weather(response.data)
            else:
                self.logger.warning(f"날씨 API 호출 실패: {response.error}")
                return None

        except Exception as e:
            self.logger.error(f"현재 날씨 수집 실패: {e}")
            return None

    async def _fetch_weather_forecast(
        self, lat: float, lon: float, days: int = 7
    ) -> Optional[List[Dict[str, Any]]]:
        """날씨 예보 정보 수집 (통합 API 클라이언트 사용)"""
        try:
            params = {
                "lat": lat,
                "lon": lon,
                "units": "metric",
                "lang": "kr",
                "cnt": days * 8,  # 3시간 간격으로 하루 8개
            }

            # 통합 API 클라이언트로 호출
            response = await self.unified_client.call_api(
                api_provider=APIProvider.WEATHER,
                endpoint="forecast",
                params=params,
                store_raw=True,  # 원본 데이터 저장
                cache_ttl=1800,  # 30분 캐시
            )

            if response.success:
                return self._parse_weather_forecast(response.data)
            else:
                self.logger.warning(f"예보 API 호출 실패: {response.error}")
                return None

        except Exception as e:
            self.logger.error(f"날씨 예보 수집 실패: {e}")
            return None

    def _parse_current_weather(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """현재 날씨 데이터 파싱"""
        try:
            # visibility는 미터 단위를 킬로미터로 변환하고 범위 제한
            visibility_m = data.get("visibility", 10000)
            visibility_km = min(visibility_m / 1000.0, 999.99)  # DECIMAL(5,2) 제한

            return {
                "temperature": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "weather_code": data["weather"][0]["id"],
                "weather_main": data["weather"][0]["main"],
                "weather_description": data["weather"][0]["description"],
                "wind_speed": data["wind"].get("speed", 0),
                "wind_direction": data["wind"].get("deg", 0),
                "clouds": data["clouds"]["all"],
                "visibility": visibility_km,
                "uv_index": data.get("uvi", 0),
                "recorded_at": datetime.utcfromtimestamp(data["dt"]),
            }
        except KeyError as e:
            self.logger.error(f"현재 날씨 데이터 파싱 실패: {e}")
            raise

    def _parse_weather_forecast(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """날씨 예보 데이터 파싱"""
        try:
            forecasts = []

            for item in data["list"]:
                forecast = {
                    "forecast_time": datetime.utcfromtimestamp(item["dt"]),
                    "temperature": item["main"]["temp"],
                    "feels_like": item["main"]["feels_like"],
                    "temp_min": item["main"]["temp_min"],
                    "temp_max": item["main"]["temp_max"],
                    "humidity": item["main"]["humidity"],
                    "pressure": item["main"]["pressure"],
                    "weather_code": item["weather"][0]["id"],
                    "weather_main": item["weather"][0]["main"],
                    "weather_description": item["weather"][0]["description"],
                    "wind_speed": item["wind"].get("speed", 0),
                    "wind_direction": item["wind"].get("deg", 0),
                    "clouds": item["clouds"]["all"],
                    "precipitation_probability": item.get("pop", 0) * 100,
                    "rain_3h": item.get("rain", {}).get("3h", 0),
                    "snow_3h": item.get("snow", {}).get("3h", 0),
                }
                forecasts.append(forecast)

            return forecasts

        except KeyError as e:
            self.logger.error(f"날씨 예보 데이터 파싱 실패: {e}")
            raise

    async def _save_current_weather(
        self, region_code: str, weather_data: Dict[str, Any]
    ):
        """현재 날씨 정보 저장"""
        try:
            query = """
            INSERT INTO current_weather (
                region_code, temperature, humidity, precipitation,
                wind_speed, wind_direction, atmospheric_pressure,
                weather_condition, visibility, observed_at
            ) VALUES (
                %s, %s, %s, 0,
                %s, %s, %s,
                %s, %s, %s
            )
            ON CONFLICT ON CONSTRAINT unique_current_weather_region_time
            DO UPDATE SET
                temperature = EXCLUDED.temperature,
                humidity = EXCLUDED.humidity,
                wind_speed = EXCLUDED.wind_speed,
                wind_direction = EXCLUDED.wind_direction,
                atmospheric_pressure = EXCLUDED.atmospheric_pressure,
                weather_condition = EXCLUDED.weather_condition,
                visibility = EXCLUDED.visibility,
                observed_at = EXCLUDED.observed_at
            """

            params = (
                region_code,
                weather_data["temperature"],
                weather_data["humidity"],
                weather_data["wind_speed"],
                weather_data["wind_direction"],
                weather_data["pressure"],
                weather_data["weather_description"],
                weather_data["visibility"],
                weather_data["recorded_at"],
            )
            self.db_manager.execute_update(query, params)

        except Exception as e:
            self.logger.error(f"현재 날씨 저장 실패: {e}")
            raise

    async def _save_weather_forecast(
        self, region_code: str, forecast_data: List[Dict[str, Any]]
    ) -> int:
        """날씨 예보 정보 저장"""
        try:
            # 기존 예보 데이터 삭제 (7일 이후)
            cleanup_query = """
            DELETE FROM weather_forecast
            WHERE region_code = %s
            AND forecast_date < CURRENT_DATE - INTERVAL '7 days'
            """
            self.db_manager.execute_update(cleanup_query, (region_code,))

            # 새 예보 데이터 저장
            insert_query = """
            INSERT INTO weather_forecast (
                region_code, forecast_date, forecast_type, min_temp, max_temp,
                precipitation_prob, weather_condition, forecast_issued_at
            ) VALUES (
                %s, %s, 'short',
                %s, %s, %s,
                %s, NOW()
            )
            ON CONFLICT ON CONSTRAINT unique_weather_forecast_region_date_type
            DO UPDATE SET
                min_temp = EXCLUDED.min_temp,
                max_temp = EXCLUDED.max_temp,
                precipitation_prob = EXCLUDED.precipitation_prob,
                weather_condition = EXCLUDED.weather_condition,
                forecast_issued_at = EXCLUDED.forecast_issued_at
            """

            count = 0
            for forecast in forecast_data:
                forecast_date = forecast["forecast_time"].date()
                params = (
                    region_code,
                    forecast_date,
                    forecast["temp_min"],
                    forecast["temp_max"],
                    forecast["precipitation_probability"],
                    forecast["weather_description"],
                )
                self.db_manager.execute_update(insert_query, params)
                count += 1

            return count

        except Exception as e:
            self.logger.error(f"날씨 예보 저장 실패: {e}")
            raise

    async def _update_weather_cache(self):
        """날씨 데이터 캐시 업데이트"""
        try:
            # Redis 캐시 업데이트
            # TODO: Redis 캐시 구현
            pass

        except Exception as e:
            self.logger.error(f"날씨 캐시 업데이트 실패: {e}")
            # 캐시 실패는 전체 작업을 실패시키지 않음


# 작업 실행 함수
async def weather_update_task() -> Dict[str, Any]:
    """날씨 업데이트 작업 실행 함수"""
    job = WeatherUpdateJob()
    return await job.execute()
