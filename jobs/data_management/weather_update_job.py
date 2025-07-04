"""
날씨 데이터 업데이트 배치 작업

외부 날씨 API로부터 최신 날씨 정보를 수집하고 업데이트하는 작업
실행 주기: 1시간마다
"""

from datetime import datetime
from typing import Dict, List, Any, Optional
import aiohttp

from app.core.logger import get_logger
from config.settings import get_weather_api_settings
from app.core.database_manager import DatabaseManager


class WeatherUpdateJob:
    """날씨 데이터 업데이트 작업"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.settings = get_weather_api_settings()
        self.db_manager = DatabaseManager()
        self.session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30),
            connector=aiohttp.TCPConnector(limit=20),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self.session:
            await self.session.close()

    async def execute(self) -> Dict[str, Any]:
        """날씨 데이터 업데이트 실행"""
        self.logger.info("날씨 데이터 업데이트 작업 시작")

        async with self:
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
                    "success_rate": (len(regions) - len(failed_regions)) / len(regions)
                    if regions
                    else 0,
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

            return await self.db_manager.fetch_all_async(query)

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
        """현재 날씨 정보 수집"""
        try:
            url = f"{self.settings.weather_api_base_url}/weather"
            params = {
                "lat": lat,
                "lon": lon,
                "appid": self.settings.weather_api_key,
                "units": "metric",
                "lang": "kr",
            }

            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_current_weather(data)
                else:
                    self.logger.warning(f"날씨 API 호출 실패: {response.status}")
                    return None

        except Exception as e:
            self.logger.error(f"현재 날씨 수집 실패: {e}")
            return None

    async def _fetch_weather_forecast(
        self, lat: float, lon: float, days: int = 7
    ) -> Optional[List[Dict[str, Any]]]:
        """날씨 예보 정보 수집"""
        try:
            url = f"{self.settings.weather_api_base_url}/forecast"
            params = {
                "lat": lat,
                "lon": lon,
                "appid": self.settings.weather_api_key,
                "units": "metric",
                "lang": "kr",
                "cnt": days * 8,  # 3시간 간격으로 하루 8개
            }

            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._parse_weather_forecast(data)
                else:
                    self.logger.warning(f"예보 API 호출 실패: {response.status}")
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
                %(region_code)s, %(temperature)s, %(humidity)s, 0,
                %(wind_speed)s, %(wind_direction)s, %(pressure)s,
                %(weather_description)s, %(visibility)s, %(recorded_at)s
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

            params = {**weather_data, "region_code": region_code}
            await self.db_manager.execute_async(query, params)

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
            WHERE region_code = %(region_code)s
            AND forecast_date < CURRENT_DATE - INTERVAL '7 days'
            """
            await self.db_manager.execute_async(
                cleanup_query, {"region_code": region_code}
            )

            # 새 예보 데이터 저장
            insert_query = """
            INSERT INTO weather_forecast (
                region_code, forecast_date, forecast_type, min_temp, max_temp,
                precipitation_prob, weather_condition, forecast_issued_at
            ) VALUES (
                %(region_code)s, %(forecast_date)s, 'short',
                %(temp_min)s, %(temp_max)s, %(precipitation_probability)s,
                %(weather_description)s, NOW()
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
                params = {
                    "region_code": region_code,
                    "forecast_date": forecast_date,
                    "temp_min": forecast["temp_min"],
                    "temp_max": forecast["temp_max"],
                    "precipitation_probability": forecast["precipitation_probability"],
                    "weather_description": forecast["weather_description"],
                }
                await self.db_manager.execute_async(insert_query, params)
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
