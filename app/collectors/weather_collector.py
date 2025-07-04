"""
날씨 데이터 수집기

기상청 API를 통해 날씨 데이터를 수집하는 기능을 제공합니다.
"""

import requests
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from config.settings import get_api_config
from app.core.multi_api_key_manager import get_api_key_manager, APIProvider
from config.constants import (
    WEATHER_COORDINATES,
    OBSERVATION_STATIONS,
    WEATHER_CONDITIONS,
)
from app.core.logger import get_logger


class WeatherDataCollector:
    """기상청 날씨 데이터 수집기"""

    def __init__(self):
        self.config = get_api_config()
        self.base_url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0"
        self.historical_url = "http://apis.data.go.kr/1360000/AsosHourlyInfoService"
        self.logger = get_logger(__name__)

        # 다중 API 키 시스템 사용
        self.key_manager = get_api_key_manager()
        active_key = self.key_manager.get_active_key(APIProvider.KMA)
        self.kma_api_key = active_key.key if active_key else self.config.kma_api_key

        if not self.kma_api_key:
            self.logger.warning("기상청 API 키가 설정되지 않았습니다.")

    def get_current_weather(self, region_name: str) -> Optional[Dict]:
        """현재 날씨 정보 조회 (초단기실황)"""

        if region_name not in WEATHER_COORDINATES:
            self.logger.error(f"지원하지 않는 지역: {region_name}")
            return None

        coords = WEATHER_COORDINATES[region_name]
        now = datetime.now()

        # 초단기실황은 매시간 30분에 생성되므로 시간 조정
        if now.minute < 30:
            base_time = (now - timedelta(hours=1)).strftime("%H30")
            base_date = (now - timedelta(hours=1)).strftime("%Y%m%d")
        else:
            base_time = now.strftime("%H30")
            base_date = now.strftime("%Y%m%d")

        params = {
            "serviceKey": self.kma_api_key,
            "numOfRows": "10",
            "pageNo": "1",
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": coords["nx"],
            "ny": coords["ny"],
        }

        try:
            start_time = time.time()
            url = f"{self.base_url}/getUltraSrtNcst"
            response = requests.get(url, params=params, timeout=self.config.timeout)
            response.raise_for_status()

            duration = time.time() - start_time
            self.logger.debug(
                f"API 호출 완료: 기상청 현재날씨, 응답시간: {duration:.3f}초"
            )

            data = response.json()

            if data["response"]["header"]["resultCode"] == "00":
                items = data["response"]["body"]["items"]["item"]
                weather_data = self._parse_current_weather(items, region_name)
                return weather_data
            else:
                self.logger.error(
                    f"API 오류: {data['response']['header']['resultMsg']}"
                )
                return None

        except requests.exceptions.RequestException as e:
            self.logger.error(f"현재 날씨 조회 네트워크 오류: {e}")
            return None
        except Exception as e:
            self.logger.error(f"현재 날씨 조회 오류: {e}")
            return None

    def get_weather_forecast(self, region_name: str, days: int = 3) -> List[Dict]:
        """단기 예보 조회"""

        if region_name not in WEATHER_COORDINATES:
            self.logger.error(f"지원하지 않는 지역: {region_name}")
            return []

        coords = WEATHER_COORDINATES[region_name]
        now = datetime.now()

        # 단기예보 발표시간 결정
        current_hour = now.hour
        if current_hour < 5:
            base_time = "2300"
            base_date = (now - timedelta(days=1)).strftime("%Y%m%d")
        elif current_hour < 11:
            base_time = "0500"
            base_date = now.strftime("%Y%m%d")
        elif current_hour < 17:
            base_time = "1100"
            base_date = now.strftime("%Y%m%d")
        elif current_hour < 23:
            base_time = "1700"
            base_date = now.strftime("%Y%m%d")
        else:
            base_time = "2300"
            base_date = now.strftime("%Y%m%d")

        params = {
            "serviceKey": self.kma_api_key,
            "numOfRows": "1000",
            "pageNo": "1",
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": coords["nx"],
            "ny": coords["ny"],
        }

        try:
            start_time = time.time()
            url = f"{self.base_url}/getVilageFcst"
            response = requests.get(url, params=params, timeout=self.config.timeout)
            response.raise_for_status()

            duration = time.time() - start_time
            self.logger.debug(f"API 호출 완료: 기상청 예보, 응답시간: {duration:.3f}초")

            data = response.json()

            if data["response"]["header"]["resultCode"] == "00":
                items = data["response"]["body"]["items"]["item"]
                forecast_data = self._parse_forecast_data(items, region_name, days)
                return forecast_data
            else:
                self.logger.error(
                    f"예보 API 오류: {data['response']['header']['resultMsg']}"
                )
                return []

        except requests.exceptions.RequestException as e:
            self.logger.error(f"예보 조회 네트워크 오류: {e}")
            return []
        except Exception as e:
            self.logger.error(f"예보 조회 오류: {e}")
            return []

    def get_historical_weather(
        self, region_name: str, start_date: str, end_date: str
    ) -> List[Dict]:
        """과거 날씨 데이터 조회"""

        if region_name not in OBSERVATION_STATIONS:
            self.logger.error(f"지원하지 않는 지역: {region_name}")
            return []

        station_id = OBSERVATION_STATIONS[region_name]
        historical_data = []

        # 날짜 범위 순회
        current_date = datetime.strptime(start_date, "%Y%m%d")
        end_date_obj = datetime.strptime(end_date, "%Y%m%d")

        while current_date <= end_date_obj:
            params = {
                "serviceKey": self.kma_api_key,
                "numOfRows": "24",
                "pageNo": "1",
                "dataType": "JSON",
                "dataCd": "ASOS",
                "dateCd": "HR",
                "startDt": current_date.strftime("%Y%m%d"),
                "startHh": "01",
                "endDt": current_date.strftime("%Y%m%d"),
                "endHh": "24",
                "stnIds": station_id,
            }

            try:
                start_time = time.time()
                url = f"{self.historical_url}/getWthrDataList"
                response = requests.get(url, params=params, timeout=self.config.timeout)
                response.raise_for_status()

                duration = time.time() - start_time
                self.logger.debug(
                    f"API 호출 완료: 기상청 과거데이터, 응답시간: {duration:.3f}초"
                )

                data = response.json()

                if data["response"]["header"]["resultCode"] == "00":
                    if "item" in data["response"]["body"]["items"]:
                        items = data["response"]["body"]["items"]["item"]
                        daily_data = self._parse_historical_data(
                            items, region_name, current_date.strftime("%Y%m%d")
                        )
                        if daily_data:
                            historical_data.append(daily_data)

                # API 호출 제한을 위한 대기
                time.sleep(0.1)

            except requests.exceptions.RequestException as e:
                self.logger.warning(
                    f"과거 날씨 네트워크 오류 [{current_date.strftime('%Y%m%d')}]: {e}"
                )
            except Exception as e:
                self.logger.warning(
                    f"과거 날씨 조회 오류 [{current_date.strftime('%Y%m%d')}]: {e}"
                )

            current_date += timedelta(days=1)

        return historical_data

    def _parse_current_weather(self, items: List[Dict], region_name: str) -> Dict:
        """현재 날씨 데이터 파싱"""
        weather_data = {
            "region_name": region_name,
            "observation_time": datetime.now(),
            "temperature": None,
            "humidity": None,
            "precipitation": None,
            "wind_speed": None,
            "weather_condition": None,
        }

        for item in items:
            category = item["category"]
            value = item["obsrValue"]

            try:
                if category == "T1H":  # 기온
                    weather_data["temperature"] = float(value)
                elif category == "REH":  # 습도
                    weather_data["humidity"] = float(value)
                elif category == "RN1":  # 1시간 강수량
                    weather_data["precipitation"] = float(value)
                elif category == "WSD":  # 풍속
                    weather_data["wind_speed"] = float(value)
                elif category == "PTY":  # 강수형태
                    weather_data["weather_condition"] = WEATHER_CONDITIONS.get(
                        value, "맑음"
                    )
            except (ValueError, TypeError) as e:
                self.logger.warning(f"날씨 데이터 파싱 오류 [{category}]: {value}, {e}")

        return weather_data

    def _parse_forecast_data(
        self, items: List[Dict], region_name: str, days: int
    ) -> List[Dict]:
        """예보 데이터 파싱"""
        forecast_data = []
        daily_data = {}

        for item in items:
            fcst_date = item["fcstDate"]
            fcst_time = item["fcstTime"]
            category = item["category"]
            value = item["fcstValue"]

            # 요청한 일수만큼만 처리
            try:
                forecast_date = datetime.strptime(fcst_date, "%Y%m%d")
                if forecast_date > datetime.now() + timedelta(days=days):
                    continue
            except ValueError:
                continue

            date_key = fcst_date
            if date_key not in daily_data:
                daily_data[date_key] = {
                    "region_name": region_name,
                    "forecast_date": forecast_date,
                    "min_temp": None,
                    "max_temp": None,
                    "precipitation_prob": None,
                    "weather_condition": None,
                }

            try:
                if category == "TMN":  # 최저기온
                    daily_data[date_key]["min_temp"] = float(value)
                elif category == "TMX":  # 최고기온
                    daily_data[date_key]["max_temp"] = float(value)
                elif category == "POP":  # 강수확률
                    daily_data[date_key]["precipitation_prob"] = int(value)
                elif category == "PTY" and fcst_time == "1200":  # 낮 12시 강수형태
                    daily_data[date_key]["weather_condition"] = WEATHER_CONDITIONS.get(
                        value, "맑음"
                    )
            except (ValueError, TypeError) as e:
                self.logger.warning(f"예보 데이터 파싱 오류 [{category}]: {value}, {e}")

        return list(daily_data.values())

    def _parse_historical_data(
        self, items: List[Dict], region_name: str, date: str
    ) -> Optional[Dict]:
        """과거 날씨 데이터 파싱"""
        temps = []
        humidity_values = []
        precipitation = 0
        wind_speeds = []

        for item in items:
            try:
                if item.get("ta") and item["ta"] != "":  # 기온
                    temps.append(float(item["ta"]))
                if item.get("hm") and item["hm"] != "":  # 습도
                    humidity_values.append(float(item["hm"]))
                if item.get("rn") and item["rn"] not in ["", "-"]:  # 강수량
                    precipitation += float(item["rn"])
                if item.get("ws") and item["ws"] != "":  # 풍속
                    wind_speeds.append(float(item["ws"]))
            except (ValueError, TypeError) as e:
                self.logger.debug(f"과거 데이터 파싱 경고: {e}")

        if not temps:
            return None

        try:
            return {
                "region_name": region_name,
                "weather_date": datetime.strptime(date, "%Y%m%d"),
                "avg_temp": sum(temps) / len(temps),
                "max_temp": max(temps),
                "min_temp": min(temps),
                "humidity": sum(humidity_values) / len(humidity_values)
                if humidity_values
                else None,
                "precipitation": precipitation,
                "wind_speed": sum(wind_speeds) / len(wind_speeds)
                if wind_speeds
                else None,
            }
        except Exception as e:
            self.logger.warning(f"과거 데이터 통계 계산 오류: {e}")
            return None
