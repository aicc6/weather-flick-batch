"""
여행지 추천 엔진

날씨 데이터와 과거 통계를 기반으로 여행지별 최적 방문 시기를 계산하고
추천 점수를 산출하는 모듈입니다.
"""

import numpy as np
from datetime import datetime
from typing import Dict, List, Optional
from dataclasses import dataclass
from enum import Enum


class RecommendationLevel(Enum):
    EXCELLENT = "excellent"  # 매우 좋음 (8.5-10점)
    GOOD = "good"  # 좋음 (7.0-8.4점)
    FAIR = "fair"  # 보통 (5.0-6.9점)
    POOR = "poor"  # 나쁨 (0-4.9점)


@dataclass
class WeatherScore:
    """날씨 점수 구성 요소"""

    temperature_score: float  # 기온 점수 (0-10)
    precipitation_score: float  # 강수 점수 (0-10)
    humidity_score: float  # 습도 점수 (0-10)
    wind_score: float  # 바람 점수 (0-10)
    overall_score: float  # 종합 점수 (0-10)


@dataclass
class TravelRecommendation:
    """여행 추천 결과"""

    region_code: str
    region_name: str
    attraction_id: Optional[str]
    date_period: datetime
    weather_score: WeatherScore
    recommendation_level: RecommendationLevel
    recommendation_reason: str
    best_activities: List[str]


class WeatherBasedRecommendationEngine:
    """날씨 기반 여행지 추천 엔진"""

    def __init__(self):
        # 활동별 최적 날씨 조건 정의
        self.activity_weather_preferences = {
            "해변/물놀이": {
                "optimal_temp_range": (25, 32),
                "max_precipitation": 5,
                "max_humidity": 70,
                "max_wind_speed": 8,
            },
            "등산/하이킹": {
                "optimal_temp_range": (15, 28),
                "max_precipitation": 3,
                "max_humidity": 65,
                "max_wind_speed": 12,
            },
            "도심관광": {
                "optimal_temp_range": (10, 30),
                "max_precipitation": 10,
                "max_humidity": 80,
                "max_wind_speed": 15,
            },
            "야외축제": {
                "optimal_temp_range": (18, 28),
                "max_precipitation": 2,
                "max_humidity": 70,
                "max_wind_speed": 10,
            },
            "사진촬영": {
                "optimal_temp_range": (5, 35),
                "max_precipitation": 0,
                "max_humidity": 85,
                "max_wind_speed": 20,
            },
        }

        # 계절별 가중치
        self.seasonal_weights = {
            "temperature": {
                "spring": 0.3,
                "summer": 0.4,
                "autumn": 0.3,
                "winter": 0.25,
            },
            "precipitation": {
                "spring": 0.25,
                "summer": 0.35,
                "autumn": 0.25,
                "winter": 0.2,
            },
            "humidity": {"spring": 0.2, "summer": 0.3, "autumn": 0.2, "winter": 0.15},
            "wind": {"spring": 0.25, "summer": 0.2, "autumn": 0.25, "winter": 0.4},
        }

    def calculate_temperature_score(self, temp: float, month: int) -> float:
        """기온 점수 계산"""
        # 월별 최적 기온 범위
        optimal_ranges = {
            1: (-5, 10),
            2: (0, 12),
            3: (8, 18),
            4: (15, 23),
            5: (18, 26),
            6: (22, 30),
            7: (24, 32),
            8: (24, 32),
            9: (20, 28),
            10: (15, 25),
            11: (8, 18),
            12: (-2, 12),
        }

        min_temp, max_temp = optimal_ranges[month]

        if min_temp <= temp <= max_temp:
            # 최적 범위 내: 8-10점
            center = (min_temp + max_temp) / 2
            distance_from_center = abs(temp - center)
            max_distance = (max_temp - min_temp) / 2
            score = 10 - (distance_from_center / max_distance) * 2
            return max(8, score)
        else:
            # 최적 범위 밖: 감점
            if temp < min_temp:
                # 너무 춥거나 너무 더운 경우
                distance = min_temp - temp
                score = max(0, 8 - distance * 0.5)
            else:
                distance = temp - max_temp
                score = max(0, 8 - distance * 0.3)
            return score

    def calculate_precipitation_score(self, precipitation: float) -> float:
        """강수량 점수 계산"""
        if precipitation == 0:
            return 10
        elif precipitation <= 1:
            return 9
        elif precipitation <= 5:
            return 8 - (precipitation - 1) * 0.5
        elif precipitation <= 10:
            return 6 - (precipitation - 5) * 0.4
        elif precipitation <= 20:
            return 4 - (precipitation - 10) * 0.2
        else:
            return max(0, 2 - (precipitation - 20) * 0.1)

    def calculate_humidity_score(self, humidity: float, temp: float) -> float:
        """습도 점수 계산 (기온 고려)"""
        if temp < 10:
            # 추운 날씨: 습도 덜 중요
            if humidity <= 80:
                return 10 - (humidity - 40) * 0.05
            else:
                return max(6, 10 - (humidity - 80) * 0.2)
        elif temp < 25:
            # 온화한 날씨: 40-70% 최적
            if 40 <= humidity <= 70:
                return 10 - abs(humidity - 55) * 0.1
            else:
                return max(4, 10 - abs(humidity - 55) * 0.15)
        else:
            # 더운 날씨: 낮은 습도 선호
            if humidity <= 60:
                return 10 - (humidity - 30) * 0.1
            else:
                return max(2, 10 - (humidity - 60) * 0.2)

    def calculate_wind_score(self, wind_speed: float, season: str) -> float:
        """풍속 점수 계산"""
        seasonal_optimal = {
            "spring": (2, 8),
            "summer": (3, 10),
            "autumn": (2, 8),
            "winter": (0, 6),
        }

        min_wind, max_wind = seasonal_optimal.get(season, (2, 8))

        if min_wind <= wind_speed <= max_wind:
            return 9 + (wind_speed - min_wind) / (max_wind - min_wind)
        elif wind_speed < min_wind:
            # 바람이 너무 약함
            return 7 + (wind_speed / min_wind) * 2
        else:
            # 바람이 너무 강함
            if wind_speed <= max_wind + 5:
                return 9 - (wind_speed - max_wind) * 0.8
            else:
                return max(0, 5 - (wind_speed - max_wind - 5) * 0.3)

    def get_season(self, month: int) -> str:
        """월을 계절로 변환"""
        if month in [3, 4, 5]:
            return "spring"
        elif month in [6, 7, 8]:
            return "summer"
        elif month in [9, 10, 11]:
            return "autumn"
        else:
            return "winter"

    def calculate_weather_score(self, weather_data: Dict, month: int) -> WeatherScore:
        """종합 날씨 점수 계산"""
        season = self.get_season(month)

        # 개별 점수 계산
        temp_score = self.calculate_temperature_score(
            weather_data.get("avg_temp", 20), month
        )
        precip_score = self.calculate_precipitation_score(
            weather_data.get("precipitation", 0)
        )
        humidity_score = self.calculate_humidity_score(
            weather_data.get("humidity", 60), weather_data.get("avg_temp", 20)
        )
        wind_score = self.calculate_wind_score(
            weather_data.get("wind_speed", 5), season
        )

        # 계절별 가중치 적용하여 종합 점수 계산
        weights = self.seasonal_weights
        overall_score = (
            temp_score * weights["temperature"][season]
            + precip_score * weights["precipitation"][season]
            + humidity_score * weights["humidity"][season]
            + wind_score * weights["wind"][season]
        ) / sum(weights[key][season] for key in weights.keys())

        return WeatherScore(
            temperature_score=round(temp_score, 2),
            precipitation_score=round(precip_score, 2),
            humidity_score=round(humidity_score, 2),
            wind_score=round(wind_score, 2),
            overall_score=round(overall_score, 2),
        )

    def get_recommendation_level(self, score: float) -> RecommendationLevel:
        """점수를 기반으로 추천 등급 결정"""
        if score >= 8.5:
            return RecommendationLevel.EXCELLENT
        elif score >= 7.0:
            return RecommendationLevel.GOOD
        elif score >= 5.0:
            return RecommendationLevel.FAIR
        else:
            return RecommendationLevel.POOR

    def get_recommended_activities(self, weather_data: Dict, score: float) -> List[str]:
        """날씨 조건에 맞는 추천 활동"""
        temp = weather_data.get("avg_temp", 20)
        precipitation = weather_data.get("precipitation", 0)
        humidity = weather_data.get("humidity", 60)
        wind_speed = weather_data.get("wind_speed", 5)

        recommended = []

        for activity, conditions in self.activity_weather_preferences.items():
            temp_min, temp_max = conditions["optimal_temp_range"]

            if (
                temp_min <= temp <= temp_max
                and precipitation <= conditions["max_precipitation"]
                and humidity <= conditions["max_humidity"]
                and wind_speed <= conditions["max_wind_speed"]
            ):
                recommended.append(activity)

        # 점수가 낮으면 실내 활동 추천
        if score < 6.0:
            recommended.extend(["실내 관광", "박물관/미술관", "쇼핑"])

        return recommended[:3]  # 최대 3개 활동 추천

    def generate_recommendation_reason(
        self,
        weather_score: WeatherScore,
        recommendation_level: RecommendationLevel,
        weather_data: Dict,
    ) -> str:
        """추천 이유 생성"""
        temp = weather_data.get("avg_temp", 20)
        precipitation = weather_data.get("precipitation", 0)

        reasons = []

        if weather_score.temperature_score >= 8:
            reasons.append(f"쾌적한 기온({temp:.1f}°C)")
        elif weather_score.temperature_score < 5:
            if temp < 5:
                reasons.append("추운 날씨")
            elif temp > 35:
                reasons.append("매우 더운 날씨")

        if weather_score.precipitation_score >= 8:
            reasons.append("맑은 날씨")
        elif weather_score.precipitation_score < 6:
            reasons.append(f"강수 예상({precipitation:.1f}mm)")

        if weather_score.humidity_score < 5:
            reasons.append("높은 습도")

        if recommendation_level == RecommendationLevel.EXCELLENT:
            return f"최적의 여행 조건: {', '.join(reasons)}"
        elif recommendation_level == RecommendationLevel.GOOD:
            return f"좋은 여행 조건: {', '.join(reasons)}"
        elif recommendation_level == RecommendationLevel.FAIR:
            return f"보통의 여행 조건: {', '.join(reasons)}"
        else:
            return f"여행하기 어려운 조건: {', '.join(reasons)}"

    def calculate_monthly_recommendations(
        self, historical_weather_data: List[Dict], region_info: Dict
    ) -> List[TravelRecommendation]:
        """월별 여행 추천 계산"""
        monthly_recommendations = []

        # 월별 데이터 집계
        monthly_data = {}
        for data in historical_weather_data:
            month = data["weather_date"].month
            if month not in monthly_data:
                monthly_data[month] = []
            monthly_data[month].append(data)

        # 각 월별 추천 생성
        for month, data_list in monthly_data.items():
            # 월별 평균 계산
            avg_weather = {
                "avg_temp": np.mean(
                    [d["avg_temp"] for d in data_list if d["avg_temp"]]
                ),
                "precipitation": np.mean(
                    [d["precipitation"] for d in data_list if d["precipitation"]]
                ),
                "humidity": np.mean(
                    [d["humidity"] for d in data_list if d["humidity"]]
                ),
                "wind_speed": np.mean(
                    [d["wind_speed"] for d in data_list if d["wind_speed"]]
                ),
            }

            # 점수 계산
            weather_score = self.calculate_weather_score(avg_weather, month)
            recommendation_level = self.get_recommendation_level(
                weather_score.overall_score
            )
            recommended_activities = self.get_recommended_activities(
                avg_weather, weather_score.overall_score
            )
            reason = self.generate_recommendation_reason(
                weather_score, recommendation_level, avg_weather
            )

            # 추천 생성
            recommendation = TravelRecommendation(
                region_code=region_info["region_code"],
                region_name=region_info["region_name"],
                attraction_id=region_info.get("attraction_id"),
                date_period=datetime(2024, month, 1),  # 예시 연도
                weather_score=weather_score,
                recommendation_level=recommendation_level,
                recommendation_reason=reason,
                best_activities=recommended_activities,
            )

            monthly_recommendations.append(recommendation)

        return monthly_recommendations


# 사용 예시
if __name__ == "__main__":
    engine = WeatherBasedRecommendationEngine()

    # 예시 날씨 데이터
    sample_weather_data = [
        {
            "weather_date": datetime(2023, 5, 15),
            "avg_temp": 22.5,
            "precipitation": 2.3,
            "humidity": 65.0,
            "wind_speed": 4.2,
        },
        {
            "weather_date": datetime(2023, 6, 15),
            "avg_temp": 28.1,
            "precipitation": 15.7,
            "humidity": 78.0,
            "wind_speed": 3.8,
        },
    ]

    region_info = {
        "region_code": "11",
        "region_name": "서울",
        "attraction_id": "seoul_001",
    }

    # 월별 추천 계산
    recommendations = engine.calculate_monthly_recommendations(
        sample_weather_data, region_info
    )

    for rec in recommendations:
        print(f"\n=== {rec.date_period.month}월 추천 ===")
        print(f"지역: {rec.region_name}")
        print(f"종합 점수: {rec.weather_score.overall_score}/10")
        print(f"추천 등급: {rec.recommendation_level.value}")
        print(f"추천 이유: {rec.recommendation_reason}")
        print(f"추천 활동: {', '.join(rec.best_activities)}")
        print(
            f"세부 점수 - 기온: {rec.weather_score.temperature_score}, "
            f"강수: {rec.weather_score.precipitation_score}, "
            f"습도: {rec.weather_score.humidity_score}, "
            f"바람: {rec.weather_score.wind_score}"
        )
