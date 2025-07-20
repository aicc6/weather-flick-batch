"""
상수 정의 모듈

애플리케이션에서 사용하는 상수들을 정의합니다.
"""

from enum import Enum


class JobType(Enum):
    """배치 작업 타입"""

    WEATHER_DATA = "weather_data"
    TOURIST_DATA = "tourist_data"
    SCORE_CALCULATION = "score_calculation"
    DATA_QUALITY_CHECK = "data_quality_check"
    WEATHER_CHANGE_NOTIFICATION = "weather_change_notification"


class JobStatus(Enum):
    """작업 상태"""

    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"  # DB 제약조건에 맞게 수정
    COMPLETED = "success"  # 하위 호환성을 위한 별칭
    FAILED = "failure"  # DB 제약조건에 맞게 수정
    FAILURE = "failure"  # 하위 호환성을 위한 별칭
    CANCELLED = "cancelled"


class RecommendationLevel(Enum):
    """추천 등급"""

    EXCELLENT = "excellent"  # 매우 좋음 (8.5-10점)
    GOOD = "good"  # 좋음 (7.0-8.4점)
    FAIR = "fair"  # 보통 (5.0-6.9점)
    POOR = "poor"  # 나쁨 (0-4.9점)


class ContentType(Enum):
    """관광 콘텐츠 타입"""

    TOURIST_SPOT = "12"  # 관광지
    CULTURAL_FACILITY = "14"  # 문화시설
    FESTIVAL = "15"  # 축제공연행사
    TRAVEL_COURSE = "25"  # 여행코스
    LEISURE_SPORTS = "28"  # 레포츠
    ACCOMMODATION = "32"  # 숙박
    SHOPPING = "38"  # 쇼핑
    RESTAURANT = "39"  # 음식점


# 지역 코드 매핑
AREA_CODES = {
    "서울": "1",
    "인천": "2",
    "대전": "3",
    "대구": "4",
    "광주": "5",
    "부산": "6",
    "울산": "7",
    "세종": "8",
    "경기": "31",
    "강원": "32",
    "충북": "33",
    "충남": "34",
    "경북": "35",
    "경남": "36",
    "전북": "37",
    "전남": "38",
    "제주": "39",
}

# 날씨 코드 매핑
WEATHER_CONDITIONS = {"0": "맑음", "1": "비", "2": "비/눈", "3": "눈", "4": "소나기"}

# 기상청 좌표 매핑 (nx, ny) 및 위경도
WEATHER_COORDINATES = {
    "서울": {"nx": 60, "ny": 127, "lat": 37.5665, "lon": 126.9780},
    "부산": {"nx": 98, "ny": 76, "lat": 35.1796, "lon": 129.0756},
    "대구": {"nx": 89, "ny": 90, "lat": 35.8714, "lon": 128.6014},
    "인천": {"nx": 55, "ny": 124, "lat": 37.4563, "lon": 126.7052},
    "광주": {"nx": 58, "ny": 74, "lat": 35.1595, "lon": 126.8526},
    "대전": {"nx": 67, "ny": 100, "lat": 36.3504, "lon": 127.3845},
    "울산": {"nx": 102, "ny": 84, "lat": 35.5384, "lon": 129.3114},
    "세종": {"nx": 66, "ny": 103, "lat": 36.4800, "lon": 127.2890},
    "제주": {"nx": 52, "ny": 38, "lat": 33.4996, "lon": 126.5312},
}

# 관측소 코드 매핑
OBSERVATION_STATIONS = {
    "서울": "108",
    "부산": "159",
    "대구": "143",
    "인천": "112",
    "광주": "156",
    "대전": "133",
    "울산": "152",
    "제주": "184",
}

# 데이터 품질 임계값
DATA_QUALITY_THRESHOLDS = {
    "completeness": 0.95,  # 완성도 95% 이상
    "validity": 0.98,  # 유효성 98% 이상
    "consistency": 0.99,  # 일관성 99% 이상
    "timeliness": 24,  # 최신성 24시간 이내
}

# API 제한
API_LIMITS = {
    "kto_requests_per_minute": 60,
    "kma_requests_per_minute": 60,
    "max_retry_attempts": 3,
    "retry_delay_seconds": 1,
}

# 파일 경로
DATA_PATHS = {
    "raw_data": "data/raw",
    "processed_data": "data/processed",
    "sample_data": "data/sample",
    "logs": "logs",
    "exports": "data/exports",
}

# 날짜 형식
DATE_FORMATS = {
    "api_date": "%Y%m%d",
    "api_time": "%H%M",
    "log_datetime": "%Y-%m-%d %H:%M:%S",
    "file_datetime": "%Y%m%d_%H%M%S",
}
