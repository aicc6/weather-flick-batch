"""
통합 데이터베이스 모델 정의
모든 서비스에서 공통으로 사용하는 SQLAlchemy ORM 모델들

각 모델의 주석에는 다음과 같은 정보가 포함됩니다:
- 사용처: 해당 모델을 사용하는 서비스 목록
- 설명: 테이블의 용도와 주요 기능
"""

from __future__ import annotations
import enum
import uuid
from datetime import datetime, date
from typing import Any, Optional, List

from pydantic import BaseModel, Field, ConfigDict, validator
from sqlalchemy import (
    DECIMAL,
    Boolean,
    Column,
    Date,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


# ===========================================
# Enum 정의
# ===========================================

class AdminStatus(enum.Enum):
    """관리자 계정 상태"""
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"
    LOCKED = "LOCKED"


class TravelPlanStatus(enum.Enum):
    """여행 계획 상태"""
    PLANNING = "PLANNING"
    CONFIRMED = "CONFIRMED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    CANCELLED = "CANCELLED"


class UserRole(enum.Enum):
    """사용자 역할"""
    USER = "USER"
    ADMIN = "ADMIN"


# ===========================================
# 사용자 및 인증 관련 테이블
# ===========================================

class User(Base):
    """
    사용자 정보 테이블
    사용처: weather-flick-back, weather-flick-admin-back
    설명: 일반 사용자 계정 정보 및 프로필 관리
    """
    __tablename__ = "users"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    user_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=True)  # OAuth 사용자는 비밀번호가 없을 수 있음
    nickname = Column(String, index=True, nullable=False)
    profile_image = Column(String)
    preferences = Column(JSONB, default=dict)
    preferred_region = Column(String)  # 선호 지역
    preferred_theme = Column(String)  # 선호 테마
    bio = Column(Text)  # 자기소개
    is_active = Column(Boolean, default=True)
    is_email_verified = Column(Boolean, default=False)
    role = Column(Enum(UserRole), default=UserRole.USER)
    google_id = Column(String, unique=True, nullable=True)  # 구글 OAuth ID
    auth_provider = Column(String, default="local")  # 인증 제공자 (local, google 등)
    last_login = Column(DateTime)
    login_count = Column(Integer, default=0)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # id 속성을 user_id의 별칭으로 추가
    @property
    def id(self):
        return self.user_id


class Admin(Base):
    """
    관리자 정보 테이블
    사용처: weather-flick-admin-back
    설명: 관리자 계정 정보 및 권한 관리
    """
    __tablename__ = "admins"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    admin_id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    password_hash = Column(String, nullable=False)
    name = Column(String)
    phone = Column(String)
    status = Column(Enum(AdminStatus), default=AdminStatus.ACTIVE)
    is_superuser = Column(Boolean, default=False)  # 슈퍼관리자 여부
    last_login_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())


class RefreshToken(Base):
    """
    리프레시 토큰 관리 테이블
    사용처: weather-flick-back
    설명: 사용자 인증용 리프레시 토큰 저장 및 관리
    """
    __tablename__ = "refresh_tokens"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    token = Column(String, unique=True, index=True, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class PasswordResetToken(Base):
    """
    비밀번호 재설정 토큰 테이블
    사용처: weather-flick-back
    설명: 비밀번호 재설정 요청 시 생성되는 토큰 관리
    """
    __tablename__ = "password_reset_tokens"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    token = Column(String, unique=True, index=True, nullable=False)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class EmailVerification(Base):
    """
    이메일 인증 토큰 테이블
    사용처: weather-flick-back
    설명: 사용자 이메일 인증을 위한 토큰 관리
    """
    __tablename__ = "email_verifications"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, index=True, nullable=False)
    code = Column(String, nullable=False)
    is_used = Column(Boolean, default=False)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, server_default=func.now())


# ===========================================
# 여행 계획 관련 테이블
# ===========================================

class TravelPlan(Base):
    """
    여행 계획 테이블
    사용처: weather-flick-back
    설명: 사용자가 생성한 여행 계획 정보
    """
    __tablename__ = "travel_plans"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    plan_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    status = Column(Enum(TravelPlanStatus), default=TravelPlanStatus.PLANNING)
    budget = Column(DECIMAL(10, 2))
    participants = Column(Integer, default=1)
    transportation = Column(String)  # 교통수단 (car, public, etc.)
    start_location = Column(String)  # 출발지
    theme = Column(String)  # 여행 테마 (healing, activity, culture, etc.)
    itinerary = Column(JSONB, default=dict)  # 일정 정보
    weather_info = Column(JSONB, default=dict)  # 날씨 정보
    plan_type = Column(String, default="manual")  # manual 또는 custom
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class TravelRoute(Base):
    """
    여행 경로 테이블
    사용처: weather-flick-back
    설명: 여행 계획 내 일별 경로 정보
    """
    __tablename__ = "travel_routes"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    route_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    plan_id = Column(UUID(as_uuid=True), ForeignKey("travel_plans.plan_id", ondelete="CASCADE"), nullable=False)
    day = Column(Integer, nullable=False)  # 며칠째
    sequence = Column(Integer, nullable=False)  # 순서
    departure_name = Column(String, nullable=False)  # 출발지명
    departure_lat = Column(Float)
    departure_lng = Column(Float)
    destination_name = Column(String, nullable=False)  # 도착지명
    destination_lat = Column(Float)
    destination_lng = Column(Float)
    transport_type = Column(String)  # walk, car, transit
    duration = Column(Integer)  # 소요시간(분)
    distance = Column(Float)  # 거리(km)
    cost = Column(Float)  # 비용
    route_data = Column(JSONB)  # 상세 경로 정보
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class TransportationDetail(Base):
    """
    교통수단 상세 정보 테이블
    사용처: weather-flick-back
    설명: 대중교통 이용 시 상세 정보 (지하철, 버스 노선 등)
    """
    __tablename__ = "transportation_details"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    detail_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    route_id = Column(UUID(as_uuid=True), ForeignKey("travel_routes.route_id", ondelete="CASCADE"), nullable=False)
    transport_name = Column(String)  # 노선명 (예: 2호선, 100번 버스)
    transport_color = Column(String)  # 노선 색상
    departure_station = Column(String)  # 출발역/정류장
    arrival_station = Column(String)  # 도착역/정류장
    departure_time = Column(DateTime)  # 출발 시간
    arrival_time = Column(DateTime)  # 도착 시간
    fare = Column(Float)  # 요금
    transfer_info = Column(JSONB)  # 환승 정보
    created_at = Column(DateTime, server_default=func.now())


# ===========================================
# 지역 및 날씨 관련 테이블
# ===========================================

class Region(Base):
    """
    지역 정보 테이블
    사용처: weather-flick-back, weather-flick-admin-back, weather-flick-batch
    설명: 기본 지역 코드 및 계층 정보
    """
    __tablename__ = "regions"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    region_code = Column(String, primary_key=True)
    region_name = Column(String, nullable=False)
    parent_region_code = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    region_level = Column(Integer)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    is_active = Column(Boolean, default=True)
    grid_x = Column(Integer)
    grid_y = Column(Integer)
    region_name_full = Column(String)
    region_name_en = Column(String)
    region_id = Column(String)
    center_latitude = Column(Float)
    center_longitude = Column(Float)
    administrative_code = Column(String)
    api_mappings = Column(JSONB)
    coordinate_info = Column(JSONB)




class HistoricalWeatherDaily(Base):
    """
    일별 과거 날씨 데이터 테이블
    사용처: weather-flick-batch
    설명: 지역별 일별 과거 날씨 통계 데이터
    """
    __tablename__ = "historical_weather_daily"
    __table_args__ = (
        UniqueConstraint('region_code', 'weather_date', name='uq_historical_weather_daily'),
        {"extend_existing": True, "autoload_replace": False}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    region_code = Column(String, nullable=False, index=True)
    weather_date = Column(Date, nullable=False, index=True)
    avg_temp = Column(Float)
    max_temp = Column(Float)
    min_temp = Column(Float)
    total_precipitation = Column(Float)
    avg_humidity = Column(Float)
    avg_wind_speed = Column(Float)
    weather_condition = Column(String)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class WeatherForecast(Base):
    """
    날씨 예보 테이블
    사용처: weather-flick-batch
    설명: 지역별 날씨 예보 데이터 (최대 10일)
    """
    __tablename__ = "weather_forecast"
    __table_args__ = (
        UniqueConstraint('region_code', 'forecast_date', 'forecast_time',
                        name='uq_weather_forecast_region_datetime'),
        {"extend_existing": True, "autoload_replace": False}
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    region_code = Column(String, nullable=False, index=True)
    forecast_date = Column(Date, nullable=False, index=True)
    forecast_time = Column(String)  # HH:MM
    temperature = Column(Float)
    feels_like = Column(Float)
    min_temp = Column(Float)
    max_temp = Column(Float)
    humidity = Column(Float)
    precipitation_probability = Column(Float)
    precipitation_amount = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(String)
    weather_condition = Column(String)
    weather_description = Column(String)
    uv_index = Column(Float)
    visibility = Column(Float)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class WeatherCurrent(Base):
    """
    현재 날씨 데이터 테이블
    사용처: weather-flick-batch
    설명: 지역별 실시간 날씨 데이터
    """
    __tablename__ = "weather_current"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(Integer, primary_key=True, autoincrement=True)
    region_code = Column(String, nullable=False, unique=True, index=True)
    temperature = Column(Float)
    feels_like = Column(Float)
    humidity = Column(Float)
    pressure = Column(Float)
    wind_speed = Column(Float)
    wind_direction = Column(String)
    weather_condition = Column(String)
    weather_description = Column(String)
    visibility = Column(Float)
    uv_index = Column(Float)
    raw_data_id = Column(String, index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class WeatherData(Base):
    """
    날씨 데이터 테이블 (레거시)
    사용처: weather-flick-admin-back
    설명: 기존 날씨 데이터 테이블 (새로운 테이블로 마이그레이션 예정)
    """
    __tablename__ = "weather_data"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    weather_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    location = Column(String, index=True, nullable=False)
    temperature = Column(Float)
    humidity = Column(Integer)
    condition = Column(String)
    forecast_date = Column(Date)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


# ===========================================
# 여행지 및 추천 관련 테이블
# ===========================================

class Destination(Base):
    """
    여행지 정보 테이블
    사용처: weather-flick-admin-back
    설명: 추천 여행지 기본 정보
    """
    __tablename__ = "destinations"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    destination_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, index=True, nullable=False)
    province = Column(String, nullable=False)
    region = Column(String)
    category = Column(String)
    is_indoor = Column(Boolean, default=False)
    tags = Column(JSONB, default=list)
    latitude = Column(Float)
    longitude = Column(Float)
    amenities = Column(JSONB, default=dict)
    image_url = Column(String)
    rating = Column(Float)
    recommendation_weight = Column(Float, default=1.0)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class FavoritePlace(Base):
    """
    즐겨찾기 장소 테이블
    사용처: weather-flick-back
    설명: 사용자가 즐겨찾기한 장소 정보
    """
    __tablename__ = "favorite_places"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    place_name = Column(String, nullable=False)
    place_type = Column(String, nullable=False)  # tourist_attraction, restaurant, etc.
    place_id = Column(String)  # 외부 API의 장소 ID
    address = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    description = Column(Text)
    created_at = Column(DateTime, server_default=func.now())


class Review(Base):
    """
    리뷰 테이블
    사용처: weather-flick-back
    설명: 여행지 및 여행 계획에 대한 사용자 리뷰
    """
    __tablename__ = "reviews"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    review_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    destination_id = Column(UUID(as_uuid=True), ForeignKey("destinations.destination_id", ondelete="CASCADE"))
    travel_plan_id = Column(UUID(as_uuid=True), ForeignKey("travel_plans.plan_id", ondelete="CASCADE"))
    rating = Column(Integer, nullable=False)  # 1-5
    content = Column(Text)
    photos = Column(JSONB, default=list)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class TravelWeatherScore(Base):
    """
    여행지 날씨 점수 테이블
    사용처: weather-flick-back, weather-flick-admin-back
    설명: 날씨 기반 여행지 점수 계산
    """
    __tablename__ = "travel_weather_scores"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    score_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    destination_id = Column(UUID(as_uuid=True), ForeignKey("destinations.destination_id"))
    weather_condition = Column(String, nullable=False)
    season = Column(String, nullable=False)
    score = Column(Float, nullable=False)
    factors = Column(JSONB, default=dict)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


# ===========================================
# 관광 정보 관련 테이블
# ===========================================

class TouristAttraction(Base):
    """
    관광지 정보 테이블
    사용처: weather-flick-back, weather-flick-admin-back, weather-flick-batch
    설명: 한국관광공사 API에서 수집한 관광지 정보
    """
    __tablename__ = "tourist_attractions"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    content_id = Column(String, primary_key=True, index=True)
    region_code = Column(String, nullable=False, index=True)
    attraction_name = Column(String, nullable=False, index=True)
    category_code = Column(String, index=True)
    category_name = Column(String)
    sub_category_code = Column(String)
    sub_category_name = Column(String)
    address = Column(String)
    detail_address = Column(String)
    zipcode = Column(String)
    tel = Column(String)
    homepage = Column(Text)
    email = Column(String)
    use_time = Column(Text)
    rest_date = Column(Text)
    admission_fee = Column(Text)
    credit_card = Column(String)
    pet_allowed = Column(String)
    disabled_facility = Column(String)
    parking = Column(Text)
    stroller = Column(String)
    description = Column(Text)
    overview = Column(Text)
    created_time = Column(String)
    modified_time = Column(String)
    first_image = Column(String)
    first_image_small = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    map_level = Column(Integer)
    directions = Column(Text)
    additional_info = Column(JSONB)
    detail_intro_info = Column(JSONB)
    detail_additional_info = Column(JSONB)
    nearby_facilities = Column(JSONB)
    data_quality_score = Column(Float, default=0.5)
    raw_data_id = Column(String, index=True)
    last_sync_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class Restaurant(Base):
    """
    음식점 정보 테이블
    사용처: weather-flick-back, weather-flick-admin-back, weather-flick-batch
    설명: 한국관광공사 API에서 수집한 음식점 정보
    """
    __tablename__ = "restaurants"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    content_id = Column(String, primary_key=True, index=True)
    region_code = Column(String, nullable=False, index=True)
    restaurant_name = Column(String, nullable=False, index=True)
    category_code = Column(String, index=True)
    sub_category_code = Column(String)
    address = Column(String)
    detail_address = Column(String)
    zipcode = Column(String)
    tel = Column(String)
    homepage = Column(Text)
    overview = Column(Text)
    first_image = Column(String)
    first_image_small = Column(String)
    cuisine_type = Column(String)
    specialty_dish = Column(String)
    operating_hours = Column(Text)
    rest_date = Column(String)
    reservation_info = Column(String)
    credit_card = Column(String)
    smoking = Column(String)
    parking = Column(Text)
    room_available = Column(String)
    children_friendly = Column(String)
    takeout = Column(String)
    delivery = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    data_quality_score = Column(Float, default=0.5)
    raw_data_id = Column(String, index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    last_sync_at = Column(DateTime)
    processing_status = Column(String, default="pending")


class CulturalFacility(Base):
    """
    문화시설 정보 테이블
    사용처: weather-flick-back, weather-flick-admin-back, weather-flick-batch
    설명: 박물관, 미술관, 공연장 등 문화시설 정보
    """
    __tablename__ = "cultural_facilities"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    content_id = Column(String, primary_key=True, index=True)
    region_code = Column(String, nullable=False, index=True)
    facility_name = Column(String, nullable=False, index=True)
    facility_type = Column(String)  # museum, art_gallery, theater, etc.
    category_code = Column(String, index=True)
    address = Column(String)
    detail_address = Column(String)
    zipcode = Column(String)
    tel = Column(String)
    homepage = Column(Text)
    email = Column(String)
    operating_hours = Column(Text)
    rest_date = Column(String)
    admission_fee = Column(Text)
    parking_info = Column(Text)
    credit_card = Column(String)
    pet_allowed = Column(String)
    disabled_facility = Column(Text)
    overview = Column(Text)
    programs = Column(JSONB)
    exhibitions = Column(JSONB)
    first_image = Column(String)
    first_image_small = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    data_quality_score = Column(Float, default=0.5)
    raw_data_id = Column(String, index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    last_sync_at = Column(DateTime)


class FestivalEvent(Base):
    """
    축제/행사 정보 테이블
    사용처: weather-flick-back, weather-flick-admin-back, weather-flick-batch
    설명: 지역별 축제 및 행사 정보
    """
    __tablename__ = "festivals_events"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    content_id = Column(String, primary_key=True, index=True)
    region_code = Column(String, nullable=False, index=True)
    event_name = Column(String, nullable=False, index=True)
    category_code = Column(String, index=True)
    event_start_date = Column(Date, index=True)
    event_end_date = Column(Date, index=True)
    event_place = Column(String)
    address = Column(String)
    detail_address = Column(String)
    tel = Column(String)
    homepage = Column(Text)
    overview = Column(Text)
    event_program = Column(Text)
    event_info = Column(Text)
    play_time = Column(String)
    use_time_festival = Column(Text)
    sponsor = Column(String)
    sponsor_tel = Column(String)
    organizer = Column(String)
    organizer_tel = Column(String)
    sub_event = Column(Text)
    age_limit = Column(String)
    booking_place = Column(String)
    place_info = Column(Text)
    cost_info = Column(Text)
    discount_info = Column(Text)
    parking_info = Column(Text)
    first_image = Column(String)
    first_image_small = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    data_quality_score = Column(Float, default=0.5)
    raw_data_id = Column(String, index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    last_sync_at = Column(DateTime)


class Shopping(Base):
    """
    쇼핑 정보 테이블
    사용처: weather-flick-back, weather-flick-admin-back, weather-flick-batch
    설명: 쇼핑센터, 재래시장, 면세점 등 쇼핑 정보
    """
    __tablename__ = "shopping"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    content_id = Column(String, primary_key=True, index=True)
    region_code = Column(String, nullable=False, index=True)
    shop_name = Column(String, nullable=False, index=True)
    shop_type = Column(String)  # department_store, market, duty_free, etc.
    category_code = Column(String, index=True)
    address = Column(String)
    detail_address = Column(String)
    zipcode = Column(String)
    tel = Column(String)
    homepage = Column(Text)
    opening_hours = Column(Text)
    rest_date = Column(String)
    parking_info = Column(Text)
    credit_card = Column(String)
    sale_item = Column(Text)
    sale_item_cost = Column(Text)
    fair_day = Column(String)  # 장날 (재래시장)
    shop_guide = Column(Text)
    culture_center = Column(String)
    toilet = Column(String)
    overview = Column(Text)
    first_image = Column(String)
    first_image_small = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    data_quality_score = Column(Float, default=0.5)
    raw_data_id = Column(String, index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    last_sync_at = Column(DateTime)


class LeisureSports(Base):
    """
    레저스포츠 시설 정보 테이블
    사용처: weather-flick-admin-back, weather-flick-batch
    설명: 스키장, 골프장, 수상레포츠 등 레저스포츠 시설 정보
    """
    __tablename__ = "leisure_sports"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    content_id = Column(String, primary_key=True, index=True)
    region_code = Column(String, nullable=False, index=True)
    facility_name = Column(String, nullable=False, index=True)
    sports_type = Column(String)  # ski, golf, water_sports, etc.
    category_code = Column(String, index=True)
    sub_category_code = Column(String)
    address = Column(String)
    detail_address = Column(String)
    zipcode = Column(String)
    tel = Column(String)
    homepage = Column(Text)
    reservation_info = Column(Text)
    admission_fee = Column(Text)
    use_time = Column(Text)
    rest_date = Column(String)
    parking_info = Column(Text)
    rental_info = Column(Text)
    capacity = Column(String)
    amenities = Column(Text)
    scale = Column(String)
    operating_hours = Column(Text)
    overview = Column(Text)
    first_image = Column(String)
    first_image_small = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    data_quality_score = Column(Float, default=0.5)
    processing_status = Column(String, default="pending")
    raw_data_id = Column(String, index=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    last_sync_at = Column(DateTime)

    # 추가 상세 정보 필드들
    booktour = Column(String)
    createdtime = Column(String)
    modifiedtime = Column(String)
    telname = Column(String)
    faxno = Column(String)
    mlevel = Column(Integer)
    detail_intro_info = Column(JSONB)
    detail_additional_info = Column(JSONB)
    sigungu_code = Column(String)


class PetTourInfo(Base):
    """
    반려동물 관광정보 테이블
    사용처: weather-flick-back
    설명: 반려동물 동반 가능 관광지 정보
    """
    __tablename__ = "pet_tour_info"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    content_id = Column(String, index=True)
    content_type_id = Column(String)
    title = Column(String)
    address = Column(String)
    tel = Column(String)
    homepage = Column(Text)
    overview = Column(Text)
    first_image = Column(String)
    created_time = Column(String)
    modified_time = Column(String)
    area_code = Column(String)
    sigungu_code = Column(String)
    cat1 = Column(String)
    cat2 = Column(String)
    cat3 = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    mlevel = Column(Integer)
    pet_acpt_abl = Column(String)  # 반려동물 수용 가능 여부
    pet_info = Column(Text)  # 반려동물 관련 상세 정보
    data_quality_score = Column(Float, default=0.5)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


# ===========================================
# 시스템 관리 관련 테이블
# ===========================================

class ApiRawData(Base):
    """
    API 원시 데이터 테이블
    사용처: weather-flick-batch
    설명: 외부 API에서 수집한 원시 데이터 저장
    """
    __tablename__ = "api_raw_data"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    api_provider = Column(String(50), nullable=False)  # 'tourapi', 'kma', 'google_maps' 등
    endpoint = Column(String(200), nullable=False)
    request_method = Column(String(10), default='GET')
    request_params = Column(JSONB)
    request_headers = Column(JSONB)
    response_status = Column(Integer)
    raw_response = Column(JSONB, nullable=False)
    response_size = Column(Integer)  # bytes
    request_duration = Column(Integer)  # milliseconds
    api_key_hash = Column(String(64))  # API 키의 해시값 (보안)
    created_at = Column(DateTime, server_default=func.now())
    expires_at = Column(DateTime)
    is_archived = Column(Boolean, default=False)
    file_path = Column(String(500))


class UserActivityLog(Base):
    """
    사용자 활동 로그 테이블
    사용처: weather-flick-admin-back
    설명: 관리자 대시보드용 사용자 활동 추적
    """
    __tablename__ = "user_activity_logs"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    log_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False)
    action = Column(String, nullable=False)
    resource = Column(String)
    details = Column(JSONB, default=dict)
    ip_address = Column(String)
    user_agent = Column(String)
    created_at = Column(DateTime, server_default=func.now())


class SystemLog(Base):
    """
    시스템 로그 테이블
    사용처: weather-flick-admin-back
    설명: 시스템 이벤트 및 오류 로그
    """
    __tablename__ = "system_logs"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    log_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    level = Column(String, nullable=False)  # INFO, WARNING, ERROR, CRITICAL
    source = Column(String, nullable=False)
    message = Column(Text, nullable=False)
    details = Column(JSONB, default=dict)
    created_at = Column(DateTime, server_default=func.now())


class BatchJobLog(Base):
    """
    배치 작업 로그 테이블
    사용처: weather-flick-batch
    설명: 배치 작업 실행 이력 및 결과
    """
    __tablename__ = "batch_job_logs"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    job_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_name = Column(String, nullable=False)
    job_type = Column(String, nullable=False)
    status = Column(String, nullable=False)  # running, completed, failed
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    error_message = Column(Text)
    details = Column(JSONB, default=dict)
    created_at = Column(DateTime, server_default=func.now())


class ApiKeyUsage(Base):
    """
    API 키 사용 현황 테이블
    사용처: weather-flick-batch
    설명: 외부 API 키 사용량 추적
    """
    __tablename__ = "api_key_usage"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(Integer, primary_key=True, autoincrement=True)
    api_provider = Column(String, nullable=False)  # kto, kma, google 등
    api_key_index = Column(Integer, nullable=False)
    endpoint = Column(String)
    request_count = Column(Integer, default=0)
    success_count = Column(Integer, default=0)
    error_count = Column(Integer, default=0)
    usage_date = Column(Date, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


# ===========================================
# 추천 코스 관련 테이블
# ===========================================

class RecommendCourse(Base):
    """
    추천 여행 코스 테이블
    사용처: weather-flick-back
    설명: 사전 정의된 추천 여행 코스 정보
    """
    __tablename__ = "recommend_courses"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(Integer, primary_key=True, index=True)
    course_name = Column(String, nullable=False)
    description = Column(Text)
    region = Column(String)
    duration = Column(String)  # 1박2일, 2박3일 등
    theme = Column(String)  # 가족여행, 힐링, 액티비티 등
    course_data = Column(JSONB)  # 코스 상세 정보
    image_url = Column(String)
    view_count = Column(Integer, default=0)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class RecommendReview(Base):
    """
    추천 코스 리뷰 테이블
    사용처: weather-flick-back
    설명: 추천 코스에 대한 사용자 리뷰
    """
    __tablename__ = "recommend_reviews"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    course_id = Column(Integer, ForeignKey("recommend_courses.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    rating = Column(Integer, nullable=False)  # 1-5
    content = Column(Text, nullable=False)
    parent_id = Column(UUID(as_uuid=True), ForeignKey("recommend_reviews.id", ondelete="CASCADE"))  # 답글용
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class RecommendLike(Base):
    """
    추천 코스 좋아요 테이블
    사용처: weather-flick-back
    설명: 추천 코스에 대한 사용자 좋아요
    """
    __tablename__ = "recommend_likes"
    __table_args__ = (
        UniqueConstraint('course_id', 'user_id', name='uq_recommend_likes_course_user'),
        {"extend_existing": True, "autoload_replace": False}
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    course_id = Column(Integer, ForeignKey("recommend_courses.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, server_default=func.now())


class ReviewLike(Base):
    """
    리뷰 좋아요/싫어요 테이블
    사용처: weather-flick-back
    설명: 추천 코스 리뷰에 대한 좋아요/싫어요
    """
    __tablename__ = "review_likes"
    __table_args__ = (
        UniqueConstraint('review_id', 'user_id', name='uq_review_likes_review_user'),
        {"extend_existing": True, "autoload_replace": False}
    )

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    review_id = Column(UUID(as_uuid=True), ForeignKey("recommend_reviews.id", ondelete="CASCADE"), nullable=False)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    is_like = Column(Boolean, nullable=False)  # True: 좋아요, False: 싫어요
    created_at = Column(DateTime, server_default=func.now())


# ===========================================
# FCM 토큰 관련 테이블
# ===========================================

class FCMToken(Base):
    """
    FCM 토큰 테이블
    사용처: weather-flick-back, weather-flick-batch
    설명: 푸시 알림을 위한 FCM 토큰 관리
    """
    __tablename__ = "fcm_tokens"
    __table_args__ = {"extend_existing": True, "autoload_replace": False}

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id", ondelete="CASCADE"), nullable=False)
    token = Column(String, unique=True, nullable=False)
    device_type = Column(String)  # ios, android, web
    device_id = Column(String)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


# ===========================================
# 데이터베이스 테이블에만 존재하는 테이블
# ===========================================

# 위에서 이미 정의된 테이블들:
# - api_raw_data
# - historical_weather_daily
# - weather_current
# - weather_forecast


# ===========================================
# Pydantic 스키마 정의
# ===========================================

# ===========================================
# 날씨 관련 스키마
# ===========================================

class WeatherRequest(BaseModel):
    """
    날씨 요청 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    city: str
    country: str | None = None


class WeatherCondition(BaseModel):
    """
    날씨 상태 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    temperature: float
    feels_like: float
    humidity: int
    pressure: float
    condition: str
    description: str
    icon: str
    wind_speed: float
    wind_direction: int
    visibility: float
    uv_index: float


class WeatherResponse(BaseModel):
    """
    날씨 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    city: str
    country: str
    current: WeatherCondition
    timezone: str
    local_time: str


class ForecastDay(BaseModel):
    """
    일일 예보 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    date: str
    temperature_max: float
    temperature_min: float
    condition: str
    description: str
    icon: str
    humidity: int
    wind_speed: float
    precipitation_chance: float


class ForecastResponse(BaseModel):
    """
    예보 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    city: str
    country: str
    forecast: list[ForecastDay]
    timezone: str


# ===========================================
# 인증 관련 스키마
# ===========================================

class TokenData(BaseModel):
    """
    토큰 데이터 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    email: str | None = None
    role: str | None = None


class UserCreate(BaseModel):
    """
    사용자 생성 스키마
    사용처: weather-flick-back
    """
    email: str
    password: str
    nickname: str


class UserResponse(BaseModel):
    """
    사용자 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    user_id: uuid.UUID
    email: str
    nickname: str | None = None
    profile_image: str | None = None
    preferred_region: str | None = None
    preferred_theme: str | None = None
    bio: str | None = None
    role: str
    is_active: bool
    created_at: datetime

    class Config:
        from_attributes = True


class Token(BaseModel):
    """
    인증 토큰 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    access_token: str
    token_type: str
    expires_in: int
    user_info: UserResponse


class UserUpdate(BaseModel):
    """
    사용자 정보 수정 스키마
    사용처: weather-flick-back
    """
    nickname: str | None = None
    profile_image: str | None = None
    preferences: list[str | None] = []
    preferred_region: str | None = None
    preferred_theme: str | None = None
    bio: str | None = None


class PasswordChange(BaseModel):
    """
    비밀번호 변경 스키마
    사용처: weather-flick-back
    """
    current_password: str
    new_password: str


class GoogleLoginRequest(BaseModel):
    """
    구글 로그인 요청 스키마
    사용처: weather-flick-back
    """
    code: str
    redirect_uri: str


class GoogleLoginResponse(BaseModel):
    """
    구글 로그인 응답 스키마
    사용처: weather-flick-back
    """
    access_token: str
    refresh_token: str
    token_type: str
    expires_in: int
    user_info: UserResponse
    is_new_user: bool


class GoogleAuthUrlResponse(BaseModel):
    """
    구글 인증 URL 응답 스키마
    사용처: weather-flick-back
    """
    auth_url: str
    state: str


class GoogleAuthCodeRequest(BaseModel):
    """
    구글 인증 코드 요청 스키마
    사용처: weather-flick-back
    """
    auth_code: str


class EmailVerificationRequest(BaseModel):
    """
    이메일 인증 요청 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    email: str
    nickname: str


class EmailVerificationConfirm(BaseModel):
    """
    이메일 인증 확인 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    email: str
    code: str


class EmailVerificationResponse(BaseModel):
    """
    이메일 인증 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    message: str
    success: bool


class ResendVerificationRequest(BaseModel):
    """
    이메일 재인증 요청 스키마
    사용처: weather-flick-back
    """
    email: str
    nickname: str


class ForgotPasswordRequest(BaseModel):
    """
    비밀번호 찾기 요청 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    email: str

    class Config:
        json_schema_extra = {
            "example": {
                "email": "user@example.com"
            }
        }


class ForgotPasswordResponse(BaseModel):
    """
    비밀번호 찾기 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    message: str
    success: bool = True

    class Config:
        json_schema_extra = {
            "example": {
                "message": "임시 비밀번호가 이메일로 전송되었습니다.",
                "success": True
            }
        }


class WithdrawRequest(BaseModel):
    """
    회원탈퇴 요청 스키마
    사용처: weather-flick-back
    """
    password: str | None = None  # 소셜 로그인 사용자는 비밀번호 불필요
    reason: str | None = None  # 탈퇴 사유 (선택사항)

    class Config:
        json_schema_extra = {
            "example": {
                "password": "current_password",
                "reason": "서비스 이용이 불필요해짐"
            }
        }


class WithdrawResponse(BaseModel):
    """
    회원탈퇴 응답 스키마
    사용처: weather-flick-back
    """
    message: str
    success: bool = True

    class Config:
        json_schema_extra = {
            "example": {
                "message": "회원탈퇴가 완료되었습니다.",
                "success": True
            }
        }


# ===========================================
# 공통 응답 스키마
# ===========================================

class StandardResponse(BaseModel):
    """
    표준 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    success: bool
    message: str
    data: dict[str, Any | None] = {}


class PaginationInfo(BaseModel):
    """
    페이지네이션 정보 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    page: int
    page_size: int
    total_count: int
    total_pages: int


# ===========================================
# 여행지 및 추천 관련 스키마
# ===========================================

class DestinationCreate(BaseModel):
    """
    여행지 생성 스키마
    사용처: weather-flick-admin-back
    """
    name: str
    province: str
    region: str | None = None
    category: str | None = None
    is_indoor: bool | None = False
    tags: list[str | None] = []
    latitude: float | None = None
    longitude: float | None = None
    amenities: dict[str, Any | None] = {}
    image_url: str | None = None


class DestinationResponse(BaseModel):
    """
    여행지 응답 스키마
    사용처: weather-flick-admin-back
    """
    destination_id: uuid.UUID
    name: str
    province: str
    region: str | None = None
    category: str | None = None
    is_indoor: bool | None = None
    tags: list[str | None] = []
    latitude: float | None = None
    longitude: float | None = None
    amenities: dict[str, Any | None] = {}
    image_url: str | None = None
    rating: float | None = None
    recommendation_weight: float | None = None

    class Config:
        from_attributes = True


class RecommendationRequest(BaseModel):
    """
    추천 요청 스키마
    사용처: weather-flick-back
    """
    destination_types: list[str | None] = []
    budget_range: dict[str, float | None] = {}
    travel_dates: dict[str, str | None] = {}
    preferences: dict[str, Any | None] = {}


class RecommendationResponse(BaseModel):
    """
    추천 응답 스키마
    사용처: weather-flick-back
    """
    destinations: list[dict]
    total_count: int
    recommendation_score: float


# ===========================================
# 여행 계획 관련 스키마
# ===========================================

class TravelPlanCreate(BaseModel):
    """
    여행 계획 생성 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    title: str
    description: str | None = None
    start_date: str
    end_date: str
    budget: float | None = None
    itinerary: Optional[dict[str, List[dict[str, Any]]]] = None
    participants: int | None = None
    transportation: str | None = None
    start_location: str | None = None  # 출발지
    weather_info: Optional[dict[str, Any]] = None  # 날씨 정보
    theme: str | None = None  # 테마 추가
    status: str | None = None  # 상태 추가
    plan_type: str | None = None  # 여행 계획 타입 ('manual' 또는 'custom')


class TravelPlanUpdate(BaseModel):
    """
    여행 계획 수정 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    title: str | None = None
    description: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    budget: float | None = None
    status: str | None = None
    itinerary: Optional[dict[str, List[dict[str, Any]]]] = None
    participants: int | None = None
    transportation: str | None = None
    start_location: str | None = None
    weather_info: Optional[dict[str, Any]] = None


class TravelPlanResponse(BaseModel):
    """
    여행 계획 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    plan_id: uuid.UUID
    user_id: uuid.UUID
    title: str
    description: str | None = None
    start_date: date
    end_date: date
    budget: float | None = None
    status: str
    itinerary: Optional[dict[str, List[dict[str, Any]]]] = None
    participants: int | None = None
    transportation: str | None = None
    start_location: str | None = None
    weather_info: Optional[dict[str, Any]] = None
    plan_type: str | None = None
    created_at: datetime

    @validator('budget', pre=True)
    def decimal_to_float(cls, v):
        if v is None:
            return v
        try:
            return float(v)
        except Exception:
            return None

    class Config:
        from_attributes = True


# ===========================================
# 검색 관련 스키마
# ===========================================

class SearchRequest(BaseModel):
    """
    검색 요청 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    query: str
    category: str | None = None
    location: str | None = None
    limit: int | None = 10


class SearchResult(BaseModel):
    """
    검색 결과 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    results: list[dict[str, Any]]
    total_count: int
    category: str


# ===========================================
# 시설 정보 관련 스키마
# ===========================================

class RestaurantResponse(BaseModel):
    """
    음식점 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    content_id: str
    region_code: str
    restaurant_name: str
    category_code: str | None = None
    sub_category_code: str | None = None
    address: str | None = None
    detail_address: str | None = None
    zipcode: str | None = None
    tel: str | None = None
    homepage: str | None = None
    overview: str | None = None
    first_image: str | None = None
    first_image_small: str | None = None
    cuisine_type: str | None = None
    specialty_dish: str | None = None
    operating_hours: str | None = None
    rest_date: str | None = None
    reservation_info: str | None = None
    credit_card: str | None = None
    smoking: str | None = None
    parking: str | None = None
    room_available: str | None = None
    children_friendly: str | None = None
    takeout: str | None = None
    delivery: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    data_quality_score: float | None = None
    raw_data_id: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_sync_at: datetime | None = None
    processing_status: str | None = None


class AccommodationResponse(BaseModel):
    """
    숙박시설 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    id: str
    name: str
    type: str  # hotel, motel, guesthouse, etc.
    address: str
    phone: str | None = None
    rating: float | None = None
    price_range: str | None = None
    amenities: list[str | None] = []
    latitude: float | None = None
    longitude: float | None = None


class TransportationResponse(BaseModel):
    """
    교통수단 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    id: str
    name: str
    type: str  # bus, subway, taxi, etc.
    route: str | None = None
    schedule: dict[str, Any | None] = {}
    fare: str | None = None
    contact: str | None = None


class FavoritePlaceResponse(BaseModel):
    """
    즐겨찾기 장소 응답 스키마
    사용처: weather-flick-back
    """
    id: int
    place_name: str
    place_type: str
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    description: str | None = None
    created_at: datetime

    class Config:
        from_attributes = True


class CityInfoResponse(BaseModel):
    """
    도시 정보 응답 스키마
    사용처: weather-flick-admin-back
    """
    city_name: str
    region: str
    population: int | None = None
    area: float | None = None
    description: str | None = None
    attractions: list[str | None] = []
    weather_info: dict[str, Any | None] = {}


# ===========================================
# 리뷰 관련 스키마
# ===========================================

class ReviewCreate(BaseModel):
    """
    리뷰 생성 스키마
    사용처: weather-flick-back
    """
    destination_id: uuid.UUID
    travel_plan_id: uuid.UUID | None = None
    rating: int
    content: str | None = None
    photos: list[str | None] = []


class ReviewResponse(BaseModel):
    """
    리뷰 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    review_id: uuid.UUID
    user_id: uuid.UUID
    destination_id: uuid.UUID
    travel_plan_id: uuid.UUID | None = None
    rating: int
    content: str | None = None
    photos: list[str | None] = []
    created_at: datetime

    class Config:
        from_attributes = True


class RecommendReviewCreate(BaseModel):
    """
    추천 코스 리뷰 생성 스키마
    사용처: weather-flick-back
    """
    course_id: int
    rating: int = Field(..., ge=1, le=5)
    content: str
    nickname: str
    parent_id: uuid.UUID | None = None  # 답글용


class RecommendReviewResponse(BaseModel):
    """
    추천 코스 리뷰 응답 스키마
    사용처: weather-flick-back
    """
    id: uuid.UUID
    course_id: int
    user_id: uuid.UUID
    nickname: str
    rating: int
    content: str
    created_at: datetime
    parent_id: uuid.UUID | None = None  # 답글용
    children: list[RecommendReviewResponse] = []  # 트리 구조용
    likeCount: int = 0  # 추천수 필드 추가
    dislikeCount: int = 0  # 싫어요 수 필드 추가

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


class RecommendLikeCreate(BaseModel):
    """
    추천 코스 좋아요 생성 스키마
    사용처: weather-flick-back
    """
    course_id: int


class RecommendLikeResponse(BaseModel):
    """
    추천 코스 좋아요 응답 스키마
    사용처: weather-flick-back
    """
    id: uuid.UUID
    course_id: int
    user_id: uuid.UUID
    created_at: datetime

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)


class ReviewLikeCreate(BaseModel):
    """
    리뷰 좋아요/싫어요 생성 스키마
    사용처: weather-flick-back
    """
    review_id: uuid.UUID
    is_like: bool


class ReviewLikeResponse(BaseModel):
    """
    리뷰 좋아요/싫어요 응답 스키마
    사용처: weather-flick-back
    """
    id: uuid.UUID
    review_id: uuid.UUID
    user_id: uuid.UUID
    is_like: bool
    created_at: datetime

    class Config:
        from_attributes = True


# ===========================================
# 관광지 정보 관련 스키마
# ===========================================

class TouristAttractionResponse(BaseModel):
    """
    관광지 정보 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    content_id: str
    region_code: str
    attraction_name: str
    category_code: str | None = None
    category_name: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    tel: str | None = None
    homepage: str | None = None
    description: str | None = None
    overview: str | None = None
    first_image: str | None = None
    data_quality_score: float | None = None
    created_at: datetime | None = None

    class Config:
        from_attributes = True


class CulturalFacilityResponse(BaseModel):
    """
    문화시설 정보 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    content_id: str
    region_code: str
    facility_name: str
    facility_type: str | None = None
    category_code: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    tel: str | None = None
    homepage: str | None = None
    admission_fee: str | None = None
    operating_hours: str | None = None
    parking_info: str | None = None
    overview: str | None = None
    first_image: str | None = None
    data_quality_score: float | None = None
    created_at: datetime | None = None

    class Config:
        from_attributes = True


class FestivalEventResponse(BaseModel):
    """
    축제/행사 정보 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    content_id: str
    region_code: str
    event_name: str
    category_code: str | None = None
    event_start_date: date | None = None
    event_end_date: date | None = None
    event_place: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    tel: str | None = None
    homepage: str | None = None
    event_program: str | None = None
    sponsor: str | None = None
    organizer: str | None = None
    cost_info: str | None = None
    overview: str | None = None
    first_image: str | None = None
    data_quality_score: float | None = None
    created_at: datetime | None = None

    class Config:
        from_attributes = True


class LeisureSportsResponse(BaseModel):
    """
    레저스포츠 시설 응답 스키마
    사용처: weather-flick-admin-back
    """
    content_id: str
    region_code: str
    facility_name: str
    category_code: str | None = None
    sub_category_code: str | None = None
    raw_data_id: str | None = None
    sports_type: str | None = None
    reservation_info: str | None = None
    admission_fee: str | None = None
    parking_info: str | None = None
    rental_info: str | None = None
    capacity: str | None = None
    operating_hours: str | None = None
    address: str | None = None
    detail_address: str | None = None
    zipcode: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    tel: str | None = None
    homepage: str | None = None
    overview: str | None = None
    first_image: str | None = None
    first_image_small: str | None = None
    data_quality_score: float | None = None
    processing_status: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_sync_at: datetime | None = None
    booktour: str | None = None
    createdtime: str | None = None
    modifiedtime: str | None = None
    telname: str | None = None
    faxno: str | None = None
    mlevel: int | None = None
    detail_intro_info: dict | None = None
    detail_additional_info: dict | None = None
    sigungu_code: str | None = None

    class Config:
        from_attributes = True


class ShoppingResponse(BaseModel):
    """
    쇼핑 정보 응답 스키마
    사용처: weather-flick-back, weather-flick-admin-back
    """
    content_id: str
    region_code: str
    shop_name: str
    shop_type: str | None = None
    category_code: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    tel: str | None = None
    homepage: str | None = None
    opening_hours: str | None = None
    rest_date: str | None = None
    parking_info: str | None = None
    credit_card: str | None = None
    sale_item: str | None = None
    overview: str | None = None
    first_image: str | None = None
    data_quality_score: float | None = None
    created_at: datetime | None = None

    class Config:
        from_attributes = True


class PetTourInfoResponse(BaseModel):
    """
    반려동물 관광정보 응답 스키마
    사용처: weather-flick-back
    """
    id: uuid.UUID
    content_id: str | None = None
    content_type_id: str | None = None
    title: str | None = None
    address: str | None = None
    latitude: float | None = None
    longitude: float | None = None
    area_code: str | None = None
    sigungu_code: str | None = None
    tel: str | None = None
    homepage: str | None = None
    overview: str | None = None
    first_image: str | None = None
    cat1: str | None = None
    cat2: str | None = None
    cat3: str | None = None
    pet_acpt_abl: str | None = None
    pet_info: str | None = None
    data_quality_score: float | None = None
    created_at: datetime | None = None

    class Config:
        from_attributes = True




# ===========================================
# 경로 정보 관련 스키마
# ===========================================

class TravelRouteCreate(BaseModel):
    """
    여행 경로 생성 스키마
    사용처: weather-flick-back
    """
    plan_id: uuid.UUID
    day: int
    sequence: int
    departure_name: str
    departure_lat: float | None = None
    departure_lng: float | None = None
    destination_name: str
    destination_lat: float | None = None
    destination_lng: float | None = None
    transport_type: str | None = None


class TravelRouteUpdate(BaseModel):
    """
    여행 경로 수정 스키마
    사용처: weather-flick-back
    """
    day: int | None = None
    sequence: int | None = None
    departure_name: str | None = None
    departure_lat: float | None = None
    departure_lng: float | None = None
    destination_name: str | None = None
    destination_lat: float | None = None
    destination_lng: float | None = None
    transport_type: str | None = None
    duration: int | None = None
    distance: float | None = None
    cost: float | None = None


class TravelRouteResponse(BaseModel):
    """
    여행 경로 응답 스키마
    사용처: weather-flick-back
    """
    route_id: uuid.UUID
    plan_id: uuid.UUID
    day: int
    sequence: int
    departure_name: str
    departure_lat: float | None = None
    departure_lng: float | None = None
    destination_name: str
    destination_lat: float | None = None
    destination_lng: float | None = None
    transport_type: str | None = None
    route_data: dict[str, Any] | None = None
    duration: int | None = None
    distance: float | None = None
    cost: float | None = None
    created_at: datetime

    class Config:
        from_attributes = True


class TransportationDetailCreate(BaseModel):
    """
    교통수단 상세 정보 생성 스키마
    사용처: weather-flick-back
    """
    route_id: uuid.UUID
    transport_name: str | None = None
    transport_color: str | None = None
    departure_station: str | None = None
    arrival_station: str | None = None
    departure_time: datetime | None = None
    arrival_time: datetime | None = None
    fare: float | None = None
    transfer_info: dict[str, Any] | None = None


class TransportationDetailResponse(BaseModel):
    """
    교통수단 상세 정보 응답 스키마
    사용처: weather-flick-back
    """
    detail_id: uuid.UUID
    route_id: uuid.UUID
    transport_name: str | None = None
    transport_color: str | None = None
    departure_station: str | None = None
    arrival_station: str | None = None
    departure_time: datetime | None = None
    arrival_time: datetime | None = None
    fare: float | None = None
    transfer_info: dict[str, Any] | None = None
    created_at: datetime

    class Config:
        from_attributes = True


class RouteCalculationRequest(BaseModel):
    """
    경로 계산 요청 스키마
    사용처: weather-flick-back
    """
    departure_lat: float
    departure_lng: float
    destination_lat: float
    destination_lng: float
    transport_type: str = "walk"  # walk, car, transit


class RouteCalculationResponse(BaseModel):
    """
    경로 계산 응답 스키마
    사용처: weather-flick-back
    """
    success: bool
    duration: int | None = None  # 분
    distance: float | None = None  # km
    cost: float | None = None  # 원
    route_data: dict[str, Any] | None = None
    transport_type: str
    message: str | None = None


# ===========================================
# 맞춤 일정 추천 관련 스키마
# ===========================================

class CustomTravelRecommendationRequest(BaseModel):
    """
    맞춤 일정 추천 요청 스키마
    사용처: weather-flick-back
    """
    region_code: str = Field(..., description="지역 코드")
    region_name: str = Field(..., description="지역 이름")
    period: str = Field(..., description="여행 기간 (당일치기, 1박2일 등)")
    days: int = Field(..., ge=1, le=7, description="여행 일수")
    who: str = Field(..., description="동행자 유형 (solo, couple, family, friends, colleagues, group)")
    styles: List[str] = Field(..., description="여행 스타일 (activity, hotplace, nature, landmark, healing, culture, local, shopping, food, pet)")
    schedule: str = Field(..., description="일정 유형 (packed, relaxed)")


class PlaceRecommendation(BaseModel):
    """
    추천 장소 스키마
    사용처: weather-flick-back
    """
    id: str = Field(..., description="장소 ID")
    name: str = Field(..., description="장소 이름")
    time: str = Field(..., description="방문 시간 (예: 09:00-11:00)")
    tags: List[str] = Field(..., description="장소 태그")
    description: str = Field(..., description="장소 설명")
    rating: float = Field(..., ge=0, le=5, description="평점")
    image: Optional[str] = Field(None, description="이미지 URL")
    address: Optional[str] = Field(None, description="주소")
    latitude: Optional[float] = Field(None, description="위도")
    longitude: Optional[float] = Field(None, description="경도")


class DayItinerary(BaseModel):
    """
    일별 여행 일정 스키마
    사용처: weather-flick-back
    """
    day: int = Field(..., description="일차")
    date: Optional[str] = Field(None, description="날짜")
    places: List[PlaceRecommendation] = Field(..., description="추천 장소 목록")
    weather: Optional[dict] = Field(None, description="날씨 정보")


class CustomTravelRecommendationResponse(BaseModel):
    """
    맞춤 일정 추천 응답 스키마
    사용처: weather-flick-back
    """
    days: List[DayItinerary] = Field(..., description="일별 여행 일정")
    weather_summary: Optional[dict] = Field(None, description="전체 날씨 요약")
    total_places: int = Field(..., description="총 추천 장소 수")
    recommendation_type: str = Field(..., description="추천 유형")
    created_at: datetime = Field(default_factory=lambda: datetime.now())


# ===========================================
# 데이터베이스에만 존재하는 누락된 테이블 모델들
# ===========================================


class Accommodation(Base):
    """
    숙박시설 정보 테이블
    사용처: weather-flick-batch
    설명: 한국관광공사 API 기반 숙박시설 정보 수집용
    """

    __tablename__ = "accommodations"

    # Primary Key
    content_id = Column(String(20), primary_key=True, index=True)

    # Foreign Keys
    region_code = Column(String, ForeignKey("regions.region_code"), nullable=False, index=True)
    raw_data_id = Column(UUID(as_uuid=True), index=True)

    # 기본 정보
    accommodation_name = Column(String, nullable=False)
    accommodation_type = Column(String, nullable=False)
    address = Column(String, nullable=False)
    tel = Column(String)

    # 위치 정보
    latitude = Column(Float)
    longitude = Column(Float)

    # 카테고리 정보
    category_code = Column(String(10))
    sub_category_code = Column(String(10))

    # 시설 정보
    parking = Column(String)

    # 메타데이터
    created_at = Column(DateTime, server_default=func.now())


class Transportation(Base):
    """
    교통수단 정보 테이블
    사용처: weather-flick-batch
    설명: 대중교통 및 교통수단 정보 수집
    """

    __tablename__ = "transportation"

    transport_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    region_code = Column(String, ForeignKey("regions.region_code"), nullable=False, index=True)

    # 교통수단 정보
    transport_type = Column(String, nullable=False)  # bus, subway, train 등
    transport_name = Column(String, nullable=False)
    route_number = Column(String)

    # 운행 정보
    operating_hours = Column(String)
    frequency = Column(String)  # 운행 간격
    fare = Column(String)

    # 노선 정보
    start_point = Column(String)
    end_point = Column(String)
    route_description = Column(Text)

    created_at = Column(DateTime, server_default=func.now())


class CategoryCode(Base):
    """
    카테고리 코드 테이블
    사용처: weather-flick-batch
    설명: 한국관광공사 카테고리 코드 수집 및 관리
    """

    __tablename__ = "category_codes"

    category_id = Column(Integer, primary_key=True, index=True)
    category_code = Column(String(10), unique=True, nullable=False)
    category_name = Column(String, nullable=False)
    parent_category_code = Column(String(10))
    level = Column(Integer, default=1)  # 카테고리 레벨
    description = Column(Text)
    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class DataCollectionLog(Base):
    """
    데이터 수집 로그 테이블
    사용처: weather-flick-batch
    설명: 외부 API 데이터 수집 상세 로그
    """

    __tablename__ = "data_collection_logs"

    log_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    collection_type = Column(String, nullable=False)  # tourist_attraction, weather 등

    # 수집 정보
    api_endpoint = Column(String, nullable=False)
    request_params = Column(JSONB)
    response_status = Column(Integer)
    response_time = Column(Float)  # 응답 시간 (초)

    # 결과
    records_collected = Column(Integer, default=0)
    records_processed = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    # 에러 정보
    error_message = Column(Text)
    error_details = Column(JSONB)

    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    created_at = Column(DateTime, server_default=func.now())

    # 인덱스
    __table_args__ = (
        Index("idx_collection_log_type_started", "collection_type", "started_at"),
    )


class ApiKey(Base):
    """
    API 키 관리 테이블
    사용처: weather-flick-batch
    설명: 외부 API 키 로테이션 및 사용량 관리
    """

    __tablename__ = "api_keys"

    key_id = Column(Integer, primary_key=True, index=True)
    service_name = Column(String, nullable=False)  # tour_api, weather_api 등
    api_key = Column(String, nullable=False)
    key_alias = Column(String)  # 키 별칭

    # 사용량 제한
    daily_limit = Column(Integer)
    monthly_limit = Column(Integer)

    # 현재 사용량
    daily_usage = Column(Integer, default=0)
    monthly_usage = Column(Integer, default=0)
    last_used_at = Column(DateTime)
    usage_reset_at = Column(DateTime)

    # 상태
    is_active = Column(Boolean, default=True)
    error_count = Column(Integer, default=0)
    last_error_at = Column(DateTime)
    last_error_message = Column(Text)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # 유니크 제약조건
    __table_args__ = (
        UniqueConstraint("service_name", "api_key", name="uq_service_api_key"),
        Index("idx_api_key_service", "service_name", "is_active"),
    )


class BatchJob(Base):
    """
    배치 작업 정의 테이블
    사용처: weather-flick-batch
    설명: 정기적으로 실행되는 배치 작업 정의 및 스케줄링
    """

    __tablename__ = "batch_jobs"

    job_id = Column(Integer, primary_key=True, index=True)
    job_name = Column(String, unique=True, nullable=False)
    job_type = Column(String, nullable=False)  # data_collection, data_processing 등
    description = Column(Text)

    # 실행 설정
    is_active = Column(Boolean, default=True)
    schedule_cron = Column(String)  # 크론 표현식
    timeout_minutes = Column(Integer, default=60)
    retry_count = Column(Integer, default=3)

    # 마지막 실행 정보
    last_run_at = Column(DateTime)
    last_success_at = Column(DateTime)
    last_failure_at = Column(DateTime)
    last_error_message = Column(Text)

    # 실행 통계
    total_runs = Column(Integer, default=0)
    successful_runs = Column(Integer, default=0)
    failed_runs = Column(Integer, default=0)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class BatchJobSchedule(Base):
    """
    배치 작업 스케줄 테이블
    사용처: weather-flick-batch
    설명: 배치 작업의 실행 스케줄 관리
    """

    __tablename__ = "batch_job_schedules"

    schedule_id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("batch_jobs.job_id"), nullable=False)

    # 스케줄 정보
    scheduled_time = Column(DateTime, nullable=False, index=True)
    priority = Column(Integer, default=5)  # 1-10, 높을수록 우선순위 높음

    # 실행 상태
    status = Column(String, default="pending")  # pending, running, completed, failed, cancelled
    started_at = Column(DateTime)
    completed_at = Column(DateTime)

    # 실행 결과
    result_summary = Column(JSONB)
    error_message = Column(Text)

    created_at = Column(DateTime, server_default=func.now())

    # 인덱스
    __table_args__ = (
        Index("idx_schedule_status_time", "status", "scheduled_time"),
        Index("idx_schedule_job", "job_id"),
    )


class DataSyncStatus(Base):
    """
    데이터 동기화 상태 테이블
    사용처: weather-flick-batch
    설명: 외부 데이터 소스와의 동기화 상태 추적
    """

    __tablename__ = "data_sync_status"

    sync_id = Column(Integer, primary_key=True, index=True)
    data_source = Column(String, nullable=False)  # tour_api, weather_api 등
    sync_type = Column(String, nullable=False)  # full, incremental

    # 동기화 범위
    sync_target = Column(String)  # regions, attractions 등
    sync_filter = Column(JSONB)  # 동기화 필터 조건

    # 진행 상태
    status = Column(String, default="pending")  # pending, running, completed, failed
    progress_percent = Column(Float, default=0)
    current_page = Column(Integer)
    total_pages = Column(Integer)

    # 결과
    records_fetched = Column(Integer, default=0)
    records_created = Column(Integer, default=0)
    records_updated = Column(Integer, default=0)
    records_deleted = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    # 시간 정보
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    next_sync_at = Column(DateTime)

    # 에러 정보
    error_message = Column(Text)
    error_details = Column(JSONB)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    # 유니크 제약조건
    __table_args__ = (
        UniqueConstraint("data_source", "sync_type", "sync_target", name="uq_sync_source_type_target"),
    )


class ErrorLog(Base):
    """
    에러 로그 테이블
    사용처: weather-flick-batch
    설명: 배치 시스템 전체 에러 로그
    """

    __tablename__ = "error_logs"

    error_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    service_name = Column(String, nullable=False)  # weather-flick-batch
    error_type = Column(String, nullable=False)  # database, api, validation 등
    error_level = Column(String, nullable=False)  # error, warning, critical

    # 에러 정보
    error_message = Column(Text, nullable=False)
    error_trace = Column(Text)  # 스택 트레이스
    error_data = Column(JSONB)  # 추가 컨텍스트 데이터

    # 배치 작업 정보
    job_id = Column(Integer)
    batch_operation = Column(String)

    created_at = Column(DateTime, server_default=func.now())

    # 인덱스
    __table_args__ = (
        Index("idx_error_service_created", "service_name", "created_at"),
        Index("idx_error_type_level", "error_type", "error_level"),
    )


class EventLog(Base):
    """
    이벤트 로그 테이블
    사용처: weather-flick-batch
    설명: 중요 배치 시스템 이벤트 로그
    """

    __tablename__ = "event_logs"

    event_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    service_name = Column(String, nullable=False)
    event_type = Column(String, nullable=False)  # batch_start, batch_complete, data_sync 등
    event_name = Column(String, nullable=False)  # 구체적인 이벤트명

    # 이벤트 정보
    event_data = Column(JSONB)

    # 배치 작업 정보
    job_id = Column(Integer)

    created_at = Column(DateTime, server_default=func.now())

    # 인덱스
    __table_args__ = (
        Index("idx_event_service_type", "service_name", "event_type"),
        Index("idx_event_created", "created_at"),
    )


class DataTransformationLog(Base):
    """
    데이터 변환 로그 테이블
    사용처: weather-flick-batch
    설명: 데이터 변환 작업 상세 로그
    """

    __tablename__ = "data_transformation_logs"

    log_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    transformation_type = Column(String, nullable=False)  # api_to_db, data_cleaning 등
    source_type = Column(String, nullable=False)
    target_type = Column(String, nullable=False)

    # 변환 정보
    records_input = Column(Integer, default=0)
    records_output = Column(Integer, default=0)
    records_failed = Column(Integer, default=0)

    # 실행 정보
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    status = Column(String, default="running")  # running, completed, failed

    # 에러 정보
    error_message = Column(Text)
    error_details = Column(JSONB)

    created_at = Column(DateTime, server_default=func.now())


class TravelCourse(Base):
    """
    여행 코스 테이블
    사용처: weather-flick-batch
    설명: 한국관광공사 여행 코스 정보 수집
    """

    __tablename__ = "travel_courses"

    content_id = Column(String(20), primary_key=True, index=True)
    region_code = Column(String, ForeignKey("regions.region_code"), nullable=False, index=True)

    # 기본 정보
    course_name = Column(String, nullable=False)
    course_theme = Column(String)
    course_distance = Column(Float)  # 코스 총 거리 (km)
    estimated_duration = Column(Integer)  # 예상 소요 시간 (분)

    # 코스 설명
    overview = Column(Text)
    course_intro = Column(Text)
    course_detail = Column(Text)

    # 난이도 및 추천 정보
    difficulty_level = Column(String)  # easy, medium, hard
    recommended_season = Column(String)
    recommended_who = Column(String)  # family, couple, solo 등

    # 위치 정보
    start_latitude = Column(Float)
    start_longitude = Column(Float)
    end_latitude = Column(Float)
    end_longitude = Column(Float)

    # 카테고리 정보
    category_code = Column(String(10))
    sub_category_code = Column(String(10))

    # 추가 정보
    tags = Column(JSONB)
    images = Column(JSONB)

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class TravelCourseSpot(Base):
    """
    여행 코스 구성 지점 테이블
    사용처: weather-flick-batch
    설명: 여행 코스를 구성하는 개별 지점 정보 수집
    """

    __tablename__ = "travel_course_spots"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign Keys
    course_id = Column(String(20), ForeignKey("travel_courses.content_id"), nullable=False, index=True)
    spot_content_id = Column(String(20), index=True)  # 관광지/시설의 content_id

    # 순서 및 정보
    sequence = Column(Integer, nullable=False)  # 코스 내 순서
    spot_name = Column(String, nullable=False)
    spot_type = Column(String)  # 관광지, 식당, 숙박 등

    # 시간 정보
    recommended_duration = Column(Integer)  # 추천 체류 시간 (분)
    arrival_time = Column(String)  # 도착 시간
    departure_time = Column(String)  # 출발 시간

    # 교통 정보
    distance_from_previous = Column(Float)  # 이전 지점으로부터의 거리 (km)
    transport_to_next = Column(String)  # 다음 지점까지의 교통수단

    # 추가 정보
    description = Column(Text)
    tips = Column(Text)  # 팁이나 주의사항

    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class TravelCourseLike(Base):
    """
    여행 코스 좋아요 테이블
    사용처: weather-flick-batch
    설명: 사용자가 좋아요한 여행 코스 수집 데이터
    """

    __tablename__ = "travel_course_likes"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(UUID(as_uuid=True), ForeignKey("users.user_id"), nullable=False, index=True)
    title = Column(String(255), nullable=False)
    subtitle = Column(String(255))
    summary = Column(Text)
    description = Column(Text)
    region = Column(String(50))
    itinerary = Column(JSONB)