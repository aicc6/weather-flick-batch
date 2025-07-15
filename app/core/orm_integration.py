"""
SQLAlchemy ORM 통합 모듈

기존 Raw SQL 시스템과 SQLAlchemy ORM을 통합하여 점진적 마이그레이션을 지원합니다.
weather-flick-back의 모델 구조를 재사용하면서 배치 시스템에 최적화된 기능을 제공합니다.
"""

import sys
import os
import logging
from typing import Optional, Dict, Any, List, Type
from datetime import datetime
from contextlib import asynccontextmanager, contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy.pool import QueuePool

logger = logging.getLogger(__name__)

# weather-flick-back 모델들을 import 할 수 있도록 경로 추가
current_file = os.path.abspath(__file__)
current_dir = os.path.dirname(current_file)  # /path/to/weather-flick-batch/app/core
app_dir = os.path.dirname(current_dir)      # /path/to/weather-flick-batch/app
batch_root = os.path.dirname(app_dir)       # /path/to/weather-flick-batch
aicc6_root = os.path.dirname(batch_root)    # /path/to/aicc6
back_project_path = os.path.join(aicc6_root, "weather-flick-back")

logger.info(f"현재 파일: {current_file}")
logger.info(f"배치 루트: {batch_root}")
logger.info(f"aicc6 루트: {aicc6_root}")
logger.info(f"백엔드 프로젝트 경로: {back_project_path}")
logger.info(f"백엔드 프로젝트 존재: {os.path.exists(back_project_path)}")

if os.path.exists(back_project_path):
    # 기존 app 모듈이 sys.modules에 있다면 제거 (이름 충돌 방지)
    if 'app' in sys.modules:
        del sys.modules['app']
    if 'app.models' in sys.modules:
        del sys.modules['app.models']
    
    sys.path.insert(0, back_project_path)
    logger.info("백엔드 프로젝트 경로가 sys.path에 추가되었습니다")

# Base 클래스 정의 (weather-flick-back과 호환)
Base = declarative_base()

try:
    # weather-flick-back의 모델들 import 시도
    from app.models import (
        TouristAttraction, CulturalFacility, FestivalEvent,
        Restaurant, Accommodation, Shopping, PetTourInfo,
        WeatherData, Region,
        User, Admin, Role
    )
    MODELS_AVAILABLE = True
    logger.info("weather-flick-back 모델들을 성공적으로 import했습니다")
except ImportError as e:
    logger.warning(f"weather-flick-back 모델 import 실패: {e}")
    MODELS_AVAILABLE = False
    # 기본 모델들 정의 (임시)
    TouristAttraction = None
    CulturalFacility = None
    FestivalEvent = None
    Restaurant = None
    Accommodation = None
    Shopping = None
    PetTourInfo = None
    WeatherData = None
    Region = None


class ORMDatabaseManager:
    """SQLAlchemy ORM 기반 데이터베이스 매니저"""
    
    def __init__(self, database_url: str = None):
        """
        ORM 데이터베이스 매니저 초기화
        
        Args:
            database_url: 데이터베이스 연결 URL
        """
        self.database_url = database_url or self._get_default_database_url()
        
        # 동기 엔진 설정
        self.sync_engine = create_engine(
            self.database_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False  # 개발 시에는 True로 설정
        )
        
        # 비동기 엔진 설정
        async_url = self.database_url.replace("postgresql://", "postgresql+asyncpg://")
        self.async_engine = create_async_engine(
            async_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            echo=False
        )
        
        # 세션 팩토리
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.sync_engine
        )
        
        self.AsyncSessionLocal = async_sessionmaker(
            bind=self.async_engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        logger.info("ORM 데이터베이스 매니저 초기화 완료")
    
    def _get_default_database_url(self) -> str:
        """기본 데이터베이스 URL 생성"""
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_user = os.getenv("DB_USER", "postgres")
        db_password = os.getenv("DB_PASSWORD", "")
        db_name = os.getenv("DB_NAME", "weather_flick")
        
        return f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    @contextmanager
    def get_session(self):
        """동기 세션 컨텍스트 매니저"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    @asynccontextmanager
    async def get_async_session(self):
        """비동기 세션 컨텍스트 매니저"""
        async with self.AsyncSessionLocal() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
    
    def execute_raw_sql(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Raw SQL 실행 (기존 시스템과 호환성 유지)
        
        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터
        
        Returns:
            쿼리 결과 리스트
        """
        with self.get_session() as session:
            result = session.execute(text(query), params or {})
            if result.returns_rows:
                return [dict(row._mapping) for row in result.fetchall()]
            return []
    
    async def execute_raw_sql_async(self, query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        비동기 Raw SQL 실행
        
        Args:
            query: 실행할 SQL 쿼리
            params: 쿼리 파라미터
        
        Returns:
            쿼리 결과 리스트
        """
        async with self.get_async_session() as session:
            result = await session.execute(text(query), params or {})
            if result.returns_rows:
                return [dict(row._mapping) for row in result.fetchall()]
            return []


class TourismDataManager:
    """관광지 데이터 ORM 관리자"""
    
    def __init__(self, orm_manager: ORMDatabaseManager):
        self.orm_manager = orm_manager
        self.logger = logging.getLogger(__name__)
    
    def create_tourist_attraction(self, data: Dict[str, Any]) -> Optional[Any]:
        """
        관광지 데이터 생성 (ORM 사용)
        
        Args:
            data: 관광지 데이터 딕셔너리
        
        Returns:
            생성된 관광지 객체 또는 None
        """
        if not MODELS_AVAILABLE or not TouristAttraction:
            self.logger.warning("TouristAttraction 모델을 사용할 수 없습니다")
            return None
        
        try:
            with self.orm_manager.get_session() as session:
                attraction = TouristAttraction(**data)
                session.add(attraction)
                session.flush()  # ID 생성을 위해
                return attraction
        except Exception as e:
            self.logger.error(f"관광지 생성 실패: {e}")
            return None
    
    def upsert_tourist_attraction(self, data: Dict[str, Any]) -> bool:
        """
        관광지 데이터 UPSERT (하이브리드 방식)
        
        ORM으로 조회하고, 성능이 중요한 부분은 Raw SQL 사용
        """
        if not MODELS_AVAILABLE or not TouristAttraction:
            # Raw SQL 폴백
            return self._upsert_tourist_attraction_raw(data)
        
        try:
            with self.orm_manager.get_session() as session:
                # ORM으로 기존 데이터 조회
                existing = session.query(TouristAttraction).filter_by(
                    content_id=data.get('content_id')
                ).first()
                
                if existing:
                    # 업데이트
                    for key, value in data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    existing.updated_at = datetime.now()
                else:
                    # 생성
                    attraction = TouristAttraction(**data)
                    session.add(attraction)
                
                return True
                
        except Exception as e:
            self.logger.error(f"관광지 UPSERT 실패: {e}")
            return self._upsert_tourist_attraction_raw(data)
    
    def _upsert_tourist_attraction_raw(self, data: Dict[str, Any]) -> bool:
        """Raw SQL을 사용한 관광지 UPSERT (폴백)"""
        try:
            # 기존 database_manager의 로직 재사용
            from app.core.database_manager_extension import get_extended_database_manager
            
            db_manager = get_extended_database_manager()
            return db_manager.upsert_tourist_attraction(data)
            
        except Exception as e:
            self.logger.error(f"Raw SQL 관광지 UPSERT 실패: {e}")
            return False
    
    def get_tourist_attractions_by_region(self, region_code: str, limit: int = 100) -> List[Any]:
        """
        지역별 관광지 조회 (ORM 사용)
        
        Args:
            region_code: 지역 코드
            limit: 조회 제한 수
        
        Returns:
            관광지 목록
        """
        if not MODELS_AVAILABLE or not TouristAttraction:
            return []
        
        try:
            with self.orm_manager.get_session() as session:
                attractions = session.query(TouristAttraction).filter_by(
                    region_code=region_code
                ).limit(limit).all()
                
                return attractions
                
        except Exception as e:
            self.logger.error(f"지역별 관광지 조회 실패: {e}")
            return []
    
    def get_attractions_statistics(self) -> Dict[str, Any]:
        """
        관광지 통계 조회 (하이브리드 방식)
        
        복잡한 집계는 Raw SQL 사용
        """
        try:
            query = """
            SELECT 
                region_code,
                COUNT(*) as total_count,
                COUNT(CASE WHEN created_at >= CURRENT_DATE - INTERVAL '30 days' THEN 1 END) as recent_count,
                AVG(CASE WHEN rating IS NOT NULL THEN rating END) as avg_rating
            FROM tourist_attractions 
            GROUP BY region_code
            ORDER BY total_count DESC
            """
            
            return self.orm_manager.execute_raw_sql(query)
            
        except Exception as e:
            self.logger.error(f"관광지 통계 조회 실패: {e}")
            return {}


class HybridQueryBuilder:
    """하이브리드 쿼리 빌더 (ORM + Raw SQL)"""
    
    def __init__(self, orm_manager: ORMDatabaseManager):
        self.orm_manager = orm_manager
        self.logger = logging.getLogger(__name__)
    
    def build_tourism_search_query(self, filters: Dict[str, Any]) -> str:
        """
        관광지 검색 쿼리 동적 생성
        
        Args:
            filters: 검색 필터 딕셔너리
        
        Returns:
            생성된 SQL 쿼리
        """
        base_query = """
        SELECT 
            ta.*,
            r.region_name,
            CASE WHEN ta.rating IS NOT NULL THEN ta.rating ELSE 0 END as display_rating
        FROM tourist_attractions ta
        LEFT JOIN regions r ON ta.region_code = r.region_code
        WHERE 1=1
        """
        
        conditions = []
        
        if filters.get('region_code'):
            conditions.append("ta.region_code = %(region_code)s")
        
        if filters.get('category_code'):
            conditions.append("ta.category_code = %(category_code)s")
        
        if filters.get('keyword'):
            conditions.append("""
                (ta.attraction_name ILIKE %(keyword)s 
                 OR ta.description ILIKE %(keyword)s)
            """)
        
        if filters.get('min_rating'):
            conditions.append("ta.rating >= %(min_rating)s")
        
        if conditions:
            base_query += " AND " + " AND ".join(conditions)
        
        # 정렬
        order_by = filters.get('order_by', 'ta.created_at DESC')
        base_query += f" ORDER BY {order_by}"
        
        # 제한
        if filters.get('limit'):
            base_query += " LIMIT %(limit)s"
        
        if filters.get('offset'):
            base_query += " OFFSET %(offset)s"
        
        return base_query
    
    def execute_tourism_search(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """관광지 검색 실행"""
        try:
            query = self.build_tourism_search_query(filters)
            return self.orm_manager.execute_raw_sql(query, filters)
        except Exception as e:
            self.logger.error(f"관광지 검색 실행 실패: {e}")
            return []


# 싱글톤 인스턴스들
_orm_manager: Optional[ORMDatabaseManager] = None
_tourism_manager: Optional[TourismDataManager] = None
_query_builder: Optional[HybridQueryBuilder] = None


def get_orm_manager() -> ORMDatabaseManager:
    """ORM 매니저 싱글톤 인스턴스 반환"""
    global _orm_manager
    if _orm_manager is None:
        _orm_manager = ORMDatabaseManager()
    return _orm_manager


def get_tourism_manager() -> TourismDataManager:
    """관광지 데이터 매니저 싱글톤 인스턴스 반환"""
    global _tourism_manager
    if _tourism_manager is None:
        _tourism_manager = TourismDataManager(get_orm_manager())
    return _tourism_manager


def get_query_builder() -> HybridQueryBuilder:
    """하이브리드 쿼리 빌더 싱글톤 인스턴스 반환"""
    global _query_builder
    if _query_builder is None:
        _query_builder = HybridQueryBuilder(get_orm_manager())
    return _query_builder


def reset_orm_instances():
    """ORM 인스턴스들 재설정 (테스트용)"""
    global _orm_manager, _tourism_manager, _query_builder
    _orm_manager = None
    _tourism_manager = None
    _query_builder = None


# 편의 함수들
def execute_orm_query(model_class: Type, filters: Dict[str, Any] = None, limit: int = None) -> List[Any]:
    """
    간단한 ORM 쿼리 실행
    
    Args:
        model_class: SQLAlchemy 모델 클래스
        filters: 필터 조건
        limit: 결과 제한 수
    
    Returns:
        쿼리 결과 리스트
    """
    if not MODELS_AVAILABLE:
        return []
    
    try:
        orm_manager = get_orm_manager()
        with orm_manager.get_session() as session:
            query = session.query(model_class)
            
            if filters:
                query = query.filter_by(**filters)
            
            if limit:
                query = query.limit(limit)
            
            return query.all()
            
    except Exception as e:
        logger.error(f"ORM 쿼리 실행 실패: {e}")
        return []


def is_orm_available() -> bool:
    """ORM 사용 가능 여부 확인"""
    return MODELS_AVAILABLE