"""
비동기 데이터베이스 세션 관리
"""
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from contextlib import asynccontextmanager
from typing import AsyncGenerator
import logging

from config.settings import get_database_config

logger = logging.getLogger(__name__)

# Base는 models.py에서 가져옴
from app.models import Base

class AsyncDatabaseManager:
    """비동기 데이터베이스 세션 관리자"""
    
    def __init__(self):
        self.config = get_database_config()
        # PostgreSQL 연결 URL 생성
        db_url = f"postgresql+asyncpg://{self.config.user}:{self.config.password}@{self.config.host}:{self.config.port}/{self.config.database}"
        
        self.engine = create_async_engine(
            db_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True
        )
        
        self.async_session_factory = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )
        
        logger.info("비동기 데이터베이스 매니저 초기화 완료")
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """비동기 데이터베이스 세션 생성"""
        async with self.async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
    
    async def close(self):
        """엔진 종료"""
        try:
            # 모든 활성 연결 종료 대기
            await self.engine.dispose()
            logger.info("비동기 데이터베이스 엔진이 정상적으로 종료되었습니다")
        except Exception as e:
            logger.error(f"비동기 데이터베이스 엔진 종료 중 오류: {e}")
            # RuntimeError는 이미 닫힌 이벤트 루프 관련 오류일 가능성이 높으므로 무시
            if "Event loop is closed" not in str(e):
                raise


# 전역 인스턴스
_async_db_manager = None

def get_async_db_manager() -> AsyncDatabaseManager:
    """비동기 데이터베이스 매니저 인스턴스 반환"""
    global _async_db_manager
    if _async_db_manager is None:
        _async_db_manager = AsyncDatabaseManager()
    return _async_db_manager

def reset_async_db_manager():
    """비동기 데이터베이스 매니저 인스턴스 초기화"""
    global _async_db_manager
    _async_db_manager = None