"""
데이터베이스 커넥션 풀 관리자

성능 향상을 위한 커넥션 풀링 구현
"""

import psycopg2
import psycopg2.pool
import asyncpg
from typing import Optional, Dict, Any
from contextlib import contextmanager, asynccontextmanager
import logging
from dataclasses import dataclass
import threading
import time
from queue import Queue, Empty

from config.settings import get_database_config


@dataclass
class PoolConfig:
    """커넥션 풀 설정"""
    
    # 동기 풀 설정
    sync_min_connections: int = 2
    sync_max_connections: int = 10
    
    # 비동기 풀 설정  
    async_min_connections: int = 2
    async_max_connections: int = 15
    
    # 성능 설정
    connection_timeout: int = 30
    idle_timeout: int = 300
    max_retries: int = 3
    health_check_interval: int = 60


class DatabaseConnectionPool:
    """데이터베이스 커넥션 풀 관리자"""
    
    def __init__(self, config: PoolConfig = None):
        self.config = config or PoolConfig()
        self.db_config = get_database_config()
        self.logger = logging.getLogger(__name__)
        
        # 커넥션 풀
        self._sync_pool: Optional[psycopg2.pool.ThreadedConnectionPool] = None
        self._async_pool: Optional[asyncpg.pool.Pool] = None
        
        # 통계
        self.stats = {
            'sync_pool': {
                'total_connections': 0,
                'active_connections': 0,
                'pool_hits': 0,
                'pool_misses': 0,
                'connection_errors': 0
            },
            'async_pool': {
                'total_connections': 0,
                'active_connections': 0,
                'pool_hits': 0,
                'pool_misses': 0,
                'connection_errors': 0
            }
        }
        
        # 헬스 체크
        self._health_check_thread = None
        self._shutdown_event = threading.Event()
        
    def initialize_sync_pool(self):
        """동기 커넥션 풀 초기화"""
        
        if self._sync_pool is not None:
            return
            
        try:
            self._sync_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.config.sync_min_connections,
                maxconn=self.config.sync_max_connections,
                host=self.db_config.host,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                port=self.db_config.port,
                connect_timeout=self.config.connection_timeout
            )
            
            self.stats['sync_pool']['total_connections'] = self.config.sync_min_connections
            self.logger.info(f"동기 커넥션 풀 초기화 완료: {self.config.sync_min_connections}-{self.config.sync_max_connections}")
            
            # 헬스 체크 시작
            self._start_health_check()
            
        except Exception as e:
            self.logger.error(f"동기 커넥션 풀 초기화 실패: {e}")
            raise
    
    async def initialize_async_pool(self):
        """비동기 커넥션 풀 초기화"""
        
        if self._async_pool is not None:
            return
            
        try:
            self._async_pool = await asyncpg.create_pool(
                host=self.db_config.host,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.database,
                port=self.db_config.port,
                min_size=self.config.async_min_connections,
                max_size=self.config.async_max_connections,
                timeout=self.config.connection_timeout
            )
            
            self.stats['async_pool']['total_connections'] = self.config.async_min_connections
            self.logger.info(f"비동기 커넥션 풀 초기화 완료: {self.config.async_min_connections}-{self.config.async_max_connections}")
            
        except Exception as e:
            self.logger.error(f"비동기 커넥션 풀 초기화 실패: {e}")
            raise
    
    @contextmanager
    def get_sync_connection(self):
        """동기 커넥션 획득"""
        
        if self._sync_pool is None:
            self.initialize_sync_pool()
        
        connection = None
        start_time = time.time()
        
        try:
            # 풀에서 커넥션 획득
            connection = self._sync_pool.getconn()
            
            if connection is None:
                self.stats['sync_pool']['pool_misses'] += 1
                raise Exception("커넥션 풀에서 연결을 가져올 수 없습니다")
            
            self.stats['sync_pool']['pool_hits'] += 1
            self.stats['sync_pool']['active_connections'] += 1
            
            # 커넥션 상태 확인
            if connection.closed:
                self._sync_pool.putconn(connection)
                connection = self._sync_pool.getconn()
            
            connection.autocommit = False
            yield connection
            connection.commit()
            
        except Exception as e:
            if connection:
                connection.rollback()
            self.stats['sync_pool']['connection_errors'] += 1
            self.logger.error(f"동기 커넥션 오류: {e}")
            raise
            
        finally:
            duration = time.time() - start_time
            self.stats['sync_pool']['active_connections'] -= 1
            
            if connection:
                try:
                    self._sync_pool.putconn(connection)
                except Exception as e:
                    self.logger.warning(f"커넥션 반환 오류: {e}")
            
            self.logger.debug(f"동기 커넥션 사용 완료: {duration:.3f}초")
    
    @asynccontextmanager
    async def get_async_connection(self):
        """비동기 커넥션 획득"""
        
        if self._async_pool is None:
            await self.initialize_async_pool()
        
        start_time = time.time()
        
        try:
            async with self._async_pool.acquire() as connection:
                self.stats['async_pool']['pool_hits'] += 1
                self.stats['async_pool']['active_connections'] += 1
                
                yield connection
                
        except Exception as e:
            self.stats['async_pool']['connection_errors'] += 1
            self.logger.error(f"비동기 커넥션 오류: {e}")
            raise
            
        finally:
            duration = time.time() - start_time
            self.stats['async_pool']['active_connections'] -= 1
            self.logger.debug(f"비동기 커넥션 사용 완료: {duration:.3f}초")
    
    def _start_health_check(self):
        """헬스 체크 스레드 시작"""
        
        if self._health_check_thread is not None:
            return
            
        def health_check_worker():
            """헬스 체크 작업"""
            while not self._shutdown_event.wait(self.config.health_check_interval):
                try:
                    self._perform_health_check()
                except Exception as e:
                    self.logger.error(f"헬스 체크 오류: {e}")
        
        self._health_check_thread = threading.Thread(
            target=health_check_worker,
            daemon=True,
            name="db-health-check"
        )
        self._health_check_thread.start()
        self.logger.info("데이터베이스 헬스 체크 시작")
    
    def _perform_health_check(self):
        """헬스 체크 수행"""
        
        # 동기 풀 체크
        if self._sync_pool:
            try:
                with self.get_sync_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        
            except Exception as e:
                self.logger.warning(f"동기 풀 헬스 체크 실패: {e}")
    
    def get_pool_stats(self) -> Dict[str, Any]:
        """풀 통계 조회"""
        
        stats = self.stats.copy()
        
        # 실시간 풀 상태 추가
        if self._sync_pool:
            # ThreadedConnectionPool 내부 상태는 접근이 제한적
            stats['sync_pool']['configured_min'] = self.config.sync_min_connections
            stats['sync_pool']['configured_max'] = self.config.sync_max_connections
        
        if self._async_pool:
            stats['async_pool']['size'] = self._async_pool.get_size()
            stats['async_pool']['idle_connections'] = self._async_pool.get_idle_size()
            stats['async_pool']['configured_min'] = self.config.async_min_connections
            stats['async_pool']['configured_max'] = self.config.async_max_connections
        
        return stats
    
    def close_all_pools(self):
        """모든 커넥션 풀 종료"""
        
        self.logger.info("커넥션 풀 종료 시작")
        
        # 헬스 체크 중지
        if self._health_check_thread:
            self._shutdown_event.set()
            self._health_check_thread.join(timeout=5)
        
        # 동기 풀 종료
        if self._sync_pool:
            try:
                self._sync_pool.closeall()
                self.logger.info("동기 커넥션 풀 종료 완료")
            except Exception as e:
                self.logger.error(f"동기 커넥션 풀 종료 오류: {e}")
            finally:
                self._sync_pool = None
        
        # 비동기 풀 종료는 별도 메서드로 처리 (async)
        
    async def close_async_pool(self):
        """비동기 커넥션 풀 종료"""
        
        if self._async_pool:
            try:
                await self._async_pool.close()
                self.logger.info("비동기 커넥션 풀 종료 완료")
            except Exception as e:
                self.logger.error(f"비동기 커넥션 풀 종료 오류: {e}")
            finally:
                self._async_pool = None


# 싱글톤 인스턴스
_connection_pool = None


def get_connection_pool(config: PoolConfig = None) -> DatabaseConnectionPool:
    """커넥션 풀 인스턴스 반환"""
    global _connection_pool
    if _connection_pool is None:
        _connection_pool = DatabaseConnectionPool(config)
    return _connection_pool


def reset_connection_pool():
    """커넥션 풀 리셋"""
    global _connection_pool
    if _connection_pool:
        _connection_pool.close_all_pools()
        _connection_pool = None