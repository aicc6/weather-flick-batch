"""
Redis 클라이언트 유틸리티
"""

import redis
import json
from typing import Any, Optional, Dict
import logging

from config.settings import get_monitoring_settings


class RedisClient:
    """Redis 클라이언트 관리 클래스"""

    def __init__(self):
        self.settings = get_monitoring_settings()
        self.logger = logging.getLogger(__name__)
        self._client = None
        self._connection_failed = False

    def get_client(self) -> Optional[redis.Redis]:
        """Redis 클라이언트 생성 및 반환"""
        if self._client is None and not self._connection_failed:
            try:
                # Redis 6.0+ ACL 인증 시도
                try:
                    self._client = redis.Redis(
                        host=self.settings.redis_host,
                        port=self.settings.redis_port,
                        password=self.settings.redis_password
                        if self.settings.redis_password
                        else None,
                        db=self.settings.redis_db,
                        decode_responses=True,
                        socket_timeout=5,
                        socket_connect_timeout=5,
                        retry_on_timeout=True,
                        health_check_interval=30,
                    )
                except redis.AuthenticationError:
                    # 레거시 인증 방식 시도
                    self._client = redis.Redis(
                        host=self.settings.redis_host,
                        port=self.settings.redis_port,
                        password=self.settings.redis_password
                        if self.settings.redis_password
                        else None,
                        db=self.settings.redis_db,
                        decode_responses=True,
                        socket_timeout=5,
                        socket_connect_timeout=5,
                        retry_on_timeout=True,
                        health_check_interval=30,
                    )
                # 연결 테스트
                self._client.ping()
                self.logger.info("Redis 연결 성공")
            except Exception as e:
                self.logger.warning(f"Redis 연결 실패: {e}. 캐시 기능 없이 계속 실행됩니다.")
                self._connection_failed = True
                self._client = None

        return self._client

    def set_cache(self, key: str, value: Any, expire: int = 3600) -> bool:
        """캐시 데이터 저장"""
        try:
            client = self.get_client()
            if not client:
                self.logger.debug(f"Redis 클라이언트 없음, 캐시 저장 건너뜀: {key}")
                return False
                
            if isinstance(value, (dict, list)):
                value = json.dumps(value, ensure_ascii=False)

            result = client.setex(key, expire, value)
            self.logger.debug(f"캐시 저장: {key}")
            return result
        except Exception as e:
            self.logger.error(f"캐시 저장 실패 [{key}]: {e}")
            return False

    def get_cache(self, key: str) -> Optional[Any]:
        """캐시 데이터 조회"""
        try:
            client = self.get_client()
            if not client:
                self.logger.debug(f"Redis 클라이언트 없음, 캐시 조회 건너뜀: {key}")
                return None
                
            value = client.get(key)

            if value is None:
                return None

            # JSON 문자열인지 확인하고 파싱 시도
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value

        except Exception as e:
            self.logger.error(f"캐시 조회 실패 [{key}]: {e}")
            return None

    def delete_cache(self, key: str) -> bool:
        """캐시 데이터 삭제"""
        try:
            client = self.get_client()
            if not client:
                self.logger.debug(f"Redis 클라이언트 없음, 캐시 삭제 건너뜀: {key}")
                return False
                
            result = client.delete(key)
            self.logger.debug(f"캐시 삭제: {key}")
            return bool(result)
        except Exception as e:
            self.logger.error(f"캐시 삭제 실패 [{key}]: {e}")
            return False

    def exists(self, key: str) -> bool:
        """캐시 키 존재 확인"""
        try:
            client = self.get_client()
            if not client:
                self.logger.debug(f"Redis 클라이언트 없음, 캐시 존재 확인 건너뜀: {key}")
                return False
                
            return bool(client.exists(key))
        except Exception as e:
            self.logger.error(f"캐시 존재 확인 실패 [{key}]: {e}")
            return False

    def clear_pattern(self, pattern: str) -> int:
        """패턴 매칭 키들 일괄 삭제"""
        try:
            client = self.get_client()
            keys = client.keys(pattern)
            if keys:
                deleted = client.delete(*keys)
                self.logger.info(f"패턴 [{pattern}] 캐시 {deleted}개 삭제")
                return deleted
            return 0
        except Exception as e:
            self.logger.error(f"패턴 캐시 삭제 실패 [{pattern}]: {e}")
            return 0

    def get_info(self) -> Dict[str, Any]:
        """Redis 서버 정보 조회"""
        try:
            client = self.get_client()
            info = client.info()
            return {
                "redis_version": info.get("redis_version"),
                "used_memory_human": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "total_commands_processed": info.get("total_commands_processed"),
                "keyspace_hits": info.get("keyspace_hits"),
                "keyspace_misses": info.get("keyspace_misses"),
            }
        except Exception as e:
            self.logger.error(f"Redis 정보 조회 실패: {e}")
            return {}

    def close(self):
        """Redis 연결 종료"""
        if self._client:
            try:
                self._client.close()
                self.logger.info("Redis 연결 종료")
            except Exception as e:
                self.logger.error(f"Redis 연결 종료 실패: {e}")
            finally:
                self._client = None


# 전역 Redis 클라이언트 인스턴스
_redis_client = None


def get_redis_client() -> RedisClient:
    """전역 Redis 클라이언트 반환"""
    global _redis_client
    if _redis_client is None:
        _redis_client = RedisClient()
    return _redis_client


# Redis 연결 테스트 함수
def test_redis_connection() -> bool:
    """Redis 연결 테스트"""
    try:
        client = get_redis_client()
        redis_client = client.get_client()
        
        if not redis_client:
            print("⚠️ Redis 연결을 사용할 수 없지만 시스템은 정상 작동합니다")
            return False
            
        redis_client.ping()
        print("✅ Redis 연결 성공!")

        # 기본 정보 출력
        info = client.get_info()
        if info:
            print(f"Redis 버전: {info.get('redis_version')}")
            print(f"사용 메모리: {info.get('used_memory_human')}")
            print(f"연결된 클라이언트: {info.get('connected_clients')}")

        return True
    except Exception as e:
        print(f"❌ Redis 연결 실패: {e}")
        print("⚠️ 캐시 기능 없이 시스템이 계속 실행됩니다")
        return False


if __name__ == "__main__":
    # 직접 실행시 연결 테스트
    test_redis_connection()

