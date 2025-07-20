"""
통합 데이터베이스 접근 레이어

모든 데이터베이스 작업을 일관된 인터페이스로 처리하며,
동기/비동기 메서드를 명확히 분리하고 누락된 기능을 보완합니다.
"""

import psycopg2
import psycopg2.extras
import asyncpg
import json
import threading
from contextlib import contextmanager, asynccontextmanager
from typing import Dict, List, Any, Optional, Generator, AsyncGenerator
from datetime import datetime
import logging
from abc import ABC, abstractmethod

from config.settings import get_database_config
from app.core.database_connection_pool import get_connection_pool, PoolConfig


class DatabaseError(Exception):
    """데이터베이스 관련 예외"""

    pass


class ConnectionError(DatabaseError):
    """연결 관련 예외"""

    pass


class QueryError(DatabaseError):
    """쿼리 실행 관련 예외"""

    pass


class BaseDatabaseManager(ABC):
    """데이터베이스 매니저 추상 기본 클래스"""

    def __init__(self):
        self.config = get_database_config()
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def serialize_for_db(value):
        """딕셔너리나 리스트 등 복잡한 객체를 PostgreSQL에 저장 가능한 JSON 문자열로 변환"""
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, default=str)
        return value

    @abstractmethod
    def fetch_all(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """모든 결과를 반환하는 SELECT 쿼리 실행"""
        pass

    @abstractmethod
    def fetch_one(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """단일 결과를 반환하는 SELECT 쿼리 실행"""
        pass

    @abstractmethod
    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """INSERT/UPDATE/DELETE 쿼리 실행하여 영향받은 행 수 반환"""
        pass

    @abstractmethod
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """배치 INSERT/UPDATE/DELETE 실행"""
        pass

    @abstractmethod
    def upsert(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        unique_conflict_columns: List[str],
    ) -> int:
        """UPSERT 작업 실행"""
        pass


class SyncDatabaseManager(BaseDatabaseManager):
    """동기 데이터베이스 매니저 (커넥션 풀 사용)"""
    
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, pool_config: PoolConfig = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, pool_config: PoolConfig = None):
        # 이미 초기화된 경우 건너뛰기
        if hasattr(self, '_initialized'):
            return
            
        super().__init__()
        self.connection_pool = get_connection_pool(pool_config)
        
        # 커넥션 풀 초기화
        try:
            self.connection_pool.initialize_sync_pool()
            self.logger.info("동기 데이터베이스 매니저 초기화 완료 (커넥션 풀 사용)")
            self._initialized = True
        except Exception as e:
            self.logger.error(f"커넥션 풀 초기화 실패: {e}")
            raise

    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """커넥션 풀에서 연결 획득"""
        with self.connection_pool.get_sync_connection() as connection:
            yield connection

    @contextmanager
    def get_cursor(self) -> Generator[psycopg2.extras.RealDictCursor, None, None]:
        """커서 컨텍스트 매니저"""
        with self.get_connection() as connection:
            try:
                with connection.cursor(
                    cursor_factory=psycopg2.extras.RealDictCursor
                ) as cursor:
                    yield cursor
            except Exception as e:
                self.logger.error(f"커서 작업 오류: {e}")
                raise QueryError(f"쿼리 실행 실패: {e}")

    def fetch_all(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """모든 결과를 반환하는 SELECT 쿼리 실행"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            self.logger.error(f"SELECT 쿼리 실행 실패: {e}")
            raise QueryError(f"SELECT 쿼리 실행 실패: {e}")

    def fetch_one(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """단일 결과를 반환하는 SELECT 쿼리 실행"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                result = cursor.fetchone()
                return dict(result) if result else None
        except Exception as e:
            self.logger.error(f"SELECT 쿼리 실행 실패: {e}")
            raise QueryError(f"SELECT 쿼리 실행 실패: {e}")

    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """INSERT/UPDATE/DELETE 쿼리 실행하여 영향받은 행 수 반환"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                return cursor.rowcount
        except Exception as e:
            self.logger.error(f"UPDATE 쿼리 실행 실패: {e}")
            raise QueryError(f"UPDATE 쿼리 실행 실패: {e}")

    def execute_query(self, query: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
        """RETURNING 절이 있는 쿼리 실행 (INSERT/UPDATE/DELETE with RETURNING)"""
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                # RETURNING 절이 있는 경우에만 fetchone 호출
                if "RETURNING" in query.upper():
                    result = cursor.fetchone()
                    return dict(result) if result else None
                else:
                    return {"rowcount": cursor.rowcount}
        except Exception as e:
            self.logger.error(f"쿼리 실행 실패: {e}")
            raise QueryError(f"쿼리 실행 실패: {e}")

    def execute_query_returning_one(self, query: str, params: Optional[tuple] = None) -> Optional[Dict[str, Any]]:
        """RETURNING 절이 있는 쿼리 실행하여 단일 결과 반환"""
        return self.execute_query(query, params)

    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """배치 INSERT/UPDATE/DELETE 실행"""
        if not params_list:
            return 0

        try:
            # params_list의 각 tuple의 복잡한 객체들을 직렬화
            processed_params_list = []
            for params in params_list:
                processed_params = tuple(
                    self.serialize_for_db(param) for param in params
                )
                processed_params_list.append(processed_params)

            with self.get_cursor() as cursor:
                cursor.executemany(query, processed_params_list)
                return cursor.rowcount
        except Exception as e:
            self.logger.error(f"배치 쿼리 실행 실패: {e}")
            raise QueryError(f"배치 쿼리 실행 실패: {e}")

    def upsert(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        unique_conflict_columns: List[str],
    ) -> int:
        """UPSERT 작업 실행"""
        if not data:
            return 0

        if not unique_conflict_columns:
            raise ValueError("unique_conflict_columns는 비어 있을 수 없습니다.")

        # 데이터 전처리: 복잡한 객체들을 JSON으로 변환
        processed_data = []
        for item in data:
            processed_item = {}
            for key, value in item.items():
                processed_item[key] = self.serialize_for_db(value)
            processed_data.append(processed_item)

        columns = list(processed_data[0].keys())
        update_columns = [col for col in columns if col not in unique_conflict_columns]

        if not update_columns:
            update_statement = "NOTHING"
        else:
            update_statement = f"UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in update_columns])}"
            # updated_at 컬럼이 있으면 자동 업데이트
            if "updated_at" not in update_columns and "updated_at" in columns:
                update_statement += ", updated_at = CURRENT_TIMESTAMP"

        query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES %s
        ON CONFLICT ({", ".join(unique_conflict_columns)}) DO {update_statement}
        """

        try:
            with self.get_cursor() as cursor:
                psycopg2.extras.execute_values(
                    cursor, query, [tuple(d.values()) for d in processed_data]
                )
                affected_rows = cursor.rowcount
                self.logger.info(
                    f"{table_name} 테이블에 {affected_rows}개 행 UPSERT 완료"
                )
                return affected_rows
        except Exception as e:
            self.logger.error(f"{table_name} 테이블 UPSERT 실패: {e}")
            raise QueryError(f"UPSERT 실행 실패: {e}")

    # ========== 특화된 데이터 삽입 메서드들 ==========

    def insert_weather_data(self, weather_data: List[Dict[str, Any]]) -> int:
        """날씨 데이터 삽입/업데이트"""
        if not weather_data:
            return 0

        # 표준 날씨 데이터 형식으로 변환
        formatted_data = []
        for data in weather_data:
            formatted_item = {
                "region_code": data.get("region_code"),
                "weather_date": data.get("weather_date") or data.get("date"),
                "temperature": data.get("temperature") or data.get("temp"),
                "max_temp": data.get("max_temp") or data.get("max_temperature"),
                "min_temp": data.get("min_temp") or data.get("min_temperature"),
                "humidity": data.get("humidity"),
                "precipitation": data.get("precipitation") or data.get("rainfall"),
                "wind_speed": data.get("wind_speed"),
                "weather_condition": data.get("weather_condition")
                or data.get("condition"),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            # None 값 제거
            formatted_item = {k: v for k, v in formatted_item.items() if v is not None}
            if formatted_item:  # 비어있지 않은 경우만 추가
                formatted_data.append(formatted_item)

        if not formatted_data:
            self.logger.warning("유효한 날씨 데이터가 없습니다.")
            return 0

        try:
            return self.upsert(
                "historical_weather_daily",
                formatted_data,
                ["region_code", "weather_date"],
            )
        except Exception as e:
            self.logger.error(f"날씨 데이터 삽입 실패: {e}")
            raise

    def insert_forecast_data(self, forecast_data: List[Dict[str, Any]]) -> int:
        """예보 데이터 삽입/업데이트"""
        if not forecast_data:
            return 0

        # 표준 예보 데이터 형식으로 변환
        formatted_data = []
        for data in forecast_data:
            formatted_item = {
                "region_code": data.get("region_code"),
                "nx": data.get("nx"),
                "ny": data.get("ny"),
                "forecast_date": data.get("forecast_date") or data.get("date"),
                "forecast_time": data.get("forecast_time"),
                "temperature": data.get("temperature") or data.get("temp"),
                "max_temp": data.get("max_temp") or data.get("max_temperature"),
                "min_temp": data.get("min_temp") or data.get("min_temperature"),
                "humidity": data.get("humidity"),
                "precipitation_probability": data.get("precipitation_probability")
                or data.get("rain_prob"),
                "precipitation_prob": data.get("precipitation_prob") or data.get("precipitation_probability") or data.get("rain_prob"),
                "precipitation": data.get("precipitation") or data.get("rainfall"),
                "wind_speed": data.get("wind_speed"),
                "wind_direction": data.get("wind_direction"),
                "sky_condition": data.get("sky_condition"),
                "weather_condition": data.get("weather_condition")
                or data.get("condition"),
                "base_date": data.get("base_date"),
                "base_time": data.get("base_time"),
                "forecast_type": data.get("forecast_type", "short"),
                "data_quality_score": data.get("data_quality_score"),
                "raw_data_id": data.get("raw_data_id"),
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "last_sync_at": datetime.now(),
            }
            # None 값 제거
            formatted_item = {k: v for k, v in formatted_item.items() if v is not None}
            if formatted_item:  # 비어있지 않은 경우만 추가
                formatted_data.append(formatted_item)

        if not formatted_data:
            self.logger.warning("유효한 예보 데이터가 없습니다.")
            return 0

        try:
            return self.upsert(
                "weather_forecast",
                formatted_data,
                ["region_code", "forecast_date", "forecast_time"],
            )
        except Exception as e:
            self.logger.error(f"예보 데이터 삽입 실패: {e}")
            raise

    def insert_tourist_attractions(self, attractions: List[Dict[str, Any]]) -> int:
        """관광지 정보 삽입/업데이트 (새로운 10개 필드 지원)"""
        if not attractions:
            return 0

        # 표준 관광지 데이터 형식으로 변환
        formatted_data = []
        for attraction in attractions:
            # 다양한 필드명 매핑
            content_id = (
                attraction.get("content_id")
                or attraction.get("contentid")
                or attraction.get("attraction_id")
            )

            if not content_id:
                # ID가 없으면 제목과 지역을 기반으로 생성
                import hashlib

                title = (
                    attraction.get("title") or attraction.get("attraction_name") or ""
                )
                region = (
                    attraction.get("region_code") or attraction.get("area_code") or ""
                )
                content_id = hashlib.md5(f"{title}_{region}".encode()).hexdigest()[:20]

            formatted_item = {
                # 기본 필드들
                "content_id": content_id,
                "region_code": attraction.get("region_code")
                or attraction.get("area_code"),
                "attraction_name": (
                    attraction.get("attraction_name")
                    or attraction.get("title")
                    or attraction.get("name")
                ),
                "category_code": (
                    attraction.get("category_code")
                    or attraction.get("content_type_id")
                    or attraction.get("contentTypeId")
                ),
                "category_name": (
                    attraction.get("category_name")
                    or attraction.get("content_type_name")
                ),
                "address": attraction.get("address") or attraction.get("addr1"),
                "address_detail": attraction.get("address_detail")
                or attraction.get("addr2"),
                "latitude": attraction.get("latitude") or attraction.get("mapy"),
                "longitude": attraction.get("longitude") or attraction.get("mapx"),
                "description": attraction.get("description")
                or attraction.get("overview"),
                "image_url": attraction.get("image_url")
                or attraction.get("firstimage"),
                "phone": attraction.get("phone") or attraction.get("tel"),
                "homepage": attraction.get("homepage"),
                
                # 새로 추가된 10개 필드들
                "booktour": attraction.get("booktour") or attraction.get("book_tour"),
                "createdtime": attraction.get("createdtime") or attraction.get("created_time"),
                "modifiedtime": attraction.get("modifiedtime") or attraction.get("modified_time"),
                "telname": attraction.get("telname") or attraction.get("tel_name"),
                "faxno": attraction.get("faxno") or attraction.get("fax_no"),
                "zipcode": attraction.get("zipcode") or attraction.get("zip_code"),
                "mlevel": attraction.get("mlevel") or attraction.get("map_level"),
                "detail_intro_info": attraction.get("detail_intro_info") or attraction.get("intro_info"),
                "detail_additional_info": attraction.get("detail_additional_info") or attraction.get("additional_info"),
                
                # 시간 스탬프
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }

            # None 값 제거하고 필수 필드 확인
            formatted_item = {k: v for k, v in formatted_item.items() if v is not None}
            if formatted_item.get("content_id") and formatted_item.get(
                "attraction_name"
            ):
                formatted_data.append(formatted_item)

        if not formatted_data:
            self.logger.warning("유효한 관광지 데이터가 없습니다.")
            return 0

        try:
            return self.upsert("tourist_attractions", formatted_data, ["content_id"])
        except Exception as e:
            self.logger.error(f"관광지 데이터 삽입 실패: {e}")
            raise

    def log_job_result(
        self,
        job_name: str,
        job_type: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        processed_records: int,
        error_message: Optional[str] = None,
    ) -> None:
        """배치 작업 결과 로깅"""
        query = """
        INSERT INTO batch_job_logs (job_name, job_type, status, start_time, end_time, error_message, result)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        try:
            self.execute_update(
                query,
                (
                    job_name,
                    job_type,
                    status,
                    start_time,
                    end_time,
                    error_message,
                    json.dumps({"processed_records": processed_records}),
                ),
            )
            self.logger.info(f"작업 로그 저장 완료: {job_name}")
        except Exception as e:
            self.logger.error(f"작업 로그 저장 실패: {e}")
            # 로그 저장 실패는 전체 작업을 중단시키지 않음
            pass

    # ========== 호환성을 위한 메서드들 ==========

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """SELECT 쿼리 실행 (호환성)"""
        return self.fetch_all(query, params)


class AsyncDatabaseManager(BaseDatabaseManager):
    """비동기 데이터베이스 매니저 (커넥션 풀 사용)"""

    def __init__(self, pool_config: PoolConfig = None):
        super().__init__()
        self.connection_pool = get_connection_pool(pool_config)

    @asynccontextmanager
    async def get_connection(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """커넥션 풀에서 비동기 연결 획득"""
        async with self.connection_pool.get_async_connection() as connection:
            yield connection

    async def fetch_all(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """비동기 모든 결과를 반환하는 SELECT 쿼리 실행"""
        try:
            async with self.get_connection() as connection:
                if params:
                    rows = await connection.fetch(query, *params)
                else:
                    rows = await connection.fetch(query)
                return [dict(row) for row in rows]
        except Exception as e:
            self.logger.error(f"비동기 SELECT 쿼리 실행 실패: {e}")
            raise QueryError(f"비동기 SELECT 쿼리 실행 실패: {e}")

    async def fetch_one(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """비동기 단일 결과를 반환하는 SELECT 쿼리 실행"""
        try:
            async with self.get_connection() as connection:
                if params:
                    row = await connection.fetchrow(query, *params)
                else:
                    row = await connection.fetchrow(query)
                return dict(row) if row else None
        except Exception as e:
            self.logger.error(f"비동기 SELECT 쿼리 실행 실패: {e}")
            raise QueryError(f"비동기 SELECT 쿼리 실행 실패: {e}")

    async def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """비동기 INSERT/UPDATE/DELETE 쿼리 실행"""
        try:
            # params가 있으면 복잡한 객체들을 직렬화
            if params:
                processed_params = tuple(
                    self.serialize_for_db(param) for param in params
                )
            else:
                processed_params = params

            async with self.get_connection() as connection:
                if processed_params:
                    result = await connection.execute(query, *processed_params)
                else:
                    result = await connection.execute(query)
                # PostgreSQL의 경우 "INSERT 0 5" 형태로 반환되므로 숫자 부분만 추출
                return int(result.split()[-1]) if result else 0
        except Exception as e:
            self.logger.error(f"비동기 UPDATE 쿼리 실행 실패: {e}")
            raise QueryError(f"비동기 UPDATE 쿼리 실행 실패: {e}")

    async def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """비동기 배치 INSERT/UPDATE/DELETE 실행"""
        if not params_list:
            return 0

        try:
            # params_list의 각 tuple의 복잡한 객체들을 직렬화
            processed_params_list = []
            for params in params_list:
                processed_params = tuple(
                    self.serialize_for_db(param) for param in params
                )
                processed_params_list.append(processed_params)

            async with self.get_connection() as connection:
                await connection.executemany(query, processed_params_list)
                return len(processed_params_list)  # asyncpg는 rowcount를 반환하지 않음
        except Exception as e:
            self.logger.error(f"비동기 배치 쿼리 실행 실패: {e}")
            raise QueryError(f"비동기 배치 쿼리 실행 실패: {e}")

    async def upsert(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        unique_conflict_columns: List[str],
    ) -> int:
        """비동기 UPSERT 작업 실행"""
        # 동기 버전과 동일한 로직, 비동기 실행만 다름
        if not data:
            return 0

        if not unique_conflict_columns:
            raise ValueError("unique_conflict_columns는 비어 있을 수 없습니다.")

        # 데이터 전처리: 복잡한 객체들을 JSON으로 변환
        processed_data = []
        for item in data:
            processed_item = {}
            for key, value in item.items():
                processed_item[key] = self.serialize_for_db(value)
            processed_data.append(processed_item)

        columns = list(processed_data[0].keys())
        update_columns = [col for col in columns if col not in unique_conflict_columns]

        if not update_columns:
            update_statement = "NOTHING"
        else:
            update_statement = f"UPDATE SET {', '.join([f'{col} = EXCLUDED.{col}' for col in update_columns])}"
            if "updated_at" not in update_columns and "updated_at" in columns:
                update_statement += ", updated_at = CURRENT_TIMESTAMP"

        # asyncpg는 execute_values를 지원하지 않으므로 다른 방식 사용
        values_list = []
        for i, item in enumerate(processed_data):
            placeholders = [f"${j + i * len(columns) + 1}" for j in range(len(columns))]
            values_list.append(f"({', '.join(placeholders)})")

        query = f"""
        INSERT INTO {table_name} ({", ".join(columns)})
        VALUES {", ".join(values_list)}
        ON CONFLICT ({", ".join(unique_conflict_columns)}) DO {update_statement}
        """

        # 모든 값을 평면화
        flat_params = []
        for item in processed_data:
            flat_params.extend(item.values())

        try:
            result = await self.execute_update(query, tuple(flat_params))
            self.logger.info(f"{table_name} 테이블에 {result}개 행 비동기 UPSERT 완료")
            return result
        except Exception as e:
            self.logger.error(f"{table_name} 테이블 비동기 UPSERT 실패: {e}")
            raise QueryError(f"비동기 UPSERT 실행 실패: {e}")

    async def close_pool(self):
        """연결 풀 종료"""
        if self._connection_pool:
            await self._connection_pool.close()
            self._connection_pool = None
            self.logger.info("비동기 데이터베이스 연결 풀 종료 완료")

    # ========== 호환성을 위한 메서드들 ==========

    async def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """비동기 SELECT 쿼리 실행 (호환성)"""
        return await self.fetch_all(query, params)


class UnifiedDatabaseManager:
    """통합 데이터베이스 매니저 (동기/비동기 모두 지원)"""

    def __init__(self, use_async: bool = False):
        self.use_async = use_async
        self.sync_manager = SyncDatabaseManager()
        self.async_manager = AsyncDatabaseManager() if use_async else None
        self.logger = logging.getLogger(__name__)

    # ========== 동기 메서드들 ==========

    def fetch_all(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """모든 결과를 반환하는 SELECT 쿼리 실행"""
        return self.sync_manager.fetch_all(query, params)

    def fetch_one(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """단일 결과를 반환하는 SELECT 쿼리 실행"""
        return self.sync_manager.fetch_one(query, params)

    def execute_update(self, query: str, params: Optional[tuple] = None) -> int:
        """INSERT/UPDATE/DELETE 쿼리 실행"""
        return self.sync_manager.execute_update(query, params)

    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """배치 INSERT/UPDATE/DELETE 실행"""
        return self.sync_manager.execute_many(query, params_list)

    def upsert(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        unique_conflict_columns: List[str],
    ) -> int:
        """UPSERT 작업 실행"""
        return self.sync_manager.upsert(table_name, data, unique_conflict_columns)

    # ========== 비동기 메서드들 ==========

    async def fetch_all_async(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """비동기 모든 결과를 반환하는 SELECT 쿼리 실행"""
        if not self.async_manager:
            raise RuntimeError("비동기 매니저가 초기화되지 않았습니다.")
        return await self.async_manager.fetch_all(query, params)

    async def fetch_one_async(
        self, query: str, params: Optional[tuple] = None
    ) -> Optional[Dict[str, Any]]:
        """비동기 단일 결과를 반환하는 SELECT 쿼리 실행"""
        if not self.async_manager:
            raise RuntimeError("비동기 매니저가 초기화되지 않았습니다.")
        return await self.async_manager.fetch_one(query, params)

    async def execute_update_async(
        self, query: str, params: Optional[tuple] = None
    ) -> int:
        """비동기 INSERT/UPDATE/DELETE 쿼리 실행"""
        if not self.async_manager:
            raise RuntimeError("비동기 매니저가 초기화되지 않았습니다.")
        return await self.async_manager.execute_update(query, params)

    async def upsert_async(
        self,
        table_name: str,
        data: List[Dict[str, Any]],
        unique_conflict_columns: List[str],
    ) -> int:
        """비동기 UPSERT 작업 실행"""
        if not self.async_manager:
            raise RuntimeError("비동기 매니저가 초기화되지 않았습니다.")
        return await self.async_manager.upsert(
            table_name, data, unique_conflict_columns
        )

    # ========== 특화된 메서드들 ==========

    def insert_weather_data(self, weather_data: List[Dict[str, Any]]) -> int:
        """날씨 데이터 삽입/업데이트"""
        return self.sync_manager.insert_weather_data(weather_data)

    def insert_forecast_data(self, forecast_data: List[Dict[str, Any]]) -> int:
        """예보 데이터 삽입/업데이트"""
        return self.sync_manager.insert_forecast_data(forecast_data)

    def insert_tourist_attractions(self, attractions: List[Dict[str, Any]]) -> int:
        """관광지 정보 삽입/업데이트"""
        return self.sync_manager.insert_tourist_attractions(attractions)

    def log_job_result(
        self,
        job_name: str,
        job_type: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        processed_records: int,
        error_message: Optional[str] = None,
    ) -> None:
        """배치 작업 결과 로깅"""
        return self.sync_manager.log_job_result(
            job_name,
            job_type,
            status,
            start_time,
            end_time,
            processed_records,
            error_message,
        )

    def get_pending_batch_jobs(self) -> List[Dict[str, Any]]:
        """PENDING 상태의 배치 작업 조회"""
        query = """
        SELECT id, job_type, parameters, created_at, created_by
        FROM batch_job_executions 
        WHERE status = 'PENDING' 
        AND created_at > NOW() - INTERVAL '1 hour'
        ORDER BY created_at ASC
        LIMIT 10
        """
        try:
            return self.fetch_all(query)
        except Exception as e:
            self.logger.error(f"PENDING 작업 조회 실패: {e}")
            return []

    # ========== 호환성을 위한 메서드들 ==========

    def execute_query(
        self, query: str, params: Optional[tuple] = None
    ) -> List[Dict[str, Any]]:
        """SELECT 쿼리 실행 (호환성)"""
        return self.fetch_all(query, params)

    def get_connection(self):
        """연결 컨텍스트 매니저 반환 (호환성)"""
        return self.sync_manager.get_connection()

    def get_cursor(self):
        """커서 컨텍스트 매니저 반환 (호환성)"""
        return self.sync_manager.get_cursor()

    async def execute_async(self, query: str, params: Optional[tuple] = None) -> int:
        """비동기 쿼리 실행 (호환성)"""
        return await self.execute_update_async(query, params)

    async def close_pool(self):
        """연결 풀 종료"""
        if self.async_manager:
            await self.async_manager.close_pool()


# ========== 싱글톤 인스턴스 관리 ==========

_sync_db_manager = None
_async_db_manager = None
_unified_db_manager = None


def DatabaseManager() -> UnifiedDatabaseManager:
    """통합 데이터베이스 매니저 인스턴스 반환 (동기)"""
    global _unified_db_manager
    if _unified_db_manager is None:
        _unified_db_manager = UnifiedDatabaseManager(use_async=False)
    return _unified_db_manager


def get_async_db_manager() -> UnifiedDatabaseManager:
    """통합 데이터베이스 매니저 인스턴스 반환 (비동기)"""
    global _async_db_manager
    if _async_db_manager is None:
        _async_db_manager = UnifiedDatabaseManager(use_async=True)
    return _async_db_manager


# 하위 호환성을 위한 별칭
def get_db_manager() -> UnifiedDatabaseManager:
    """통합 데이터베이스 매니저 인스턴스 반환 (동기)"""
    global _unified_db_manager
    if _unified_db_manager is None:
        _unified_db_manager = UnifiedDatabaseManager(use_async=False)
    return _unified_db_manager

DatabaseManager = UnifiedDatabaseManager
