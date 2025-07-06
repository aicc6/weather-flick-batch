"""
리팩토링된 데이터베이스 유틸리티 모듈 (DEPRECATED)

⚠️  DEPRECATION WARNING ⚠️
이 모듈은 하위 호환성을 위해 유지되고 있지만, 새로운 코드에서는 사용하지 마세요.
대신 app.core.database_manager.DatabaseManager를 사용하세요.

기존 utils/database.py의 모든 기능을 포함하며,
누락된 메서드들을 보완하고 동기/비동기 메서드를 명확히 분리했습니다.

마이그레이션 가이드:
- from utils.database_refactored import DatabaseManager
+ from app.core.database_manager import DatabaseManager
"""

import asyncio
from typing import Dict, List, Any
from datetime import datetime
import logging

from app.core.database_manager import (
    UnifiedDatabaseManager,
    DatabaseError,
    ConnectionError,
    QueryError,
)


# 하위 호환성을 위한 기존 인터페이스 유지
class DatabaseManager(UnifiedDatabaseManager):
    """
    기존 DatabaseManager와 호환되는 래퍼 클래스

    모든 기존 메서드를 지원하면서 새로운 기능을 추가로 제공합니다.
    """

    def __init__(self):
        super().__init__(use_async=False)
        # 기존 코드 호환성을 위한 속성들
        self.config = self.sync_manager.config
        self.logger = self.sync_manager.logger
        self._executor = None  # 더 이상 사용하지 않지만 호환성 유지

    # ========== 기존 인터페이스 완전 호환 ==========

    def get_cursor(self):
        """기존 get_cursor 메서드 호환"""
        return self.sync_manager.get_cursor()

    # fetch_all_async는 이미 UnifiedDatabaseManager에서 제공
    # execute_async는 이미 UnifiedDatabaseManager에서 제공

    async def close_pool(self):
        """기존 close_pool 메서드 호환"""
        if self.async_manager:
            await self.async_manager.close_pool()

    # ========== 개선된 ThreadPoolExecutor 기반 비동기 메서드 ==========

    async def fetch_all_thread_async(
        self, query: str, params=None
    ) -> List[Dict[str, Any]]:
        """
        ThreadPoolExecutor 기반 비동기 fetch_all (기존 호환성)

        기존 코드에서 사용하던 패턴과 동일하게 동작합니다.
        """
        import concurrent.futures

        if not hasattr(self, "_thread_executor"):
            self._thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._thread_executor, self.fetch_all, query, params
        )

    async def execute_thread_async(self, query: str, params=None) -> int:
        """
        ThreadPoolExecutor 기반 비동기 execute (기존 호환성)

        기존 execute_async 메서드와 동일하게 동작합니다.
        """
        import concurrent.futures

        if not hasattr(self, "_thread_executor"):
            self._thread_executor = concurrent.futures.ThreadPoolExecutor(max_workers=5)

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._thread_executor, self.execute_update, query, params
        )


def get_db_manager() -> DatabaseManager:
    """
    데이터베이스 매니저 인스턴스 반환 (싱글톤)

    기존 코드와 완전히 호환되며, 모든 새로운 기능을 포함합니다.
    """
    global _db_manager_instance
    if "_db_manager_instance" not in globals():
        _db_manager_instance = DatabaseManager()
    return _db_manager_instance


# ========== 사용 예제 및 마이그레이션 가이드 ==========


class DatabaseUsageExamples:
    """
    새로운 데이터베이스 매니저 사용 예제
    """

    def __init__(self):
        self.db = get_db_manager()
        self.logger = logging.getLogger(__name__)

    def example_basic_queries(self):
        """기본 쿼리 사용 예제"""

        # 1. SELECT 쿼리 (기존과 동일)
        users = self.db.fetch_all("SELECT * FROM users WHERE active = %s", (True,))
        print(f"활성 사용자: {len(users)}명")

        # 2. 단일 결과 조회 (기존과 동일)
        user = self.db.fetch_one("SELECT * FROM users WHERE id = %s", (1,))
        if user:
            print(f"사용자: {user['name']}")

        # 3. INSERT/UPDATE/DELETE (기존과 동일)
        affected_rows = self.db.execute_update(
            "UPDATE users SET last_login = %s WHERE id = %s", (datetime.now(), 1)
        )
        print(f"업데이트된 행: {affected_rows}개")

    def example_weather_data_insertion(self):
        """날씨 데이터 삽입 예제 (새로운 기능)"""

        weather_data = [
            {
                "region_code": "1",
                "weather_date": "2025-07-03",
                "temperature": 25.5,
                "max_temp": 28.0,
                "min_temp": 22.0,
                "humidity": 65,
                "precipitation": 0.0,
                "wind_speed": 2.5,
                "weather_condition": "맑음",
            },
            {
                "region_code": "2",
                "weather_date": "2025-07-03",
                "temperature": 23.0,
                "max_temp": 26.0,
                "min_temp": 20.0,
                "humidity": 70,
                "precipitation": 5.2,
                "wind_speed": 3.1,
                "weather_condition": "흐림",
            },
        ]

        # 새로운 전용 메서드 사용
        inserted_count = self.db.insert_weather_data(weather_data)
        print(f"날씨 데이터 {inserted_count}건 저장 완료")

    def example_tourist_attractions_insertion(self):
        """관광지 데이터 삽입 예제 (개선된 기능)"""

        attractions = [
            {
                "content_id": "126508",
                "region_code": "1",
                "title": "경복궁",
                "category_code": "12",
                "category_name": "관광지",
                "address": "서울특별시 종로구 사직로 161",
                "latitude": 37.578592,
                "longitude": 126.977041,
            },
            {
                "contentid": "264302",  # 다른 필드명도 자동 매핑
                "area_code": "6",  # 다른 필드명도 자동 매핑
                "attraction_name": "해운대해수욕장",  # 다른 필드명도 자동 매핑
                "content_type_id": "12",
                "addr1": "부산광역시 해운대구 우동",
                "mapy": 35.158698,  # 다른 필드명도 자동 매핑
                "mapx": 129.160387,  # 다른 필드명도 자동 매핑
            },
        ]

        # 개선된 관광지 삽입 메서드 (다양한 필드명 자동 매핑)
        inserted_count = self.db.insert_tourist_attractions(attractions)
        print(f"관광지 데이터 {inserted_count}건 저장 완료")

    def example_upsert_operations(self):
        """UPSERT 작업 예제"""

        regions_data = [
            {
                "region_code": "1",
                "region_name": "서울특별시",
                "latitude": 37.5665,
                "longitude": 126.9780,
                "updated_at": datetime.now(),
            },
            {
                "region_code": "2",
                "region_name": "부산광역시",
                "latitude": 35.1796,
                "longitude": 129.0756,
                "updated_at": datetime.now(),
            },
        ]

        # UPSERT 작업 (중복 시 업데이트, 없으면 삽입)
        upserted_count = self.db.upsert(
            "regions",
            regions_data,
            ["region_code"],  # 고유 제약 조건 컬럼
        )
        print(f"지역 데이터 {upserted_count}건 UPSERT 완료")

    async def example_async_operations(self):
        """비동기 작업 예제"""

        # 비동기 데이터베이스 매니저 사용
        async_db = UnifiedDatabaseManager(use_async=True)

        try:
            # 비동기 SELECT 쿼리
            users = await async_db.fetch_all_async(
                "SELECT * FROM users WHERE created_at > %s", (datetime.now().date(),)
            )
            print(f"오늘 가입한 사용자: {len(users)}명")

            # 비동기 UPSERT
            data = [{"id": 1, "name": "테스트", "updated_at": datetime.now()}]
            count = await async_db.upsert_async("test_table", data, ["id"])
            print(f"비동기 UPSERT 완료: {count}건")

        finally:
            # 연결 풀 정리
            await async_db.close_pool()

    def example_error_handling(self):
        """오류 처리 예제"""

        try:
            # 의도적으로 실패하는 쿼리 (테스트용 - 실제 존재하지 않는 테이블)
            self.db.fetch_all("SELECT * FROM intentionally_non_existent_test_table")
        except QueryError as e:
            self.logger.error(f"쿼리 오류: {e}")
        except ConnectionError as e:
            self.logger.error(f"연결 오류: {e}")
        except DatabaseError as e:
            self.logger.error(f"데이터베이스 오류: {e}")

    def example_job_logging(self):
        """작업 로깅 예제"""

        start_time = datetime.now()

        try:
            # 작업 실행 시뮬레이션
            processed_records = 100

            # 작업 완료 로깅
            self.db.log_job_result(
                job_name="example_job",
                job_type="data_collection",
                status="completed",
                start_time=start_time,
                end_time=datetime.now(),
                processed_records=processed_records,
            )

        except Exception as e:
            # 작업 실패 로깅
            self.db.log_job_result(
                job_name="example_job",
                job_type="data_collection",
                status="failed",
                start_time=start_time,
                end_time=datetime.now(),
                processed_records=0,
                error_message=str(e),
            )


# ========== 마이그레이션 도우미 함수들 ==========


def migrate_from_old_db_manager():
    """
    기존 DatabaseManager 코드를 새로운 버전으로 마이그레이션하는 가이드
    """
    print("=== 데이터베이스 매니저 마이그레이션 가이드 ===")
    print()

    print("1. 기존 코드 (그대로 작동):")
    print("   db = get_db_manager()")
    print("   results = db.fetch_all('SELECT * FROM users')")
    print("   count = db.execute_update('UPDATE users SET active = true')")
    print()

    print("2. 새로운 전용 메서드 활용:")
    print("   # 날씨 데이터")
    print("   db.insert_weather_data(weather_data_list)")
    print("   # 예보 데이터")
    print("   db.insert_forecast_data(forecast_data_list)")
    print("   # 관광지 데이터")
    print("   db.insert_tourist_attractions(attractions_list)")
    print()

    print("3. 비동기 작업 (선택사항):")
    print("   async_db = get_async_db_manager()")
    print("   results = await async_db.fetch_all_async('SELECT * FROM users')")
    print("   await async_db.close_pool()  # 작업 완료 후")
    print()

    print("4. 오류 처리 개선:")
    print("   try:")
    print("       db.fetch_all('SELECT * FROM table')")
    print("   except QueryError:")
    print("       # 쿼리 관련 오류 처리")
    print("   except ConnectionError:")
    print("       # 연결 관련 오류 처리")


if __name__ == "__main__":
    # 마이그레이션 가이드 출력
    migrate_from_old_db_manager()

    # 사용 예제 실행
    examples = DatabaseUsageExamples()

    print("\n=== 사용 예제 실행 ===")

    # 기본 쿼리 예제
    try:
        examples.example_basic_queries()
    except Exception as e:
        print(f"기본 쿼리 예제 오류: {e}")

    # 작업 로깅 예제
    try:
        examples.example_job_logging()
        print("작업 로깅 예제 완료")
    except Exception as e:
        print(f"작업 로깅 예제 오류: {e}")

    print("\n=== 마이그레이션 완료 ===")
