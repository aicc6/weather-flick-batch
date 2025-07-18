"""
배치 INSERT 최적화 모듈

이 모듈은 weather-flick-batch 시스템의 데이터베이스 INSERT 성능을 최적화합니다.
주요 기능:
- 배치 단위 INSERT/UPSERT 처리
- 트랜잭션 크기 최적화
- 메모리 효율적인 대용량 데이터 처리
- 오류 복구 및 부분 실패 처리
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Any, Tuple
from dataclasses import dataclass
import psycopg2.extras

from app.core.database_manager import DatabaseManager
from app.core.logger import get_logger


@dataclass
class BatchConfig:
    """배치 처리 설정"""
    batch_size: int = 1000  # 배치당 레코드 수
    max_memory_mb: int = 100  # 최대 메모리 사용량 (MB)
    transaction_timeout: int = 30  # 트랜잭션 타임아웃 (초)
    retry_attempts: int = 3  # 재시도 횟수
    retry_delay: float = 1.0  # 재시도 지연 (초)


@dataclass
class BatchResult:
    """배치 처리 결과"""
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    execution_time: float = 0.0
    error_details: List[str] = None
    
    def __post_init__(self):
        if self.error_details is None:
            self.error_details = []
    
    @property
    def success_rate(self) -> float:
        if self.total_records == 0:
            return 0.0
        return self.successful_records / self.total_records
    
    @property
    def records_per_second(self) -> float:
        if self.execution_time == 0:
            return 0.0
        return self.successful_records / self.execution_time


class BatchInsertOptimizer:
    """배치 INSERT 최적화 클래스"""
    
    def __init__(self, db_manager: DatabaseManager = None, config: BatchConfig = None):
        self.db_manager = db_manager or DatabaseManager()
        self.config = config or BatchConfig()
        self.logger = get_logger(__name__)
        
    async def batch_insert_weather_current(
        self, 
        weather_data: List[Dict[str, Any]], 
        raw_data_id: str
    ) -> BatchResult:
        """현재 날씨 데이터 배치 INSERT"""
        
        start_time = datetime.now()
        result = BatchResult(total_records=len(weather_data))
        
        if not weather_data:
            return result
            
        try:
            # 데이터 변환
            batch_data = self._prepare_current_weather_batch(weather_data, raw_data_id)
            
            # UPSERT 쿼리 (중복 데이터 처리)
            query = """
                INSERT INTO weather_current (
                    region_code, region_name, weather_date, year, month, day,
                    avg_temp, max_temp, min_temp, humidity, precipitation, 
                    wind_speed, weather_condition, visibility, uv_index, raw_data_id
                ) VALUES %s
                ON CONFLICT (region_code, weather_date) 
                DO UPDATE SET
                    avg_temp = EXCLUDED.avg_temp,
                    max_temp = EXCLUDED.max_temp,
                    min_temp = EXCLUDED.min_temp,
                    humidity = EXCLUDED.humidity,
                    precipitation = EXCLUDED.precipitation,
                    wind_speed = EXCLUDED.wind_speed,
                    weather_condition = EXCLUDED.weather_condition,
                    visibility = EXCLUDED.visibility,
                    uv_index = EXCLUDED.uv_index,
                    raw_data_id = EXCLUDED.raw_data_id
            """
            
            # 배치 실행
            success_count = await self._execute_batch_with_retry(query, batch_data)
            result.successful_records = success_count
            
        except Exception as e:
            self.logger.error(f"현재 날씨 배치 INSERT 실패: {e}")
            result.error_details.append(str(e))
            
        finally:
            result.failed_records = result.total_records - result.successful_records
            result.execution_time = (datetime.now() - start_time).total_seconds()
            
        return result
    
    async def batch_insert_weather_forecast(
        self, 
        forecast_data: List[Dict[str, Any]], 
        raw_data_id: str
    ) -> BatchResult:
        """날씨 예보 데이터 배치 INSERT"""
        
        start_time = datetime.now()
        result = BatchResult(total_records=len(forecast_data))
        
        if not forecast_data:
            return result
            
        try:
            # 데이터 변환
            batch_data = self._prepare_forecast_batch(forecast_data, raw_data_id)
            
            # INSERT 쿼리 (예보 데이터는 일반적으로 중복되지 않음)
            query = """
                INSERT INTO weather_forecast (
                    region_code, nx, ny, forecast_date, forecast_time, 
                    temperature, min_temp, max_temp, weather_condition, 
                    forecast_type, raw_data_id, created_at
                ) VALUES %s
                ON CONFLICT (region_code, forecast_date, forecast_time) 
                DO UPDATE SET
                    temperature = EXCLUDED.temperature,
                    min_temp = EXCLUDED.min_temp,
                    max_temp = EXCLUDED.max_temp,
                    weather_condition = EXCLUDED.weather_condition,
                    raw_data_id = EXCLUDED.raw_data_id,
                    updated_at = CURRENT_TIMESTAMP
            """
            
            # 배치 실행
            success_count = await self._execute_batch_with_retry(query, batch_data)
            result.successful_records = success_count
            
        except Exception as e:
            self.logger.error(f"날씨 예보 배치 INSERT 실패: {e}")
            result.error_details.append(str(e))
            
        finally:
            result.failed_records = result.total_records - result.successful_records
            result.execution_time = (datetime.now() - start_time).total_seconds()
            
        return result
    
    async def batch_insert_tourism_data(
        self, 
        tourism_data: List[Dict[str, Any]], 
        table_name: str,
        conflict_columns: List[str] = None
    ) -> BatchResult:
        """관광지 데이터 배치 INSERT (범용)"""
        
        start_time = datetime.now()
        result = BatchResult(total_records=len(tourism_data))
        
        if not tourism_data:
            return result
            
        try:
            # 동적 쿼리 생성
            columns = list(tourism_data[0].keys())
            placeholders = ', '.join(['%s'] * len(columns))
            columns_str = ', '.join(columns)
            
            # 기본 INSERT 쿼리
            base_query = f"""
                INSERT INTO {table_name} ({columns_str}) 
                VALUES %s
            """
            
            # UPSERT 처리 (충돌 컬럼이 있는 경우)
            if conflict_columns:
                update_set = ', '.join([
                    f"{col} = EXCLUDED.{col}" 
                    for col in columns 
                    if col not in conflict_columns
                ])
                conflict_str = ', '.join(conflict_columns)
                query = f"{base_query} ON CONFLICT ({conflict_str}) DO UPDATE SET {update_set}"
            else:
                query = base_query
            
            # 데이터 변환
            batch_data = [tuple(item[col] for col in columns) for item in tourism_data]
            
            # 배치 실행
            success_count = await self._execute_batch_with_retry(query, batch_data)
            result.successful_records = success_count
            
        except Exception as e:
            self.logger.error(f"관광지 데이터 배치 INSERT 실패 [{table_name}]: {e}")
            result.error_details.append(str(e))
            
        finally:
            result.failed_records = result.total_records - result.successful_records
            result.execution_time = (datetime.now() - start_time).total_seconds()
            
        return result
    
    def _prepare_current_weather_batch(
        self, 
        weather_data: List[Dict[str, Any]], 
        raw_data_id: str
    ) -> List[Tuple]:
        """현재 날씨 데이터를 배치 INSERT 형식으로 변환"""
        
        batch_data = []
        for data in weather_data:
            # 날짜 정보 추출
            weather_date = data.get("weather_date", datetime.now().date())
            if isinstance(weather_date, datetime):
                weather_date = weather_date.date()
            elif isinstance(weather_date, str):
                weather_date = datetime.strptime(weather_date, "%Y-%m-%d").date()
                
            row = (
                data.get("region_code", "00"),
                data.get("region_name", ""),
                weather_date,
                weather_date.year,
                weather_date.month,
                weather_date.day,
                data.get("avg_temp", data.get("temperature")),
                data.get("max_temp", data.get("temperature")),
                data.get("min_temp", data.get("temperature")),
                data.get("humidity"),
                data.get("precipitation", 0),
                data.get("wind_speed"),
                data.get("weather_condition", ""),
                data.get("visibility"),
                data.get("uv_index"),
                raw_data_id
            )
            batch_data.append(row)
            
        return batch_data
    
    def _prepare_forecast_batch(
        self, 
        forecast_data: List[Dict[str, Any]], 
        raw_data_id: str
    ) -> List[Tuple]:
        """날씨 예보 데이터를 배치 INSERT 형식으로 변환"""
        
        batch_data = []
        for data in forecast_data:
            row = (
                data.get("region_code", "00"),
                data.get("nx", 60),  # 기본값 (서울)
                data.get("ny", 127),  # 기본값 (서울)
                data.get("forecast_date"),
                data.get("forecast_time", "1200"),
                data.get("temperature"),
                data.get("min_temp"),
                data.get("max_temp"),
                data.get("weather_condition", ""),
                data.get("forecast_type", "short"),
                raw_data_id,
                datetime.now()
            )
            batch_data.append(row)
            
        return batch_data
    
    async def _execute_batch_with_retry(
        self, 
        query: str, 
        batch_data: List[Tuple]
    ) -> int:
        """재시도 로직이 포함된 배치 실행"""
        
        for attempt in range(self.config.retry_attempts):
            try:
                return await self._execute_batch_internal(query, batch_data)
                
            except Exception as e:
                if attempt < self.config.retry_attempts - 1:
                    self.logger.warning(
                        f"배치 실행 실패 (재시도 {attempt + 1}/{self.config.retry_attempts}): {e}"
                    )
                    await asyncio.sleep(self.config.retry_delay * (2 ** attempt))  # 지수 백오프
                else:
                    raise
        
        return 0
    
    async def _execute_batch_internal(
        self, 
        query: str, 
        batch_data: List[Tuple]
    ) -> int:
        """내부 배치 실행 로직"""
        
        if not batch_data:
            return 0
        
        # 메모리 사용량 체크
        estimated_memory = self._estimate_memory_usage(batch_data)
        if estimated_memory > self.config.max_memory_mb:
            # 청크 단위로 분할 처리
            return await self._execute_chunked_batch(query, batch_data)
        
        # 단일 배치 처리
        total_processed = 0
        
        # VALUES 절을 사용한 배치 INSERT (psycopg2 방식)
        if self.db_manager.use_async:
            # 비동기 처리 (asyncpg 사용 시)
            total_processed = await self._execute_async_batch(query, batch_data)
        else:
            # 동기 처리 (psycopg2 사용 시)
            total_processed = self._execute_sync_batch(query, batch_data)
        
        self.logger.info(f"배치 실행 완료: {total_processed}건 처리")
        return total_processed
    
    async def _execute_chunked_batch(
        self, 
        query: str, 
        batch_data: List[Tuple]
    ) -> int:
        """청크 단위 배치 처리 (메모리 최적화)"""
        
        total_processed = 0
        chunk_size = self.config.batch_size
        
        for i in range(0, len(batch_data), chunk_size):
            chunk = batch_data[i:i + chunk_size]
            chunk_processed = await self._execute_batch_internal(query, chunk)
            total_processed += chunk_processed
            
            # 진행률 로깅
            progress = min((i + chunk_size) / len(batch_data) * 100, 100)
            self.logger.debug(f"배치 처리 진행률: {progress:.1f}% ({total_processed}건 완료)")
        
        return total_processed
    
    async def _execute_async_batch(
        self, 
        query: str, 
        batch_data: List[Tuple]
    ) -> int:
        """비동기 배치 실행 (asyncpg)"""
        
        # asyncpg의 executemany 사용
        try:
            # VALUES 절 변환이 필요한 경우 처리
            if '%s' in query:
                # psycopg2 스타일을 asyncpg 스타일로 변환
                asyncpg_query = query.replace('%s', '${}').format(*range(1, len(batch_data[0]) + 1))
                result = await self.db_manager.execute_many_async(asyncpg_query, batch_data)
            else:
                result = await self.db_manager.execute_many_async(query, batch_data)
                
            return len(batch_data) if result else 0
            
        except Exception as e:
            self.logger.error(f"비동기 배치 실행 실패: {e}")
            raise
    
    def _execute_sync_batch(
        self, 
        query: str, 
        batch_data: List[Tuple]
    ) -> int:
        """동기 배치 실행 (psycopg2)"""
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # execute_values 사용 (가장 빠른 배치 INSERT 방법)
                psycopg2.extras.execute_values(
                    cursor,
                    query,
                    batch_data,
                    template=None,
                    page_size=self.config.batch_size
                )
                return cursor.rowcount
                
        except Exception as e:
            self.logger.error(f"동기 배치 실행 실패: {e}")
            raise
    
    def _estimate_memory_usage(self, batch_data: List[Tuple]) -> float:
        """배치 데이터의 예상 메모리 사용량 계산 (MB)"""
        
        if not batch_data:
            return 0.0
        
        # 샘플 행으로 추정
        sample_row = batch_data[0]
        estimated_row_size = sum(
            len(str(val).encode('utf-8')) if val is not None else 8 
            for val in sample_row
        )
        
        total_size_bytes = estimated_row_size * len(batch_data)
        return total_size_bytes / (1024 * 1024)  # MB 변환


# 편의 함수들
async def optimize_weather_current_insert(
    weather_data: List[Dict[str, Any]], 
    raw_data_id: str,
    config: BatchConfig = None
) -> BatchResult:
    """현재 날씨 데이터 최적화 INSERT"""
    optimizer = BatchInsertOptimizer(config=config)
    return await optimizer.batch_insert_weather_current(weather_data, raw_data_id)


async def optimize_weather_forecast_insert(
    forecast_data: List[Dict[str, Any]], 
    raw_data_id: str,
    config: BatchConfig = None
) -> BatchResult:
    """날씨 예보 데이터 최적화 INSERT"""
    optimizer = BatchInsertOptimizer(config=config)
    return await optimizer.batch_insert_weather_forecast(forecast_data, raw_data_id)


async def optimize_tourism_data_insert(
    tourism_data: List[Dict[str, Any]], 
    table_name: str,
    conflict_columns: List[str] = None,
    config: BatchConfig = None
) -> BatchResult:
    """관광지 데이터 최적화 INSERT"""
    optimizer = BatchInsertOptimizer(config=config)
    return await optimizer.batch_insert_tourism_data(tourism_data, table_name, conflict_columns)