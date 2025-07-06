-- Weather Forecasts 테이블 추가 인덱스 최적화
-- 실행일: 2025-01-06
-- 목적: 조회 성능 향상을 위한 추가 인덱스 생성

-- 배치 작업에서 자주 사용되는 쿼리 패턴에 최적화된 인덱스 생성

-- 1. 지역별 최신 예보 조회용 인덱스
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_region_latest
ON weather_forecasts(region_code, forecast_date DESC, forecast_time DESC);

-- 2. 날짜 범위 검색용 인덱스 (데이터 정리 작업용)
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_date_only
ON weather_forecasts(forecast_date);

-- 3. 생성 시간 기반 인덱스 (배치 작업 모니터링용)
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_created_at
ON weather_forecasts(created_at);

-- 4. nx, ny 좌표 기반 인덱스 (지역 매핑용)
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_coordinates
ON weather_forecasts(nx, ny);

-- 5. 예보 타입별 인덱스 (단기/중기 예보 구분)
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_type_date
ON weather_forecasts(forecast_type, forecast_date)
WHERE forecast_type IS NOT NULL;

-- 6. 복합 조건 검색용 인덱스 (API 응답 최적화)
-- 참고: WHERE 절에 날짜 함수 사용시 IMMUTABLE 함수만 가능하므로 조건 제거
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_api_query
ON weather_forecasts(region_code, forecast_date, forecast_time);

-- 7. 데이터 품질 체크용 인덱스
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_quality
ON weather_forecasts(region_code, forecast_date)
WHERE min_temp IS NULL OR max_temp IS NULL OR weather_condition IS NULL;

-- 통계 정보 업데이트
ANALYZE weather_forecasts;

-- 인덱스 생성 결과 확인
SELECT 
    schemaname,
    tablename,
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'weather_forecasts'
    AND indexname LIKE 'idx_weather_forecasts_%'
ORDER BY indexname;

-- 테이블 크기 및 인덱스 사용량 확인
SELECT 
    pg_size_pretty(pg_total_relation_size('weather_forecasts')) as total_size,
    pg_size_pretty(pg_relation_size('weather_forecasts')) as table_size,
    pg_size_pretty(pg_total_relation_size('weather_forecasts') - pg_relation_size('weather_forecasts')) as indexes_size;

COMMENT ON INDEX idx_weather_forecasts_region_latest IS '지역별 최신 예보 조회 최적화';
COMMENT ON INDEX idx_weather_forecasts_date_only IS '날짜 범위 검색 최적화';
COMMENT ON INDEX idx_weather_forecasts_created_at IS '배치 작업 모니터링 최적화';
COMMENT ON INDEX idx_weather_forecasts_coordinates IS '좌표 기반 지역 매핑 최적화';
COMMENT ON INDEX idx_weather_forecasts_type_date IS '예보 타입별 조회 최적화';
COMMENT ON INDEX idx_weather_forecasts_api_query IS 'API 응답 최적화 (최근 7일)';
COMMENT ON INDEX idx_weather_forecasts_quality IS '데이터 품질 체크 최적화';