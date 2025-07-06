-- Weather Forecasts 테이블 최적화: forecast_date 컬럼 타입 변경
-- 실행일: 2025-01-06
-- 목적: VARCHAR(8) -> DATE 타입 변경으로 성능 및 데이터 무결성 향상

-- 주의: 이 스크립트는 단계별로 실행되어야 합니다
-- CONCURRENTLY 인덱스는 트랜잭션 외부에서 별도 실행

-- 트랜잭션 시작 (인덱스 생성 제외)
BEGIN;

-- 1단계: 새로운 DATE 타입 컬럼 추가
ALTER TABLE weather_forecasts 
ADD COLUMN forecast_date_new DATE;

-- 2단계: 기존 데이터를 새 컬럼으로 변환
UPDATE weather_forecasts 
SET forecast_date_new = TO_DATE(forecast_date, 'YYYYMMDD')
WHERE forecast_date ~ '^\d{8}$' AND LENGTH(forecast_date) = 8;

-- 3단계: 변환 결과 검증
DO $$
DECLARE
    original_count INTEGER;
    converted_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO original_count FROM weather_forecasts;
    SELECT COUNT(*) INTO converted_count FROM weather_forecasts WHERE forecast_date_new IS NOT NULL;
    
    IF original_count != converted_count THEN
        RAISE EXCEPTION '데이터 변환 실패: 원본 %건, 변환 %건', original_count, converted_count;
    END IF;
    
    RAISE NOTICE '데이터 변환 성공: %건 처리 완료', converted_count;
END $$;

-- 4단계: NOT NULL 제약조건 추가
ALTER TABLE weather_forecasts 
ALTER COLUMN forecast_date_new SET NOT NULL;

-- 5단계: 기존 컬럼 삭제 및 새 컬럼 이름 변경
ALTER TABLE weather_forecasts 
DROP COLUMN forecast_date;

ALTER TABLE weather_forecasts 
RENAME COLUMN forecast_date_new TO forecast_date;

-- 커밋 (인덱스 생성 전)
COMMIT;

-- 6단계: 성능 최적화를 위한 인덱스 생성 (트랜잭션 외부)
-- 주의: 이 부분은 별도로 실행되어야 함
CREATE INDEX idx_weather_forecasts_region_date 
ON weather_forecasts(region_code, forecast_date);

CREATE INDEX idx_weather_forecasts_date_time 
ON weather_forecasts(forecast_date, forecast_time);

-- 7단계: 통계 정보 업데이트
ANALYZE weather_forecasts;

-- 변경 결과 확인
SELECT 
    column_name,
    data_type,
    is_nullable,
    column_default
FROM information_schema.columns 
WHERE table_name = 'weather_forecasts' 
    AND column_name = 'forecast_date';

-- 인덱스 확인
SELECT 
    indexname,
    indexdef
FROM pg_indexes 
WHERE tablename = 'weather_forecasts' 
    AND indexname LIKE '%forecast%';

COMMENT ON COLUMN weather_forecasts.forecast_date IS '예보 날짜 (DATE 타입으로 최적화됨, 기존 YYYYMMDD VARCHAR에서 변환)';