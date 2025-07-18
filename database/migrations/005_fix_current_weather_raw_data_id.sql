-- =====================================================
-- weather_current 테이블 raw_data_id 컬럼 추가 마이그레이션
-- 작성일: 2025-07-07
-- 수정일: 2025-07-18 (테이블명 변경: current_weather -> weather_current)
-- 목적: 배치 실행 시 발생하는 raw_data_id 컬럼 오류 수정
-- =====================================================

-- 1. weather_current 테이블에 raw_data_id 컬럼 추가
DO $$ 
BEGIN
    -- raw_data_id 컬럼이 없으면 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'weather_current' 
        AND column_name = 'raw_data_id'
    ) THEN
        ALTER TABLE weather_current 
        ADD COLUMN raw_data_id VARCHAR(255);
        
        RAISE NOTICE 'weather_current 테이블에 raw_data_id 컬럼을 추가했습니다.';
    ELSE
        RAISE NOTICE 'weather_current 테이블에 raw_data_id 컬럼이 이미 존재합니다.';
    END IF;
    
    -- updated_at 컬럼이 없으면 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'weather_current' 
        AND column_name = 'updated_at'
    ) THEN
        ALTER TABLE weather_current 
        ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        
        RAISE NOTICE 'weather_current 테이블에 updated_at 컬럼을 추가했습니다.';
    ELSE
        RAISE NOTICE 'weather_current 테이블에 updated_at 컬럼이 이미 존재합니다.';
    END IF;
END $$;

-- 2. weather_forecasts 테이블의 raw_data_id 컬럼 타입 확인 및 수정
DO $$ 
BEGIN
    -- weather_forecasts 테이블의 raw_data_id가 UUID 타입인지 확인
    IF EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'weather_forecasts' 
        AND column_name = 'raw_data_id'
        AND data_type != 'uuid'
    ) THEN
        -- 기존 데이터가 있다면 백업하고 타입 변경
        RAISE NOTICE 'weather_forecasts.raw_data_id 컬럼 타입을 UUID로 변경합니다.';
        
        -- 잘못된 데이터 정리 (UUID가 아닌 문자열 데이터)
        UPDATE weather_forecasts 
        SET raw_data_id = NULL 
        WHERE raw_data_id IS NOT NULL 
        AND raw_data_id::text !~ '^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$';
        
        -- 컬럼 타입 변경
        ALTER TABLE weather_forecasts 
        ALTER COLUMN raw_data_id TYPE UUID USING raw_data_id::UUID;
        
    ELSE
        RAISE NOTICE 'weather_forecasts.raw_data_id 컬럼 타입이 이미 UUID입니다.';
    END IF;
END $$;

-- 3. 인덱스 생성 (성능 향상)
CREATE INDEX IF NOT EXISTS idx_weather_current_raw_data_id ON weather_current(raw_data_id);
CREATE INDEX IF NOT EXISTS idx_weather_current_weather_date ON weather_current(weather_date);
CREATE INDEX IF NOT EXISTS idx_weather_forecasts_raw_data_id ON weather_forecasts(raw_data_id);

-- 4. 마이그레이션 로그 기록
INSERT INTO batch_job_logs (job_name, job_type, status, started_at, completed_at, execution_context)
VALUES (
    'fix_weather_current_raw_data_id', 
    'schema_migration', 
    'success', 
    CURRENT_TIMESTAMP, 
    CURRENT_TIMESTAMP,
    '{"migration": "005_fix_weather_current_raw_data_id", "description": "weather_current 테이블 raw_data_id 컬럼 오류 수정"}'::jsonb
);

COMMENT ON COLUMN weather_current.raw_data_id IS '원본 API 데이터 참조 ID (문자열)';
COMMENT ON COLUMN weather_current.updated_at IS '레코드 마지막 수정 시간';