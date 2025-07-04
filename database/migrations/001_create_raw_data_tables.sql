-- =====================================================
-- Weather Flick 외부 API 원데이터 관리 스키마
-- 작성일: 2025-07-04
-- =====================================================

-- 1. 통합 원본 데이터 테이블
CREATE TABLE IF NOT EXISTS api_raw_data (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    api_provider VARCHAR(50) NOT NULL,         -- 'KTO', 'KMA', 'GOOGLE', 'NAVER'
    endpoint VARCHAR(200) NOT NULL,            -- API 엔드포인트
    request_method VARCHAR(10) DEFAULT 'GET',  -- HTTP 메서드
    request_params JSONB,                      -- 요청 파라미터
    request_headers JSONB,                     -- 요청 헤더
    response_status INTEGER,                   -- HTTP 응답 코드
    raw_response JSONB NOT NULL,               -- 원본 API 응답
    response_size INTEGER,                     -- 응답 크기 (bytes)
    request_duration INTEGER,                  -- 요청 소요 시간 (ms)
    api_key_hash VARCHAR(64),                  -- 사용된 API 키 해시 (보안)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP,                      -- 데이터 만료 시간
    is_archived BOOLEAN DEFAULT FALSE,         -- 아카이브 여부
    file_path VARCHAR(500),                    -- 파일 시스템 백업 경로
    
    -- 인덱스
    CONSTRAINT chk_api_provider CHECK (api_provider IN ('KTO', 'KMA', 'GOOGLE', 'NAVER')),
    CONSTRAINT chk_response_status CHECK (response_status >= 100 AND response_status < 600)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_api_raw_data_provider_endpoint ON api_raw_data(api_provider, endpoint);
CREATE INDEX IF NOT EXISTS idx_api_raw_data_created_at ON api_raw_data(created_at);
CREATE INDEX IF NOT EXISTS idx_api_raw_data_expires_at ON api_raw_data(expires_at);
CREATE INDEX IF NOT EXISTS idx_api_raw_data_archived ON api_raw_data(is_archived);

-- 2. KTO API 메타데이터 테이블
CREATE TABLE IF NOT EXISTS kto_api_metadata (
    raw_data_id UUID PRIMARY KEY,
    content_type_id VARCHAR(10),               -- '12'(관광지), '32'(숙박) 등
    area_code VARCHAR(10),                     -- 지역 코드
    sigungu_code VARCHAR(10),                  -- 시군구 코드
    total_count INTEGER,                       -- 전체 건수
    page_no INTEGER,                           -- 페이지 번호
    num_of_rows INTEGER,                       -- 페이지당 건수
    sync_batch_id VARCHAR(50),                 -- 배치 실행 ID
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (raw_data_id) REFERENCES api_raw_data(id) ON DELETE CASCADE
);

-- 3. KMA API 메타데이터 테이블
CREATE TABLE IF NOT EXISTS kma_api_metadata (
    raw_data_id UUID PRIMARY KEY,
    base_date DATE,                            -- 기준 날짜
    base_time VARCHAR(4),                      -- 기준 시간 (HHMM)
    nx INTEGER,                                -- 격자 X
    ny INTEGER,                                -- 격자 Y
    forecast_type VARCHAR(20),                 -- 'ultra_srt_ncst', 'vilage_fcst'
    region_name VARCHAR(100),                  -- 지역명
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (raw_data_id) REFERENCES api_raw_data(id) ON DELETE CASCADE,
    CONSTRAINT chk_forecast_type CHECK (forecast_type IN ('ultra_srt_ncst', 'ultra_srt_fcst', 'vilage_fcst'))
);

-- 4. 데이터 변환 로그 테이블
CREATE TABLE IF NOT EXISTS data_transformation_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    raw_data_id UUID NOT NULL,
    target_table VARCHAR(100) NOT NULL,       -- 대상 테이블명
    transformation_rule VARCHAR(100),         -- 변환 규칙명
    input_record_count INTEGER DEFAULT 0,     -- 입력 레코드 수
    output_record_count INTEGER DEFAULT 0,    -- 출력 레코드 수
    error_count INTEGER DEFAULT 0,            -- 오류 레코드 수
    transformation_time_ms INTEGER,           -- 변환 소요 시간 (ms)
    status VARCHAR(20) DEFAULT 'pending',     -- 'pending', 'processing', 'success', 'partial_failure', 'failure'
    error_details JSONB,                      -- 오류 상세 정보
    quality_score DECIMAL(5,2),               -- 품질 점수 (0-100)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    
    FOREIGN KEY (raw_data_id) REFERENCES api_raw_data(id) ON DELETE CASCADE,
    CONSTRAINT chk_transformation_status CHECK (status IN ('pending', 'processing', 'success', 'partial_failure', 'failure'))
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_transformation_logs_status ON data_transformation_logs(status);
CREATE INDEX IF NOT EXISTS idx_transformation_logs_target_table ON data_transformation_logs(target_table);
CREATE INDEX IF NOT EXISTS idx_transformation_logs_created_at ON data_transformation_logs(created_at);

-- 5. 기존 테이블에 원본 데이터 참조 컬럼 추가
-- tourist_attractions 테이블 확장
DO $$ 
BEGIN
    -- raw_data_id 컬럼 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tourist_attractions' 
        AND column_name = 'raw_data_id'
    ) THEN
        ALTER TABLE tourist_attractions 
        ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
    END IF;
    
    -- last_sync_at 컬럼 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tourist_attractions' 
        AND column_name = 'last_sync_at'
    ) THEN
        ALTER TABLE tourist_attractions 
        ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    END IF;
    
    -- data_quality_score 컬럼 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tourist_attractions' 
        AND column_name = 'data_quality_score'
    ) THEN
        ALTER TABLE tourist_attractions 
        ADD COLUMN data_quality_score DECIMAL(5,2);
    END IF;
    
    -- processing_status 컬럼 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'tourist_attractions' 
        AND column_name = 'processing_status'
    ) THEN
        ALTER TABLE tourist_attractions 
        ADD COLUMN processing_status VARCHAR(20) DEFAULT 'processed';
    END IF;
END $$;

-- weather_forecast 테이블 확장
DO $$ 
BEGIN
    -- raw_data_id 컬럼 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'weather_forecast' 
        AND column_name = 'raw_data_id'
    ) THEN
        ALTER TABLE weather_forecast 
        ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
    END IF;
    
    -- last_sync_at 컬럼 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'weather_forecast' 
        AND column_name = 'last_sync_at'
    ) THEN
        ALTER TABLE weather_forecast 
        ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
    END IF;
    
    -- data_quality_score 컬럼 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'weather_forecast' 
        AND column_name = 'data_quality_score'
    ) THEN
        ALTER TABLE weather_forecast 
        ADD COLUMN data_quality_score DECIMAL(5,2);
    END IF;
END $$;

-- 6. batch_job_logs 테이블 생성 (누락된 테이블)
CREATE TABLE IF NOT EXISTS batch_job_logs (
    id SERIAL PRIMARY KEY,
    job_name VARCHAR(255) NOT NULL,
    job_type VARCHAR(100),
    status VARCHAR(50) DEFAULT 'pending',
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    execution_context JSONB,                   -- 실행 컨텍스트
    processed_records INTEGER DEFAULT 0,       -- 처리된 레코드 수
    failed_records INTEGER DEFAULT 0,          -- 실패한 레코드 수
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_batch_status CHECK (status IN ('pending', 'running', 'success', 'failure', 'cancelled'))
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_batch_job_logs_status ON batch_job_logs(status);
CREATE INDEX IF NOT EXISTS idx_batch_job_logs_job_name ON batch_job_logs(job_name);
CREATE INDEX IF NOT EXISTS idx_batch_job_logs_created_at ON batch_job_logs(created_at);

-- 7. 데이터 품질 체크 임계값 설정 테이블
CREATE TABLE IF NOT EXISTS data_quality_thresholds (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL UNIQUE,
    completeness_threshold DECIMAL(3,2) DEFAULT 0.90,  -- 완성도 임계값 (90%)
    validity_threshold DECIMAL(3,2) DEFAULT 0.95,      -- 유효성 임계값 (95%)
    consistency_threshold DECIMAL(3,2) DEFAULT 0.99,   -- 일관성 임계값 (99%)
    overall_threshold DECIMAL(3,2) DEFAULT 0.85,       -- 종합 품질 임계값 (85%)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 기본 임계값 설정
INSERT INTO data_quality_thresholds (table_name, completeness_threshold, validity_threshold, consistency_threshold, overall_threshold)
VALUES 
    ('tourist_attractions', 0.90, 0.95, 0.99, 0.85),
    ('weather_forecast', 0.95, 0.98, 0.99, 0.90),
    ('accommodations', 0.85, 0.90, 0.95, 0.80),
    ('festivals_events', 0.80, 0.90, 0.95, 0.75)
ON CONFLICT (table_name) DO NOTHING;

-- 8. 파티션 테이블 생성 (대용량 데이터 관리용)
-- api_raw_data 월별 파티션
CREATE TABLE IF NOT EXISTS api_raw_data_y2025m07 PARTITION OF api_raw_data 
FOR VALUES FROM ('2025-07-01') TO ('2025-08-01');

CREATE TABLE IF NOT EXISTS api_raw_data_y2025m08 PARTITION OF api_raw_data 
FOR VALUES FROM ('2025-08-01') TO ('2025-09-01');

-- 9. 뷰 생성 - 최근 데이터 조회용
CREATE OR REPLACE VIEW recent_api_calls AS
SELECT 
    api_provider,
    endpoint,
    COUNT(*) as call_count,
    AVG(request_duration) as avg_duration_ms,
    MAX(created_at) as last_call_at,
    COUNT(CASE WHEN response_status = 200 THEN 1 END) * 100.0 / COUNT(*) as success_rate
FROM api_raw_data 
WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY api_provider, endpoint
ORDER BY call_count DESC;

-- 10. 권한 설정
-- batch 사용자에게 필요한 권한 부여
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO aicc6;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO aicc6;

-- 스키마 생성 완료 로그
INSERT INTO batch_job_logs (job_name, job_type, status, started_at, completed_at)
VALUES ('schema_migration_001', 'schema', 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

COMMENT ON TABLE api_raw_data IS '외부 API 원본 응답 데이터 저장';
COMMENT ON TABLE kto_api_metadata IS 'KTO API 호출 메타데이터';
COMMENT ON TABLE kma_api_metadata IS 'KMA API 호출 메타데이터';
COMMENT ON TABLE data_transformation_logs IS '데이터 변환 과정 로그';
COMMENT ON TABLE batch_job_logs IS '배치 작업 실행 로그';
COMMENT ON TABLE data_quality_thresholds IS '데이터 품질 임계값 설정';