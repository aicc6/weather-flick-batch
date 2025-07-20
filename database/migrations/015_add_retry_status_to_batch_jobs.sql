-- 배치 작업 실행 테이블에 retry_status 컬럼 추가
-- 날짜: 2025-07-18
-- 설명: batch_job_executions 테이블에 누락된 retry_status 컬럼을 추가하여 모델과 스키마 일치

-- 1. retry_status 컬럼 추가
ALTER TABLE batch_job_executions
ADD COLUMN IF NOT EXISTS retry_status VARCHAR(50);

-- 2. 기존 데이터에 대한 기본값 설정
UPDATE batch_job_executions
SET retry_status = 'NOT_RETRIED'
WHERE retry_status IS NULL;

-- 3. NOT NULL 제약 조건 추가
ALTER TABLE batch_job_executions
ALTER COLUMN retry_status SET NOT NULL;

-- 4. 인덱스 추가 (성능 최적화)
CREATE INDEX IF NOT EXISTS idx_batch_job_executions_retry_status
ON batch_job_executions(retry_status);

-- 5. 배치 작업 상세 로그 테이블도 확인 및 수정
-- batch_job_details 테이블이 없다면 생성
CREATE TABLE IF NOT EXISTS batch_job_details (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(255) NOT NULL REFERENCES batch_job_executions(id) ON DELETE CASCADE,
    log_level VARCHAR(20) NOT NULL DEFAULT 'INFO',
    message TEXT,
    details JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 6. 인덱스 추가
CREATE INDEX IF NOT EXISTS idx_batch_job_details_job_id
ON batch_job_details(job_id);

CREATE INDEX IF NOT EXISTS idx_batch_job_details_created_at
ON batch_job_details(created_at);

-- 7. 복합 인덱스 추가
CREATE INDEX IF NOT EXISTS idx_batch_job_details_job_created
ON batch_job_details(job_id, created_at);

-- 8. 마이그레이션 완료 로그
INSERT INTO migration_log (migration_name, applied_at, description)
VALUES (
    '015_add_retry_status_to_batch_jobs',
    CURRENT_TIMESTAMP,
    '배치 작업 테이블에 retry_status 컬럼 추가 및 관련 인덱스 생성'
) ON CONFLICT DO NOTHING;
