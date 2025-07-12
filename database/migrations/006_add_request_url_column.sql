-- =====================================================
-- request_url 컬럼 추가 마이그레이션
-- 작성일: 2025-07-12
-- 설명: api_raw_data 테이블에 누락된 request_url 컬럼 추가
-- =====================================================

-- api_raw_data 테이블에 request_url 컬럼 추가
ALTER TABLE api_raw_data 
ADD COLUMN IF NOT EXISTS request_url TEXT;

-- 인덱스 추가 (선택적)
CREATE INDEX IF NOT EXISTS idx_api_raw_data_request_url 
ON api_raw_data(request_url);

-- 코멘트 추가
COMMENT ON COLUMN api_raw_data.request_url IS '완전한 요청 URL (파라미터 포함)';