-- 수동 중복 정리 스크립트
-- 가장 문제가 되는 중복들을 단계적으로 정리

BEGIN;

-- 1. 백업 테이블 생성
CREATE TABLE IF NOT EXISTS cleanup_backup AS SELECT * FROM unified_regions;

-- 2. 중복이 가장 많은 "중구" 정리 (7개 -> 1개)
-- 첫 번째 것을 제외하고 나머지 6개 삭제
WITH jung_gu_duplicates AS (
    SELECT region_id, row_number() OVER (ORDER BY created_at) as rn
    FROM unified_regions 
    WHERE region_name = '중구' AND region_level = 2
)
DELETE FROM unified_regions ur
WHERE ur.region_id IN (
    SELECT region_id FROM jung_gu_duplicates WHERE rn > 1
);

-- 3. "동구" 정리 (6개 -> 1개)
WITH dong_gu_duplicates AS (
    SELECT region_id, row_number() OVER (ORDER BY created_at) as rn
    FROM unified_regions 
    WHERE region_name = '동구' AND region_level = 2
)
DELETE FROM unified_regions ur
WHERE ur.region_id IN (
    SELECT region_id FROM dong_gu_duplicates WHERE rn > 1
);

-- 4. "서구" 정리 (5개 -> 1개)
WITH seo_gu_duplicates AS (
    SELECT region_id, row_number() OVER (ORDER BY created_at) as rn
    FROM unified_regions 
    WHERE region_name = '서구' AND region_level = 2
)
DELETE FROM unified_regions ur
WHERE ur.region_id IN (
    SELECT region_id FROM seo_gu_duplicates WHERE rn > 1
);

-- 5. "남구" 정리 (4개 -> 1개)
WITH nam_gu_duplicates AS (
    SELECT region_id, row_number() OVER (ORDER BY created_at) as rn
    FROM unified_regions 
    WHERE region_name = '남구' AND region_level = 2
)
DELETE FROM unified_regions ur
WHERE ur.region_id IN (
    SELECT region_id FROM nam_gu_duplicates WHERE rn > 1
);

-- 6. "북구" 정리 (4개 -> 1개)
WITH buk_gu_duplicates AS (
    SELECT region_id, row_number() OVER (ORDER BY created_at) as rn
    FROM unified_regions 
    WHERE region_name = '북구' AND region_level = 2
)
DELETE FROM unified_regions ur
WHERE ur.region_id IN (
    SELECT region_id FROM buk_gu_duplicates WHERE rn > 1
);

-- 7. 결과 확인
SELECT 
    'cleanup_result' as status,
    (SELECT COUNT(*) FROM unified_regions) as total_regions,
    (SELECT COUNT(*) FROM unified_regions WHERE region_name = '중구' AND region_level = 2) as jung_gu_count,
    (SELECT COUNT(*) FROM unified_regions WHERE region_name = '동구' AND region_level = 2) as dong_gu_count,
    (SELECT COUNT(*) FROM unified_regions WHERE region_name = '서구' AND region_level = 2) as seo_gu_count,
    (SELECT COUNT(*) FROM unified_regions WHERE region_name = '남구' AND region_level = 2) as nam_gu_count,
    (SELECT COUNT(*) FROM unified_regions WHERE region_name = '북구' AND region_level = 2) as buk_gu_count;

-- 8. 로그 기록
INSERT INTO region_sync_logs (
    sync_type, 
    sync_batch_id, 
    api_provider, 
    processed_count, 
    sync_status, 
    started_at, 
    completed_at
) VALUES (
    'mapping_sync',  -- 허용된 타입 사용
    'cleanup_' || to_char(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS'),
    'SYSTEM',
    (SELECT 318 - COUNT(*) FROM unified_regions),  -- 삭제된 개수
    'success',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

COMMIT;