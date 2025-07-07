-- 안전한 중복 데이터 제거 스크립트
-- 외래 키 제약 조건을 고려한 단계적 정리

BEGIN;

-- 1. 백업 테이블 생성
CREATE TABLE IF NOT EXISTS unified_regions_backup AS SELECT * FROM unified_regions;
CREATE TABLE IF NOT EXISTS region_api_mappings_backup AS SELECT * FROM region_api_mappings;
CREATE TABLE IF NOT EXISTS coordinate_transformations_backup AS SELECT * FROM coordinate_transformations;

-- 2. 중복 지역 식별 및 통합 테이블 생성
CREATE TEMP TABLE regions_to_keep AS
SELECT 
    region_name,
    region_level,
    (array_agg(region_id ORDER BY created_at DESC))[1] as keep_region_id,
    array_agg(region_id ORDER BY created_at DESC) as all_region_ids
FROM unified_regions
GROUP BY region_name, region_level
HAVING COUNT(*) > 1;

-- 3. 중복 제거할 지역 ID 목록
CREATE TEMP TABLE regions_to_delete AS
SELECT 
    unnest(all_region_ids[2:]) as delete_region_id,
    keep_region_id
FROM regions_to_keep;

-- 4. API 매핑 테이블 업데이트 (중복 지역을 유지할 지역으로 변경)
UPDATE region_api_mappings 
SET region_id = rtd.keep_region_id
FROM regions_to_delete rtd
WHERE region_api_mappings.region_id = rtd.delete_region_id;

-- 5. 좌표 변환 테이블 업데이트
UPDATE coordinate_transformations 
SET region_id = rtd.keep_region_id
FROM regions_to_delete rtd
WHERE coordinate_transformations.region_id = rtd.delete_region_id;

-- 6. 이제 안전하게 중복 지역 삭제
DELETE FROM unified_regions 
WHERE region_id IN (SELECT delete_region_id FROM regions_to_delete);

-- 7. 결과 확인
SELECT 
    'cleanup_summary' as summary_type,
    (SELECT COUNT(*) FROM unified_regions) as regions_after_cleanup,
    (SELECT COUNT(*) FROM region_api_mappings) as mappings_after_cleanup,
    (SELECT COUNT(*) FROM coordinate_transformations) as coordinates_after_cleanup,
    (SELECT COUNT(*) FROM regions_to_delete) as deleted_regions;

-- 8. 중복 제거 로그 기록
INSERT INTO region_sync_logs (
    sync_type, 
    sync_batch_id, 
    api_provider, 
    processed_count, 
    sync_status, 
    started_at, 
    completed_at
) VALUES (
    'cleanup_duplicates',
    'safe_cleanup_' || to_char(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS'),
    'SYSTEM',
    (SELECT COUNT(*) FROM regions_to_delete),
    'success',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

COMMIT;