-- 중복 데이터 제거 스크립트
-- 주의: 프로덕션 환경에서는 반드시 백업 후 실행

BEGIN;

-- 1. 백업 테이블 생성
CREATE TABLE IF NOT EXISTS unified_regions_backup AS SELECT * FROM unified_regions;
CREATE TABLE IF NOT EXISTS region_api_mappings_backup AS SELECT * FROM region_api_mappings;
CREATE TABLE IF NOT EXISTS coordinate_transformations_backup AS SELECT * FROM coordinate_transformations;

-- 2. 통합 지역 테이블 중복 제거
-- 가장 최근에 생성된 레코드를 유지 (created_at 기준)
DELETE FROM unified_regions ur1
WHERE ur1.region_id NOT IN (
    SELECT 
        (array_agg(ur2.region_id ORDER BY ur2.created_at DESC))[1]
    FROM unified_regions ur2
    WHERE ur2.region_name = ur1.region_name 
      AND ur2.region_level = ur1.region_level
    GROUP BY ur2.region_name, ur2.region_level
);

-- 3. API 매핑 테이블 중복 제거
-- 매핑 신뢰도가 높은 레코드 우선 유지
DELETE FROM region_api_mappings ram1
WHERE ram1.mapping_id NOT IN (
    SELECT 
        (array_agg(ram2.mapping_id ORDER BY ram2.mapping_confidence DESC, ram2.created_at DESC))[1]
    FROM region_api_mappings ram2
    WHERE ram2.api_provider = ram1.api_provider
      AND ram2.api_region_code = ram1.api_region_code
    GROUP BY ram2.api_provider, ram2.api_region_code
);

-- 4. 좌표 변환 정보 중복 제거
-- 검증된 좌표 정보 우선 유지
DELETE FROM coordinate_transformations ct1
WHERE ct1.transform_id NOT IN (
    SELECT 
        (array_agg(ct2.transform_id ORDER BY ct2.is_verified DESC, ct2.transform_accuracy ASC, ct2.created_at DESC))[1]
    FROM coordinate_transformations ct2
    WHERE ABS(ct2.wgs84_latitude - ct1.wgs84_latitude) < 0.0001
      AND ABS(ct2.wgs84_longitude - ct1.wgs84_longitude) < 0.0001
      AND ct2.region_id = ct1.region_id
);

-- 5. 제거 결과 확인
SELECT 
    'cleanup_summary' as summary_type,
    (SELECT COUNT(*) FROM unified_regions) as regions_after_cleanup,
    (SELECT COUNT(*) FROM region_api_mappings) as mappings_after_cleanup,
    (SELECT COUNT(*) FROM coordinate_transformations) as coordinates_after_cleanup;

-- 6. 중복 제거 로그 기록
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
    'manual_cleanup_' || to_char(CURRENT_TIMESTAMP, 'YYYYMMDD_HH24MISS'),
    'SYSTEM',
    (SELECT COUNT(*) FROM unified_regions_backup) - (SELECT COUNT(*) FROM unified_regions),
    'success',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

COMMIT;