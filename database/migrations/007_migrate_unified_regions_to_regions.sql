-- =====================================================
-- UnifiedRegions를 Regions로 마이그레이션
-- 작성일: 2025-07-13
-- 설명: unified_regions 테이블의 데이터를 regions 테이블로 통합
-- =====================================================

-- 1. regions 테이블에 필요한 컬럼 추가
ALTER TABLE regions 
ADD COLUMN IF NOT EXISTS region_name_full VARCHAR(150),
ADD COLUMN IF NOT EXISTS region_name_en VARCHAR(100),
ADD COLUMN IF NOT EXISTS center_latitude DECIMAL(10, 8),
ADD COLUMN IF NOT EXISTS center_longitude DECIMAL(11, 8),
ADD COLUMN IF NOT EXISTS administrative_code VARCHAR(20),
ADD COLUMN IF NOT EXISTS boundary_data JSONB,
ADD COLUMN IF NOT EXISTS api_mappings JSONB,  -- API별 매핑 정보 통합
ADD COLUMN IF NOT EXISTS coordinate_info JSONB,  -- 좌표 변환 정보 통합
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

-- 인덱스 추가
CREATE INDEX IF NOT EXISTS idx_regions_name_full ON regions(region_name_full);
CREATE INDEX IF NOT EXISTS idx_regions_administrative_code ON regions(administrative_code);
CREATE INDEX IF NOT EXISTS idx_regions_is_active ON regions(is_active);
CREATE INDEX IF NOT EXISTS idx_regions_api_mappings ON regions USING GIN(api_mappings);

-- 2. 기존 unified_regions 데이터를 regions로 마이그레이션
-- 2.1. 먼저 기존 regions에 없는 unified_regions 데이터 삽입
INSERT INTO regions (
    region_code,
    region_name,
    region_name_full,
    region_name_en,
    parent_region_code,
    region_level,
    latitude,
    longitude,
    center_latitude,
    center_longitude,
    administrative_code,
    boundary_data,
    is_active,
    created_at,
    updated_at
)
SELECT 
    ur.region_code,
    ur.region_name,
    ur.region_name_full,
    ur.region_name_en,
    parent_ur.region_code as parent_region_code,
    ur.region_level,
    ur.center_latitude as latitude,
    ur.center_longitude as longitude,
    ur.center_latitude,
    ur.center_longitude,
    ur.administrative_code,
    ur.boundary_data,
    ur.is_active,
    ur.created_at,
    CURRENT_TIMESTAMP
FROM unified_regions ur
LEFT JOIN unified_regions parent_ur ON ur.parent_region_id = parent_ur.region_id
WHERE NOT EXISTS (
    SELECT 1 FROM regions r WHERE r.region_code = ur.region_code
);

-- 2.2. 기존 regions 데이터 업데이트
UPDATE regions r
SET 
    region_name_full = COALESCE(r.region_name_full, ur.region_name_full),
    region_name_en = ur.region_name_en,
    center_latitude = COALESCE(ur.center_latitude, r.latitude),
    center_longitude = COALESCE(ur.center_longitude, r.longitude),
    administrative_code = ur.administrative_code,
    boundary_data = ur.boundary_data,
    is_active = COALESCE(ur.is_active, TRUE),
    updated_at = CURRENT_TIMESTAMP
FROM unified_regions ur
WHERE r.region_code = ur.region_code;

-- 3. API 매핑 정보 통합 (region_api_mappings -> regions.api_mappings)
UPDATE regions r
SET api_mappings = subquery.mappings,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT 
        ur.region_code,
        jsonb_object_agg(
            ram.api_provider,
            jsonb_build_object(
                'api_region_code', ram.api_region_code,
                'api_region_name', ram.api_region_name,
                'additional_codes', ram.additional_codes,
                'mapping_confidence', ram.mapping_confidence
            )
        ) as mappings
    FROM unified_regions ur
    JOIN region_api_mappings ram ON ur.region_id = ram.region_id
    WHERE ram.is_active = true
    GROUP BY ur.region_code
) as subquery
WHERE r.region_code = subquery.region_code;

-- 4. 좌표 변환 정보 통합 (coordinate_transformations -> regions.coordinate_info)
UPDATE regions r
SET coordinate_info = subquery.coord_info,
    updated_at = CURRENT_TIMESTAMP
FROM (
    SELECT 
        ur.region_code,
        jsonb_build_object(
            'kma_grid_nx', ct.kma_grid_nx,
            'kma_grid_ny', ct.kma_grid_ny,
            'kma_station_code', ct.kma_station_code,
            'transform_accuracy', ct.transform_accuracy,
            'calculation_method', ct.calculation_method,
            'is_verified', ct.is_verified
        ) as coord_info
    FROM unified_regions ur
    JOIN coordinate_transformations ct ON ur.region_id = ct.region_id
    WHERE ct.is_verified = true
) as subquery
WHERE r.region_code = subquery.region_code;

-- 5. 마이그레이션 검증
DO $$
DECLARE
    unified_count INTEGER;
    regions_count INTEGER;
    mapping_count INTEGER;
    coord_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO unified_count FROM unified_regions WHERE is_active = true;
    SELECT COUNT(*) INTO regions_count FROM regions WHERE is_active = true;
    SELECT COUNT(*) INTO mapping_count FROM regions WHERE api_mappings IS NOT NULL;
    SELECT COUNT(*) INTO coord_count FROM regions WHERE coordinate_info IS NOT NULL;
    
    RAISE NOTICE '마이그레이션 결과:';
    RAISE NOTICE '- 원본 unified_regions 수: %', unified_count;
    RAISE NOTICE '- 마이그레이션된 regions 수: %', regions_count;
    RAISE NOTICE '- API 매핑 정보가 있는 지역 수: %', mapping_count;
    RAISE NOTICE '- 좌표 변환 정보가 있는 지역 수: %', coord_count;
END $$;

-- 6. 뷰 생성 (기존 코드 호환성을 위한 임시 조치)
CREATE OR REPLACE VIEW unified_regions_view AS
SELECT 
    md5(region_code)::uuid as region_id,  -- 임시 UUID 생성
    region_code,
    region_name,
    region_name_full,
    region_name_en,
    md5(parent_region_code)::uuid as parent_region_id,
    region_level,
    center_latitude,
    center_longitude,
    boundary_data,
    administrative_code,
    is_active,
    created_at,
    updated_at
FROM regions;

-- 7. 백업 테이블 생성 (안전을 위해)
CREATE TABLE IF NOT EXISTS unified_regions_backup AS 
SELECT * FROM unified_regions;

CREATE TABLE IF NOT EXISTS region_api_mappings_backup AS 
SELECT * FROM region_api_mappings;

CREATE TABLE IF NOT EXISTS coordinate_transformations_backup AS 
SELECT * FROM coordinate_transformations;

-- 8. 권한 설정
GRANT SELECT, INSERT, UPDATE, DELETE ON regions TO aicc6;
GRANT SELECT ON unified_regions_view TO aicc6;

-- 9. 마이그레이션 로그
INSERT INTO batch_job_logs (job_name, job_type, status, started_at, completed_at, processed_records, execution_context)
VALUES (
    'unified_regions_migration',
    'migration',
    'success',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP,
    (SELECT COUNT(*) FROM unified_regions),
    jsonb_build_object(
        'source_table', 'unified_regions',
        'target_table', 'regions',
        'migration_date', CURRENT_TIMESTAMP
    )
);

-- =====================================================
-- 주의사항:
-- 1. 이 스크립트는 되돌릴 수 있도록 백업 테이블을 생성합니다
-- 2. 실제 테이블 삭제는 다음 단계에서 진행합니다
-- 3. 애플리케이션 코드 수정 후 테스트가 완료되면 삭제합니다
-- =====================================================