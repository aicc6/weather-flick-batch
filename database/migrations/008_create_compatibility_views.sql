-- ====================================================================
-- 호환성 뷰 생성 스크립트
-- 목적: unified_regions 관련 테이블을 참조하는 레거시 코드를 위한 임시 뷰
-- 작성일: 2025-01-14
-- ====================================================================

-- 1. unified_regions_view 생성
DROP VIEW IF EXISTS unified_regions_view CASCADE;

CREATE VIEW unified_regions_view AS
SELECT 
    -- UUID 생성 (region_code 기반)
    CASE 
        WHEN length(region_code) = 36 AND region_code ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' 
        THEN region_code::uuid
        ELSE md5(region_code)::uuid
    END AS region_id,
    region_code,
    region_name,
    region_name_full,
    region_name_en,
    parent_region_code AS parent_region_id,
    region_level,
    center_latitude,
    center_longitude,
    boundary_data,
    administrative_code,
    is_active,
    created_at,
    updated_at
FROM regions;

COMMENT ON VIEW unified_regions_view IS '레거시 코드 호환성을 위한 임시 뷰 (unified_regions 테이블 에뮬레이션)';

-- 2. region_api_mappings_view 생성 (임시로 빈 뷰)
DROP VIEW IF EXISTS region_api_mappings_view CASCADE;

CREATE VIEW region_api_mappings_view AS
SELECT 
    -- mapping_id 생성
    md5(region_code || '-KTO')::uuid AS mapping_id,
    -- region_id (unified_regions_view와 동일한 로직)
    CASE 
        WHEN length(region_code) = 36 AND region_code ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' 
        THEN region_code::uuid
        ELSE md5(region_code)::uuid
    END AS region_id,
    'KTO' AS api_provider,
    region_code AS api_region_code,
    region_name AS api_region_name,
    '{}'::jsonb AS additional_codes,
    1.0::float AS mapping_confidence,
    true AS is_active,
    created_at AS last_verified,
    created_at,
    updated_at
FROM regions
WHERE region_code IS NOT NULL;

COMMENT ON VIEW region_api_mappings_view IS '레거시 코드 호환성을 위한 임시 뷰 (region_api_mappings 테이블 에뮬레이션)';

-- 3. coordinate_transformations_view 생성
DROP VIEW IF EXISTS coordinate_transformations_view CASCADE;

CREATE VIEW coordinate_transformations_view AS
SELECT 
    -- transform_id 생성
    md5(region_code || '-coordinate-transform')::uuid AS transform_id,
    -- region_id (unified_regions_view와 동일한 로직)
    CASE 
        WHEN length(region_code) = 36 AND region_code ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' 
        THEN region_code::uuid
        ELSE md5(region_code)::uuid
    END AS region_id,
    -- WGS84 좌표 (기본 좌표 사용)
    COALESCE(center_latitude, latitude) AS wgs84_latitude,
    COALESCE(center_longitude, longitude) AS wgs84_longitude,
    -- KMA 그리드 좌표
    grid_x AS kma_grid_nx,
    grid_y AS kma_grid_ny,
    -- KMA 관측소 코드
    NULL::varchar AS kma_station_code,
    -- 변환 정확도
    5.0::float AS transform_accuracy,
    -- 계산 방법
    'auto'::varchar AS calculation_method,
    -- 검증 여부
    false AS is_verified,
    created_at,
    updated_at
FROM regions
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL;

COMMENT ON VIEW coordinate_transformations_view IS '레거시 코드 호환성을 위한 임시 뷰 (coordinate_transformations 테이블 에뮬레이션)';

-- 4. 인덱스 힌트를 위한 통계 업데이트
ANALYZE regions;

-- 5. 뷰 권한 설정 (필요한 경우)
-- GRANT SELECT ON unified_regions_view TO your_app_user;
-- GRANT SELECT ON region_api_mappings_view TO your_app_user;
-- GRANT SELECT ON coordinate_transformations_view TO your_app_user;

-- 확인 쿼리
SELECT 
    'unified_regions_view' as view_name, 
    count(*) as row_count 
FROM unified_regions_view
UNION ALL
SELECT 
    'region_api_mappings_view', 
    count(*) 
FROM region_api_mappings_view
UNION ALL
SELECT 
    'coordinate_transformations_view', 
    count(*) 
FROM coordinate_transformations_view;