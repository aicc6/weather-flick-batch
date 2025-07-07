-- 중복 데이터 상세 분석
-- 실제 삭제 전 중복 상황을 자세히 파악

-- 1. 중복 지역 상세 정보
SELECT 
    ur.region_name,
    ur.region_level,
    COUNT(*) as duplicate_count,
    array_agg(ur.region_id ORDER BY ur.created_at) as region_ids,
    array_agg(ur.region_code ORDER BY ur.created_at) as region_codes,
    array_agg(ur.administrative_code ORDER BY ur.created_at) as admin_codes,
    array_agg(ur.created_at ORDER BY ur.created_at) as created_dates
FROM unified_regions ur
GROUP BY ur.region_name, ur.region_level
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
LIMIT 10;

-- 2. 각 중복 그룹의 첫 번째 레코드 (유지할 것)
WITH duplicate_groups AS (
    SELECT 
        region_name,
        region_level,
        (array_agg(region_id ORDER BY created_at))[1] as keep_id,
        array_agg(region_id ORDER BY created_at DESC)[2:] as delete_ids
    FROM unified_regions
    GROUP BY region_name, region_level
    HAVING COUNT(*) > 1
)
SELECT 
    'Regions to keep' as action,
    COUNT(*) as count
FROM duplicate_groups

UNION ALL

SELECT 
    'Regions to delete' as action,
    SUM(array_length(delete_ids, 1)) as count
FROM duplicate_groups;

-- 3. 참조 관계 확인
SELECT 
    'Self references (parent_region_id)' as reference_type,
    COUNT(*) as count
FROM unified_regions 
WHERE parent_region_id IS NOT NULL

UNION ALL

SELECT 
    'API mappings references' as reference_type,
    COUNT(*) as count
FROM region_api_mappings

UNION ALL

SELECT 
    'Coordinate references' as reference_type,
    COUNT(*) as count
FROM coordinate_transformations;