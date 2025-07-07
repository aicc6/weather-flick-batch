-- 중복 데이터 확인 쿼리
-- 1. 통합 지역 테이블 중복 확인
SELECT 
    '통합 지역 테이블 중복' as check_type,
    region_name,
    COUNT(*) as duplicate_count
FROM unified_regions 
GROUP BY region_name, region_level
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 2. API 매핑 테이블 중복 확인
SELECT 
    'API 매핑 중복' as check_type,
    api_provider,
    api_region_code,
    COUNT(*) as duplicate_count
FROM region_api_mappings
GROUP BY api_provider, api_region_code
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 3. 좌표 변환 정보 중복 확인
SELECT 
    '좌표 변환 중복' as check_type,
    wgs84_latitude,
    wgs84_longitude,
    COUNT(*) as duplicate_count
FROM coordinate_transformations
GROUP BY wgs84_latitude, wgs84_longitude
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC;

-- 4. 전체 데이터 현황
SELECT 
    '전체 현황' as category,
    (SELECT COUNT(*) FROM unified_regions) as total_regions,
    (SELECT COUNT(*) FROM region_api_mappings) as total_mappings,
    (SELECT COUNT(*) FROM coordinate_transformations) as total_coordinates;