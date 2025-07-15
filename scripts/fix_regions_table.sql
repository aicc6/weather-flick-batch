-- =============================================
-- regions 테이블 구조 수정 스크립트
-- unified_regions 마이그레이션을 위한 컬럼 추가
-- =============================================

-- 1. 누락된 컬럼 추가
ALTER TABLE regions 
ADD COLUMN IF NOT EXISTS region_name_full VARCHAR,
ADD COLUMN IF NOT EXISTS region_name_en VARCHAR,
ADD COLUMN IF NOT EXISTS region_id VARCHAR PRIMARY KEY DEFAULT gen_random_uuid()::text,
ADD COLUMN IF NOT EXISTS center_latitude DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS center_longitude DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS administrative_code VARCHAR,
ADD COLUMN IF NOT EXISTS api_mappings JSONB DEFAULT '{}',
ADD COLUMN IF NOT EXISTS coordinate_info JSONB DEFAULT '{}';

-- 2. 기존 latitude/longitude 데이터를 center_latitude/center_longitude로 복사
UPDATE regions 
SET center_latitude = latitude,
    center_longitude = longitude
WHERE latitude IS NOT NULL 
  AND longitude IS NOT NULL
  AND center_latitude IS NULL;

-- 3. grid_x, grid_y 정보를 coordinate_info JSONB로 이동
UPDATE regions 
SET coordinate_info = jsonb_build_object(
    'kma_grid_nx', grid_x,
    'kma_grid_ny', grid_y,
    'transform_accuracy', 5.0,
    'calculation_method', 'manual',
    'is_verified', false
)
WHERE grid_x IS NOT NULL 
  AND grid_y IS NOT NULL;

-- 4. 기본 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_regions_region_code ON regions(region_code);
CREATE INDEX IF NOT EXISTS idx_regions_region_name ON regions(region_name);
CREATE INDEX IF NOT EXISTS idx_regions_is_active ON regions(is_active);
CREATE INDEX IF NOT EXISTS idx_regions_api_mappings ON regions USING GIN(api_mappings);
CREATE INDEX IF NOT EXISTS idx_regions_coordinate_info ON regions USING GIN(coordinate_info);

-- 5. 확인
SELECT 
    COUNT(*) as total_regions,
    COUNT(CASE WHEN api_mappings IS NOT NULL THEN 1 END) as with_api_mappings,
    COUNT(CASE WHEN coordinate_info IS NOT NULL THEN 1 END) as with_coordinates,
    COUNT(CASE WHEN center_latitude IS NOT NULL THEN 1 END) as with_center_coords
FROM regions;