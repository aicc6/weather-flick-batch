-- =====================================================
-- Weather Flick 지역 정보 통합 관리 시스템 스키마
-- 기상청 API와 한국관광공사 API 지역 정보 통합 관리
-- 작성일: 2025-07-05
-- =====================================================

-- 1. 통합 지역 마스터 테이블
CREATE TABLE IF NOT EXISTS unified_regions (
    region_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    region_code VARCHAR(20) NOT NULL UNIQUE,     -- 통합 지역 코드 (KTO 기준)
    region_name VARCHAR(100) NOT NULL,           -- 표준 지역명
    region_name_full VARCHAR(150),               -- 전체 행정구역명
    region_name_en VARCHAR(100),                 -- 영문명
    parent_region_id UUID REFERENCES unified_regions(region_id),
    region_level INTEGER NOT NULL,               -- 1:시도, 2:시군구, 3:읍면동
    center_latitude DECIMAL(10, 8),              -- 중심점 위도
    center_longitude DECIMAL(11, 8),             -- 중심점 경도
    boundary_data JSONB,                         -- 경계 정보 (GeoJSON 형태)
    administrative_code VARCHAR(20),             -- 행정구역 코드
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_unified_regions_code ON unified_regions(region_code);
CREATE INDEX IF NOT EXISTS idx_unified_regions_name ON unified_regions(region_name);
CREATE INDEX IF NOT EXISTS idx_unified_regions_level ON unified_regions(region_level);
CREATE INDEX IF NOT EXISTS idx_unified_regions_parent ON unified_regions(parent_region_id);
CREATE INDEX IF NOT EXISTS idx_unified_regions_location ON unified_regions(center_latitude, center_longitude);

-- 2. API별 지역 코드 매핑 테이블
CREATE TABLE IF NOT EXISTS region_api_mappings (
    mapping_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    region_id UUID NOT NULL REFERENCES unified_regions(region_id),
    api_provider VARCHAR(20) NOT NULL,           -- 'KTO', 'KMA', 'NAVER', 'GOOGLE'
    api_region_code VARCHAR(50),                 -- API별 지역 코드
    api_region_name VARCHAR(100),                -- API별 지역명
    additional_codes JSONB,                      -- nx, ny, 관측소코드 등 추가 정보
    mapping_confidence DECIMAL(3, 2),           -- 매핑 신뢰도 (0.00-1.00)
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT chk_api_provider CHECK (api_provider IN ('KTO', 'KMA', 'NAVER', 'GOOGLE')),
    CONSTRAINT chk_mapping_confidence CHECK (mapping_confidence >= 0.00 AND mapping_confidence <= 1.00),
    CONSTRAINT unique_api_mapping UNIQUE (api_provider, api_region_code, region_id)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_mappings_provider_code ON region_api_mappings(api_provider, api_region_code);
CREATE INDEX IF NOT EXISTS idx_mappings_region ON region_api_mappings(region_id);
CREATE INDEX IF NOT EXISTS idx_mappings_active ON region_api_mappings(is_active);

-- 3. 좌표 변환 정보 테이블
CREATE TABLE IF NOT EXISTS coordinate_transformations (
    transform_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    region_id UUID NOT NULL REFERENCES unified_regions(region_id),
    wgs84_latitude DECIMAL(10, 8) NOT NULL,      -- WGS84 위도
    wgs84_longitude DECIMAL(11, 8) NOT NULL,     -- WGS84 경도
    kma_grid_nx INTEGER,                         -- 기상청 격자 X
    kma_grid_ny INTEGER,                         -- 기상청 격자 Y
    kma_station_code VARCHAR(10),                -- 기상청 관측소 코드
    transform_accuracy DECIMAL(5, 2),           -- 변환 정확도 (km)
    calculation_method VARCHAR(50),              -- 변환 방법 ('lcc_projection', 'approximate', 'manual')
    is_verified BOOLEAN DEFAULT FALSE,           -- 검증 완료 여부
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_coordinate_transform UNIQUE (region_id, wgs84_latitude, wgs84_longitude),
    CONSTRAINT chk_calculation_method CHECK (calculation_method IN ('lcc_projection', 'approximate', 'manual', 'interpolation'))
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_transforms_region ON coordinate_transformations(region_id);
CREATE INDEX IF NOT EXISTS idx_transforms_coordinates ON coordinate_transformations(wgs84_latitude, wgs84_longitude);
CREATE INDEX IF NOT EXISTS idx_transforms_grid ON coordinate_transformations(kma_grid_nx, kma_grid_ny);
CREATE INDEX IF NOT EXISTS idx_transforms_station ON coordinate_transformations(kma_station_code);
CREATE INDEX IF NOT EXISTS idx_transforms_verified ON coordinate_transformations(is_verified);

-- 4. 지역 동기화 로그 테이블
CREATE TABLE IF NOT EXISTS region_sync_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sync_type VARCHAR(20) NOT NULL,              -- 'kto_sync', 'kma_sync', 'mapping_sync'
    sync_batch_id VARCHAR(50),                   -- 배치 실행 ID
    api_provider VARCHAR(20),                    -- 동기화 대상 API
    processed_count INTEGER DEFAULT 0,           -- 처리된 지역 수
    created_count INTEGER DEFAULT 0,             -- 신규 생성된 지역 수
    updated_count INTEGER DEFAULT 0,             -- 업데이트된 지역 수
    error_count INTEGER DEFAULT 0,               -- 오류 발생 수
    sync_status VARCHAR(20) DEFAULT 'pending',   -- 'pending', 'running', 'success', 'failure'
    error_details JSONB,                         -- 오류 상세 정보
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    
    CONSTRAINT chk_sync_type CHECK (sync_type IN ('kto_sync', 'kma_sync', 'mapping_sync', 'coordinate_sync')),
    CONSTRAINT chk_sync_status CHECK (sync_status IN ('pending', 'running', 'success', 'failure', 'partial'))
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_sync_logs_type ON region_sync_logs(sync_type);
CREATE INDEX IF NOT EXISTS idx_sync_logs_status ON region_sync_logs(sync_status);
CREATE INDEX IF NOT EXISTS idx_sync_logs_started ON region_sync_logs(started_at);

-- 5. 기본 지역 데이터 삽입 (KTO 표준 시도 코드)
INSERT INTO unified_regions (region_code, region_name, region_name_full, region_name_en, region_level, administrative_code)
VALUES 
    ('1', '서울', '서울특별시', 'Seoul', 1, '11'),
    ('2', '인천', '인천광역시', 'Incheon', 1, '28'),
    ('3', '대전', '대전광역시', 'Daejeon', 1, '30'),
    ('4', '대구', '대구광역시', 'Daegu', 1, '27'),
    ('5', '광주', '광주광역시', 'Gwangju', 1, '29'),
    ('6', '부산', '부산광역시', 'Busan', 1, '26'),
    ('7', '울산', '울산광역시', 'Ulsan', 1, '31'),
    ('8', '세종', '세종특별자치시', 'Sejong', 1, '36'),
    ('31', '경기', '경기도', 'Gyeonggi', 1, '41'),
    ('32', '강원', '강원특별자치도', 'Gangwon', 1, '51'),
    ('33', '충북', '충청북도', 'Chungbuk', 1, '43'),
    ('34', '충남', '충청남도', 'Chungnam', 1, '44'),
    ('35', '경북', '경상북도', 'Gyeongbuk', 1, '47'),
    ('36', '경남', '경상남도', 'Gyeongnam', 1, '48'),
    ('37', '전북', '전북특별자치도', 'Jeonbuk', 1, '52'),
    ('38', '전남', '전라남도', 'Jeonnam', 1, '46'),
    ('39', '제주', '제주특별자치도', 'Jeju', 1, '50')
ON CONFLICT (region_code) DO NOTHING;

-- 6. 기상청 지역 매핑 데이터 삽입
INSERT INTO region_api_mappings (region_id, api_provider, api_region_code, api_region_name, additional_codes, mapping_confidence)
SELECT 
    ur.region_id,
    'KMA' as api_provider,
    CASE ur.region_name
        WHEN '서울' THEN 'seoul'
        WHEN '부산' THEN 'busan'
        WHEN '대구' THEN 'daegu'
        WHEN '인천' THEN 'incheon'
        WHEN '광주' THEN 'gwangju'
        WHEN '대전' THEN 'daejeon'
        WHEN '울산' THEN 'ulsan'
        WHEN '세종' THEN 'sejong'
        WHEN '제주' THEN 'jeju'
        ELSE NULL
    END as api_region_code,
    ur.region_name as api_region_name,
    CASE ur.region_name
        WHEN '서울' THEN '{"nx": 60, "ny": 127, "station_code": "108"}'::jsonb
        WHEN '부산' THEN '{"nx": 98, "ny": 76, "station_code": "159"}'::jsonb
        WHEN '대구' THEN '{"nx": 89, "ny": 90, "station_code": "143"}'::jsonb
        WHEN '인천' THEN '{"nx": 55, "ny": 124, "station_code": "112"}'::jsonb
        WHEN '광주' THEN '{"nx": 58, "ny": 74, "station_code": "156"}'::jsonb
        WHEN '대전' THEN '{"nx": 67, "ny": 100, "station_code": "133"}'::jsonb
        WHEN '울산' THEN '{"nx": 102, "ny": 84, "station_code": "152"}'::jsonb
        WHEN '세종' THEN '{"nx": 66, "ny": 103, "station_code": null}'::jsonb
        WHEN '제주' THEN '{"nx": 52, "ny": 38, "station_code": "184"}'::jsonb
        ELSE NULL
    END as additional_codes,
    CASE ur.region_name
        WHEN '서울' THEN 1.00
        WHEN '부산' THEN 1.00
        WHEN '대구' THEN 1.00
        WHEN '인천' THEN 1.00
        WHEN '광주' THEN 1.00
        WHEN '대전' THEN 1.00
        WHEN '울산' THEN 1.00
        WHEN '세종' THEN 0.90
        WHEN '제주' THEN 1.00
        ELSE 0.00
    END as mapping_confidence
FROM unified_regions ur
WHERE ur.region_level = 1 
  AND ur.region_name IN ('서울', '부산', '대구', '인천', '광주', '대전', '울산', '세종', '제주')
ON CONFLICT (api_provider, api_region_code, region_id) DO NOTHING;

-- 7. KTO API 매핑 데이터 삽입
INSERT INTO region_api_mappings (region_id, api_provider, api_region_code, api_region_name, additional_codes, mapping_confidence)
SELECT 
    ur.region_id,
    'KTO' as api_provider,
    ur.region_code as api_region_code,
    ur.region_name as api_region_name,
    ('{"area_code": "' || ur.region_code || '", "administrative_code": "' || ur.administrative_code || '"}')::jsonb as additional_codes,
    1.00 as mapping_confidence
FROM unified_regions ur
WHERE ur.region_level = 1
ON CONFLICT (api_provider, api_region_code, region_id) DO NOTHING;

-- 8. 중심점 좌표 데이터 삽입 (대략적인 시도 중심점)
UPDATE unified_regions SET 
    center_latitude = CASE region_name
        WHEN '서울' THEN 37.5665
        WHEN '부산' THEN 35.1796
        WHEN '대구' THEN 35.8714
        WHEN '인천' THEN 37.4563
        WHEN '광주' THEN 35.1595
        WHEN '대전' THEN 36.3504
        WHEN '울산' THEN 35.5384
        WHEN '세종' THEN 36.4800
        WHEN '제주' THEN 33.4996
        WHEN '경기' THEN 37.4138
        WHEN '강원' THEN 37.8228
        WHEN '충북' THEN 36.8
        WHEN '충남' THEN 36.5
        WHEN '경북' THEN 36.4
        WHEN '경남' THEN 35.4606
        WHEN '전북' THEN 35.7175
        WHEN '전남' THEN 34.8679
        ELSE NULL
    END,
    center_longitude = CASE region_name
        WHEN '서울' THEN 126.9780
        WHEN '부산' THEN 129.0756
        WHEN '대구' THEN 128.6014
        WHEN '인천' THEN 126.7052
        WHEN '광주' THEN 126.8526
        WHEN '대전' THEN 127.3845
        WHEN '울산' THEN 129.3114
        WHEN '세종' THEN 127.2890
        WHEN '제주' THEN 126.5312
        WHEN '경기' THEN 127.5183
        WHEN '강원' THEN 128.1555
        WHEN '충북' THEN 127.7
        WHEN '충남' THEN 126.8
        WHEN '경북' THEN 128.9
        WHEN '경남' THEN 128.2132
        WHEN '전북' THEN 127.153
        WHEN '전남' THEN 126.9910
        ELSE NULL
    END
WHERE region_level = 1;

-- 9. 좌표 변환 정보 삽입 (기상청 격자 좌표가 있는 지역)
INSERT INTO coordinate_transformations (region_id, wgs84_latitude, wgs84_longitude, kma_grid_nx, kma_grid_ny, kma_station_code, transform_accuracy, calculation_method, is_verified)
SELECT 
    ur.region_id,
    ur.center_latitude,
    ur.center_longitude,
    (ram.additional_codes->>'nx')::integer as kma_grid_nx,
    (ram.additional_codes->>'ny')::integer as kma_grid_ny,
    ram.additional_codes->>'station_code' as kma_station_code,
    5.0 as transform_accuracy,  -- 5km 격자 기준
    'manual' as calculation_method,
    true as is_verified
FROM unified_regions ur
JOIN region_api_mappings ram ON ur.region_id = ram.region_id
WHERE ram.api_provider = 'KMA' 
  AND ram.additional_codes->>'nx' IS NOT NULL
  AND ram.additional_codes->>'ny' IS NOT NULL
  AND ur.center_latitude IS NOT NULL
  AND ur.center_longitude IS NOT NULL
ON CONFLICT (region_id, wgs84_latitude, wgs84_longitude) DO NOTHING;

-- 10. 뷰 생성 - 통합 지역 정보 조회용
CREATE OR REPLACE VIEW unified_region_details AS
SELECT 
    ur.region_id,
    ur.region_code,
    ur.region_name,
    ur.region_name_full,
    ur.region_name_en,
    ur.region_level,
    ur.center_latitude,
    ur.center_longitude,
    ur.administrative_code,
    -- KTO 매핑 정보
    kto_map.api_region_code as kto_area_code,
    kto_map.additional_codes->>'administrative_code' as kto_admin_code,
    -- KMA 매핑 정보
    kma_map.api_region_code as kma_region_code,
    (kma_map.additional_codes->>'nx')::integer as kma_grid_nx,
    (kma_map.additional_codes->>'ny')::integer as kma_grid_ny,
    kma_map.additional_codes->>'station_code' as kma_station_code,
    -- 좌표 변환 정보
    ct.transform_accuracy,
    ct.is_verified as coordinate_verified
FROM unified_regions ur
LEFT JOIN region_api_mappings kto_map ON ur.region_id = kto_map.region_id AND kto_map.api_provider = 'KTO'
LEFT JOIN region_api_mappings kma_map ON ur.region_id = kma_map.region_id AND kma_map.api_provider = 'KMA'
LEFT JOIN coordinate_transformations ct ON ur.region_id = ct.region_id;

-- 11. 권한 설정
GRANT SELECT, INSERT, UPDATE, DELETE ON unified_regions TO aicc6;
GRANT SELECT, INSERT, UPDATE, DELETE ON region_api_mappings TO aicc6;
GRANT SELECT, INSERT, UPDATE, DELETE ON coordinate_transformations TO aicc6;
GRANT SELECT, INSERT, UPDATE, DELETE ON region_sync_logs TO aicc6;
GRANT SELECT ON unified_region_details TO aicc6;

-- 12. 스키마 생성 완료 로그
INSERT INTO batch_job_logs (job_name, job_type, status, started_at, completed_at, processed_records)
VALUES ('region_unification_schema_003', 'schema', 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 
        (SELECT COUNT(*) FROM unified_regions));

-- 테이블 코멘트 추가
COMMENT ON TABLE unified_regions IS '통합 지역 마스터 테이블 - KTO와 KMA API 지역 정보 통합 관리';
COMMENT ON TABLE region_api_mappings IS 'API별 지역 코드 매핑 테이블 - 각 API의 지역 코드를 통합 지역 ID에 매핑';
COMMENT ON TABLE coordinate_transformations IS '좌표 변환 정보 테이블 - WGS84와 기상청 격자 좌표 간 변환 정보';
COMMENT ON TABLE region_sync_logs IS '지역 동기화 로그 테이블 - 지역 정보 동기화 과정 추적';
COMMENT ON VIEW unified_region_details IS '통합 지역 정보 조회 뷰 - 지역별 모든 API 매핑 정보 통합 조회';

-- 마이그레이션 완료 확인
SELECT 
    '003_create_region_unification_tables.sql' as migration_file,
    'completed' as status,
    CURRENT_TIMESTAMP as completed_at,
    (SELECT COUNT(*) FROM unified_regions) as total_regions,
    (SELECT COUNT(*) FROM region_api_mappings) as total_mappings,
    (SELECT COUNT(*) FROM coordinate_transformations) as total_coordinates;