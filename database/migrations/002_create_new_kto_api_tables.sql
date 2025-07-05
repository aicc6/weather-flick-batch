-- =====================================================
-- 신규 KTO API 테이블 생성 스크립트
-- 작성일: 2025-07-05
-- 목적: detailPetTour2, lclsSystmCode2, areaBasedSyncList2, ldongCode2 API 지원
-- =====================================================

-- 1. 반려동물 동반여행 정보 테이블 (detailPetTour2)
CREATE TABLE IF NOT EXISTS pet_tour_info (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id VARCHAR(50),                    -- 콘텐츠ID
    content_type_id VARCHAR(10),               -- 콘텐츠타입ID
    title VARCHAR(500),                        -- 제목
    address VARCHAR(500),                      -- 주소
    latitude DECIMAL(10,8),                    -- 위도
    longitude DECIMAL(11,8),                   -- 경도
    area_code VARCHAR(10),                     -- 지역코드
    sigungu_code VARCHAR(10),                  -- 시군구코드
    tel VARCHAR(100),                          -- 전화번호
    homepage TEXT,                             -- 홈페이지
    overview TEXT,                             -- 개요
    cat1 VARCHAR(10),                          -- 대분류
    cat2 VARCHAR(10),                          -- 중분류  
    cat3 VARCHAR(10),                          -- 소분류
    first_image TEXT,                          -- 대표이미지(원본)
    first_image2 TEXT,                         -- 대표이미지(썸네일)
    pet_acpt_abl VARCHAR(500),                 -- 반려동물동반가능정보
    pet_info TEXT,                             -- 반려동물관련추가정보
    raw_data_id UUID REFERENCES api_raw_data(id) ON DELETE SET NULL,
    data_quality_score DECIMAL(5,2),
    processing_status VARCHAR(20) DEFAULT 'processed',
    last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. 분류체계 코드 테이블 (lclsSystmCode2)
CREATE TABLE IF NOT EXISTS classification_system_codes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(10) NOT NULL,                 -- 분류코드
    name VARCHAR(200) NOT NULL,                -- 분류명
    parent_code VARCHAR(10),                   -- 상위분류코드
    level_depth INTEGER DEFAULT 1,             -- 분류깊이
    sort_order INTEGER DEFAULT 0,              -- 정렬순서
    use_yn CHAR(1) DEFAULT 'Y',               -- 사용여부
    raw_data_id UUID REFERENCES api_raw_data(id) ON DELETE SET NULL,
    data_quality_score DECIMAL(5,2),
    processing_status VARCHAR(20) DEFAULT 'processed',
    last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(code)
);

-- 3. 지역기반 동기화 목록 테이블 (areaBasedSyncList2)
CREATE TABLE IF NOT EXISTS area_based_sync_list (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id VARCHAR(50) NOT NULL,           -- 콘텐츠ID
    content_type_id VARCHAR(10),               -- 콘텐츠타입ID
    title VARCHAR(500),                        -- 제목
    area_code VARCHAR(10),                     -- 지역코드
    sigungu_code VARCHAR(10),                  -- 시군구코드
    cat1 VARCHAR(10),                          -- 대분류
    cat2 VARCHAR(10),                          -- 중분류
    cat3 VARCHAR(10),                          -- 소분류
    addr1 VARCHAR(500),                        -- 주소
    addr2 VARCHAR(500),                        -- 상세주소
    zipcode VARCHAR(10),                       -- 우편번호
    latitude DECIMAL(10,8),                    -- 위도
    longitude DECIMAL(11,8),                   -- 경도
    tel VARCHAR(100),                          -- 전화번호
    first_image TEXT,                          -- 대표이미지(원본)
    first_image2 TEXT,                         -- 대표이미지(썸네일)
    created_time VARCHAR(14),                  -- 콘텐츠최초등록일
    modified_time VARCHAR(14),                 -- 콘텐츠최종수정일
    book_tour CHAR(1),                         -- 교과서속여행지여부
    raw_data_id UUID REFERENCES api_raw_data(id) ON DELETE SET NULL,
    data_quality_score DECIMAL(5,2),
    processing_status VARCHAR(20) DEFAULT 'processed',
    last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(content_id)
);

-- 4. 법정동 코드 테이블 (ldongCode2) 
CREATE TABLE IF NOT EXISTS legal_dong_codes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code VARCHAR(10) NOT NULL,                 -- 법정동코드
    name VARCHAR(100) NOT NULL,                -- 법정동명
    parent_code VARCHAR(10),                   -- 상위법정동코드  
    full_name VARCHAR(200),                    -- 전체명 (시도+시군구+읍면동)
    sido_name VARCHAR(50),                     -- 시도명
    sigungu_name VARCHAR(100),                 -- 시군구명
    eupmyeondong_name VARCHAR(100),            -- 읍면동명
    level_depth INTEGER DEFAULT 1,             -- 행정구역 깊이
    use_yn CHAR(1) DEFAULT 'Y',               -- 사용여부
    raw_data_id UUID REFERENCES api_raw_data(id) ON DELETE SET NULL,
    data_quality_score DECIMAL(5,2),
    processing_status VARCHAR(20) DEFAULT 'processed',
    last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(code)
);

-- 인덱스 생성
-- pet_tour_info 인덱스
CREATE INDEX IF NOT EXISTS idx_pet_tour_info_content_id ON pet_tour_info(content_id);
CREATE INDEX IF NOT EXISTS idx_pet_tour_info_area_codes ON pet_tour_info(area_code, sigungu_code);
CREATE INDEX IF NOT EXISTS idx_pet_tour_info_location ON pet_tour_info(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_pet_tour_info_sync_at ON pet_tour_info(last_sync_at);

-- classification_system_codes 인덱스
CREATE INDEX IF NOT EXISTS idx_classification_codes_code ON classification_system_codes(code);
CREATE INDEX IF NOT EXISTS idx_classification_codes_parent ON classification_system_codes(parent_code);
CREATE INDEX IF NOT EXISTS idx_classification_codes_level ON classification_system_codes(level_depth);

-- area_based_sync_list 인덱스
CREATE INDEX IF NOT EXISTS idx_area_sync_list_content_id ON area_based_sync_list(content_id);
CREATE INDEX IF NOT EXISTS idx_area_sync_list_area_codes ON area_based_sync_list(area_code, sigungu_code);
CREATE INDEX IF NOT EXISTS idx_area_sync_list_location ON area_based_sync_list(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_area_sync_list_modified ON area_based_sync_list(modified_time);

-- legal_dong_codes 인덱스
CREATE INDEX IF NOT EXISTS idx_legal_dong_codes_code ON legal_dong_codes(code);
CREATE INDEX IF NOT EXISTS idx_legal_dong_codes_parent ON legal_dong_codes(parent_code);
CREATE INDEX IF NOT EXISTS idx_legal_dong_codes_level ON legal_dong_codes(level_depth);
CREATE INDEX IF NOT EXISTS idx_legal_dong_codes_sido ON legal_dong_codes(sido_name);

-- 기존 테이블들과 유사한 구조로 확장 (필요시)
-- cultural_facilities, festivals_events, travel_courses, leisure_sports, accommodations, shopping, restaurants 테이블들도 
-- raw_data_id, data_quality_score, processing_status, last_sync_at 컬럼 추가 확인

DO $$ 
BEGIN
    -- cultural_facilities 테이블 확장
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'cultural_facilities') THEN
        -- raw_data_id 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'cultural_facilities' 
            AND column_name = 'raw_data_id'
        ) THEN
            ALTER TABLE cultural_facilities 
            ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
        END IF;
        
        -- last_sync_at 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'cultural_facilities' 
            AND column_name = 'last_sync_at'
        ) THEN
            ALTER TABLE cultural_facilities 
            ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- data_quality_score 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'cultural_facilities' 
            AND column_name = 'data_quality_score'
        ) THEN
            ALTER TABLE cultural_facilities 
            ADD COLUMN data_quality_score DECIMAL(5,2);
        END IF;
        
        -- processing_status 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'cultural_facilities' 
            AND column_name = 'processing_status'
        ) THEN
            ALTER TABLE cultural_facilities 
            ADD COLUMN processing_status VARCHAR(20) DEFAULT 'processed';
        END IF;
    END IF;

    -- festivals_events 테이블 확장
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'festivals_events') THEN
        -- raw_data_id 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'festivals_events' 
            AND column_name = 'raw_data_id'
        ) THEN
            ALTER TABLE festivals_events 
            ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
        END IF;
        
        -- last_sync_at 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'festivals_events' 
            AND column_name = 'last_sync_at'
        ) THEN
            ALTER TABLE festivals_events 
            ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- data_quality_score 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'festivals_events' 
            AND column_name = 'data_quality_score'
        ) THEN
            ALTER TABLE festivals_events 
            ADD COLUMN data_quality_score DECIMAL(5,2);
        END IF;
        
        -- processing_status 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'festivals_events' 
            AND column_name = 'processing_status'
        ) THEN
            ALTER TABLE festivals_events 
            ADD COLUMN processing_status VARCHAR(20) DEFAULT 'processed';
        END IF;
    END IF;

    -- travel_courses 테이블 확장
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'travel_courses') THEN
        -- raw_data_id 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'travel_courses' 
            AND column_name = 'raw_data_id'
        ) THEN
            ALTER TABLE travel_courses 
            ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
        END IF;
        
        -- last_sync_at 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'travel_courses' 
            AND column_name = 'last_sync_at'
        ) THEN
            ALTER TABLE travel_courses 
            ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- data_quality_score 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'travel_courses' 
            AND column_name = 'data_quality_score'
        ) THEN
            ALTER TABLE travel_courses 
            ADD COLUMN data_quality_score DECIMAL(5,2);
        END IF;
        
        -- processing_status 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'travel_courses' 
            AND column_name = 'processing_status'
        ) THEN
            ALTER TABLE travel_courses 
            ADD COLUMN processing_status VARCHAR(20) DEFAULT 'processed';
        END IF;
    END IF;

    -- leisure_sports 테이블 확장
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'leisure_sports') THEN
        -- raw_data_id 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'leisure_sports' 
            AND column_name = 'raw_data_id'
        ) THEN
            ALTER TABLE leisure_sports 
            ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
        END IF;
        
        -- last_sync_at 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'leisure_sports' 
            AND column_name = 'last_sync_at'
        ) THEN
            ALTER TABLE leisure_sports 
            ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- data_quality_score 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'leisure_sports' 
            AND column_name = 'data_quality_score'
        ) THEN
            ALTER TABLE leisure_sports 
            ADD COLUMN data_quality_score DECIMAL(5,2);
        END IF;
        
        -- processing_status 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'leisure_sports' 
            AND column_name = 'processing_status'
        ) THEN
            ALTER TABLE leisure_sports 
            ADD COLUMN processing_status VARCHAR(20) DEFAULT 'processed';
        END IF;
    END IF;

    -- accommodations 테이블 확장
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'accommodations') THEN
        -- raw_data_id 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'accommodations' 
            AND column_name = 'raw_data_id'
        ) THEN
            ALTER TABLE accommodations 
            ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
        END IF;
        
        -- last_sync_at 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'accommodations' 
            AND column_name = 'last_sync_at'
        ) THEN
            ALTER TABLE accommodations 
            ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- data_quality_score 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'accommodations' 
            AND column_name = 'data_quality_score'
        ) THEN
            ALTER TABLE accommodations 
            ADD COLUMN data_quality_score DECIMAL(5,2);
        END IF;
        
        -- processing_status 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'accommodations' 
            AND column_name = 'processing_status'
        ) THEN
            ALTER TABLE accommodations 
            ADD COLUMN processing_status VARCHAR(20) DEFAULT 'processed';
        END IF;
    END IF;

    -- shopping 테이블 확장
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'shopping') THEN
        -- raw_data_id 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'shopping' 
            AND column_name = 'raw_data_id'
        ) THEN
            ALTER TABLE shopping 
            ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
        END IF;
        
        -- last_sync_at 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'shopping' 
            AND column_name = 'last_sync_at'
        ) THEN
            ALTER TABLE shopping 
            ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- data_quality_score 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'shopping' 
            AND column_name = 'data_quality_score'
        ) THEN
            ALTER TABLE shopping 
            ADD COLUMN data_quality_score DECIMAL(5,2);
        END IF;
        
        -- processing_status 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'shopping' 
            AND column_name = 'processing_status'
        ) THEN
            ALTER TABLE shopping 
            ADD COLUMN processing_status VARCHAR(20) DEFAULT 'processed';
        END IF;
    END IF;

    -- restaurants 테이블 확장
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'restaurants') THEN
        -- raw_data_id 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'restaurants' 
            AND column_name = 'raw_data_id'
        ) THEN
            ALTER TABLE restaurants 
            ADD COLUMN raw_data_id UUID REFERENCES api_raw_data(id);
        END IF;
        
        -- last_sync_at 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'restaurants' 
            AND column_name = 'last_sync_at'
        ) THEN
            ALTER TABLE restaurants 
            ADD COLUMN last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
        END IF;
        
        -- data_quality_score 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'restaurants' 
            AND column_name = 'data_quality_score'
        ) THEN
            ALTER TABLE restaurants 
            ADD COLUMN data_quality_score DECIMAL(5,2);
        END IF;
        
        -- processing_status 컬럼 추가
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'restaurants' 
            AND column_name = 'processing_status'
        ) THEN
            ALTER TABLE restaurants 
            ADD COLUMN processing_status VARCHAR(20) DEFAULT 'processed';
        END IF;
    END IF;
END $$;

-- 데이터 품질 임계값 설정 추가
INSERT INTO data_quality_thresholds (table_name, completeness_threshold, validity_threshold, consistency_threshold, overall_threshold)
VALUES 
    ('pet_tour_info', 0.85, 0.90, 0.95, 0.80),
    ('classification_system_codes', 0.98, 0.99, 0.99, 0.95),
    ('area_based_sync_list', 0.90, 0.95, 0.98, 0.85),
    ('legal_dong_codes', 0.98, 0.99, 0.99, 0.95),
    ('cultural_facilities', 0.85, 0.90, 0.95, 0.80),
    ('travel_courses', 0.80, 0.85, 0.90, 0.75),
    ('leisure_sports', 0.80, 0.85, 0.90, 0.75),
    ('shopping', 0.80, 0.85, 0.90, 0.75)
ON CONFLICT (table_name) DO NOTHING;

-- 권한 설정
GRANT SELECT, INSERT, UPDATE, DELETE ON pet_tour_info TO aicc6;
GRANT SELECT, INSERT, UPDATE, DELETE ON classification_system_codes TO aicc6;
GRANT SELECT, INSERT, UPDATE, DELETE ON area_based_sync_list TO aicc6;
GRANT SELECT, INSERT, UPDATE, DELETE ON legal_dong_codes TO aicc6;

-- 스키마 생성 완료 로그
INSERT INTO batch_job_logs (job_name, job_type, status, started_at, completed_at)
VALUES ('schema_migration_002', 'schema', 'success', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);

-- 테이블 설명 추가
COMMENT ON TABLE pet_tour_info IS '반려동물 동반여행 정보 (detailPetTour2 API)';
COMMENT ON TABLE classification_system_codes IS '분류체계 코드 정보 (lclsSystmCode2 API)';
COMMENT ON TABLE area_based_sync_list IS '지역기반 동기화 목록 (areaBasedSyncList2 API)';
COMMENT ON TABLE legal_dong_codes IS '법정동 코드 정보 (ldongCode2 API)';