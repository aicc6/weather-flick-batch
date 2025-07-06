-- =====================================================
-- 스키마 확장 1단계: 기존 테이블 필드 확장 및 이미지 테이블 생성
-- 작성일: 2025-07-06
-- 목적: 한국관광공사 API 완전 데이터 수집을 위한 스키마 확장
-- =====================================================

-- 1. 기존 테이블들에 공통 필드 추가
-- =====================================================

DO $$ 
DECLARE
    table_names TEXT[] := ARRAY[
        'tourist_attractions', 'cultural_facilities', 'festivals_events', 
        'travel_courses', 'leisure_sports', 'accommodations', 
        'shopping', 'restaurants'
    ];
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        -- 테이블이 존재하는지 확인
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = table_name
        ) THEN
            RAISE NOTICE '테이블 % 확장 시작', table_name;
            
            -- homepage 컬럼 추가
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'homepage'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN homepage TEXT', table_name);
                RAISE NOTICE '  - homepage 컬럼 추가 완료';
            END IF;
            
            -- booktour 컬럼 추가 (교과서속여행지여부)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'booktour'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN booktour CHAR(1)', table_name);
                RAISE NOTICE '  - booktour 컬럼 추가 완료';
            END IF;
            
            -- createdtime 컬럼 추가 (콘텐츠 최초 등록일)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'createdtime'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN createdtime VARCHAR(14)', table_name);
                RAISE NOTICE '  - createdtime 컬럼 추가 완료';
            END IF;
            
            -- modifiedtime 컬럼 추가 (콘텐츠 최종 수정일)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'modifiedtime'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN modifiedtime VARCHAR(14)', table_name);
                RAISE NOTICE '  - modifiedtime 컬럼 추가 완료';
            END IF;
            
            -- telname 컬럼 추가 (전화번호명)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'telname'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN telname VARCHAR(100)', table_name);
                RAISE NOTICE '  - telname 컬럼 추가 완료';
            END IF;
            
            -- faxno 컬럼 추가 (팩스번호)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'faxno'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN faxno VARCHAR(50)', table_name);
                RAISE NOTICE '  - faxno 컬럼 추가 완료';
            END IF;
            
            -- zipcode 컬럼 추가 (우편번호)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'zipcode'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN zipcode VARCHAR(10)', table_name);
                RAISE NOTICE '  - zipcode 컬럼 추가 완료';
            END IF;
            
            -- mlevel 컬럼 추가 (맵레벨)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'mlevel'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN mlevel INTEGER', table_name);
                RAISE NOTICE '  - mlevel 컬럼 추가 완료';
            END IF;
            
            -- overview 컬럼 타입 확장 (TEXT로 변경)
            IF EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'overview'
                AND data_type != 'text'
            ) THEN
                EXECUTE format('ALTER TABLE %I ALTER COLUMN overview TYPE TEXT', table_name);
                RAISE NOTICE '  - overview 컬럼 타입 확장 완료';
            END IF;
            
            -- detail_intro_info 컬럼 추가 (컨텐츠 타입별 세부 정보)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'detail_intro_info'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN detail_intro_info JSONB', table_name);
                RAISE NOTICE '  - detail_intro_info 컬럼 추가 완료';
            END IF;
            
            -- detail_additional_info 컬럼 추가 (부대시설, 이용안내 등)
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = table_name AND column_name = 'detail_additional_info'
            ) THEN
                EXECUTE format('ALTER TABLE %I ADD COLUMN detail_additional_info JSONB', table_name);
                RAISE NOTICE '  - detail_additional_info 컬럼 추가 완료';
            END IF;
            
            RAISE NOTICE '테이블 % 확장 완료', table_name;
        ELSE
            RAISE NOTICE '테이블 %가 존재하지 않습니다. 건너뜁니다.', table_name;
        END IF;
    END LOOP;
END $$;

-- 2. 이미지 정보 테이블 생성
-- =====================================================

CREATE TABLE IF NOT EXISTS content_images (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id VARCHAR(50) NOT NULL,               -- 콘텐츠 ID
    content_type_id VARCHAR(10),                   -- 콘텐츠 타입 ID
    img_name VARCHAR(500),                         -- 이미지명
    origin_img_url TEXT,                           -- 원본 이미지 URL
    small_image_url TEXT,                          -- 썸네일 이미지 URL
    serial_num INTEGER,                            -- 이미지 순번
    cpyrht_div_cd VARCHAR(10),                     -- 저작권 구분 코드
    img_size VARCHAR(20),                          -- 이미지 크기
    img_width INTEGER,                             -- 이미지 가로 사이즈
    img_height INTEGER,                            -- 이미지 세로 사이즈
    raw_data_id UUID REFERENCES api_raw_data(id) ON DELETE SET NULL,
    data_quality_score DECIMAL(5,2),
    processing_status VARCHAR(20) DEFAULT 'processed',
    last_sync_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. 상세 정보 테이블 생성 (선택적)
-- =====================================================

CREATE TABLE IF NOT EXISTS content_detail_info (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    content_id VARCHAR(50) NOT NULL,               -- 콘텐츠 ID
    content_type_id VARCHAR(10),                   -- 콘텐츠 타입 ID
    info_name VARCHAR(200),                        -- 정보명
    info_text TEXT,                                -- 정보내용
    serial_num INTEGER,                            -- 순번
    raw_data_id UUID REFERENCES api_raw_data(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. 인덱스 생성
-- =====================================================

-- content_images 테이블 인덱스
CREATE INDEX IF NOT EXISTS idx_content_images_content_id ON content_images(content_id);
CREATE INDEX IF NOT EXISTS idx_content_images_content_type ON content_images(content_type_id);
CREATE INDEX IF NOT EXISTS idx_content_images_serial ON content_images(content_id, serial_num);
CREATE INDEX IF NOT EXISTS idx_content_images_sync_at ON content_images(last_sync_at);

-- content_detail_info 테이블 인덱스
CREATE INDEX IF NOT EXISTS idx_content_detail_info_content_id ON content_detail_info(content_id);
CREATE INDEX IF NOT EXISTS idx_content_detail_info_content_type ON content_detail_info(content_type_id);
CREATE INDEX IF NOT EXISTS idx_content_detail_info_serial ON content_detail_info(content_id, serial_num);

-- 기존 테이블들에 새로운 필드 인덱스 추가
DO $$ 
DECLARE
    table_names TEXT[] := ARRAY[
        'tourist_attractions', 'cultural_facilities', 'festivals_events', 
        'travel_courses', 'leisure_sports', 'accommodations', 
        'shopping', 'restaurants'
    ];
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = table_name
        ) THEN
            -- modifiedtime 인덱스 추가 (변경 감지용)
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_modifiedtime ON %I(modifiedtime)', 
                          table_name, table_name);
            
            -- createdtime 인덱스 추가
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_createdtime ON %I(createdtime)', 
                          table_name, table_name);
            
            -- booktour 인덱스 추가 (교과서속여행지 필터링용)
            EXECUTE format('CREATE INDEX IF NOT EXISTS idx_%s_booktour ON %I(booktour)', 
                          table_name, table_name);
        END IF;
    END LOOP;
END $$;

-- 5. 외래키 제약조건 추가 (선택적)
-- =====================================================

-- content_images와 각 테이블 간의 참조 무결성은 application level에서 관리
-- (다중 테이블 참조로 인한 FK 제약조건 복잡성 회피)

-- 6. 데이터 품질 임계값 설정 추가
-- =====================================================

INSERT INTO data_quality_thresholds (table_name, completeness_threshold, validity_threshold, consistency_threshold, overall_threshold)
VALUES 
    ('content_images', 0.90, 0.95, 0.98, 0.85),
    ('content_detail_info', 0.85, 0.90, 0.95, 0.80)
ON CONFLICT (table_name) DO NOTHING;

-- 기존 테이블들의 품질 임계값 업데이트 (새 필드 고려)
UPDATE data_quality_thresholds 
SET 
    completeness_threshold = 0.80,  -- 새 필드들로 인한 완성도 기준 완화
    validity_threshold = 0.90,
    consistency_threshold = 0.95,
    overall_threshold = 0.75,
    updated_at = CURRENT_TIMESTAMP
WHERE table_name IN (
    'tourist_attractions', 'cultural_facilities', 'festivals_events', 
    'travel_courses', 'leisure_sports', 'accommodations', 
    'shopping', 'restaurants'
);

-- 7. 권한 설정
-- =====================================================

GRANT SELECT, INSERT, UPDATE, DELETE ON content_images TO aicc6;
GRANT SELECT, INSERT, UPDATE, DELETE ON content_detail_info TO aicc6;

-- 8. 테이블 코멘트 추가
-- =====================================================

COMMENT ON TABLE content_images IS '컨텐츠 이미지 정보 (detailImage2 API)';
COMMENT ON TABLE content_detail_info IS '컨텐츠 상세 정보 (detailInfo2 API)';

-- 새로운 컬럼 코멘트 추가
DO $$ 
DECLARE
    table_names TEXT[] := ARRAY[
        'tourist_attractions', 'cultural_facilities', 'festivals_events', 
        'travel_courses', 'leisure_sports', 'accommodations', 
        'shopping', 'restaurants'
    ];
    table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY table_names
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = table_name
        ) THEN
            EXECUTE format('COMMENT ON COLUMN %I.homepage IS ''홈페이지 URL''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.booktour IS ''교과서속여행지여부 (Y/N)''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.createdtime IS ''콘텐츠 최초 등록일 (YYYYMMDDHHMMSS)''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.modifiedtime IS ''콘텐츠 최종 수정일 (YYYYMMDDHHMMSS)''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.telname IS ''전화번호명''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.faxno IS ''팩스번호''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.zipcode IS ''우편번호''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.mlevel IS ''맵레벨''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.detail_intro_info IS ''컨텐츠 타입별 세부 정보 (JSON)''', table_name);
            EXECUTE format('COMMENT ON COLUMN %I.detail_additional_info IS ''부대시설/이용안내 등 추가 정보 (JSON)''', table_name);
        END IF;
    END LOOP;
END $$;

-- 9. 마이그레이션 로그 기록
-- =====================================================

INSERT INTO batch_job_logs (job_name, job_type, status, started_at, completed_at, details)
VALUES (
    'schema_enhancement_phase1', 
    'schema', 
    'success', 
    CURRENT_TIMESTAMP, 
    CURRENT_TIMESTAMP,
    '기존 테이블 필드 확장, 이미지 테이블 생성, 상세정보 테이블 생성 완료'
);

-- 10. 확장된 스키마 검증
-- =====================================================

DO $$ 
DECLARE
    table_names TEXT[] := ARRAY[
        'tourist_attractions', 'cultural_facilities', 'festivals_events', 
        'travel_courses', 'leisure_sports', 'accommodations', 
        'shopping', 'restaurants'
    ];
    table_name TEXT;
    column_count INTEGER;
BEGIN
    RAISE NOTICE '=== 스키마 확장 검증 시작 ===';
    
    FOREACH table_name IN ARRAY table_names
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.tables 
            WHERE table_name = table_name
        ) THEN
            SELECT COUNT(*) INTO column_count
            FROM information_schema.columns 
            WHERE table_name = table_name;
            
            RAISE NOTICE '테이블 %: % 개 컬럼', table_name, column_count;
        END IF;
    END LOOP;
    
    -- 새로운 테이블 존재 확인
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'content_images') THEN
        RAISE NOTICE '새 테이블 content_images 생성 완료';
    END IF;
    
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'content_detail_info') THEN
        RAISE NOTICE '새 테이블 content_detail_info 생성 완료';
    END IF;
    
    RAISE NOTICE '=== 스키마 확장 검증 완료 ===';
END $$;