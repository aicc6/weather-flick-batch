-- leisure_sports 테이블 생성
-- 레포츠/레저 시설 정보를 저장하는 테이블

CREATE TABLE IF NOT EXISTS leisure_sports (
    -- 기본 식별자
    content_id VARCHAR(20) PRIMARY KEY,
    region_code VARCHAR(10) NOT NULL,
    sigungu_code VARCHAR(10),
    raw_data_id UUID,
    
    -- 시설 기본 정보
    facility_name VARCHAR(200) NOT NULL,
    category_code VARCHAR(10),
    sub_category_code VARCHAR(10),
    
    -- 위치 정보
    address VARCHAR(255),
    detail_address VARCHAR(255),
    zipcode VARCHAR(10),
    latitude NUMERIC(10, 8),
    longitude NUMERIC(11, 8),
    
    -- 연락처 정보
    tel VARCHAR(50),
    homepage TEXT,
    telname VARCHAR(100),
    faxno VARCHAR(50),
    
    -- 시설 상세 정보
    sports_type VARCHAR(100),
    reservation_info TEXT,
    operating_hours VARCHAR(500),
    admission_fee VARCHAR(500),
    parking_info VARCHAR(500),
    rental_info TEXT,
    capacity VARCHAR(100),
    
    -- 콘텐츠 정보
    overview TEXT,
    first_image VARCHAR(500),
    first_image_small VARCHAR(500),
    
    -- KTO API 추가 필드
    booktour VARCHAR(50),
    createdtime VARCHAR(50),
    modifiedtime VARCHAR(50),
    mlevel INTEGER,
    
    -- 상세 정보 (JSON)
    detail_intro_info JSONB,
    detail_additional_info JSONB,
    
    -- 메타데이터
    data_quality_score NUMERIC(3, 2),
    processing_status VARCHAR(50),
    
    -- 타임스탬프
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    last_sync_at TIMESTAMP DEFAULT NOW()
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_leisure_sports_region_code ON leisure_sports(region_code);
CREATE INDEX IF NOT EXISTS idx_leisure_sports_sigungu_code ON leisure_sports(sigungu_code);
CREATE INDEX IF NOT EXISTS idx_leisure_sports_category ON leisure_sports(category_code, sub_category_code);
CREATE INDEX IF NOT EXISTS idx_leisure_sports_sports_type ON leisure_sports(sports_type);
CREATE INDEX IF NOT EXISTS idx_leisure_sports_latitude ON leisure_sports(latitude);
CREATE INDEX IF NOT EXISTS idx_leisure_sports_longitude ON leisure_sports(longitude);
CREATE INDEX IF NOT EXISTS idx_leisure_sports_updated_at ON leisure_sports(updated_at);
CREATE INDEX IF NOT EXISTS idx_leisure_sports_last_sync_at ON leisure_sports(last_sync_at);

-- 트리거 함수: updated_at 자동 업데이트
CREATE OR REPLACE FUNCTION update_leisure_sports_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 트리거 생성
DROP TRIGGER IF EXISTS trg_leisure_sports_updated_at ON leisure_sports;
CREATE TRIGGER trg_leisure_sports_updated_at
    BEFORE UPDATE ON leisure_sports
    FOR EACH ROW
    EXECUTE FUNCTION update_leisure_sports_updated_at();

-- 코멘트 추가
COMMENT ON TABLE leisure_sports IS '레포츠/레저 시설 정보';
COMMENT ON COLUMN leisure_sports.content_id IS '콘텐츠 ID (PK)';
COMMENT ON COLUMN leisure_sports.region_code IS '지역 코드';
COMMENT ON COLUMN leisure_sports.sigungu_code IS '시군구 코드';
COMMENT ON COLUMN leisure_sports.facility_name IS '시설명';
COMMENT ON COLUMN leisure_sports.sports_type IS '스포츠/레저 유형';
COMMENT ON COLUMN leisure_sports.reservation_info IS '예약 정보';
COMMENT ON COLUMN leisure_sports.operating_hours IS '운영 시간';
COMMENT ON COLUMN leisure_sports.admission_fee IS '입장료/이용료';
COMMENT ON COLUMN leisure_sports.parking_info IS '주차 정보';
COMMENT ON COLUMN leisure_sports.rental_info IS '대여 정보';
COMMENT ON COLUMN leisure_sports.capacity IS '수용 인원';
COMMENT ON COLUMN leisure_sports.detail_intro_info IS '상세 소개 정보 (JSON)';
COMMENT ON COLUMN leisure_sports.detail_additional_info IS '추가 상세 정보 (JSON)';
COMMENT ON COLUMN leisure_sports.data_quality_score IS '데이터 품질 점수 (0-1)';
COMMENT ON COLUMN leisure_sports.processing_status IS '처리 상태';