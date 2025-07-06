-- =====================================================
-- 유니크 제약 조건 추가
-- 작성일: 2025-07-06
-- 목적: 새로 생성된 테이블들의 유니크 제약 조건 추가
-- =====================================================

-- content_images 테이블에 유니크 제약 조건 추가
DO $$ 
BEGIN
    -- content_images 테이블의 복합 유니크 제약 조건 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'content_images_unique_content_serial'
        AND table_name = 'content_images'
    ) THEN
        ALTER TABLE content_images 
        ADD CONSTRAINT content_images_unique_content_serial 
        UNIQUE (content_id, serial_num);
        
        RAISE NOTICE '✅ content_images 테이블에 유니크 제약 조건 추가 완료';
    ELSE
        RAISE NOTICE '⚠️ content_images 유니크 제약 조건이 이미 존재합니다';
    END IF;
    
    -- content_detail_info 테이블의 복합 유니크 제약 조건 추가
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'content_detail_info_unique_content_serial'
        AND table_name = 'content_detail_info'
    ) THEN
        ALTER TABLE content_detail_info 
        ADD CONSTRAINT content_detail_info_unique_content_serial 
        UNIQUE (content_id, serial_num);
        
        RAISE NOTICE '✅ content_detail_info 테이블에 유니크 제약 조건 추가 완료';
    ELSE
        RAISE NOTICE '⚠️ content_detail_info 유니크 제약 조건이 이미 존재합니다';
    END IF;
    
END $$;

-- 테이블 존재 여부 확인 및 보고
DO $$
DECLARE
    table_exists BOOLEAN;
BEGIN
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'content_images'
    ) INTO table_exists;
    
    IF table_exists THEN
        RAISE NOTICE '✅ content_images 테이블 존재 확인';
    ELSE
        RAISE NOTICE '❌ content_images 테이블이 존재하지 않습니다';
    END IF;
    
    SELECT EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_name = 'content_detail_info'
    ) INTO table_exists;
    
    IF table_exists THEN
        RAISE NOTICE '✅ content_detail_info 테이블 존재 확인';
    ELSE
        RAISE NOTICE '❌ content_detail_info 테이블이 존재하지 않습니다';
    END IF;
END $$;

RAISE NOTICE '=== 유니크 제약 조건 추가 작업 완료 ===';