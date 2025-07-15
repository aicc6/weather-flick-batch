
-- =============================================
-- regions 마이그레이션 정리 스크립트
-- 생성일: 2025-07-15 00:16:21
-- =============================================

-- 1. 기존 테이블 삭제 (CASCADE로 관련 객체도 함께 삭제)
DROP TABLE IF EXISTS unified_regions CASCADE;
DROP TABLE IF EXISTS region_api_mappings CASCADE;
DROP TABLE IF EXISTS coordinate_transformations CASCADE;

-- 2. 호환성 뷰 삭제
DROP VIEW IF EXISTS unified_regions_view;

-- 3. 백업 테이블 삭제 (필요시 주석 해제)
-- DROP TABLE IF EXISTS unified_regions_backup;
-- DROP TABLE IF EXISTS region_api_mappings_backup;
-- DROP TABLE IF EXISTS coordinate_transformations_backup;

-- 4. 확인
SELECT 'Cleanup completed' as status;
