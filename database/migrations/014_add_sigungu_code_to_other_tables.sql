-- Migration: Add sigungu_code column to tourist_attractions and courses tables
-- Date: 2025-07-16
-- Purpose: Fix data insertion error for KTO API data

-- Add sigungu_code column to tourist_attractions table
ALTER TABLE tourist_attractions 
ADD COLUMN IF NOT EXISTS sigungu_code VARCHAR(20);

-- Add index for better query performance
CREATE INDEX IF NOT EXISTS idx_tourist_attractions_sigungu_code 
ON tourist_attractions(sigungu_code);

-- Add comment
COMMENT ON COLUMN tourist_attractions.sigungu_code IS '시군구 코드 (KTO API)';

-- Add sigungu_code column to courses table
ALTER TABLE courses 
ADD COLUMN IF NOT EXISTS sigungu_code VARCHAR(20);

-- Add index for better query performance
CREATE INDEX IF NOT EXISTS idx_courses_sigungu_code 
ON courses(sigungu_code);

-- Add comment
COMMENT ON COLUMN courses.sigungu_code IS '시군구 코드 (KTO API)';