-- Migration: Add sigungu_code column to restaurants table
-- Date: 2025-07-16
-- Purpose: Fix data insertion error for restaurant data from KTO API

-- Add sigungu_code column to restaurants table
ALTER TABLE restaurants 
ADD COLUMN IF NOT EXISTS sigungu_code VARCHAR(20);

-- Add index for better query performance
CREATE INDEX IF NOT EXISTS idx_restaurants_sigungu_code 
ON restaurants(sigungu_code);

-- Add comment
COMMENT ON COLUMN restaurants.sigungu_code IS '시군구 코드 (KTO API)';