-- city_weather_data 테이블 삭제
-- 이 테이블은 사용되지 않고 있으며, weather_forecasts 테이블로 대체되었습니다.

-- 테이블이 존재하는 경우에만 삭제
DROP TABLE IF EXISTS city_weather_data CASCADE;

-- 관련 인덱스나 제약조건도 CASCADE 옵션으로 함께 삭제됩니다.