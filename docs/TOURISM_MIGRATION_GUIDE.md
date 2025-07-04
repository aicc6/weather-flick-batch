# 한국관광공사 종합 관광정보 마이그레이션 가이드

## 개요

한국관광공사 API에서 제공하는 모든 관광정보를 수집하고 기존 데이터와 통합하는 마이그레이션 가이드입니다.

## 🔄 마이그레이션 단계

### 1단계: 데이터베이스 스키마 확장

```bash
# PostgreSQL 데이터베이스에 마이그레이션 스크립트 실행
psql -U postgres -d weather_travel_db -f tourism_data_migration.sql
```

**추가된 테이블:**

- `category_codes` - 카테고리 코드 정보
- `cultural_facilities` - 문화시설 정보
- `festivals_events` - 축제/행사 정보
- `travel_courses` - 여행코스 정보
- `leisure_sports` - 레저/스포츠 시설
- `accommodations` - 숙박시설 정보
- `shopping` - 쇼핑 정보
- `restaurants` - 음식점 정보
- `attraction_details` - 관광지 상세 정보
- `attraction_images` - 관광지 이미지 정보

### 2단계: 종합 관광정보 수집

```bash
# 확장된 KTO API 데이터 수집 실행
cd /path/to/weather-flick-batch
python app/collectors/kto_api.py
```

**수집되는 데이터:**

- 지역 코드 (광역시도, 시군구)
- 카테고리 코드 (대/중/소분류)
- 관광지 (12)
- 문화시설 (14)
- 축제/행사 (15)
- 여행코스 (25)
- 레저/스포츠 (28)
- 숙박시설 (32)
- 쇼핑 (38)
- 음식점 (39)

### 3단계: 데이터 통합 처리

```bash
# 수집된 JSON 데이터를 데이터베이스에 저장
python app/processors/tourism_data_processor.py
```

**처리 과정:**

1. JSON 파일에서 데이터 로드
2. 데이터베이스 스키마에 맞게 필드 매핑
3. UPSERT 방식으로 데이터 저장 (중복 제거)
4. 데이터 품질 검사 실행

### 4단계: 배치 스케줄러 설정

```bash
# 새로운 배치 작업 스케줄 시작
python main_advanced.py
```

**스케줄 설정:**

- **종합 수집**: 매주 일요일 새벽 2시 (4시간 소요 예상)
- **증분 업데이트**: 매일 새벽 3시 (1시간 소요 예상)

## 📊 데이터 구조

### 주요 필드 매핑

| KTO API 필드 | 데이터베이스 필드 | 설명 |
|-------------|-----------------|------|
| `contentid` | `content_id` | 콘텐츠 ID |
| `title` | `*_name` | 시설/관광지명 |
| `areacode` | `region_code` | 지역 코드 |
| `sigungucode` | `sigungu_code` | 시군구 코드 |
| `addr1` | `address` | 주소 |
| `mapx` | `longitude` | 경도 |
| `mapy` | `latitude` | 위도 |
| `firstimage` | `first_image` | 대표 이미지 |
| `overview` | `overview` | 개요/설명 |

### 컨텐츠 타입별 분류

```sql
-- 컨텐츠 타입 ID와 테이블 매핑
'12' → tourist_attractions  -- 관광지
'14' → cultural_facilities  -- 문화시설
'15' → festivals_events     -- 축제공연행사
'25' → travel_courses       -- 여행코스
'28' → leisure_sports       -- 레포츠
'32' → accommodations       -- 숙박
'38' → shopping             -- 쇼핑
'39' → restaurants          -- 음식점
```

## 🔧 환경 설정

### 필수 환경 변수

`.env` 파일에 다음 변수들을 설정:

```env
# 한국관광공사 API
KTO_API_KEY=your_kto_api_key_here
KTO_API_BASE_URL=http://apis.data.go.kr/B551011/KorService2

# 데이터베이스
DB_HOST=localhost
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=weather_travel_db
DB_PORT=5432
```

### API 키 발급

1. [공공데이터포털](https://www.data.go.kr) 접속
2. "한국관광공사_국문 관광정보 서비스_GW" 검색
3. 활용신청 후 API 키 발급
4. `.env` 파일에 설정

## 📈 모니터링 및 품질 관리

### 데이터 품질 검사

```sql
-- 통합 관광정보 조회
SELECT * FROM v_comprehensive_tourism LIMIT 10;

-- 데이터 품질 검사 실행
SELECT * FROM check_tourism_data_quality();

-- 최근 배치 작업 로그 확인
SELECT * FROM batch_job_logs 
WHERE job_type IN ('comprehensive_tourism_sync', 'incremental_tourism_sync')
ORDER BY start_time DESC LIMIT 10;
```

### 통계 쿼리

```sql
-- 테이블별 데이터 수량
SELECT 
    'tourist_attractions' as table_name, COUNT(*) as count FROM tourist_attractions
UNION ALL
SELECT 'cultural_facilities', COUNT(*) FROM cultural_facilities
UNION ALL
SELECT 'festivals_events', COUNT(*) FROM festivals_events
UNION ALL
SELECT 'restaurants', COUNT(*) FROM restaurants
UNION ALL
SELECT 'accommodations', COUNT(*) FROM accommodations;

-- 지역별 관광정보 분포
SELECT 
    r.region_name,
    COUNT(*) as total_attractions
FROM v_comprehensive_tourism v
JOIN regions r ON v.region_code = r.region_code
GROUP BY r.region_name
ORDER BY total_attractions DESC;
```

## 🚀 운영 가이드

### 수동 실행

```bash
# 배치 작업 목록 확인
python run_batch.py list

# 종합 관광정보 수집 (전체) - API 실패 시 샘플 데이터 자동 사용
python run_batch.py run comprehensive-tourism

# 증분 업데이트 (일일) - API 실패 시 샘플 데이터 자동 사용
python run_batch.py run incremental-tourism

# 기존 관광지 데이터 동기화 (호환성)
python run_batch.py run tourism

# 전체 배치 작업 순차 실행
python run_batch.py run-all

# 직접 실행 (고급 사용자용)
python jobs/tourism/comprehensive_tourism_job.py
```

### API 장애 대응

API 서버 장애나 키 문제 발생 시:
- 자동으로 샘플 데이터로 전환
- 로그에 경고 메시지 기록
- 데이터베이스에 기본 데이터 저장

### 배치 작업 관리

```bash
# 스케줄러 시작
python main_advanced.py

# 특정 작업 즉시 실행
python -c "
from jobs.tourism.comprehensive_tourism_job import ComprehensiveTourismJob
job = ComprehensiveTourismJob()
result = job.execute()
print(f'실행 결과: {result}')
"
```

### 문제 해결

**API 500 에러 (서버 오류):**
- 자동으로 샘플 데이터로 전환됨
- 로그에서 `WARNING: API 데이터 수집 실패, 샘플 데이터로 전환` 확인
- 나중에 API 정상화되면 다시 실행

**API 호출 제한:**
- 요청 간격을 0.1-0.5초로 조절
- `time.sleep()` 값 증가

**메모리 부족:**
- 지역별로 분할 수집
- 페이지 단위로 처리

**데이터 중복:**
- UPSERT 쿼리로 자동 처리
- content_id 기준 중복 제거

**배치 작업 실패:**
```bash
# 로그 확인
tail -f logs/weather_flick_batch_error_$(date +%Y%m%d).log

# 개별 작업 테스트
python run_batch.py run incremental-tourism

# 데이터베이스 연결 확인
python -c "from utils.database import DatabaseManager; db = DatabaseManager(); print('DB 연결:', db.get_connection() is not None)"
```

## 📋 체크리스트

### 마이그레이션 전

- [ ] PostgreSQL 데이터베이스 접근 확인
- [ ] 한국관광공사 API 키 발급 및 설정
- [ ] 기존 데이터 백업 생성
- [ ] 디스크 공간 충분한지 확인 (최소 5GB 권장)

### 마이그레이션 중

- [ ] 스키마 마이그레이션 스크립트 실행
- [ ] API 데이터 수집 완료 확인
- [ ] 데이터 통합 처리 완료 확인
- [ ] 배치 스케줄러 설정 완료

### 마이그레이션 후

- [ ] 데이터 품질 검사 통과
- [ ] 통계 쿼리로 데이터 확인
- [ ] 배치 작업 정상 동작 확인
- [ ] 로그 모니터링 설정

## 🔍 예상 데이터 규모

| 데이터 타입 | 예상 수량 | 크기 |
|------------|----------|------|
| 관광지 | 10,000+ | ~50MB |
| 문화시설 | 5,000+ | ~25MB |
| 음식점 | 50,000+ | ~250MB |
| 숙박시설 | 15,000+ | ~75MB |
| 축제/행사 | 3,000+ | ~15MB |
| 쇼핑 | 8,000+ | ~40MB |
| **총계** | **90,000+** | **~450MB** |

## 📞 지원

문제 발생 시:

1. 로그 파일 확인 (`logs/` 디렉토리)
2. 배치 작업 로그 테이블 조회
3. API 응답 상태 확인
4. 데이터베이스 연결 상태 확인

---

✅ **마이그레이션 완료 후 한국관광공사에서 제공하는 모든 관광정보를 체계적으로 수집하고 관리할 수 있습니다.**

