# WeatherFlick 배치 시스템

날씨 기반 여행지 추천 서비스를 위한 포괄적인 데이터 수집 및 처리 배치 시스템입니다.

## 📋 주요 기능

### 🌤️ 데이터 수집
- **기상청 API**: 실시간 날씨, 예보, 과거 기상 데이터 수집
- **한국관광공사 API**: 관광지, 축제, 행사 정보 수집
- **지역별 데이터**: 전국 17개 시도별 상세 정보

### 🤖 지능형 추천
- **날씨 기반 추천**: 기온, 강수량, 습도, 풍속 종합 분석
- **계절별 가중치**: 계절 특성을 반영한 추천 점수 계산
- **활동별 매칭**: 날씨 조건에 맞는 최적 활동 추천

### 🔍 데이터 품질 관리
- **자동 품질 검사**: 누락, 중복, 범위 이탈 데이터 감지
- **일관성 검증**: 논리적 오류 및 데이터 무결성 검사
- **품질 점수**: 0-100점 품질 점수 산출 및 이력 관리

### 💾 시스템 운영
- **자동 백업**: PostgreSQL 데이터베이스 정기 백업 및 압축
- **스마트 알림**: Slack, 이메일을 통한 실시간 장애 알림
- **성능 모니터링**: 시스템 리소스 및 작업 성능 추적

## 🛠️ 시스템 요구사항

- **Python**: 3.8 이상
- **데이터베이스**: PostgreSQL 12 이상
- **캐시**: Redis (선택사항, 스케줄러 성능 향상)
- **운영체제**: Linux, macOS, Windows
- **메모리**: 최소 2GB RAM
- **저장공간**: 최소 10GB (백업 포함)

## 🚀 빠른 시작

### 1. 프로젝트 클론

```bash
git clone https://github.com/your-org/weather-flick-batch.git
cd weather-flick-batch
```

### 2. 가상환경 설정

```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

### 3. 의존성 설치

```bash
pip install -r requirements.txt
```

### 4. 환경 변수 설정

```bash
cp .env.example .env
# .env 파일을 편집하여 필요한 설정값들을 입력합니다
```

**필수 설정 항목:**
```bash
# API 키
KTO_API_KEY=your_kto_api_key_here
KMA_API_KEY=your_kma_api_key_here

# 데이터베이스
DB_HOST=localhost
DB_USER=weather_user
DB_PASSWORD=your_password
DB_NAME=weather_travel_db
```

### 5. 데이터베이스 초기화

```bash
# PostgreSQL 데이터베이스 생성
createdb weather_travel_db

# 스키마 적용
psql -d weather_travel_db -f weather_travel_schema.sql

# job_type enum 업데이트
psql -d weather_travel_db -f migration_add_job_types.sql
```

### 6. 시스템 실행

```bash
python main_advanced.py
```

## 📅 배치 작업 스케줄

| 작업 | 주기 | 실행 시간 | 설명 |
|------|------|-----------|------|
| 🌡️ 날씨 데이터 수집 | 1시간마다 | 매시 정각 | 실시간 날씨 및 예보 데이터 |
| 🏛️ 관광지 데이터 동기화 | 주 1회 | 일요일 04:00 | 관광공사 API 데이터 수집 |
| 🎯 추천 점수 계산 | 일 1회 | 매일 05:00 | 날씨 기반 추천 점수 생성 |
| 🔍 데이터 품질 검사 | 일 1회 | 매일 06:00 | 데이터 품질 검증 및 리포트 |
| 💾 데이터베이스 백업 | 일 1회 | 매일 02:00 | 전체 DB 백업 및 압축 |
| ❤️ 시스템 헬스체크 | 5분마다 | 연속 실행 | 시스템 상태 모니터링 |

## 🏗️ 프로젝트 구조

```
weather-flick-batch/
├── 📁 app/                     # 핵심 애플리케이션 모듈
│   ├── 📁 collectors/          # 데이터 수집기
│   │   ├── weather_collector.py    # 기상청 API 수집기
│   │   └── tourism_collector.py    # 관광공사 API 수집기
│   ├── 📁 core/               # 기본 클래스 및 로거
│   │   ├── base_job.py            # 배치 작업 기본 클래스
│   │   └── logger.py              # 중앙 로깅 시스템
│   └── 📁 schedulers/         # 스케줄러 시스템
│       └── advanced_scheduler.py  # APScheduler 기반 관리자
├── 📁 jobs/                   # 배치 작업 구현
│   ├── 📁 data_management/    # 데이터 관리 작업
│   ├── 📁 monitoring/         # 시스템 모니터링
│   ├── 📁 quality/           # 데이터 품질 검사
│   ├── 📁 recommendation/    # 추천 엔진
│   ├── 📁 system_maintenance/ # 시스템 유지보수
│   ├── 📁 tourism/           # 관광 데이터 처리
│   └── 📁 weather/           # 날씨 데이터 처리
├── 📁 config/                # 설정 관리
├── 📁 utils/                 # 유틸리티 모듈
├── 📁 tests/                 # 테스트 코드
├── 📁 data/                  # 데이터 저장소
├── 📁 logs/                  # 로그 파일
├── 📄 main_advanced.py       # 메인 실행 파일
├── 📄 requirements.txt       # Python 의존성
├── 📄 .env.example          # 환경 변수 템플릿
└── 📄 README.md             # 프로젝트 문서
```

## 🔧 개별 모듈 테스트

```bash
# 날씨 데이터 수집기 테스트
python -c "from app.collectors.weather_collector import WeatherDataCollector; print('Weather collector imported successfully')"

# 관광지 데이터 수집기 테스트
python -c "from app.collectors.tourism_collector import TourismDataCollector; print('Tourism collector imported successfully')"

# 추천 엔진 테스트
python jobs/recommendation/travel_recommendation_engine.py

# 데이터 품질 검사 테스트
python -c "from jobs.quality.data_quality_job import DataQualityJob; print('Quality checker imported successfully')"
```

## 🔑 API 키 발급 가이드

### 한국관광공사 API
1. [공공데이터포털](https://data.go.kr/) 회원가입
2. **"한국관광공사_국문 관광정보 서비스_GW"** 검색 후 신청
3. 승인 완료 후 **마이페이지 > 오픈API > 인증키** 확인
4. `.env` 파일의 `KTO_API_KEY`에 설정

### 기상청 API
1. [공공데이터포털](https://data.go.kr/) 접속
2. **"기상청_단기예보 조회서비스"** 검색 후 신청
3. 승인 완료 후 인증키 확인
4. `.env` 파일의 `KMA_API_KEY`에 설정

## 📊 모니터링 및 알림

### 🔔 알림 채널 설정

#### Slack 알림
```bash
# .env 파일에 추가
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
SLACK_CHANNEL=#alerts
```

#### 이메일 알림
```bash
# .env 파일에 추가
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your_email@gmail.com
SMTP_PASSWORD=your_app_password
ALERT_EMAILS=admin@company.com,ops@company.com
```

### 📈 모니터링 대시보드

시스템은 다음 항목들을 자동으로 모니터링합니다:

- **작업 성공률**: 각 배치 작업의 성공/실패율
- **데이터 품질**: 테이블별 품질 점수 및 이슈 추적
- **시스템 리소스**: CPU, 메모리, 디스크 사용률
- **API 응답 시간**: 외부 API 호출 성능 측정
- **데이터베이스 성능**: 쿼리 실행 시간 및 연결 상태

## 🧪 테스트

```bash
# 전체 테스트 실행
python -m pytest tests/ -v

# 커버리지 포함 테스트
python -m pytest tests/ --cov=app --cov=jobs --cov=utils

# 특정 모듈 테스트
python -m pytest tests/unit/test_weather_collector.py -v
```

## 🔧 문제 해결

### 일반적인 문제들

**Q: "pg_dump를 찾을 수 없습니다" 오류**
```bash
# PostgreSQL 클라이언트 도구 설치
# Ubuntu/Debian
sudo apt-get install postgresql-client

# macOS
brew install postgresql

# Windows
# PostgreSQL 설치 시 클라이언트 도구도 함께 설치됩니다
```

**Q: Redis 연결 실패**
```bash
# Redis가 설치되지 않은 경우 메모리 저장소로 자동 전환됩니다
# Redis 설치 (선택사항)
# Ubuntu/Debian
sudo apt-get install redis-server

# macOS
brew install redis
```

**Q: API 키 관련 오류**
- API 키가 올바른지 확인
- 공공데이터포털에서 API 사용 승인 상태 확인
- 일일 호출 제한 확인

### 로그 확인

```bash
# 실시간 로그 확인
tail -f logs/weather_flick_batch_$(date +%Y%m%d).log

# 에러 로그만 확인
tail -f logs/weather_flick_batch_error_$(date +%Y%m%d).log

# 특정 작업 로그 검색
grep "tourism_sync" logs/weather_flick_batch_*.log
```

## 🤝 기여하기

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 `LICENSE` 파일을 참조하세요.

## 📞 지원

- **이슈 신고**: [GitHub Issues](https://github.com/your-org/weather-flick-batch/issues)
- **기능 요청**: [GitHub Discussions](https://github.com/your-org/weather-flick-batch/discussions)
- **문서**: [프로젝트 Wiki](https://github.com/your-org/weather-flick-batch/wiki)

---

**WeatherFlick 배치 시스템**으로 더 정확하고 개인화된 날씨 기반 여행 추천 서비스를 구축하세요! 🌤️✈️