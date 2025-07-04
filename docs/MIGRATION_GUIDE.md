# WeatherFlick 배치 시스템 마이그레이션 가이드

## 개요

WeatherFlick 배치 시스템이 기존의 단순한 스케줄링 시스템에서 엔터프라이즈급 고급 배치 시스템으로 업그레이드되었습니다.

## 마이그레이션 이유

### 기존 시스템의 한계
- **단일 프로세스**: 확장성 제한
- **기본 스케줄링**: `schedule` 라이브러리의 제한적 기능
- **에러 처리 부족**: 재시도, 복구 메커니즘 미흡
- **모니터링 부족**: 기본적인 로깅만 제공

### 새 시스템의 장점
- **분산 스케줄링**: Redis 기반 APScheduler
- **포괄적 기능**: 재시도, 의존성, 타임아웃 지원
- **엔터프라이즈 기능**: 모니터링, 알림, 복구
- **확장성**: 클러스터, 멀티 프로세스 지원

## 변경사항

### 1. 실행 파일
```bash
# 기존 (제거 예정)
python main.py

# 새 시스템 (권장)
python main_advanced.py
```

### 2. 스케줄러
```python
# 기존 (legacy_scheduler.py)
- schedule 라이브러리 기반
- 단순 cron 스케줄링
- 제한적 기능

# 새 시스템 (advanced_scheduler.py)
- APScheduler 기반
- Redis Job Store
- 분산 처리 지원
```

### 3. 배치 작업 구조
```
기존:
jobs/
├── weather/weather_data_job.py
├── tourism/
├── recommendation/
└── quality/

새 시스템:
jobs/
├── data_management/          # 데이터 관리
│   ├── weather_update_job.py
│   └── destination_sync_job.py
├── system_maintenance/       # 시스템 유지보수
│   ├── log_cleanup_job.py
│   └── database_backup_job.py
├── monitoring/              # 모니터링
│   ├── health_check_job.py
│   └── metrics_collection_job.py
└── business_logic/          # 비즈니스 로직
    ├── recommendation_job.py
    └── analytics_job.py
```

## 마이그레이션 단계

### 1단계: 의존성 설치
```bash
# 새 의존성 설치
pip install -r requirements.txt

# 주요 추가 패키지:
# - apscheduler>=3.10.4
# - redis>=5.0.1
# - psutil>=5.9.6
# - boto3>=1.34.0
```

### 2단계: 환경 설정
```bash
# 환경 변수 설정
cp .env.example .env
# .env 파일 편집하여 Redis, AWS 등 설정
```

### 3단계: Redis 설치 및 설정
```bash
# Redis 설치 (Ubuntu)
sudo apt install redis-server

# Redis 시작
sudo systemctl start redis-server
sudo systemctl enable redis-server

# 연결 테스트
redis-cli ping
```

### 4단계: 시스템 전환
```bash
# 기존 시스템 중지
# (기존 main.py 프로세스 종료)

# 새 시스템 시작
python main_advanced.py
```

## 설정 비교

### 기존 설정 (config/settings.py)
```python
class ScheduleConfig:
    weather_data_time: str = "02:00"
    tourist_data_time: str = "03:00"
    # ... 기본적인 스케줄 설정만
```

### 새 설정 (config/batch_settings.py)
```python
@dataclass
class BatchSettings:
    # Redis 설정
    redis_host: str = "localhost"
    redis_port: int = 6379
    
    # 스케줄러 설정
    max_workers: int = 20
    timezone: str = "Asia/Seoul"
    
    # 작업 기본 설정
    default_timeout: int = 3600
    default_retry_attempts: int = 3
    # ... 포괄적 설정
```

## 기능 비교

| 기능 | 기존 시스템 | 새 시스템 |
|------|------------|-----------|
| 스케줄링 | schedule 라이브러리 | APScheduler + Redis |
| 분산 처리 | ❌ | ✅ |
| 재시도 메커니즘 | 기본적 | 고급 (지수 백오프) |
| 의존성 관리 | ❌ | ✅ |
| 타임아웃 처리 | ❌ | ✅ |
| 우선순위 | ❌ | ✅ |
| 실시간 모니터링 | 기본 로깅 | 포괄적 헬스체크 |
| 알림 시스템 | ❌ | Slack, 이메일, SMS |
| 메트릭 수집 | ❌ | ✅ |
| 자동 복구 | ❌ | ✅ |

## 테스트 방법

### 1. 기본 동작 테스트
```bash
# 새 시스템 시작
python main_advanced.py

# 로그 확인
tail -f logs/weather_flick_batch_*.log
```

### 2. 개별 작업 테스트
```python
from jobs.data_management.weather_update_job import weather_update_task
import asyncio

# 날씨 업데이트 작업 테스트
result = asyncio.run(weather_update_task())
print(result)
```

### 3. 헬스체크 테스트
```python
from jobs.monitoring.health_check_job import health_check_task
import asyncio

# 헬스체크 실행
result = asyncio.run(health_check_task())
print(result)
```

## 롤백 계획

문제 발생 시 기존 시스템으로 롤백:

```bash
# 새 시스템 중지
pkill -f main_advanced.py

# 기존 시스템 시작
python main.py
```

## 성능 비교

### 기존 시스템
- 메모리 사용량: ~50MB
- CPU 사용률: 낮음
- 기능: 기본적

### 새 시스템
- 메모리 사용량: ~100-150MB
- CPU 사용률: 중간
- 기능: 엔터프라이즈급

## 문제 해결

### 1. Redis 연결 오류
```bash
# Redis 상태 확인
sudo systemctl status redis-server

# Redis 로그 확인
sudo journalctl -u redis-server
```

### 2. 의존성 오류
```bash
# 가상환경 재생성
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. 권한 오류
```bash
# 로그 디렉토리 권한 설정
sudo mkdir -p /var/log/weather-flick-batch
sudo chown $USER:$USER /var/log/weather-flick-batch
```

## 결론

새 고급 배치 시스템은 확장성, 안정성, 모니터링 기능이 대폭 향상된 엔터프라이즈급 시스템입니다. 
기존 시스템의 모든 기능을 포함하면서 추가적인 고급 기능을 제공합니다.

**권장사항: 기존 시스템은 레거시로 유지하되, 새 시스템으로 전환하는 것을 강력히 권장합니다.**