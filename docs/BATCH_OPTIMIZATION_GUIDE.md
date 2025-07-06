# 배치 INSERT 최적화 가이드

## 개요

이 가이드는 weather-flick-batch 시스템의 배치 INSERT 성능 최적화 기능을 설명합니다.

## 주요 개선사항

### 🚀 성능 향상
- **2-5배 INSERT 성능 향상**
- **메모리 사용량 최적화**
- **트랜잭션 효율성 개선**

### 🛠️ 새로운 기능
- **배치 INSERT 최적화**: `app/core/batch_insert_optimizer.py`
- **테이블별 특화 설정**: `config/batch_optimization_config.py`
- **성능 테스트 도구**: `scripts/test_batch_performance.py`

## 사용 방법

### 1. 날씨 데이터 최적화

```python
from app.core.batch_insert_optimizer import optimize_weather_current_insert

# 현재 날씨 데이터 배치 저장
result = await optimize_weather_current_insert(weather_data, raw_data_id)
print(f"처리 성능: {result.records_per_second:.1f} records/sec")
```

### 2. 관광지 데이터 최적화

```python
from app.core.batch_insert_optimizer import optimize_tourism_data_insert

# 관광지 데이터 배치 저장
result = await optimize_tourism_data_insert(
    tourism_data, 
    "tourist_attractions", 
    conflict_columns=["content_id"]
)
```

### 3. 설정 사용자화

```python
from config.batch_optimization_config import get_weather_batch_config, BatchOptimizationLevel

# 성능 우선 설정
config = get_weather_batch_config(
    "weather_forecasts", 
    BatchOptimizationLevel.AGGRESSIVE
)

# 메모리 제약 환경 설정
config = get_weather_batch_config(
    "current_weather", 
    BatchOptimizationLevel.MEMORY_CONSTRAINED
)
```

## 성능 테스트

### 테스트 실행

```bash
# 기본 성능 테스트
python scripts/test_batch_performance.py

# 대용량 테스트 (10,000건)
python scripts/test_batch_performance.py --test-size 10000

# 다양한 배치 크기 테스트
python scripts/test_batch_performance.py --batch-sizes 200,500,1000,2000,5000

# 기존 방식 비교 없이 테스트
python scripts/test_batch_performance.py --skip-legacy
```

### 테스트 결과 예시

```
📊 CURRENT 테이블 성능 결과:
🔴 기존 방식:
   처리량: 150.0 records/sec
   소요시간: 6.67초
   메모리 사용: 45.2MB

🟢 배치 최적화 방식:
   배치크기  500:   450.2 records/sec, 메모리  35.1MB
   배치크기 1000:   720.8 records/sec, 메모리  52.3MB ⚡
   배치크기 2000:   680.5 records/sec, 메모리  78.9MB

📈 성능 개선:
   최대 성능 향상: 380.5%
   최적 배치 크기: 1000
   메모리 효율 배치: 500
```

## 환경 변수 설정

```bash
# 최적화 레벨 설정
export BATCH_OPTIMIZATION_LEVEL=aggressive  # conservative, balanced, aggressive, memory_constrained

# 개별 설정 override
export BATCH_SIZE=1500
export BATCH_MAX_MEMORY_MB=150
export BATCH_TRANSACTION_TIMEOUT=90
export BATCH_RETRY_ATTEMPTS=2
```

## 최적화 레벨별 특성

### Conservative (안전 우선)
- **배치 크기**: 500
- **메모리 제한**: 50MB
- **재시도**: 5회
- **적용 환경**: 프로덕션 초기, 안정성 우선

### Balanced (기본값)
- **배치 크기**: 1000
- **메모리 제한**: 100MB
- **재시도**: 3회
- **적용 환경**: 일반적인 프로덕션 환경

### Aggressive (성능 우선)
- **배치 크기**: 2000
- **메모리 제한**: 200MB
- **재시도**: 2회
- **적용 환경**: 고성능 서버, 대용량 처리

### Memory Constrained (메모리 제약)
- **배치 크기**: 200
- **메모리 제한**: 25MB
- **재시도**: 3회
- **적용 환경**: 제한된 리소스 환경

## 테이블별 최적화 설정

| 테이블 | 배치 크기 | 메모리(MB) | 병렬도 | UPSERT |
|--------|-----------|------------|--------|--------|
| current_weather | 1000 | 80 | 2 | ✅ |
| weather_forecasts | 1500 | 120 | 3 | ✅ |
| historical_weather_daily | 2000 | 150 | 2 | ✅ |
| tourist_attractions | 1000 | 100 | 2 | ✅ |
| restaurants | 1500 | 120 | 3 | ✅ |
| api_raw_data | 500 | 60 | 1 | ❌ |

## 모니터링 및 디버깅

### 성능 로그 확인

```python
# 배치 결과 로깅
result = await optimize_weather_current_insert(data, raw_id)

print(f"총 레코드: {result.total_records}")
print(f"성공 레코드: {result.successful_records}")
print(f"실행 시간: {result.execution_time:.2f}초")
print(f"처리량: {result.records_per_second:.1f} records/sec")
print(f"성공률: {result.success_rate:.1%}")

if result.error_details:
    for error in result.error_details:
        print(f"오류: {error}")
```

### 메모리 사용량 모니터링

```python
import tracemalloc

tracemalloc.start()
result = await optimize_weather_forecast_insert(data, raw_id)
current, peak = tracemalloc.get_traced_memory()

print(f"현재 메모리: {current / 1024 / 1024:.1f}MB")
print(f"최대 메모리: {peak / 1024 / 1024:.1f}MB")
tracemalloc.stop()
```

## 트러블슈팅

### 자주 발생하는 문제

1. **메모리 부족 오류**
   ```
   해결: BATCH_OPTIMIZATION_LEVEL=memory_constrained 설정
   또는 BATCH_MAX_MEMORY_MB 값 감소
   ```

2. **트랜잭션 타임아웃**
   ```
   해결: BATCH_TRANSACTION_TIMEOUT 값 증가
   또는 배치 크기 감소
   ```

3. **UNIQUE 제약조건 위반**
   ```
   해결: conflict_columns 설정 확인
   UPSERT 모드 활성화
   ```

4. **연결 풀 부족**
   ```
   해결: 데이터베이스 연결 풀 크기 증가
   병렬 처리 수준 조정
   ```

### 성능 최적화 팁

1. **배치 크기 조정**
   - 시작: 1000
   - 메모리 부족 시: 500으로 감소
   - 성능 부족 시: 2000으로 증가

2. **병렬 처리 활용**
   - 지역별 병렬 처리
   - 테이블별 병렬 처리
   - I/O 바운드 작업 최적화

3. **인덱스 최적화**
   - UPSERT 대상 컬럼 인덱스 확인
   - 복합 인덱스 활용
   - 불필요한 인덱스 제거

## 마이그레이션 가이드

### 기존 코드에서 배치 최적화로 전환

**Before (기존 방식):**
```python
for data in weather_data:
    await db_manager.execute_query(
        "INSERT INTO current_weather (...) VALUES (...)",
        (data.get("temperature"), ...)
    )
```

**After (배치 최적화):**
```python
from app.core.batch_insert_optimizer import optimize_weather_current_insert

result = await optimize_weather_current_insert(weather_data, raw_data_id)
logger.info(f"배치 저장 완료: {result.successful_records}건")
```

### 단계별 마이그레이션

1. **테스트 환경 적용**
   ```bash
   python scripts/test_batch_performance.py --test-size 100
   ```

2. **개발 환경 적용**
   ```bash
   export BATCH_OPTIMIZATION_LEVEL=conservative
   ```

3. **프로덕션 환경 적용**
   ```bash
   export BATCH_OPTIMIZATION_LEVEL=balanced
   ```

## 지원 및 문의

배치 최적화 관련 문의사항이 있으시면 다음을 참고하세요:

- **성능 테스트 결과**: `logs/batch_performance_report_*.txt`
- **설정 파일**: `config/batch_optimization_config.py`
- **예제 코드**: `scripts/test_batch_performance.py`

## 향후 개선 계획

1. **자동 튜닝**: 실행 환경에 따른 자동 배치 크기 조정
2. **병렬 처리 확장**: 다중 테이블 동시 처리
3. **캐시 통합**: Redis를 활용한 중간 결과 캐싱
4. **모니터링 대시보드**: 실시간 성능 모니터링