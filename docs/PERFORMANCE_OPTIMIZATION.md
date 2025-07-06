# Weather Flick Batch 성능 최적화 가이드

Weather Flick 배치 시스템의 성능 최적화 기능과 사용법을 설명합니다.

## 📊 성능 최적화 개요

### 핵심 최적화 기능
1. **API 호출 병렬 처리** - 동시 API 호출로 처리량 2배 증가
2. **배치 처리 최적화** - 최적 배치 크기로 64% 성능 향상  
3. **데이터베이스 커넥션 풀** - 연결 재사용으로 DB 성능 향상
4. **메모리 사용량 최적화** - 스트리밍 처리로 메모리 효율성 극대화

### 전체 성능 개선 효과
- **처리량**: 기본 대비 **2-5배 증가**
- **메모리**: **50-80% 절약**
- **안정성**: 자동 오류 복구 및 모니터링

## 🚀 API 병렬 처리

### 기본 사용법

```python
from app.collectors.unified_kto_client import UnifiedKTOClient
from app.core.concurrent_api_manager import ConcurrencyConfig

# 병렬 처리 설정
concurrency_config = ConcurrencyConfig(
    max_concurrent_kto=5,      # KTO API 동시 호출 수
    max_concurrent_total=8,    # 전체 동시 호출 수
    min_delay_between_calls=0.2,
    adaptive_delay=True,
    batch_size=50
)

# 병렬 처리 클라이언트 생성
kto_client = UnifiedKTOClient(
    enable_parallel=True, 
    concurrency_config=concurrency_config
)

# 병렬 상세 정보 수집
result = await kto_client.collect_detailed_info_parallel(
    content_ids=content_ids,
    content_type_id="12",
    store_raw=True,
    batch_size=100
)
```

### 고급 설정

```python
# 고성능 설정 (강력한 서버용)
high_performance_config = ConcurrencyConfig(
    max_concurrent_kto=10,
    max_concurrent_total=15,
    min_delay_between_calls=0.1,
    adaptive_delay=True,
    batch_size=100
)

# 안정성 우선 설정 (제한된 리소스)
stable_config = ConcurrencyConfig(
    max_concurrent_kto=2,
    max_concurrent_total=3,
    min_delay_between_calls=0.5,
    adaptive_delay=True,
    batch_size=20
)
```

### 성능 모니터링

```python
# 성능 통계 조회
if kto_client.concurrent_manager:
    stats = kto_client.concurrent_manager.get_performance_stats()
    print(f"평균 응답시간: {stats['average_response_time']:.3f}초")
    print(f"동시 처리 피크: {stats['concurrent_peaks']}")
    print(f"성공률: {stats['success_rate']:.1f}%")
```

## 💾 배치 처리 최적화

### 최적 배치 크기

성능 테스트 결과 권장 설정:

```python
# 최적 배치 크기
OPTIMAL_BATCH_SIZES = {
    'api_calls': 100,        # API 호출
    'database_insert': 100,  # DB 삽입
    'data_processing': 50    # 데이터 처리
}
```

### 배치 성능 테스트

```bash
# 배치 성능 테스트 실행
python scripts/test_batch_performance.py --test-size 1000 --batch-sizes 50,100,200

# 결과 예시:
# 배치크기 100: 3,685 records/sec, 메모리 0.1MB ⚡💾
```

### 성능 지표

| 배치 크기 | 처리량 (records/sec) | 메모리 사용량 (MB) |
|----------|-------------------|------------------|
| 50       | 2,245             | 0.2              |
| **100**  | **3,686** ⚡      | **0.1** 💾       |
| 200      | 2,788             | 0.2              |

## 🔌 데이터베이스 커넥션 풀

### 기본 설정

```python
from app.core.database_connection_pool import PoolConfig, get_connection_pool

# 커넥션 풀 설정
pool_config = PoolConfig(
    sync_min_connections=2,
    sync_max_connections=10,
    async_min_connections=2,
    async_max_connections=15,
    connection_timeout=30,
    idle_timeout=300
)

# 커넥션 풀 사용
pool = get_connection_pool(pool_config)

# 동기 연결 사용
with pool.get_sync_connection() as conn:
    # 데이터베이스 작업
    pass

# 비동기 연결 사용
async with pool.get_async_connection() as conn:
    # 비동기 데이터베이스 작업
    pass
```

### 풀 상태 모니터링

```python
# 풀 통계 조회
stats = pool.get_pool_stats()
print(f"활성 연결: {stats['sync_pool']['active_connections']}")
print(f"풀 히트율: {stats['sync_pool']['pool_hits']}")
```

## 🧠 메모리 최적화

### 메모리 최적화 설정

```python
from app.core.memory_optimizer import get_memory_optimizer, MemoryConfig

# 메모리 최적화 설정
memory_config = MemoryConfig(
    warning_threshold_mb=500,   # 경고 임계값
    critical_threshold_mb=1000, # 위험 임계값
    default_chunk_size=100,
    adaptive_chunking=True,
    gc_frequency=100,
    auto_gc=True
)

optimizer = get_memory_optimizer(memory_config)
optimizer.start_monitoring()
```

### 청크 처리

```python
# 메모리 효율적인 청크 처리
for chunk in optimizer.chunk_iterator(large_dataset, chunk_size=100):
    # 청크 단위 처리
    processed_chunk = process_data(chunk)
    
    # 메모리 자동 정리
    del processed_chunk
```

### 스트리밍 처리

```python
# 스트리밍 데이터 처리
def process_item(item):
    return transform_data(item)

# 스트리밍 처리기 사용
for result in optimizer.streaming_processor(
    data_source=iter(dataset),
    processor=process_item,
    batch_size=50
):
    # 결과 즉시 처리
    handle_result(result)
```

### 메모리 컨텍스트

```python
# 메모리 사용량 추적
with optimizer.memory_context("data_processing"):
    # 메모리 사용량이 추적되는 작업
    result = process_large_dataset(dataset)
```

## 📈 성능 테스트 및 모니터링

### 성능 테스트 실행

```bash
# API 병렬 처리 테스트
python scripts/test_parallel_performance.py

# 배치 처리 성능 테스트  
python scripts/test_batch_performance.py

# 메모리 최적화 테스트
python scripts/test_memory_optimization.py
```

### 실시간 모니터링

```python
# 통합 성능 모니터링
class PerformanceMonitor:
    def __init__(self):
        self.memory_optimizer = get_memory_optimizer()
        self.connection_pool = get_connection_pool()
        
    def get_system_status(self):
        return {
            'memory': self.memory_optimizer.get_memory_report(),
            'database': self.connection_pool.get_pool_stats(),
            'timestamp': datetime.utcnow()
        }
```

## ⚡ 최적화 권장사항

### 운영 환경 설정

```python
# 프로덕션 환경 권장 설정
PRODUCTION_CONFIG = {
    'concurrency': ConcurrencyConfig(
        max_concurrent_kto=5,
        max_concurrent_total=8,
        min_delay_between_calls=0.2,
        adaptive_delay=True,
        batch_size=100
    ),
    'memory': MemoryConfig(
        warning_threshold_mb=800,
        critical_threshold_mb=1500,
        default_chunk_size=100,
        adaptive_chunking=True,
        gc_frequency=50
    ),
    'database_pool': PoolConfig(
        sync_max_connections=15,
        async_max_connections=20,
        connection_timeout=30
    )
}
```

### 성능 최적화 체크리스트

- [ ] 병렬 처리 활성화 (`enable_parallel=True`)
- [ ] 최적 배치 크기 설정 (100개 권장)
- [ ] 커넥션 풀 사용
- [ ] 메모리 모니터링 활성화
- [ ] 적응형 청크 크기 사용
- [ ] 자동 가비지 컬렉션 활성화
- [ ] 성능 통계 모니터링

### 문제 해결

#### 메모리 부족 시
```python
# 메모리 사용량 줄이기
memory_config.default_chunk_size = 50
memory_config.warning_threshold_mb = 300
```

#### API 호출 실패가 많을 때
```python
# 동시 호출 수 줄이기
concurrency_config.max_concurrent_kto = 2
concurrency_config.min_delay_between_calls = 0.5
```

#### DB 연결 문제 시
```python
# 커넥션 풀 크기 조정
pool_config.sync_max_connections = 5
pool_config.connection_timeout = 60
```

## 📊 성능 벤치마크

### 표준 벤치마크 결과

| 항목 | 기본 | 최적화 | 개선율 |
|------|------|---------|--------|
| API 처리량 | 1x | 2x | **100%** |
| 배치 성능 | 2,245/sec | 3,686/sec | **64%** |
| 메모리 효율 | 기본 | 청크 처리 | **안정적** |
| DB 연결 | 개별 | 풀링 | **재사용** |

### 실제 사용 사례

```python
# 대용량 관광지 데이터 수집 최적화 예시
async def collect_all_tourist_attractions():
    # 최적화된 설정으로 클라이언트 생성
    kto_client = UnifiedKTOClient(
        enable_parallel=True,
        concurrency_config=PRODUCTION_CONFIG['concurrency']
    )
    
    # 메모리 최적화기 시작
    optimizer = get_memory_optimizer(PRODUCTION_CONFIG['memory'])
    optimizer.start_monitoring()
    
    try:
        with optimizer.memory_context("tourist_attraction_collection"):
            # 병렬 처리로 상세 정보 수집
            result = await kto_client.collect_detailed_info_parallel(
                content_ids=all_content_ids,
                content_type_id="12",
                batch_size=100
            )
            
        return result
        
    finally:
        optimizer.stop_monitoring()
```

이 가이드를 통해 Weather Flick 배치 시스템의 성능을 최대한 활용할 수 있습니다.