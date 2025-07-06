# Weather Flick Batch 데이터 수집 전략 진화 분석

## 목차
1. [초기 단순 스크립트에서 고도화된 배치 시스템으로의 진화](#1-초기-단순-스크립트에서-고도화된-배치-시스템으로의-진화)
2. [데이터 수집 전략 변화](#2-데이터-수집-전략-변화)
3. [다중 API 키 관리 시스템](#3-다중-api-키-관리-시스템)
4. [배치 작업 스케줄링](#4-배치-작업-스케줄링)
5. [데이터 처리 파이프라인](#5-데이터-처리-파이프라인)
6. [성능 최적화](#6-성능-최적화)
7. [모니터링 및 알림](#7-모니터링-및-알림)

---

## 1. 초기 단순 스크립트에서 고도화된 배치 시스템으로의 진화

### 초기 단계: 단순 스크립트
```python
# 초기 단순 접근법 (예시)
import requests

def collect_tourist_data():
    response = requests.get(f"{API_URL}?serviceKey={API_KEY}")
    data = response.json()
    save_to_db(data)
```

**특징:**
- 단일 API 호출
- 동기식 처리
- 에러 처리 미흡
- 수동 실행

### 현재: 고도화된 배치 시스템
```python
# main_advanced.py 구조
class WeatherFlickBatchSystem:
    def __init__(self):
        self.batch_manager = get_batch_manager()
        self.setup_all_jobs()
    
    def setup_all_jobs(self):
        self.setup_data_management_jobs()
        self.setup_system_maintenance_jobs()
        self.setup_monitoring_jobs()
        self.setup_business_logic_jobs()
        self.setup_quality_jobs()
```

**진화된 특징:**
- 체계적인 작업 관리 시스템
- 의존성 관리
- 자동 스케줄링
- 포괄적인 에러 처리
- 모니터링 및 로깅

---

## 2. 데이터 수집 전략 변화

### 2.1 단일 API에서 15개 API 통합까지

#### 초기: 단일 API
```python
# 한국관광공사 API만 사용
def collect_kto_data():
    # 단일 엔드포인트 호출
    pass
```

#### 현재: 다중 API 통합
```python
# unified_api_client.py
class UnifiedAPIClient:
    def __init__(self):
        self.expiry_settings = {
            APIProvider.KTO: timedelta(days=7),
            APIProvider.KMA: timedelta(hours=6),
            APIProvider.GOOGLE: timedelta(days=30),
            APIProvider.NAVER: timedelta(days=1),
        }
    
    async def call_api(self, api_provider, endpoint, params):
        # 통합 API 호출 로직
        pass
```

**장점:**
- 다양한 데이터 소스 활용
- API별 최적화된 만료 시간 설정
- 중앙화된 API 관리

**단점:**
- 복잡성 증가
- API별 다른 응답 형식 처리 필요

### 2.2 동기에서 비동기 처리로

#### 초기: 동기 처리
```python
def collect_data_sync():
    for area in areas:
        data = requests.get(url)  # 블로킹
        process_data(data)
```

#### 현재: 비동기 처리
```python
# unified_api_client.py
async def call_api(self, api_provider, endpoint, params):
    async with self.session.get(url, params=params) as response:
        response_data = await response.json()
        return APIResponse(data=response_data)
```

**장점:**
- 동시 다중 요청 가능
- 처리 속도 향상
- 리소스 효율성

### 2.3 페이징 처리 전략

```python
# comprehensive_tourism_job.py
async def collect_all_data(self, content_types=None, area_codes=None):
    # 자동 페이징 처리
    while has_more_pages:
        response = await self.unified_client.call_api(
            APIProvider.KTO,
            endpoint,
            params={'pageNo': page_no, 'numOfRows': 100}
        )
        # 데이터 수집 및 다음 페이지 확인
```

**특징:**
- 자동 페이지네이션
- 대용량 데이터 처리
- 메모리 효율적 처리

### 2.4 에러 처리 및 재시도 로직

```python
# advanced_scheduler.py
def _execute_job_with_retry_sync(self, job_id, job_function):
    config = self.job_configs[job_id]
    
    for attempt in range(config.retry_attempts + 1):
        try:
            result = job_function()
            return result
        except Exception as e:
            if attempt < config.retry_attempts:
                time.sleep(config.retry_delay * (2**attempt))  # 지수 백오프
```

**특징:**
- 지수 백오프 전략
- 작업별 재시도 설정
- 실패 로깅 및 추적

---

## 3. 다중 API 키 관리 시스템

### 3.1 일일 한도 문제 해결

```python
# multi_api_key_manager.py
class MultiAPIKeyManager:
    def __init__(self):
        self.api_keys = {
            APIProvider.KTO: [],  # 여러 키 저장
            APIProvider.KMA: [],
        }
        self._load_api_keys_from_env()
```

**핵심 기능:**
- 환경 변수에서 쉼표로 구분된 여러 키 로드
- 키별 사용량 추적
- 일일 한도 도달 시 자동 순환

### 3.2 키 순환 알고리즘

```python
def get_active_key(self, provider: APIProvider) -> Optional[APIKeyInfo]:
    keys = self.api_keys[provider]
    start_index = self.current_key_index[provider]
    
    # 순환하며 사용 가능한 키 찾기
    for i in range(len(keys)):
        current_index = (start_index + i) % len(keys)
        key_info = keys[current_index]
        
        if self._is_key_available(key_info):
            self.current_key_index[provider] = current_index
            return key_info
    
    # 모든 키가 불가능하면 가장 적게 사용된 키 반환
    return self._get_least_used_key(provider)
```

**장점:**
- 자동 키 순환
- 부하 분산
- 장애 복구

### 3.3 사용량 추적 및 캐싱

```python
def record_api_call(self, provider, key, success=True, is_rate_limited=False):
    key_info = self._find_key_info(provider, key)
    key_info.current_usage += 1
    
    if is_rate_limited:
        key_info.rate_limit_reset_time = datetime.now() + timedelta(hours=1)
    
    # 오류가 많은 키는 자동 비활성화
    if key_info.error_count >= 5:
        key_info.is_active = False
    
    self._save_cache()
```

**특징:**
- 실시간 사용량 모니터링
- 자동 키 비활성화/활성화
- 캐시 기반 상태 저장

---

## 4. 배치 작업 스케줄링

### 4.1 작업별 실행 주기

```python
# main_advanced.py
def setup_data_management_jobs(self):
    # 날씨 데이터: 1시간마다
    self.batch_manager.register_job(
        weather_config, weather_update_sync, 
        trigger="interval", hours=1
    )
    
    # 관광정보: 매주 일요일 새벽 2시
    self.batch_manager.register_job(
        comprehensive_tourism_config, comprehensive_tourism_task,
        trigger="cron", day_of_week="sun", hour=2, minute=0
    )
    
    # 데이터 품질: 매일 새벽 6시
    self.batch_manager.register_job(
        quality_config, quality_check_task,
        trigger="cron", hour=6, minute=0
    )
```

### 4.2 의존성 관리

```python
# advanced_scheduler.py
@dataclass
class BatchJobConfig:
    dependencies: List[str] = None
    
def _check_dependencies_sync(self, dependencies: List[str]) -> bool:
    for dep_job_id in dependencies:
        dep_result = self.job_results[dep_job_id]
        
        # 24시간 이내 성공 여부 확인
        if (datetime.now() - dep_result.end_time).total_seconds() > 86400:
            return False
        
        if dep_result.status != JobStatus.SUCCESS:
            return False
    
    return True
```

### 4.3 실패 처리

```python
class BatchJobType(Enum):
    WEATHER_UPDATE = "weather_update"
    DESTINATION_SYNC = "destination_sync"
    COMPREHENSIVE_TOURISM_SYNC = "comprehensive_tourism_sync"

@dataclass
class BatchJobConfig:
    retry_attempts: int = 3
    retry_delay: int = 60
    timeout: int = 3600
```

---

## 5. 데이터 처리 파이프라인

### 5.1 원본 데이터 저장

```python
# unified_api_client.py
def _store_raw_data(self, api_provider, endpoint, request_params, 
                    response_data, response_status, duration_ms, api_key):
    raw_data = {
        "api_provider": api_provider.value,
        "endpoint": endpoint,
        "request_params": request_params,
        "raw_response": response_data,
        "response_size": len(json.dumps(response_data).encode('utf-8')),
        "request_duration": duration_ms,
        "expires_at": self._calculate_expiry_time(api_provider),
    }
    
    raw_data_id = self.db_manager.insert_raw_data(raw_data)
    return raw_data_id
```

### 5.2 데이터 변환 및 정규화

```python
# data_transformation_pipeline.py
class DataTransformationPipeline:
    def transform_data(self, raw_data, content_type):
        # 컨텐츠 타입별 변환 로직
        if content_type == "12":  # 관광지
            return self._transform_tourist_attraction(raw_data)
        elif content_type == "39":  # 음식점
            return self._transform_restaurant(raw_data)
```

### 5.3 품질 검사

```python
# comprehensive_tourism_job.py
def _check_data_quality(self) -> Dict:
    quality_results = {}
    
    for table in tables_to_check:
        query = f"""
            SELECT
                COUNT(*) as total_records,
                COUNT(CASE WHEN latitude IS NULL THEN 1 END) as missing_coordinates,
                COUNT(CASE WHEN address IS NULL THEN 1 END) as missing_address
            FROM {table}
        """
        
        result = self.db_manager.fetch_one(query)
        quality_score = 100.0 - ((missing_coords + missing_addr) * 100.0 / total)
        
        quality_results[table] = {
            "total_records": total,
            "quality_score": round(quality_score, 2)
        }
```

### 5.4 통계 생성

```python
# data_priority_manager.py
class DataPriorityManager:
    def get_recommended_collection_order(self, content_types=None, max_per_type=5):
        priority_content_types = self.get_priority_sorted_content_types(content_types)
        
        collection_plan = {
            "analysis_time": datetime.now().isoformat(),
            "priority_order": [],
            "detailed_plan": {}
        }
        
        for rank, (content_type, count, name) in enumerate(priority_content_types, 1):
            area_priorities = self.get_area_priority_by_content_type(content_type)
            # 우선순위 기반 수집 계획 생성
```

---

## 6. 성능 최적화

### 6.1 병렬 처리

```python
# advanced_scheduler.py
executors = {
    "default": ThreadPoolExecutor(max_workers=20),
    "processpool": ProcessPoolExecutor(max_workers=5),
}
```

### 6.2 메모리 관리

```python
# 페이징을 통한 메모리 효율적 처리
async def collect_with_pagination(self):
    page_size = 100
    page_no = 1
    
    while True:
        data = await self.fetch_page(page_no, page_size)
        if not data:
            break
        
        # 즉시 처리하여 메모리 해제
        await self.process_and_store(data)
        page_no += 1
```

### 6.3 배치 크기 최적화

```python
# 데이터베이스 삽입 시 배치 처리
def bulk_insert(self, records, batch_size=1000):
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        self.db_manager.bulk_insert(batch)
```

---

## 7. 모니터링 및 알림

### 7.1 작업 실행 로깅

```python
# advanced_scheduler.py
def _log_job_result_sync(self, result: JobResult):
    duration = (result.end_time - result.start_time).total_seconds()
    
    if result.status == JobStatus.SUCCESS:
        self.logger.info(
            f"작업 완료: {result.job_name}, "
            f"소요시간: {duration:.2f}초, "
            f"처리건수: {result.processed_records}"
        )
    else:
        self.logger.error(
            f"작업 실패: {result.job_name}, 오류: {result.error_message}"
        )
```

### 7.2 에러 알림

```python
# 에러 발생 시 자동 알림
def notify_error(self, job_name, error):
    # Slack, Email 등으로 알림 발송
    notification_service.send_alert(
        title=f"배치 작업 실패: {job_name}",
        message=str(error),
        severity="high"
    )
```

### 7.3 성능 메트릭

```python
# 성능 모니터링
class JobResult:
    job_name: str
    start_time: datetime
    end_time: datetime
    processed_records: int
    
    @property
    def duration(self):
        return (self.end_time - self.start_time).total_seconds()
    
    @property
    def records_per_second(self):
        return self.processed_records / self.duration if self.duration > 0 else 0
```

---

## 결론

Weather Flick Batch 시스템은 단순한 스크립트에서 시작하여 고도화된 엔터프라이즈급 배치 시스템으로 진화했습니다. 

### 주요 성과:
1. **확장성**: 단일 API에서 다중 API 통합
2. **안정성**: 포괄적인 에러 처리 및 재시도 메커니즘
3. **효율성**: 비동기 처리 및 병렬 실행
4. **자동화**: 스케줄링 및 의존성 관리
5. **가시성**: 상세한 로깅 및 모니터링

### 향후 개선 방향:
1. 실시간 스트리밍 데이터 처리
2. 머신러닝 기반 수집 우선순위 최적화
3. 분산 처리 시스템 도입 (Apache Spark 등)
4. 더 정교한 캐싱 전략
5. 자동 복구 메커니즘 강화