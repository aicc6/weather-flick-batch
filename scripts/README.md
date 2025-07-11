# Weather-Flick-Batch Scripts

이 디렉토리에는 Weather-Flick 배치 시스템의 운영 및 관리에 필요한 스크립트들이 포함되어 있습니다.

## 디렉토리 구조

```
scripts/
├── testing/              # 통합 테스트 스크립트
├── sql_archive/          # SQL 스크립트 아카이브
└── *.py                  # 운영/관리 스크립트
```

## 핵심 스크립트

### 배치 실행
- **run_batch.py** - 수동 배치 작업 실행 도구
  ```bash
  python scripts/run_batch.py list                    # 작업 목록 보기
  python scripts/run_batch.py run weather             # 특정 작업 실행
  python scripts/run_batch.py status                  # 작업 상태 확인
  ```

### 모니터링 및 성능
- **test_monitoring_system.py** - 실시간 모니터링 시스템 테스트
- **test_batch_performance.py** - 배치 시스템 성능 측정
- **test_parallel_performance.py** - 병렬 처리 성능 테스트
- **test_memory_optimization.py** - 메모리 최적화 검증

### API 키 관리
- **check_api_key_status.py** - API 키 상태 및 사용량 확인
- **test_api_keys.py** - API 키 유효성 검증

### 시스템 최적화
- **optimize_weather_performance.py** - 날씨 데이터 수집 성능 최적화
- **cache_performance_optimizer.py** - 캐시 시스템 성능 최적화
- **run_archival_process.py** - 데이터 아카이빙 프로세스 실행

### 품질 관리
- **test_quality_engine.py** - 데이터 품질 엔진 테스트
- **test_priority_system.py** - 우선순위 시스템 검증

## 통합 테스트 (testing/)

CI/CD 파이프라인에서 사용되는 통합 테스트 스크립트들:

- **test_backend_integration.py** - 백엔드 시스템 통합 테스트
- **test_enhanced_batch_system.py** - 향상된 배치 시스템 테스트
- **test_enhanced_kto_collector.py** - KTO 데이터 수집기 테스트
- **test_kto_endpoints_integration.py** - KTO API 엔드포인트 통합 테스트
- **test_sql_safety.py** - SQL 인젝션 방지 테스트
- **test_kma_geocoding.py** - KMA 지오코딩 테스트

## SQL 아카이브 (sql_archive/)

데이터베이스 관련 SQL 스크립트들이 보관되어 있습니다:
- 중복 제거 쿼리
- 스키마 확인 쿼리
- 마이그레이션 스크립트

## 사용 예시

### 1. 배치 작업 상태 확인
```bash
python scripts/run_batch.py status
```

### 2. API 키 상태 모니터링
```bash
python scripts/check_api_key_status.py
```

### 3. 성능 테스트 실행
```bash
python scripts/test_batch_performance.py
```

### 4. 통합 테스트 실행
```bash
python scripts/testing/test_backend_integration.py
```

## 주의사항

1. 모든 스크립트는 프로젝트 루트에서 실행해야 합니다
2. 환경 변수 설정이 필요합니다 (.env 파일 참조)
3. 데이터베이스 연결이 필요한 스크립트는 DB 접근 권한이 필요합니다

## 정리 이력

- 2025-07-11: 일회성 스크립트 41개 제거 (60개 → 19개)
- 백업 위치: scripts_archive/20250711/