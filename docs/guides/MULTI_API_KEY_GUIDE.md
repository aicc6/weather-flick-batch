# 다중 API 키 시스템 사용 가이드

Weather Flick Batch에서 API 사용 한도 초과 문제를 해결하기 위한 다중 API 키 관리 시스템입니다.

## 🎯 주요 기능

- **자동 키 로테이션**: API 한도 초과 시 다음 키로 자동 전환
- **사용량 추적**: 키별 일일 사용량 모니터링
- **장애 복구**: 실패한 키 자동 비활성화 및 복구
- **캐시 관리**: 일일 사용량 정보를 로컬 캐시에 저장

## 🔧 설정 방법

### 1. 환경 변수 설정

`.env` 파일에 다음과 같이 API 키를 설정합니다:

#### 단일 키 사용
```bash
KTO_API_KEY=your_kto_api_key
KMA_API_KEY=your_kma_api_key
```

#### 다중 키 사용 (쉼표로 구분)
```bash
KTO_API_KEY=key1,key2,key3,key4,key5
KMA_API_KEY=key1,key2,key3
```

> 💡 **팁**: 여러 개의 API 키를 사용하면 각 키의 한도를 모두 활용할 수 있습니다. 
> 예: 5개의 키 × 1000회 = 총 5000회 사용 가능

### 2. 일일 사용 한도 설정
```bash
KTO_API_DAILY_LIMIT=1000  # 기본값: 1000
KMA_API_DAILY_LIMIT=1000  # 기본값: 1000
```

## 📋 사용 예제

### 기본 사용법
```python
from app.core.base_api_client import KTOAPIClient

# 클라이언트 생성 (다중 키 자동 지원)
client = KTOAPIClient()

# API 호출 (키 로테이션 자동 처리)
result = client.make_request("areaCode2", {"areaCode": "1"})
```

### 환경 변수 설정 예시
```bash
# .env 파일
# 5개의 한국관광공사 API 키를 쉼표로 구분하여 설정
KTO_API_KEY=abcd1234,efgh5678,ijkl9012,mnop3456,qrst7890

# 3개의 기상청 API 키를 쉼표로 구분하여 설정
KMA_API_KEY=key1_value,key2_value,key3_value

# 각 키의 일일 사용 한도 설정
KTO_API_DAILY_LIMIT=1000
KMA_API_DAILY_LIMIT=1000
```

### 키 매니저 직접 사용
```python
from app.core.multi_api_key_manager import get_api_key_manager, APIProvider

# 키 매니저 인스턴스 가져오기
key_manager = get_api_key_manager()

# 현재 활성 키 확인
active_key = key_manager.get_active_key(APIProvider.KTO)
if active_key:
    print(f"현재 키: {active_key.key[:10]}...")
    print(f"사용량: {active_key.current_usage}/{active_key.daily_limit}")

# 사용량 통계 확인
stats = key_manager.get_usage_stats()
print(f"전체 키 개수: {stats['total_keys']}")
print(f"활성 키 개수: {stats['active_keys']}")
```

## 🔄 동작 원리

### 1. 키 로테이션
- API 호출 시 현재 활성 키 사용
- 한도 초과 또는 오류 발생 시 다음 키로 자동 전환
- 모든 키가 한도 초과인 경우 대기 후 재시도

### 2. 사용량 추적
- 각 API 호출마다 키별 사용량 기록
- 성공/실패 통계 분리 관리
- 일일 한도 도달 시 해당 키 비활성화

### 3. 오류 처리
- 5회 이상 연속 실패 시 키 자동 비활성화
- Rate limit 오류 시 1시간 후 재활성화
- 일반 오류 시 10분 후 재시도 가능

### 4. 캐시 관리
- 키별 사용량을 `api_key_cache.json`에 저장
- 매일 자정에 사용량 자동 초기화
- 프로그램 재시작 시 캐시에서 사용량 복원

## 📊 모니터링

### 사용량 확인
```python
# 전체 통계 확인
stats = key_manager.get_usage_stats()
for provider, data in stats['providers'].items():
    print(f"{provider}: {data['total_usage']}/{data['total_limit']}")
    
    for key_info in data['keys']:
        print(f"  키 {key_info['index']}: {key_info['usage_percent']:.1f}% 사용")
```

### 로그 모니터링
시스템이 자동으로 다음과 같은 로그를 출력합니다:
```
🔑 KTO API 키 3개 로드됨
🔄 다른 KTO API 키로 전환합니다: abcd123456...
⚠️ KTO API 키 한도 초과: efgh789012...
📊 API 요청 진행: 500/1000
```

## 🧪 테스트

테스트 스크립트를 실행하여 시스템 동작을 확인할 수 있습니다:

```bash
python test_multi_api_key.py
```

## ⚠️ 주의사항

1. **API 키 보안**: 환경 변수에 실제 키 값을 안전하게 저장하세요
2. **한도 설정**: 실제 API 제공업체의 한도보다 낮게 설정하는 것을 권장합니다
3. **캐시 파일**: `api_key_cache.json` 파일을 정기적으로 백업하세요
4. **로그 모니터링**: 키 전환 및 오류 발생을 정기적으로 모니터링하세요

## 🔧 문제해결

### Q: 모든 키가 한도 초과로 표시됩니다
A: 다음을 확인하세요:
- 실제 API 사용 한도와 설정된 한도 비교
- 캐시 파일 삭제 후 재시작
- 키의 유효성 확인

### Q: 키 로테이션이 동작하지 않습니다
A: 다음을 확인하세요:
- 환경 변수에 여러 키가 올바르게 설정되었는지 확인
- 로그에서 키 로드 메시지 확인
- `test_multi_api_key.py` 실행하여 키 상태 확인

### Q: 캐시 파일 오류가 발생합니다
A: 다음을 시도하세요:
- `api_key_cache.json` 파일 삭제
- 프로그램 재시작
- 파일 권한 확인

## 📝 추가 정보

- 이 시스템은 한국관광공사(KTO)와 기상청(KMA) API를 지원합니다
- 새로운 API 제공업체는 `APIProvider` enum에 추가하여 확장 가능합니다
- 키 매니저는 싱글톤 패턴으로 구현되어 전역적으로 사용됩니다