# KTO API 키 관리 시스템 사용 가이드

## 개요

수동 배치 실행 시 KTO API 키 상태를 확인하고 문제가 있는 키를 자동으로 우회하거나 복구하는 시스템입니다.

## 주요 기능

### 1. 키별 상세 로깅
- 각 API 키의 인덱스 번호와 미리보기를 통한 식별
- 키별 사용량, 오류 수, 상태 정보 실시간 추적
- 문제 발생 시 상세한 오류 정보 로깅

### 2. 자동 문제 키 비활성화
- 연속 5회 오류 발생 시 자동 비활성화
- Rate limit 초과 시 1시간 후 자동 재시도
- 최근 오류 발생 키는 10분간 대기 후 재사용

### 3. 키 복구 및 모니터링
- 비활성화된 키의 자동 복구 시도
- 전체 키 건강 상태 점검
- 실시간 키 상태 모니터링

## 사용 방법

### 1. API 키 상태 확인

```bash
# 빠른 상태 확인
python scripts/test_api_keys.py basic

# 상세 모니터링
python scripts/test_api_keys.py monitor

# 전체 건강 상태 점검
python app/utils/api_key_monitor.py health
```

### 2. 수동 배치 실행 (개선된 버전)

```bash
# 기본 실행 - 자동으로 키 상태 확인 후 수집
python scripts/collect_restaurants_only.py --mode major

# 대화형 모드 - 키 관리 메뉴 포함
python scripts/collect_restaurants_only.py --interactive

# 전국 수집
python scripts/collect_restaurants_only.py --mode all

# 특정 지역 수집
python scripts/collect_restaurants_only.py --mode regions --regions "1,6,31"
```

### 3. 문제 해결

```bash
# 키 복구 시도
python app/utils/api_key_monitor.py recover

# 종합 테스트 실행
python scripts/test_api_keys.py all
```

## 대화형 모드 메뉴

```
1. 전국 모든 지역 음식점 데이터 수집
2. 특정 지역 음식점 데이터 수집  
3. 주요 지역만 수집 (서울, 부산, 경기, 제주)
4. 사용 가능한 지역 코드 보기
5. API 키 상태 확인          ← 새로 추가
6. API 키 건강 상태 점검     ← 새로 추가  
7. 문제 키 복구 시도         ← 새로 추가
0. 종료
```

## 로그 해석

### 성공 로그
```
✅ KTO API 키 #0 호출 성공: 1234567890... (사용량: 45/1000)
✅ KTO 키 #0 선택: 1234567890... (사용량: 45/1000, 오류: 0회)
```

### 문제 로그
```
❌ KTO API 키 #1 호출 실패: abcdefghij... - 오류: API 오류 (22) (연속 오류: 3회)
🚫 KTO API 키 #1 한도 초과: abcdefghij... (재시도 가능: 15:30:00)
🚨 KTO API 키 #1 자동 비활성화: abcdefghij... (누적 오류: 5회)
```

### 우회 로그
```
⏭️ KTO 키 #1 건너뜀: abcdefghij... - 비활성화됨 (오류 5회)
⏭️ KTO 키 #2 건너뜀: xxxxxxxxxx... - Rate limit 제한 중 (25분 후 해제)
```

## 자동 조치

### 1. 키 상태 확인
- 수집 시작 전 자동으로 사용 가능한 키가 있는지 확인
- 사용 가능한 키가 없으면 복구 시도 제안

### 2. 오류 기반 비활성화
- 연속 5회 오류 → 자동 비활성화
- Rate limit 초과 → 1시간 후 자동 재시도
- 최근 오류 → 10분 대기 후 재사용

### 3. 자동 복구
- 30분 이상 지난 비활성화 키 복구 시도
- 복구 후 즉시 테스트 호출로 검증
- 테스트 실패 시 다시 비활성화

## 모니터링 지표

### 건강도 계산
```
건강도 = (사용 가능한 키 수 / 전체 키 수) × 100%
```

### 상태 분류
- **🟢 건강**: 80% 이상 키 사용 가능
- **🟡 주의**: 50-80% 키 사용 가능  
- **🔴 위험**: 50% 미만 키 사용 가능
- **❌ 심각**: 사용 가능한 키 없음

## 문제 해결 가이드

### 1. 모든 키가 비활성화된 경우
```bash
# 1. 키 상태 확인
python scripts/collect_restaurants_only.py --interactive
# → 메뉴 5번 선택

# 2. 복구 시도  
# → 메뉴 7번 선택

# 3. 여전히 문제가 있다면 새 키 발급 필요
```

### 2. 일부 키만 문제가 있는 경우
```bash
# 건강 상태 점검으로 문제 키 파악
python scripts/collect_restaurants_only.py --interactive
# → 메뉴 6번 선택

# 정상 키로 수집 계속 진행 가능
```

### 3. Rate limit 문제
- 자동으로 1시간 후 재시도됩니다
- 키가 여러 개 있다면 다른 키로 자동 우회됩니다

## 환경 설정

### .env 파일 설정
```bash
# 단일 키
KTO_API_KEY=your_api_key_here

# 다중 키 (권장)
KTO_API_KEY=key1,key2,key3,key4

# 키별 일일 한도 (기본값: 1000)
KTO_API_DAILY_LIMIT=1000
```

### 로그 설정
```bash
# logs 디렉토리 생성
mkdir -p logs

# 테스트 로그는 logs/api_key_test_YYYYMMDD_HHMMSS.log에 저장됩니다
```

## 고급 사용

### 프로그래밍 방식 사용
```python
from app.utils.api_key_monitor import get_api_key_monitor
from app.core.multi_api_key_manager import APIProvider

monitor = get_api_key_monitor()

# 빠른 상태 확인
summary = monitor.get_quick_status_summary(APIProvider.KTO)
print(f"사용 가능한 키: {summary['available_keys']}개")

# 건강 상태 점검
health = await monitor.check_all_keys_health(APIProvider.KTO)

# 복구 시도
recovery = await monitor.attempt_key_recovery(APIProvider.KTO)
```

### 키 매니저 직접 사용
```python
from app.core.multi_api_key_manager import get_api_key_manager

key_manager = get_api_key_manager()

# 상세 상태 조회
status = key_manager.get_detailed_key_status(APIProvider.KTO)

# 수동 키 비활성화
key_manager.force_deactivate_key(APIProvider.KTO, "1234567890...", "수동 비활성화")

# 키 재활성화
key_manager.reactivate_key(APIProvider.KTO, "1234567890...")
```

## 주의사항

1. **API 키 보안**: 키는 로그에서 첫 10자리만 표시됩니다
2. **자동 복구**: 30분 이상 지난 키만 복구 시도됩니다
3. **Rate limit**: KTO API 한도는 보통 하루 1000회입니다
4. **캐시**: 키 상태는 data/cache/api_key_cache.json에 저장됩니다

## 지원

문제가 발생하면 다음 정보와 함께 보고해주세요:
1. 로그 파일 (logs/api_key_test_*.log)
2. API 키 상태 보고서 (메뉴 5번 결과)
3. 오류 메시지 전문