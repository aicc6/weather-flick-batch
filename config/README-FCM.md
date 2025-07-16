# Firebase FCM 설정 가이드 - Weather Flick Batch

## 1. Firebase 프로젝트 설정

1. [Firebase Console](https://console.firebase.google.com/)에 접속
2. Weather Flick 프로젝트 선택 (또는 새로 생성)
3. 프로젝트 설정 > 서비스 계정 탭으로 이동
4. "새 비공개 키 생성" 버튼 클릭
5. JSON 파일 다운로드

## 2. 서비스 계정 키 파일 설정

다운로드한 JSON 파일을 다음 위치에 저장:
```
weather-flick-batch/config/firebase-service-account.json
```

**주의사항:**
- 이 파일은 절대 Git에 커밋하지 마세요 (.gitignore에 포함됨)
- 이 파일은 Firebase Admin SDK의 모든 권한을 가지므로 안전하게 보관해야 합니다

## 3. 환경 변수 설정

`.env` 파일에 다음 설정 추가:

```env
# Firebase 설정
FIREBASE_CREDENTIALS_PATH=config/firebase-service-account.json
FIREBASE_PROJECT_ID=your-firebase-project-id
```

## 4. FCM 토큰 테이블 생성

다음 SQL을 실행하여 사용자 FCM 토큰 관리 테이블을 생성:

```sql
-- FCM 토큰 관리 테이블
CREATE TABLE IF NOT EXISTS user_fcm_tokens (
    token_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    fcm_token TEXT NOT NULL,
    device_type VARCHAR(20), -- 'web', 'ios', 'android'
    device_info JSONB, -- 추가 디바이스 정보
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP + INTERVAL '60 days'), -- FCM 토큰 만료 기간
    UNIQUE(fcm_token)
);

-- 인덱스 생성
CREATE INDEX idx_user_fcm_tokens_user_id ON user_fcm_tokens(user_id);
CREATE INDEX idx_user_fcm_tokens_active ON user_fcm_tokens(is_active) WHERE is_active = true;
CREATE INDEX idx_user_fcm_tokens_expires ON user_fcm_tokens(expires_at);
```

## 5. 알림 발송 테스트

### Python 테스트 스크립트:

```python
from app.monitoring.fcm_notification_channel import send_fcm_notification_to_user

# 테스트 알림 발송
success = send_fcm_notification_to_user(
    user_id="사용자_UUID",
    title="날씨 변화 알림",
    body="서울의 날씨가 변경되었습니다. 우산을 준비하세요!",
    data={
        "type": "weather_change",
        "plan_id": "플랜_UUID"
    },
    url="/travel-plans/플랜_UUID"
)

print(f"알림 발송 {'성공' if success else '실패'}")
```

## 6. FCM 알림 통합 완료

배치 시스템의 `WeatherChangeNotificationJob`은 이제 다음과 같이 알림을 전송합니다:

1. **이메일**: 기존대로 SMTP를 통해 전송
2. **FCM 푸시**: 모바일 앱과 웹 브라우저로 실시간 알림

사용자는 알림 설정에서 각 채널을 개별적으로 활성화/비활성화할 수 있습니다.

## 7. 주의사항

- FCM 토큰은 주기적으로 갱신될 수 있으므로, 프론트엔드에서 토큰 갱신 시 백엔드 API를 호출해야 합니다
- 무효한 토큰은 자동으로 비활성화됩니다
- 한 사용자가 여러 디바이스를 사용할 수 있으므로, 멀티캐스트 발송을 지원합니다
- 토큰 만료 기간(60일)이 지난 토큰은 주기적으로 정리해야 합니다