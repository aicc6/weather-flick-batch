-- FCM 토큰 관리 테이블 생성
CREATE TABLE IF NOT EXISTS user_fcm_tokens (
    token_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    fcm_token TEXT NOT NULL,
    device_type VARCHAR(20), -- 'web', 'ios', 'android'
    device_info JSONB, -- 추가 디바이스 정보 (user agent, 브라우저 버전 등)
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP DEFAULT (CURRENT_TIMESTAMP + INTERVAL '60 days'), -- FCM 토큰 만료 기간
    last_used_at TIMESTAMP,
    UNIQUE(fcm_token)
);

-- 인덱스 생성
CREATE INDEX idx_user_fcm_tokens_user_id ON user_fcm_tokens(user_id);
CREATE INDEX idx_user_fcm_tokens_active ON user_fcm_tokens(is_active) WHERE is_active = true;
CREATE INDEX idx_user_fcm_tokens_expires ON user_fcm_tokens(expires_at);
CREATE INDEX idx_user_fcm_tokens_device_type ON user_fcm_tokens(device_type);

-- FCM 알림 발송 이력 테이블
CREATE TABLE IF NOT EXISTS fcm_notification_logs (
    log_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id),
    token_id UUID REFERENCES user_fcm_tokens(token_id),
    notification_type VARCHAR(50) NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    data JSONB,
    status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'sent', 'failed'
    error_message TEXT,
    sent_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX idx_fcm_notification_logs_user_id ON fcm_notification_logs(user_id);
CREATE INDEX idx_fcm_notification_logs_status ON fcm_notification_logs(status);
CREATE INDEX idx_fcm_notification_logs_created_at ON fcm_notification_logs(created_at);

-- 트리거: FCM 토큰 updated_at 자동 업데이트
CREATE TRIGGER update_user_fcm_tokens_updated_at 
    BEFORE UPDATE ON user_fcm_tokens 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- user_notification_preferences 테이블에 FCM 관련 컬럼 추가 (이미 존재하지 않는 경우)
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                   WHERE table_name = 'user_notification_preferences' 
                   AND column_name = 'fcm_enabled') THEN
        ALTER TABLE user_notification_preferences 
        ADD COLUMN fcm_enabled BOOLEAN DEFAULT true;
    END IF;
END $$;

-- 만료된 FCM 토큰 정리를 위한 함수
CREATE OR REPLACE FUNCTION cleanup_expired_fcm_tokens()
RETURNS void AS $$
BEGIN
    -- 만료된 토큰을 비활성화
    UPDATE user_fcm_tokens
    SET is_active = false,
        updated_at = CURRENT_TIMESTAMP
    WHERE expires_at < CURRENT_TIMESTAMP
      AND is_active = true;
    
    -- 90일 이상 비활성화된 토큰 삭제
    DELETE FROM user_fcm_tokens
    WHERE is_active = false
      AND updated_at < CURRENT_TIMESTAMP - INTERVAL '90 days';
END;
$$ LANGUAGE plpgsql;

-- 정기적인 토큰 정리를 위한 스케줄러 작업 추가 권장
-- (PostgreSQL의 pg_cron 또는 배치 시스템에서 주기적으로 실행)