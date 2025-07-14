-- 알림 이력 테이블 생성
CREATE TABLE IF NOT EXISTS weather_notifications (
    notification_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL REFERENCES users(user_id),
    plan_id UUID NOT NULL REFERENCES travel_plans(plan_id),
    notification_type VARCHAR(50) NOT NULL, -- 'weather_change', 'severe_weather_alert' 등
    notification_status VARCHAR(20) NOT NULL DEFAULT 'pending', -- 'pending', 'sent', 'failed'
    notification_channel VARCHAR(20) NOT NULL, -- 'email', 'push', 'sms' 등
    
    -- 날씨 변경 내용
    weather_changes JSONB NOT NULL, -- 변경된 날씨 정보 상세
    previous_weather JSONB, -- 이전 날씨 정보
    current_weather JSONB, -- 현재 날씨 정보
    
    -- 알림 메타데이터
    severity VARCHAR(20) DEFAULT 'info', -- 'info', 'warning', 'critical'
    message TEXT,
    sent_at TIMESTAMP,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성
CREATE INDEX idx_weather_notifications_user_id ON weather_notifications(user_id);
CREATE INDEX idx_weather_notifications_plan_id ON weather_notifications(plan_id);
CREATE INDEX idx_weather_notifications_status ON weather_notifications(notification_status);
CREATE INDEX idx_weather_notifications_created_at ON weather_notifications(created_at);

-- 중복 알림 방지를 위한 복합 인덱스
CREATE UNIQUE INDEX idx_weather_notifications_duplicate 
ON weather_notifications(plan_id, notification_type, date_trunc('day', created_at))
WHERE notification_status = 'sent';

-- 사용자 알림 설정 테이블
CREATE TABLE IF NOT EXISTS user_notification_preferences (
    user_id UUID PRIMARY KEY REFERENCES users(user_id),
    
    -- 알림 수신 설정
    weather_change_enabled BOOLEAN DEFAULT true,
    severe_weather_alert_enabled BOOLEAN DEFAULT true,
    daily_weather_summary_enabled BOOLEAN DEFAULT false,
    
    -- 알림 채널 설정
    email_enabled BOOLEAN DEFAULT true,
    push_enabled BOOLEAN DEFAULT false,
    sms_enabled BOOLEAN DEFAULT false,
    
    -- 알림 시간 설정
    preferred_notification_time TIME DEFAULT '09:00:00',
    timezone VARCHAR(50) DEFAULT 'Asia/Seoul',
    
    -- 알림 민감도 설정
    min_temperature_change DECIMAL(3,1) DEFAULT 5.0, -- 5도 이상 변화시 알림
    rain_probability_threshold INTEGER DEFAULT 30, -- 30% 이상 강수확률시 알림
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 트리거 함수: updated_at 자동 업데이트
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 트리거 생성
CREATE TRIGGER update_weather_notifications_updated_at 
    BEFORE UPDATE ON weather_notifications 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_user_notification_preferences_updated_at 
    BEFORE UPDATE ON user_notification_preferences 
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- 기본 알림 설정 추가 (기존 사용자용)
INSERT INTO user_notification_preferences (user_id)
SELECT user_id FROM users
ON CONFLICT (user_id) DO NOTHING;