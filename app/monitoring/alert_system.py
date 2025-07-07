"""
실시간 알림 시스템

성능 이슈, 시스템 장애, API 호출 실패 등에 대한 실시간 알림을 제공합니다.
"""

import logging
import json
import smtplib
import os
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import requests

from app.monitoring.performance_monitor import PerformanceAlert, PerformanceMetricType

logger = logging.getLogger(__name__)


class AlertChannel(Enum):
    """알림 채널 타입"""
    EMAIL = "email"
    SLACK = "slack"
    WEBHOOK = "webhook"
    LOG = "log"
    SMS = "sms"


class AlertSeverity(Enum):
    """알림 심각도"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AlertConfig:
    """알림 설정"""
    channel: AlertChannel
    severity_filter: List[AlertSeverity] = field(default_factory=lambda: [AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL])
    enabled: bool = True
    rate_limit_minutes: int = 5  # 동일 알림 재전송 제한 시간
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AlertMessage:
    """알림 메시지"""
    title: str
    message: str
    severity: AlertSeverity
    source: str
    timestamp: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    alert_id: Optional[str] = None


class AlertThrottler:
    """알림 속도 제한"""
    
    def __init__(self):
        self.sent_alerts: Dict[str, datetime] = {}
    
    def should_send_alert(self, alert_key: str, rate_limit_minutes: int) -> bool:
        """알림 전송 여부 확인"""
        now = datetime.now()
        
        if alert_key not in self.sent_alerts:
            self.sent_alerts[alert_key] = now
            return True
        
        last_sent = self.sent_alerts[alert_key]
        if (now - last_sent).total_seconds() >= rate_limit_minutes * 60:
            self.sent_alerts[alert_key] = now
            return True
        
        return False
    
    def cleanup_old_entries(self, hours: int = 24):
        """오래된 항목 정리"""
        cutoff = datetime.now() - timedelta(hours=hours)
        self.sent_alerts = {
            key: timestamp for key, timestamp in self.sent_alerts.items()
            if timestamp >= cutoff
        }


class EmailNotifier:
    """이메일 알림"""
    
    def __init__(self, config: Dict[str, Any]):
        self.smtp_server = config.get("smtp_server", "smtp.gmail.com")
        self.smtp_port = config.get("smtp_port", 587)
        self.username = config.get("username", os.getenv("ALERT_EMAIL_USERNAME"))
        self.password = config.get("password", os.getenv("ALERT_EMAIL_PASSWORD"))
        self.from_email = config.get("from_email", self.username)
        self.to_emails = config.get("to_emails", [])
        
        if isinstance(self.to_emails, str):
            self.to_emails = [self.to_emails]
    
    def send_alert(self, alert: AlertMessage) -> bool:
        """이메일 알림 전송"""
        try:
            if not self.username or not self.password or not self.to_emails:
                logger.warning("이메일 설정이 불완전합니다")
                return False
            
            # 이메일 메시지 구성
            msg = MIMEMultipart()
            msg['From'] = self.from_email
            msg['To'] = ', '.join(self.to_emails)
            msg['Subject'] = f"[{alert.severity.value.upper()}] {alert.title}"
            
            # HTML 형태의 메시지 본문
            html_body = self._create_html_body(alert)
            msg.attach(MIMEText(html_body, 'html'))
            
            # SMTP 서버를 통해 전송
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.username, self.password)
                server.send_message(msg)
            
            logger.info(f"이메일 알림 전송 완료: {alert.title}")
            return True
            
        except Exception as e:
            logger.error(f"이메일 알림 전송 실패: {e}")
            return False
    
    def _create_html_body(self, alert: AlertMessage) -> str:
        """HTML 이메일 본문 생성"""
        severity_colors = {
            AlertSeverity.INFO: "#17a2b8",
            AlertSeverity.WARNING: "#ffc107", 
            AlertSeverity.ERROR: "#fd7e14",
            AlertSeverity.CRITICAL: "#dc3545"
        }
        
        color = severity_colors.get(alert.severity, "#6c757d")
        
        html = f"""
        <html>
        <body style="font-family: Arial, sans-serif; margin: 20px;">
            <div style="border-left: 4px solid {color}; padding-left: 15px;">
                <h2 style="color: {color}; margin-top: 0;">
                    {alert.severity.value.upper()} - {alert.title}
                </h2>
                <p><strong>시간:</strong> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}</p>
                <p><strong>소스:</strong> {alert.source}</p>
                <div style="background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin: 15px 0;">
                    <p style="margin: 0; white-space: pre-line;">{alert.message}</p>
                </div>
        """
        
        if alert.metadata:
            html += "<h3>추가 정보:</h3><ul>"
            for key, value in alert.metadata.items():
                html += f"<li><strong>{key}:</strong> {value}</li>"
            html += "</ul>"
        
        html += """
            </div>
            <hr style="margin-top: 30px;">
            <p style="color: #6c757d; font-size: 12px;">
                이 알림은 Weather Flick 배치 시스템에서 자동 생성되었습니다.
            </p>
        </body>
        </html>
        """
        
        return html


class SlackNotifier:
    """Slack 알림"""
    
    def __init__(self, config: Dict[str, Any]):
        self.webhook_url = config.get("webhook_url", os.getenv("SLACK_WEBHOOK_URL"))
        self.channel = config.get("channel", "#alerts")
        self.username = config.get("username", "Weather Flick Bot")
    
    def send_alert(self, alert: AlertMessage) -> bool:
        """Slack 알림 전송"""
        try:
            if not self.webhook_url:
                logger.warning("Slack webhook URL이 설정되지 않았습니다")
                return False
            
            # Slack 메시지 페이로드 구성
            payload = {
                "channel": self.channel,
                "username": self.username,
                "text": f"{alert.severity.value.upper()}: {alert.title}",
                "attachments": [{
                    "color": self._get_color_for_severity(alert.severity),
                    "fields": [
                        {
                            "title": "메시지",
                            "value": alert.message,
                            "short": False
                        },
                        {
                            "title": "소스",
                            "value": alert.source,
                            "short": True
                        },
                        {
                            "title": "시간",
                            "value": alert.timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                            "short": True
                        }
                    ],
                    "footer": "Weather Flick 모니터링",
                    "ts": int(alert.timestamp.timestamp())
                }]
            }
            
            # 추가 메타데이터가 있으면 필드에 추가
            if alert.metadata:
                for key, value in alert.metadata.items():
                    payload["attachments"][0]["fields"].append({
                        "title": key,
                        "value": str(value),
                        "short": True
                    })
            
            # Webhook으로 전송
            response = requests.post(self.webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            
            logger.info(f"Slack 알림 전송 완료: {alert.title}")
            return True
            
        except Exception as e:
            logger.error(f"Slack 알림 전송 실패: {e}")
            return False
    
    def _get_color_for_severity(self, severity: AlertSeverity) -> str:
        """심각도별 색상 반환"""
        colors = {
            AlertSeverity.INFO: "good",
            AlertSeverity.WARNING: "warning",
            AlertSeverity.ERROR: "danger",
            AlertSeverity.CRITICAL: "danger"
        }
        return colors.get(severity, "good")


class WebhookNotifier:
    """일반 Webhook 알림"""
    
    def __init__(self, config: Dict[str, Any]):
        self.webhook_url = config.get("webhook_url")
        self.headers = config.get("headers", {"Content-Type": "application/json"})
        self.timeout = config.get("timeout", 10)
    
    def send_alert(self, alert: AlertMessage) -> bool:
        """Webhook 알림 전송"""
        try:
            if not self.webhook_url:
                logger.warning("Webhook URL이 설정되지 않았습니다")
                return False
            
            # Webhook 페이로드 구성
            payload = {
                "alert_id": alert.alert_id,
                "title": alert.title,
                "message": alert.message,
                "severity": alert.severity.value,
                "source": alert.source,
                "timestamp": alert.timestamp.isoformat(),
                "metadata": alert.metadata
            }
            
            # HTTP POST 요청
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers=self.headers,
                timeout=self.timeout
            )
            response.raise_for_status()
            
            logger.info(f"Webhook 알림 전송 완료: {alert.title}")
            return True
            
        except Exception as e:
            logger.error(f"Webhook 알림 전송 실패: {e}")
            return False


class LogNotifier:
    """로그 알림"""
    
    def __init__(self, config: Dict[str, Any]):
        self.log_level_map = {
            AlertSeverity.INFO: logging.INFO,
            AlertSeverity.WARNING: logging.WARNING,
            AlertSeverity.ERROR: logging.ERROR,
            AlertSeverity.CRITICAL: logging.CRITICAL
        }
    
    def send_alert(self, alert: AlertMessage) -> bool:
        """로그 알림 출력"""
        try:
            log_level = self.log_level_map.get(alert.severity, logging.INFO)
            
            log_message = f"🚨 [{alert.severity.value.upper()}] {alert.title}"
            if alert.message:
                log_message += f" - {alert.message}"
            if alert.source:
                log_message += f" (소스: {alert.source})"
            
            logger.log(log_level, log_message)
            return True
            
        except Exception as e:
            logger.error(f"로그 알림 출력 실패: {e}")
            return False


class AlertSystem:
    """통합 알림 시스템"""
    
    def __init__(self):
        self.configs: Dict[AlertChannel, AlertConfig] = {}
        self.notifiers: Dict[AlertChannel, Any] = {}
        self.throttler = AlertThrottler()
        self.custom_handlers: List[Callable[[AlertMessage], bool]] = []
        
        # 기본 로그 알림은 항상 활성화
        self._setup_default_log_notifier()
        
        # 알림 통계
        self.alert_stats = {
            "total_alerts_sent": 0,
            "alerts_by_severity": {severity.value: 0 for severity in AlertSeverity},
            "alerts_by_channel": {channel.value: 0 for channel in AlertChannel},
            "failed_alerts": 0,
            "throttled_alerts": 0
        }
        
        logger.info("알림 시스템 초기화 완료")
    
    def _setup_default_log_notifier(self):
        """기본 로그 알림 설정"""
        self.configs[AlertChannel.LOG] = AlertConfig(
            channel=AlertChannel.LOG,
            severity_filter=list(AlertSeverity),
            enabled=True,
            rate_limit_minutes=0  # 로그는 속도 제한 없음
        )
        self.notifiers[AlertChannel.LOG] = LogNotifier({})
    
    def configure_channel(self, channel: AlertChannel, config: AlertConfig):
        """알림 채널 설정"""
        self.configs[channel] = config
        
        # 채널별 알림 객체 생성
        if channel == AlertChannel.EMAIL:
            self.notifiers[channel] = EmailNotifier(config.config)
        elif channel == AlertChannel.SLACK:
            self.notifiers[channel] = SlackNotifier(config.config)
        elif channel == AlertChannel.WEBHOOK:
            self.notifiers[channel] = WebhookNotifier(config.config)
        elif channel == AlertChannel.LOG:
            self.notifiers[channel] = LogNotifier(config.config)
        
        logger.info(f"알림 채널 설정 완료: {channel.value}")
    
    def add_custom_handler(self, handler: Callable[[AlertMessage], bool]):
        """커스텀 알림 핸들러 추가"""
        self.custom_handlers.append(handler)
        logger.info("커스텀 알림 핸들러 추가됨")
    
    def send_alert(self, alert: AlertMessage) -> Dict[str, bool]:
        """알림 전송"""
        results = {}
        
        # 알림 ID 생성 (없는 경우)
        if not alert.alert_id:
            alert.alert_id = f"{alert.source}_{alert.severity.value}_{int(alert.timestamp.timestamp())}"
        
        # 각 설정된 채널로 알림 전송
        for channel, config in self.configs.items():
            if not config.enabled:
                continue
            
            # 심각도 필터 확인
            if alert.severity not in config.severity_filter:
                continue
            
            # 속도 제한 확인
            alert_key = f"{channel.value}_{alert.alert_id}"
            if not self.throttler.should_send_alert(alert_key, config.rate_limit_minutes):
                self.alert_stats["throttled_alerts"] += 1
                results[channel.value] = False
                continue
            
            # 알림 전송 시도
            try:
                notifier = self.notifiers.get(channel)
                if notifier:
                    success = notifier.send_alert(alert)
                    results[channel.value] = success
                    
                    if success:
                        self.alert_stats["alerts_by_channel"][channel.value] += 1
                    else:
                        self.alert_stats["failed_alerts"] += 1
                else:
                    logger.warning(f"알림 채널 {channel.value}에 대한 notifier가 없습니다")
                    results[channel.value] = False
                    
            except Exception as e:
                logger.error(f"알림 전송 실패 ({channel.value}): {e}")
                results[channel.value] = False
                self.alert_stats["failed_alerts"] += 1
        
        # 커스텀 핸들러 실행
        for handler in self.custom_handlers:
            try:
                handler(alert)
            except Exception as e:
                logger.error(f"커스텀 알림 핸들러 실행 실패: {e}")
        
        # 통계 업데이트
        self.alert_stats["total_alerts_sent"] += 1
        self.alert_stats["alerts_by_severity"][alert.severity.value] += 1
        
        return results
    
    def send_performance_alert(self, perf_alert: PerformanceAlert) -> Dict[str, bool]:
        """성능 알림을 시스템 알림으로 변환하여 전송"""
        severity_map = {
            "info": AlertSeverity.INFO,
            "warning": AlertSeverity.WARNING,
            "critical": AlertSeverity.CRITICAL
        }
        
        alert = AlertMessage(
            title=f"성능 알림: {perf_alert.metric_type.value}",
            message=perf_alert.message,
            severity=severity_map.get(perf_alert.severity, AlertSeverity.WARNING),
            source=f"performance_monitor/{perf_alert.source}",
            timestamp=perf_alert.timestamp,
            metadata={
                "metric_type": perf_alert.metric_type.value,
                "current_value": perf_alert.value,
                "threshold": perf_alert.threshold,
                "original_source": perf_alert.source
            }
        )
        
        return self.send_alert(alert)
    
    def send_system_alert(self, title: str, message: str, severity: AlertSeverity = AlertSeverity.WARNING,
                         source: str = "system", metadata: Optional[Dict[str, Any]] = None) -> Dict[str, bool]:
        """시스템 알림 전송"""
        alert = AlertMessage(
            title=title,
            message=message,
            severity=severity,
            source=source,
            timestamp=datetime.now(),
            metadata=metadata or {}
        )
        
        return self.send_alert(alert)
    
    def get_alert_statistics(self) -> Dict[str, Any]:
        """알림 통계 반환"""
        return {
            **self.alert_stats,
            "configured_channels": list(self.configs.keys()),
            "active_channels": [
                channel for channel, config in self.configs.items() 
                if config.enabled
            ],
            "custom_handlers_count": len(self.custom_handlers)
        }
    
    def cleanup_throttler(self):
        """알림 속도 제한 정리"""
        self.throttler.cleanup_old_entries()


# 전역 알림 시스템 인스턴스
_alert_system: Optional[AlertSystem] = None


def get_alert_system() -> AlertSystem:
    """전역 알림 시스템 인스턴스 반환 (싱글톤)"""
    global _alert_system
    
    if _alert_system is None:
        _alert_system = AlertSystem()
    
    return _alert_system


def reset_alert_system():
    """알림 시스템 인스턴스 재설정 (테스트용)"""
    global _alert_system
    _alert_system = None


def setup_default_alerts():
    """기본 알림 설정"""
    alert_system = get_alert_system()
    
    # 환경 변수에서 설정 로드
    if os.getenv("ALERT_EMAIL_ENABLED", "false").lower() == "true":
        email_config = AlertConfig(
            channel=AlertChannel.EMAIL,
            severity_filter=[AlertSeverity.ERROR, AlertSeverity.CRITICAL],
            enabled=True,
            rate_limit_minutes=15,
            config={
                "to_emails": os.getenv("ALERT_EMAIL_RECIPIENTS", "").split(",")
            }
        )
        alert_system.configure_channel(AlertChannel.EMAIL, email_config)
    
    if os.getenv("ALERT_SLACK_ENABLED", "false").lower() == "true":
        slack_config = AlertConfig(
            channel=AlertChannel.SLACK,
            severity_filter=[AlertSeverity.WARNING, AlertSeverity.ERROR, AlertSeverity.CRITICAL],
            enabled=True,
            rate_limit_minutes=10
        )
        alert_system.configure_channel(AlertChannel.SLACK, slack_config)
    
    logger.info("기본 알림 설정 완료")