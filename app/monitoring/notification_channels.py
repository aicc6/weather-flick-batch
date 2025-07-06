"""
알림 채널 구현

다양한 채널(이메일, 슬랙, 로그, 웹훅)을 통해 알림을 보내는 기능을 제공합니다.
"""

import smtplib
import requests
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, Any, Optional, List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

from .monitoring_system import Alert, AlertLevel, ComponentType


@dataclass
class EmailConfig:
    """이메일 설정"""
    smtp_server: str
    username: str
    password: str
    from_email: str
    to_emails: List[str]
    smtp_port: int = 587
    use_tls: bool = True


@dataclass
class SlackConfig:
    """슬랙 설정"""
    webhook_url: str
    channel: Optional[str] = None
    username: Optional[str] = "WeatherFlick-Monitoring"
    icon_emoji: Optional[str] = ":warning:"


@dataclass
class WebhookConfig:
    """웹훅 설정"""
    url: str
    timeout: int = 30
    headers: Dict[str, str] = None


class NotificationChannel(ABC):
    """알림 채널 추상 클래스"""
    
    @abstractmethod
    def send_notification(self, alert: Alert) -> bool:
        """알림 발송"""
        pass


class EmailNotificationChannel(NotificationChannel):
    """이메일 알림 채널"""
    
    def __init__(self, config: EmailConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, alert: Alert) -> bool:
        """이메일 알림 발송"""
        try:
            # 이메일 메시지 생성
            msg = MIMEMultipart()
            msg['From'] = self.config.from_email
            msg['To'] = ', '.join(self.config.to_emails)
            msg['Subject'] = f"[WeatherFlick] {alert.level.value.upper()}: {alert.title}"
            
            # 이메일 본문 생성
            body = self._create_email_body(alert)
            msg.attach(MIMEText(body, 'html'))
            
            # SMTP 서버 연결 및 발송
            with smtplib.SMTP(self.config.smtp_server, self.config.smtp_port) as server:
                if self.config.use_tls:
                    server.starttls()
                
                server.login(self.config.username, self.config.password)
                server.send_message(msg)
            
            self.logger.info(f"이메일 알림 발송 완료: {alert.title}")
            return True
            
        except Exception as e:
            self.logger.error(f"이메일 알림 발송 실패: {e}")
            return False
    
    def _create_email_body(self, alert: Alert) -> str:
        """이메일 본문 생성"""
        
        # 알림 수준에 따른 색상
        color_map = {
            AlertLevel.INFO: "#36a64f",     # 녹색
            AlertLevel.WARNING: "#ff9500",  # 주황색
            AlertLevel.ERROR: "#ff0000",    # 빨간색
            AlertLevel.CRITICAL: "#8b0000"  # 진한 빨간색
        }
        
        color = color_map.get(alert.level, "#36a64f")
        
        html_body = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 0; padding: 20px; }}
                .alert-container {{ 
                    border-left: 5px solid {color}; 
                    padding: 20px; 
                    background-color: #f8f9fa; 
                    margin: 20px 0; 
                }}
                .alert-header {{ 
                    color: {color}; 
                    font-size: 24px; 
                    font-weight: bold; 
                    margin-bottom: 10px; 
                }}
                .alert-meta {{ 
                    font-size: 14px; 
                    color: #666; 
                    margin-bottom: 15px; 
                }}
                .alert-message {{ 
                    font-size: 16px; 
                    line-height: 1.5; 
                    margin-bottom: 20px; 
                }}
                .alert-details {{ 
                    background-color: #ffffff; 
                    border: 1px solid #ddd; 
                    padding: 15px; 
                    border-radius: 4px; 
                }}
                .details-title {{ 
                    font-weight: bold; 
                    margin-bottom: 10px; 
                }}
                .details-content {{ 
                    font-family: monospace; 
                    background-color: #f1f3f4; 
                    padding: 10px; 
                    border-radius: 4px; 
                    white-space: pre-wrap; 
                }}
            </style>
        </head>
        <body>
            <div class="alert-container">
                <div class="alert-header">
                    {alert.level.value.upper()}: {alert.title}
                </div>
                <div class="alert-meta">
                    <strong>구성 요소:</strong> {alert.component.value} | 
                    <strong>시간:</strong> {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')} UTC
                </div>
                <div class="alert-message">
                    {alert.message}
                </div>
        """
        
        if alert.details:
            details_json = json.dumps(alert.details, indent=2, default=str)
            html_body += f"""
                <div class="alert-details">
                    <div class="details-title">상세 정보:</div>
                    <div class="details-content">{details_json}</div>
                </div>
            """
        
        html_body += """
            </div>
            <hr>
            <p style="font-size: 12px; color: #888;">
                이 알림은 WeatherFlick 배치 모니터링 시스템에서 자동으로 발송되었습니다.
            </p>
        </body>
        </html>
        """
        
        return html_body


class SlackNotificationChannel(NotificationChannel):
    """슬랙 알림 채널"""
    
    def __init__(self, config: SlackConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, alert: Alert) -> bool:
        """슬랙 알림 발송"""
        try:
            # 슬랙 메시지 생성
            payload = self._create_slack_payload(alert)
            
            # 웹훅으로 메시지 발송
            response = requests.post(
                self.config.webhook_url,
                json=payload,
                timeout=30
            )
            
            if response.status_code == 200:
                self.logger.info(f"슬랙 알림 발송 완료: {alert.title}")
                return True
            else:
                self.logger.error(f"슬랙 알림 발송 실패: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"슬랙 알림 발송 실패: {e}")
            return False
    
    def _create_slack_payload(self, alert: Alert) -> Dict[str, Any]:
        """슬랙 메시지 페이로드 생성"""
        
        # 알림 수준에 따른 색상과 이모지
        level_config = {
            AlertLevel.INFO: {"color": "good", "emoji": ":information_source:"},
            AlertLevel.WARNING: {"color": "warning", "emoji": ":warning:"},
            AlertLevel.ERROR: {"color": "danger", "emoji": ":exclamation:"},
            AlertLevel.CRITICAL: {"color": "danger", "emoji": ":rotating_light:"}
        }
        
        config = level_config.get(alert.level, level_config[AlertLevel.INFO])
        
        # 첨부 파일 생성
        attachment = {
            "color": config["color"],
            "title": f"{config['emoji']} {alert.level.value.upper()}: {alert.title}",
            "text": alert.message,
            "fields": [
                {
                    "title": "구성 요소",
                    "value": alert.component.value,
                    "short": True
                },
                {
                    "title": "시간",
                    "value": alert.timestamp.strftime('%Y-%m-%d %H:%M:%S UTC'),
                    "short": True
                }
            ],
            "footer": "WeatherFlick 모니터링",
            "ts": int(alert.timestamp.timestamp())
        }
        
        # 상세 정보가 있으면 추가
        if alert.details:
            details_text = json.dumps(alert.details, indent=2, default=str)
            attachment["fields"].append({
                "title": "상세 정보",
                "value": f"```{details_text}```",
                "short": False
            })
        
        payload = {
            "username": self.config.username,
            "icon_emoji": self.config.icon_emoji,
            "attachments": [attachment]
        }
        
        if self.config.channel:
            payload["channel"] = self.config.channel
        
        return payload


class WebhookNotificationChannel(NotificationChannel):
    """웹훅 알림 채널"""
    
    def __init__(self, config: WebhookConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
    
    def send_notification(self, alert: Alert) -> bool:
        """웹훅 알림 발송"""
        try:
            # 웹훅 페이로드 생성
            payload = self._create_webhook_payload(alert)
            
            # 헤더 설정
            headers = {
                'Content-Type': 'application/json',
                **(self.config.headers or {})
            }
            
            # 웹훅 요청 발송
            response = requests.post(
                self.config.url,
                json=payload,
                headers=headers,
                timeout=self.config.timeout
            )
            
            if response.status_code in [200, 201, 202]:
                self.logger.info(f"웹훅 알림 발송 완료: {alert.title}")
                return True
            else:
                self.logger.error(f"웹훅 알림 발송 실패: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"웹훅 알림 발송 실패: {e}")
            return False
    
    def _create_webhook_payload(self, alert: Alert) -> Dict[str, Any]:
        """웹훅 페이로드 생성"""
        return {
            "alert_id": alert.id,
            "timestamp": alert.timestamp.isoformat(),
            "level": alert.level.value,
            "component": alert.component.value,
            "title": alert.title,
            "message": alert.message,
            "details": alert.details,
            "resolved": alert.resolved,
            "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None
        }


class LogNotificationChannel(NotificationChannel):
    """로그 알림 채널"""
    
    def __init__(self):
        self.logger = logging.getLogger("monitoring.alerts")
        
        # 로그 포맷터 설정
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        
        # 파일 핸들러 추가 (알림 전용 로그 파일)
        try:
            file_handler = logging.FileHandler('logs/monitoring_alerts.log')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
        except Exception:
            pass  # 파일 핸들러 생성 실패 시 무시
    
    def send_notification(self, alert: Alert) -> bool:
        """로그 알림 발송"""
        try:
            # 로그 메시지 생성
            log_message = self._create_log_message(alert)
            
            # 알림 수준에 따른 로그 레벨 매핑
            level_map = {
                AlertLevel.INFO: logging.INFO,
                AlertLevel.WARNING: logging.WARNING,
                AlertLevel.ERROR: logging.ERROR,
                AlertLevel.CRITICAL: logging.CRITICAL
            }
            
            log_level = level_map.get(alert.level, logging.INFO)
            self.logger.log(log_level, log_message)
            
            return True
            
        except Exception as e:
            # 로그 알림 실패는 다른 로거로 기록
            logging.getLogger(__name__).error(f"로그 알림 발송 실패: {e}")
            return False
    
    def _create_log_message(self, alert: Alert) -> str:
        """로그 메시지 생성"""
        base_message = f"[{alert.component.value}] {alert.title}: {alert.message}"
        
        if alert.details:
            details_str = json.dumps(alert.details, default=str)
            base_message += f" | Details: {details_str}"
        
        return base_message


class NotificationManager:
    """알림 관리자"""
    
    def __init__(self):
        self.channels: List[NotificationChannel] = []
        self.logger = logging.getLogger(__name__)
    
    def add_channel(self, channel: NotificationChannel):
        """알림 채널 추가"""
        self.channels.append(channel)
        self.logger.info(f"알림 채널 추가: {channel.__class__.__name__}")
    
    def remove_channel(self, channel: NotificationChannel):
        """알림 채널 제거"""
        if channel in self.channels:
            self.channels.remove(channel)
            self.logger.info(f"알림 채널 제거: {channel.__class__.__name__}")
    
    def send_alert(self, alert: Alert) -> Dict[str, bool]:
        """모든 채널로 알림 발송"""
        results = {}
        
        for channel in self.channels:
            try:
                success = channel.send_notification(alert)
                results[channel.__class__.__name__] = success
            except Exception as e:
                self.logger.error(f"채널 {channel.__class__.__name__}에서 알림 발송 실패: {e}")
                results[channel.__class__.__name__] = False
        
        return results
    
    def get_channel_count(self) -> int:
        """등록된 채널 수 반환"""
        return len(self.channels)


def create_notification_manager_from_config(config: Dict[str, Any]) -> NotificationManager:
    """설정으로부터 알림 매니저 생성"""
    manager = NotificationManager()
    
    # 로그 채널은 기본으로 추가
    manager.add_channel(LogNotificationChannel())
    
    # 이메일 설정이 있으면 이메일 채널 추가
    if config.get('email') and config['email'].get('enabled', False):
        email_config = EmailConfig(**config['email']['config'])
        manager.add_channel(EmailNotificationChannel(email_config))
    
    # 슬랙 설정이 있으면 슬랙 채널 추가
    if config.get('slack') and config['slack'].get('enabled', False):
        slack_config = SlackConfig(**config['slack']['config'])
        manager.add_channel(SlackNotificationChannel(slack_config))
    
    # 웹훅 설정이 있으면 웹훅 채널 추가
    if config.get('webhook') and config['webhook'].get('enabled', False):
        webhook_config = WebhookConfig(**config['webhook']['config'])
        manager.add_channel(WebhookNotificationChannel(webhook_config))
    
    return manager