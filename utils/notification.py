"""
알림 시스템 유틸리티

시스템 장애, 작업 실패 등의 상황에 대한 알림을 전송하는 모듈입니다.
"""

import json
import requests
import smtplib
from abc import ABC, abstractmethod
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Dict, List, Any
from dataclasses import dataclass
from enum import Enum
import logging

from config.settings import get_app_settings


class AlertLevel(Enum):
    """알림 레벨"""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AlertMessage:
    """알림 메시지"""

    title: str
    message: str
    level: AlertLevel
    timestamp: datetime
    source: str
    metadata: Dict[str, Any] = None

    def to_dict(self) -> Dict:
        return {
            "title": self.title,
            "message": self.message,
            "level": self.level.value,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "metadata": self.metadata or {},
        }


class NotificationChannel(ABC):
    """알림 채널 추상 클래스"""

    @abstractmethod
    def send(self, alert: AlertMessage) -> bool:
        """알림 전송"""
        pass


class SlackNotificationChannel(NotificationChannel):
    """Slack 알림 채널"""

    def __init__(self, webhook_url: str, channel: str = None):
        self.webhook_url = webhook_url
        self.channel = channel
        self.logger = logging.getLogger(__name__)

    def send(self, alert: AlertMessage) -> bool:
        """Slack으로 알림 전송"""
        try:
            # 레벨별 색상 설정
            color_map = {
                AlertLevel.INFO: "#36a64f",  # 녹색
                AlertLevel.WARNING: "#ff9500",  # 주황색
                AlertLevel.ERROR: "#ff0000",  # 빨간색
                AlertLevel.CRITICAL: "#8b0000",  # 진한 빨간색
            }

            # 레벨별 이모지
            emoji_map = {
                AlertLevel.INFO: ":information_source:",
                AlertLevel.WARNING: ":warning:",
                AlertLevel.ERROR: ":x:",
                AlertLevel.CRITICAL: ":rotating_light:",
            }

            payload = {
                "username": "WeatherFlick Monitor",
                "icon_emoji": ":robot_face:",
                "attachments": [
                    {
                        "color": color_map.get(alert.level, "#cccccc"),
                        "title": f"{emoji_map.get(alert.level, '')} {alert.title}",
                        "text": alert.message,
                        "fields": [
                            {
                                "title": "레벨",
                                "value": alert.level.value.upper(),
                                "short": True,
                            },
                            {"title": "소스", "value": alert.source, "short": True},
                            {
                                "title": "시간",
                                "value": alert.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                                "short": False,
                            },
                        ],
                        "footer": "WeatherFlick Batch System",
                        "ts": int(alert.timestamp.timestamp()),
                    }
                ],
            }

            if self.channel:
                payload["channel"] = self.channel

            # 메타데이터가 있으면 추가
            if alert.metadata:
                metadata_text = "\n".join(
                    [f"• {k}: {v}" for k, v in alert.metadata.items()]
                )
                payload["attachments"][0]["fields"].append(
                    {
                        "title": "추가 정보",
                        "value": f"```{metadata_text}```",
                        "short": False,
                    }
                )

            response = requests.post(self.webhook_url, json=payload, timeout=10)

            if response.status_code == 200:
                self.logger.debug(f"Slack 알림 전송 성공: {alert.title}")
                return True
            else:
                self.logger.error(f"Slack 알림 전송 실패: {response.status_code}")
                return False

        except Exception as e:
            self.logger.error(f"Slack 알림 전송 오류: {str(e)}")
            return False


class EmailNotificationChannel(NotificationChannel):
    """이메일 알림 채널"""

    def __init__(
        self,
        smtp_host: str,
        smtp_port: int,
        username: str,
        password: str,
        from_email: str,
        to_emails: List[str],
    ):
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.from_email = from_email
        self.to_emails = to_emails
        self.logger = logging.getLogger(__name__)

    def send(self, alert: AlertMessage) -> bool:
        """이메일로 알림 전송"""
        try:
            # 이메일 메시지 구성
            msg = MIMEMultipart()
            msg["From"] = self.from_email
            msg["To"] = ", ".join(self.to_emails)
            msg["Subject"] = (
                f"[WeatherFlick] {alert.level.value.upper()}: {alert.title}"
            )

            # 본문 작성
            body = f"""
WeatherFlick 배치 시스템 알림

제목: {alert.title}
레벨: {alert.level.value.upper()}
시간: {alert.timestamp.strftime("%Y-%m-%d %H:%M:%S")}
소스: {alert.source}

메시지:
{alert.message}
"""

            if alert.metadata:
                body += "\n\n추가 정보:\n"
                for key, value in alert.metadata.items():
                    body += f"• {key}: {value}\n"

            body += "\n---\nWeatherFlick 배치 시스템"

            msg.attach(MIMEText(body, "plain", "utf-8"))

            # SMTP 서버 연결 및 전송
            server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            server.starttls()
            server.login(self.username, self.password)
            server.send_message(msg)
            server.quit()

            self.logger.debug(f"이메일 알림 전송 성공: {alert.title}")
            return True

        except Exception as e:
            self.logger.error(f"이메일 알림 전송 오류: {str(e)}")
            return False


class LogNotificationChannel(NotificationChannel):
    """로그 파일 알림 채널 (폴백용)"""

    def __init__(self, log_file: str = None):
        self.log_file = log_file
        self.logger = logging.getLogger(__name__)

    def send(self, alert: AlertMessage) -> bool:
        """로그 파일에 알림 기록"""
        try:
            log_message = (
                f"[ALERT] {alert.level.value.upper()} - {alert.title}\n"
                f"Time: {alert.timestamp}\n"
                f"Source: {alert.source}\n"
                f"Message: {alert.message}"
            )

            if alert.metadata:
                log_message += f"\nMetadata: {json.dumps(alert.metadata, ensure_ascii=False, indent=2)}"

            if self.log_file:
                with open(self.log_file, "a", encoding="utf-8") as f:
                    f.write(f"{log_message}\n{'=' * 50}\n")

            # 로거에도 기록
            if alert.level == AlertLevel.CRITICAL:
                self.logger.critical(log_message)
            elif alert.level == AlertLevel.ERROR:
                self.logger.error(log_message)
            elif alert.level == AlertLevel.WARNING:
                self.logger.warning(log_message)
            else:
                self.logger.info(log_message)

            return True

        except Exception as e:
            self.logger.error(f"로그 알림 기록 오류: {str(e)}")
            return False


class NotificationManager:
    """알림 관리자"""

    def __init__(self):
        self.channels: List[NotificationChannel] = []
        self.logger = logging.getLogger(__name__)
        self.settings = get_app_settings()

        # 기본 로그 채널 추가 (항상 활성화)
        self.add_channel(LogNotificationChannel("logs/alerts.log"))

        # 환경 변수에 따라 채널 추가
        self._setup_channels()

    def _setup_channels(self):
        """환경 변수 기반 채널 설정"""
        import os

        # Slack 채널 설정
        slack_webhook = os.getenv("SLACK_WEBHOOK_URL")
        if slack_webhook:
            slack_channel = os.getenv("SLACK_CHANNEL", "#alerts")
            self.add_channel(SlackNotificationChannel(slack_webhook, slack_channel))
            self.logger.info("Slack 알림 채널 활성화")

        # 이메일 채널 설정
        smtp_host = os.getenv("SMTP_HOST")
        smtp_user = os.getenv("SMTP_USER")
        smtp_password = os.getenv("SMTP_PASSWORD")
        alert_emails = os.getenv("ALERT_EMAILS")

        if all([smtp_host, smtp_user, smtp_password, alert_emails]):
            smtp_port = int(os.getenv("SMTP_PORT", "587"))
            from_email = os.getenv("FROM_EMAIL", smtp_user)
            to_emails = [email.strip() for email in alert_emails.split(",")]

            self.add_channel(
                EmailNotificationChannel(
                    smtp_host,
                    smtp_port,
                    smtp_user,
                    smtp_password,
                    from_email,
                    to_emails,
                )
            )
            self.logger.info("이메일 알림 채널 활성화")

    def add_channel(self, channel: NotificationChannel):
        """알림 채널 추가"""
        self.channels.append(channel)

    def send_alert(
        self,
        title: str,
        message: str,
        level: AlertLevel,
        source: str,
        metadata: Dict[str, Any] = None,
    ) -> bool:
        """알림 전송"""
        alert = AlertMessage(
            title=title,
            message=message,
            level=level,
            timestamp=datetime.now(),
            source=source,
            metadata=metadata,
        )

        success_count = 0

        for channel in self.channels:
            try:
                if channel.send(alert):
                    success_count += 1
            except Exception as e:
                self.logger.error(f"알림 채널 오류: {str(e)}")

        # 최소 하나의 채널에서 성공하면 True
        return success_count > 0

    def send_job_failure_alert(
        self,
        job_name: str,
        error_message: str,
        duration: float = None,
        metadata: Dict = None,
    ):
        """작업 실패 알림"""
        title = f"배치 작업 실패: {job_name}"
        message = f"배치 작업 '{job_name}'이 실패했습니다.\n\n오류: {error_message}"

        if duration:
            message += f"\n실행 시간: {duration:.2f}초"

        alert_metadata = {"job_name": job_name, "error": error_message}
        if duration:
            alert_metadata["duration_seconds"] = duration
        if metadata:
            alert_metadata.update(metadata)

        return self.send_alert(
            title=title,
            message=message,
            level=AlertLevel.ERROR,
            source="batch_system",
            metadata=alert_metadata,
        )

    def send_system_alert(
        self,
        title: str,
        message: str,
        level: AlertLevel = AlertLevel.WARNING,
        metadata: Dict = None,
    ):
        """시스템 알림"""
        return self.send_alert(
            title=title,
            message=message,
            level=level,
            source="system_monitor",
            metadata=metadata,
        )

    def send_data_quality_alert(
        self,
        table_name: str,
        issue_count: int,
        quality_score: float,
        issues: List[str] = None,
    ):
        """데이터 품질 알림"""
        title = f"데이터 품질 이슈: {table_name}"
        message = f"테이블 '{table_name}'에서 데이터 품질 문제가 발견되었습니다.\n\n"
        message += f"품질 점수: {quality_score:.1f}/100\n문제 개수: {issue_count}개"

        if issues:
            message += "\n\n주요 문제:\n" + "\n".join(
                f"• {issue}" for issue in issues[:5]
            )

        level = (
            AlertLevel.CRITICAL
            if quality_score < 50
            else AlertLevel.ERROR if quality_score < 70 else AlertLevel.WARNING
        )

        return self.send_alert(
            title=title,
            message=message,
            level=level,
            source="data_quality_checker",
            metadata={
                "table_name": table_name,
                "quality_score": quality_score,
                "issue_count": issue_count,
            },
        )


# 전역 알림 관리자 인스턴스
_notification_manager = None


def get_notification_manager() -> NotificationManager:
    """전역 알림 관리자 반환"""
    global _notification_manager
    if _notification_manager is None:
        _notification_manager = NotificationManager()
    return _notification_manager
