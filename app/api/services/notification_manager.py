"""
배치 작업 알림 관리 서비스
"""
import asyncio
import smtplib
import aiohttp
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from string import Template
from sqlalchemy import select, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.schemas_notification import (
    NotificationChannel, NotificationEvent, NotificationLevel,
    EmailConfig, SlackConfig, WebhookConfig,
    NotificationSubscriptionCreate, NotificationSubscriptionUpdate,
    SendNotificationRequest
)
from app.models import (
    BatchJobNotificationSubscription,
    BatchJobNotificationHistory,
    BatchJobNotificationTemplate
)
from app.models_batch import BatchJobExecution
from app.core.logger import get_logger


class NotificationManager:
    """알림 관리자"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.email_config: Optional[EmailConfig] = None
        self.slack_config: Optional[SlackConfig] = None
        self.webhook_config: Optional[WebhookConfig] = None
        self.enabled = True
        self.rate_limit = 60  # 분당 최대 알림 수
        self._sent_count = 0
        self._sent_reset_time = datetime.now()
        
    def set_email_config(self, config: EmailConfig):
        """이메일 설정"""
        self.email_config = config
        
    def set_slack_config(self, config: SlackConfig):
        """슬랙 설정"""
        self.slack_config = config
        
    def set_webhook_config(self, config: WebhookConfig):
        """웹훅 설정"""
        self.webhook_config = config
        
    async def create_subscription(
        self,
        subscription_data: NotificationSubscriptionCreate,
        db: AsyncSession
    ) -> BatchJobNotificationSubscription:
        """알림 구독 생성"""
        try:
            # 중복 구독 확인
            existing = await db.execute(
                select(BatchJobNotificationSubscription).where(
                    and_(
                        BatchJobNotificationSubscription.job_type == subscription_data.job_type,
                        BatchJobNotificationSubscription.channel == subscription_data.channel,
                        BatchJobNotificationSubscription.recipient == subscription_data.recipient
                    )
                )
            )
            if existing.scalar_one_or_none():
                raise ValueError("이미 동일한 구독이 존재합니다")
            
            # 구독 생성
            subscription = BatchJobNotificationSubscription(
                job_type=subscription_data.job_type,
                channel=subscription_data.channel.value,
                events=[e.value for e in subscription_data.events],
                recipient=subscription_data.recipient,
                config=subscription_data.config,
                filters=subscription_data.filters,
                enabled=subscription_data.enabled
            )
            
            db.add(subscription)
            await db.commit()
            await db.refresh(subscription)
            
            self.logger.info(f"알림 구독 생성 완료: {subscription.subscription_id}")
            return subscription
            
        except Exception as e:
            await db.rollback()
            self.logger.error(f"알림 구독 생성 실패: {e}")
            raise
    
    async def update_subscription(
        self,
        subscription_id: int,
        update_data: NotificationSubscriptionUpdate,
        db: AsyncSession
    ) -> BatchJobNotificationSubscription:
        """알림 구독 수정"""
        try:
            # 구독 조회
            result = await db.execute(
                select(BatchJobNotificationSubscription).where(
                    BatchJobNotificationSubscription.subscription_id == subscription_id
                )
            )
            subscription = result.scalar_one_or_none()
            
            if not subscription:
                raise ValueError(f"구독을 찾을 수 없습니다: {subscription_id}")
            
            # 업데이트
            update_dict = update_data.model_dump(exclude_unset=True)
            if "events" in update_dict:
                update_dict["events"] = [e.value for e in update_dict["events"]]
                
            for key, value in update_dict.items():
                setattr(subscription, key, value)
            
            subscription.updated_at = datetime.now()
            await db.commit()
            await db.refresh(subscription)
            
            self.logger.info(f"알림 구독 수정 완료: {subscription_id}")
            return subscription
            
        except Exception as e:
            await db.rollback()
            self.logger.error(f"알림 구독 수정 실패: {e}")
            raise
    
    async def get_subscriptions(
        self,
        db: AsyncSession,
        job_type: Optional[str] = None,
        channel: Optional[NotificationChannel] = None,
        enabled_only: bool = True
    ) -> List[BatchJobNotificationSubscription]:
        """구독 목록 조회"""
        query = select(BatchJobNotificationSubscription)
        
        filters = []
        if job_type:
            filters.append(
                or_(
                    BatchJobNotificationSubscription.job_type == job_type,
                    BatchJobNotificationSubscription.job_type.is_(None)
                )
            )
        if channel:
            filters.append(BatchJobNotificationSubscription.channel == channel.value)
        if enabled_only:
            filters.append(BatchJobNotificationSubscription.enabled == True)
            
        if filters:
            query = query.where(and_(*filters))
            
        result = await db.execute(query)
        return result.scalars().all()
    
    async def send_notification(
        self,
        request: SendNotificationRequest,
        db: AsyncSession
    ):
        """알림 발송"""
        if not self.enabled:
            self.logger.info("알림이 비활성화되어 있습니다")
            return
            
        # Rate limiting 확인
        if not self._check_rate_limit():
            self.logger.warning("알림 발송 제한에 도달했습니다")
            return
            
        try:
            # 작업 정보 조회
            job_result = await db.execute(
                select(BatchJobExecution).where(
                    BatchJobExecution.id == request.job_id
                )
            )
            job = job_result.scalar_one_or_none()
            
            if not job:
                self.logger.error(f"작업을 찾을 수 없습니다: {request.job_id}")
                return
            
            # 해당 이벤트를 구독한 사용자들 조회
            subscriptions = await self.get_subscriptions(
                db=db,
                job_type=job.job_type,
                enabled_only=True
            )
            
            # 이벤트를 구독한 사용자들에게 알림 발송
            for subscription in subscriptions:
                if request.event.value not in subscription.events:
                    continue
                    
                # 필터 조건 확인
                if subscription.filters and not self._check_filters(
                    job, request, subscription.filters
                ):
                    continue
                    
                # 템플릿 조회
                template = await self._get_template(
                    db, request.event, NotificationChannel(subscription.channel)
                )
                
                # 메시지 생성
                message_data = self._prepare_message(
                    job, request, template, subscription
                )
                
                # 채널별 발송
                success = False
                error_message = None
                
                try:
                    if subscription.channel == NotificationChannel.EMAIL.value:
                        success = await self._send_email(
                            subscription.recipient,
                            message_data["subject"],
                            message_data["message"]
                        )
                    elif subscription.channel == NotificationChannel.SLACK.value:
                        success = await self._send_slack(
                            message_data["message"],
                            subscription.config
                        )
                    elif subscription.channel == NotificationChannel.WEBHOOK.value:
                        success = await self._send_webhook(
                            message_data,
                            subscription.config
                        )
                except Exception as e:
                    error_message = str(e)
                    self.logger.error(f"알림 발송 실패: {e}")
                
                # 발송 이력 저장
                history = BatchJobNotificationHistory(
                    job_id=request.job_id,
                    job_type=job.job_type,
                    event=request.event.value,
                    channel=subscription.channel,
                    recipient=subscription.recipient,
                    subject=message_data.get("subject"),
                    message=message_data["message"],
                    level=request.level.value,
                    success=success,
                    error_message=error_message
                )
                db.add(history)
                
            await db.commit()
            
        except Exception as e:
            self.logger.error(f"알림 처리 중 오류: {e}")
            await db.rollback()
    
    async def _get_template(
        self,
        db: AsyncSession,
        event: NotificationEvent,
        channel: NotificationChannel
    ) -> Optional[BatchJobNotificationTemplate]:
        """템플릿 조회"""
        result = await db.execute(
            select(BatchJobNotificationTemplate).where(
                and_(
                    BatchJobNotificationTemplate.event == event.value,
                    BatchJobNotificationTemplate.channel == channel.value
                )
            )
        )
        return result.scalar_one_or_none()
    
    def _prepare_message(
        self,
        job: BatchJobExecution,
        request: SendNotificationRequest,
        template: Optional[BatchJobNotificationTemplate],
        subscription: BatchJobNotificationSubscription
    ) -> Dict[str, str]:
        """메시지 준비"""
        # 변수 준비
        variables = {
            "job_id": job.id,
            "job_type": job.job_type,
            "status": job.status,
            "started_at": job.started_at.strftime("%Y-%m-%d %H:%M:%S") if job.started_at else "N/A",
            "completed_at": job.completed_at.strftime("%Y-%m-%d %H:%M:%S") if job.completed_at else "N/A",
            "error_message": job.error_message or "N/A",
            "event": request.event.value,
            "level": request.level.value
        }
        
        # 추가 데이터 병합
        if request.additional_data:
            variables.update(request.additional_data)
        
        # 템플릿 적용
        if template:
            subject_tmpl = Template(template.subject_template or "")
            message_tmpl = Template(template.message_template)
            
            subject = subject_tmpl.safe_substitute(variables)
            message = message_tmpl.safe_substitute(variables)
        else:
            # 기본 메시지
            subject = f"[Weather Flick Batch] {request.event.value}: {job.job_type}"
            message = f"""
작업 ID: {job.id}
작업 유형: {job.job_type}
이벤트: {request.event.value}
상태: {job.status}
시작 시간: {variables['started_at']}
완료 시간: {variables['completed_at']}
에러 메시지: {variables['error_message']}
"""
        
        return {
            "subject": subject,
            "message": message,
            **variables
        }
    
    def _check_filters(
        self,
        job: BatchJobExecution,
        request: SendNotificationRequest,
        filters: Dict[str, Any]
    ) -> bool:
        """필터 조건 확인"""
        # 예: 특정 상태만 알림
        if "status" in filters and job.status != filters["status"]:
            return False
            
        # 예: 특정 레벨 이상만 알림
        if "min_level" in filters:
            level_order = ["info", "warning", "error", "critical"]
            if level_order.index(request.level.value) < level_order.index(filters["min_level"]):
                return False
                
        return True
    
    def _check_rate_limit(self) -> bool:
        """Rate limit 확인"""
        now = datetime.now()
        
        # 1분이 지났으면 카운터 리셋
        if (now - self._sent_reset_time).total_seconds() >= 60:
            self._sent_count = 0
            self._sent_reset_time = now
            
        if self._sent_count >= self.rate_limit:
            return False
            
        self._sent_count += 1
        return True
    
    async def _send_email(self, to_email: str, subject: str, message: str) -> bool:
        """이메일 발송"""
        if not self.email_config:
            self.logger.error("이메일 설정이 없습니다")
            return False
            
        try:
            msg = MIMEMultipart()
            msg['From'] = f"{self.email_config.from_name} <{self.email_config.from_email}>"
            msg['To'] = to_email
            msg['Subject'] = subject
            
            msg.attach(MIMEText(message, 'plain', 'utf-8'))
            
            # SMTP 연결
            if self.email_config.use_tls:
                server = smtplib.SMTP(self.email_config.smtp_host, self.email_config.smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP_SSL(self.email_config.smtp_host, self.email_config.smtp_port)
                
            server.login(self.email_config.smtp_user, self.email_config.smtp_password)
            server.send_message(msg)
            server.quit()
            
            self.logger.info(f"이메일 발송 성공: {to_email}")
            return True
            
        except Exception as e:
            self.logger.error(f"이메일 발송 실패: {e}")
            return False
    
    async def _send_slack(self, message: str, config: Optional[Dict[str, Any]]) -> bool:
        """슬랙 발송"""
        if not self.slack_config:
            self.logger.error("슬랙 설정이 없습니다")
            return False
            
        try:
            webhook_url = config.get("webhook_url") if config else self.slack_config.webhook_url
            
            payload = {
                "text": message,
                "username": self.slack_config.username,
                "icon_emoji": self.slack_config.icon_emoji
            }
            
            if self.slack_config.channel:
                payload["channel"] = self.slack_config.channel
                
            async with aiohttp.ClientSession() as session:
                async with session.post(webhook_url, json=payload) as resp:
                    if resp.status == 200:
                        self.logger.info("슬랙 알림 발송 성공")
                        return True
                    else:
                        self.logger.error(f"슬랙 알림 발송 실패: {resp.status}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"슬랙 알림 발송 실패: {e}")
            return False
    
    async def _send_webhook(self, data: Dict[str, Any], config: Optional[Dict[str, Any]]) -> bool:
        """웹훅 발송"""
        if not self.webhook_config and not config:
            self.logger.error("웹훅 설정이 없습니다")
            return False
            
        try:
            webhook_config = config or self.webhook_config.__dict__
            url = webhook_config.get("url")
            method = webhook_config.get("method", "POST")
            headers = webhook_config.get("headers", {})
            timeout = webhook_config.get("timeout", 30)
            
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method,
                    url=url,
                    json=data,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=timeout)
                ) as resp:
                    if resp.status in [200, 201, 202, 204]:
                        self.logger.info("웹훅 발송 성공")
                        return True
                    else:
                        self.logger.error(f"웹훅 발송 실패: {resp.status}")
                        return False
                        
        except Exception as e:
            self.logger.error(f"웹훅 발송 실패: {e}")
            return False
    
    async def get_notification_history(
        self,
        db: AsyncSession,
        job_id: Optional[str] = None,
        channel: Optional[NotificationChannel] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        page: int = 1,
        size: int = 20
    ) -> Dict[str, Any]:
        """알림 발송 이력 조회"""
        query = select(BatchJobNotificationHistory)
        
        filters = []
        if job_id:
            filters.append(BatchJobNotificationHistory.job_id == job_id)
        if channel:
            filters.append(BatchJobNotificationHistory.channel == channel.value)
        if start_date:
            filters.append(BatchJobNotificationHistory.sent_at >= start_date)
        if end_date:
            filters.append(BatchJobNotificationHistory.sent_at <= end_date)
            
        if filters:
            query = query.where(and_(*filters))
            
        # 전체 개수
        count_result = await db.execute(
            select(func.count()).select_from(query.subquery())
        )
        total = count_result.scalar()
        
        # 페이징
        query = query.order_by(BatchJobNotificationHistory.sent_at.desc())
        query = query.offset((page - 1) * size).limit(size)
        
        result = await db.execute(query)
        notifications = result.scalars().all()
        
        return {
            "notifications": notifications,
            "total": total,
            "page": page,
            "size": size,
            "total_pages": (total + size - 1) // size
        }
    
    async def get_notification_metrics(
        self,
        db: AsyncSession,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """알림 메트릭 조회"""
        from sqlalchemy import func
        
        # 기본 쿼리
        query = select(BatchJobNotificationHistory)
        
        if start_date:
            query = query.where(BatchJobNotificationHistory.sent_at >= start_date)
        if end_date:
            query = query.where(BatchJobNotificationHistory.sent_at <= end_date)
            
        result = await db.execute(query)
        notifications = result.scalars().all()
        
        # 메트릭 계산
        total_sent = len(notifications)
        success_count = sum(1 for n in notifications if n.success)
        failure_count = total_sent - success_count
        
        by_channel = {}
        by_event = {}
        
        for notification in notifications:
            # 채널별 집계
            if notification.channel not in by_channel:
                by_channel[notification.channel] = 0
            by_channel[notification.channel] += 1
            
            # 이벤트별 집계
            if notification.event not in by_event:
                by_event[notification.event] = 0
            by_event[notification.event] += 1
        
        # 24시간 내 발송 수
        from datetime import timedelta
        last_24h = datetime.now() - timedelta(hours=24)
        last_24h_count = sum(1 for n in notifications if n.sent_at >= last_24h)
        
        return {
            "total_sent": total_sent,
            "success_count": success_count,
            "failure_count": failure_count,
            "by_channel": by_channel,
            "by_event": by_event,
            "average_send_time": 0.5,  # TODO: 실제 발송 시간 측정
            "last_24h_count": last_24h_count
        }