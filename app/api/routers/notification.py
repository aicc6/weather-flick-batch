"""
배치 작업 알림 관리 API 라우터
"""
from typing import Optional, List
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Header
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.config import settings
from app.api.schemas_notification import (
    NotificationChannel, NotificationEvent,
    NotificationSubscriptionCreate, NotificationSubscriptionUpdate,
    NotificationSubscriptionResponse, NotificationHistoryResponse,
    NotificationTemplateCreate, NotificationTemplateResponse,
    SendNotificationRequest, NotificationSettingsUpdate,
    NotificationMetrics, EmailConfig, SlackConfig, WebhookConfig
)
from app.api.services.notification_manager import NotificationManager
from app.core.async_database import get_async_db_manager
from app.core.logger import get_logger

router = APIRouter(prefix="/notifications", tags=["notifications"])
logger = get_logger(__name__)

# 알림 매니저 인스턴스 (main.py에서 초기화됨)
notification_manager: Optional[NotificationManager] = None


async def get_db():
    """데이터베이스 세션 의존성"""
    async_db_manager = get_async_db_manager()
    async with async_db_manager.get_session() as session:
        yield session


@router.post("/subscriptions", response_model=NotificationSubscriptionResponse)
async def create_subscription(
    subscription_data: NotificationSubscriptionCreate,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 구독 생성"""
    # API 키 검증
    if api_key != settings.API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    try:
        if not notification_manager:
            raise HTTPException(status_code=500, detail="알림 관리자가 초기화되지 않았습니다")
        
        subscription = await notification_manager.create_subscription(subscription_data, db)
        
        return NotificationSubscriptionResponse(
            subscription_id=subscription.subscription_id,
            job_type=subscription.job_type,
            channel=subscription.channel,
            events=subscription.events,
            recipient=subscription.recipient,
            config=subscription.config,
            filters=subscription.filters,
            enabled=subscription.enabled,
            created_at=subscription.created_at,
            updated_at=subscription.updated_at
        )
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"알림 구독 생성 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/subscriptions", response_model=List[NotificationSubscriptionResponse])
async def list_subscriptions(
    job_type: Optional[str] = Query(None, description="작업 유형 필터"),
    channel: Optional[NotificationChannel] = Query(None, description="채널 필터"),
    enabled_only: bool = Query(True, description="활성화된 구독만 조회"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 구독 목록 조회"""
    try:
        if not notification_manager:
            raise HTTPException(status_code=500, detail="알림 관리자가 초기화되지 않았습니다")
        
        subscriptions = await notification_manager.get_subscriptions(
            db=db,
            job_type=job_type,
            channel=channel,
            enabled_only=enabled_only
        )
        
        return [
            NotificationSubscriptionResponse(
                subscription_id=sub.subscription_id,
                job_type=sub.job_type,
                channel=sub.channel,
                events=sub.events,
                recipient=sub.recipient,
                config=sub.config,
                filters=sub.filters,
                enabled=sub.enabled,
                created_at=sub.created_at,
                updated_at=sub.updated_at
            )
            for sub in subscriptions
        ]
        
    except Exception as e:
        logger.error(f"알림 구독 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/subscriptions/{subscription_id}", response_model=NotificationSubscriptionResponse)
async def update_subscription(
    subscription_id: int,
    update_data: NotificationSubscriptionUpdate,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 구독 수정"""
    try:
        if not notification_manager:
            raise HTTPException(status_code=500, detail="알림 관리자가 초기화되지 않았습니다")
        
        subscription = await notification_manager.update_subscription(
            subscription_id, update_data, db
        )
        
        return NotificationSubscriptionResponse(
            subscription_id=subscription.subscription_id,
            job_type=subscription.job_type,
            channel=subscription.channel,
            events=subscription.events,
            recipient=subscription.recipient,
            config=subscription.config,
            filters=subscription.filters,
            enabled=subscription.enabled,
            created_at=subscription.created_at,
            updated_at=subscription.updated_at
        )
        
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"알림 구독 수정 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/subscriptions/{subscription_id}")
async def delete_subscription(
    subscription_id: int,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 구독 삭제"""
    try:
        if not notification_manager:
            raise HTTPException(status_code=500, detail="알림 관리자가 초기화되지 않았습니다")
        
        # 구독 조회
        from sqlalchemy import select
        from app.models import BatchJobNotificationSubscription
        
        result = await db.execute(
            select(BatchJobNotificationSubscription).where(
                BatchJobNotificationSubscription.subscription_id == subscription_id
            )
        )
        subscription = result.scalar_one_or_none()
        
        if not subscription:
            raise HTTPException(status_code=404, detail="구독을 찾을 수 없습니다")
        
        # 삭제
        await db.delete(subscription)
        await db.commit()
        
        return {"message": "알림 구독이 삭제되었습니다"}
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"알림 구독 삭제 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/send")
async def send_notification(
    request: SendNotificationRequest,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 발송 (테스트용)"""
    try:
        if not notification_manager:
            raise HTTPException(status_code=500, detail="알림 관리자가 초기화되지 않았습니다")
        
        await notification_manager.send_notification(request, db)
        
        return {"message": "알림 발송이 요청되었습니다"}
        
    except Exception as e:
        logger.error(f"알림 발송 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=List[NotificationHistoryResponse])
async def get_notification_history(
    job_id: Optional[str] = Query(None, description="작업 ID"),
    channel: Optional[NotificationChannel] = Query(None, description="채널 필터"),
    start_date: Optional[datetime] = Query(None, description="시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜"),
    page: int = Query(1, ge=1, description="페이지 번호"),
    size: int = Query(20, ge=1, le=100, description="페이지 크기"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 발송 이력 조회"""
    try:
        if not notification_manager:
            raise HTTPException(status_code=500, detail="알림 관리자가 초기화되지 않았습니다")
        
        result = await notification_manager.get_notification_history(
            db=db,
            job_id=job_id,
            channel=channel,
            start_date=start_date,
            end_date=end_date,
            page=page,
            size=size
        )
        
        return [
            NotificationHistoryResponse(
                notification_id=n.notification_id,
                job_id=n.job_id,
                job_type=n.job_type,
                event=n.event,
                channel=n.channel,
                recipient=n.recipient,
                subject=n.subject,
                message=n.message,
                level=n.level,
                sent_at=n.sent_at,
                success=n.success,
                error_message=n.error_message,
                retry_count=n.retry_count
            )
            for n in result["notifications"]
        ]
        
    except Exception as e:
        logger.error(f"알림 이력 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/metrics", response_model=NotificationMetrics)
async def get_notification_metrics(
    start_date: Optional[datetime] = Query(None, description="시작 날짜"),
    end_date: Optional[datetime] = Query(None, description="종료 날짜"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 메트릭 조회"""
    try:
        if not notification_manager:
            raise HTTPException(status_code=500, detail="알림 관리자가 초기화되지 않았습니다")
        
        metrics = await notification_manager.get_notification_metrics(
            db=db,
            start_date=start_date,
            end_date=end_date
        )
        
        return NotificationMetrics(**metrics)
        
    except Exception as e:
        logger.error(f"알림 메트릭 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/templates", response_model=NotificationTemplateResponse)
async def create_template(
    template_data: NotificationTemplateCreate,
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 템플릿 생성"""
    try:
        from app.models import BatchJobNotificationTemplate
        
        # 중복 확인
        from sqlalchemy import select, and_
        existing = await db.execute(
            select(BatchJobNotificationTemplate).where(
                and_(
                    BatchJobNotificationTemplate.event == template_data.event.value,
                    BatchJobNotificationTemplate.channel == template_data.channel.value
                )
            )
        )
        if existing.scalar_one_or_none():
            raise HTTPException(
                status_code=400,
                detail="동일한 이벤트와 채널의 템플릿이 이미 존재합니다"
            )
        
        # 템플릿 생성
        template = BatchJobNotificationTemplate(
            event=template_data.event.value,
            channel=template_data.channel.value,
            subject_template=template_data.subject_template,
            message_template=template_data.message_template,
            level=template_data.level.value,
            variables=template_data.variables
        )
        
        db.add(template)
        await db.commit()
        await db.refresh(template)
        
        return NotificationTemplateResponse(
            template_id=template.template_id,
            event=template.event,
            channel=template.channel,
            subject_template=template.subject_template,
            message_template=template.message_template,
            level=template.level,
            variables=template.variables,
            created_at=template.created_at,
            updated_at=template.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"템플릿 생성 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/templates", response_model=List[NotificationTemplateResponse])
async def list_templates(
    event: Optional[NotificationEvent] = Query(None, description="이벤트 필터"),
    channel: Optional[NotificationChannel] = Query(None, description="채널 필터"),
    db: AsyncSession = Depends(get_db),
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 템플릿 목록 조회"""
    try:
        from app.models import BatchJobNotificationTemplate
        from sqlalchemy import select, and_
        
        query = select(BatchJobNotificationTemplate)
        
        filters = []
        if event:
            filters.append(BatchJobNotificationTemplate.event == event.value)
        if channel:
            filters.append(BatchJobNotificationTemplate.channel == channel.value)
            
        if filters:
            query = query.where(and_(*filters))
            
        result = await db.execute(query)
        templates = result.scalars().all()
        
        return [
            NotificationTemplateResponse(
                template_id=t.template_id,
                event=t.event,
                channel=t.channel,
                subject_template=t.subject_template,
                message_template=t.message_template,
                level=t.level,
                variables=t.variables,
                created_at=t.created_at,
                updated_at=t.updated_at
            )
            for t in templates
        ]
        
    except Exception as e:
        logger.error(f"템플릿 목록 조회 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/settings")
async def update_notification_settings(
    settings_update: NotificationSettingsUpdate,
    api_key: str = Header(None, alias="X-API-Key")
):
    """알림 전역 설정 업데이트"""
    try:
        if not notification_manager:
            raise HTTPException(status_code=500, detail="알림 관리자가 초기화되지 않았습니다")
        
        # 이메일 설정
        if settings_update.email_config:
            notification_manager.set_email_config(settings_update.email_config)
            
        # 슬랙 설정
        if settings_update.slack_config:
            notification_manager.set_slack_config(settings_update.slack_config)
            
        # 웹훅 설정
        if settings_update.webhook_config:
            notification_manager.set_webhook_config(settings_update.webhook_config)
            
        # 활성화 여부
        if settings_update.enabled is not None:
            notification_manager.enabled = settings_update.enabled
            
        # Rate limit
        if settings_update.rate_limit is not None:
            notification_manager.rate_limit = settings_update.rate_limit
            
        return {"message": "알림 설정이 업데이트되었습니다"}
        
    except Exception as e:
        logger.error(f"알림 설정 업데이트 실패: {e}")
        raise HTTPException(status_code=500, detail=str(e))