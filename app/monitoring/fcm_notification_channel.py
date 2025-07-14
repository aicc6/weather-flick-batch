"""
FCM 푸시 알림 채널 구현

Firebase Cloud Messaging을 통한 푸시 알림 전송 기능을 제공합니다.
"""

import json
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass

import firebase_admin
from firebase_admin import credentials, messaging

from .notification_channels import NotificationChannel
from .monitoring_system import Alert


@dataclass
class FCMConfig:
    """FCM 설정"""
    credentials_path: str  # Firebase 서비스 계정 키 JSON 파일 경로
    project_id: Optional[str] = None  # Firebase 프로젝트 ID (선택사항)


class FCMNotificationChannel(NotificationChannel):
    """FCM 푸시 알림 채널"""
    
    def __init__(self, config: FCMConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._initialize_firebase()
    
    def _initialize_firebase(self):
        """Firebase Admin SDK 초기화"""
        try:
            # 이미 초기화되어 있는지 확인
            firebase_admin.get_app()
            self.logger.info("Firebase Admin SDK 이미 초기화됨")
        except ValueError:
            # 초기화되지 않은 경우 초기화 진행
            try:
                cred = credentials.Certificate(self.config.credentials_path)
                firebase_app_config = {}
                if self.config.project_id:
                    firebase_app_config['projectId'] = self.config.project_id
                
                firebase_admin.initialize_app(cred, firebase_app_config)
                self.logger.info("Firebase Admin SDK 초기화 완료")
            except Exception as e:
                self.logger.error(f"Firebase Admin SDK 초기화 실패: {e}")
                raise
    
    def send_notification(self, alert: Alert) -> bool:
        """FCM 푸시 알림 발송"""
        try:
            # FCM 토큰 조회 (alert.metadata에서 user_id로 조회)
            user_id = alert.details.get('user_id') if alert.details else None
            if not user_id:
                self.logger.warning("알림 발송 실패: user_id가 없음")
                return False
            
            # 사용자의 FCM 토큰 조회 (실제로는 DB에서 조회해야 함)
            fcm_tokens = self._get_user_fcm_tokens(user_id)
            if not fcm_tokens:
                self.logger.warning(f"사용자 {user_id}의 FCM 토큰이 없음")
                return False
            
            # FCM 메시지 생성
            message = self._create_fcm_message(alert, fcm_tokens)
            
            # 메시지 발송
            if len(fcm_tokens) == 1:
                # 단일 토큰 발송
                response = messaging.send(message)
                self.logger.info(f"FCM 알림 발송 완료: {response}")
            else:
                # 멀티캐스트 발송
                multicast_message = messaging.MulticastMessage(
                    tokens=fcm_tokens,
                    notification=message.notification,
                    data=message.data,
                    android=message.android,
                    webpush=message.webpush,
                    apns=message.apns
                )
                batch_response = messaging.send_multicast(multicast_message)
                self.logger.info(f"FCM 멀티캐스트 발송 완료: 성공 {batch_response.success_count}개, 실패 {batch_response.failure_count}개")
                
                # 실패한 토큰 처리
                if batch_response.failure_count > 0:
                    self._handle_failed_tokens(fcm_tokens, batch_response.responses)
            
            return True
            
        except Exception as e:
            self.logger.error(f"FCM 알림 발송 실패: {e}")
            return False
    
    def _create_fcm_message(self, alert: Alert, fcm_tokens: List[str]) -> messaging.Message:
        """FCM 메시지 생성"""
        # 알림 제목과 본문 생성
        title = f"[Weather Flick] {alert.title}"
        body = alert.message
        
        # 데이터 페이로드 생성
        data = {
            'alert_id': str(alert.id),
            'alert_level': alert.level.value,
            'component': alert.component.value,
            'timestamp': alert.timestamp.isoformat()
        }
        
        # 상세 정보가 있으면 추가
        if alert.details:
            # FCM 데이터는 문자열만 가능하므로 JSON으로 변환
            for key, value in alert.details.items():
                if isinstance(value, (dict, list)):
                    data[key] = json.dumps(value)
                else:
                    data[key] = str(value)
        
        # 플랫폼별 설정
        android_config = messaging.AndroidConfig(
            priority='high',
            notification=messaging.AndroidNotification(
                icon='weather_icon',
                color='#007AFF',
                sound='default'
            )
        )
        
        webpush_config = messaging.WebpushConfig(
            notification=messaging.WebpushNotification(
                icon='/pwa-192x192.png',
                badge='/pwa-64x64.png',
                vibrate=[200, 100, 200]
            ),
            fcm_options=messaging.WebpushFCMOptions(
                link=data.get('url', '/travel-plans')  # 클릭 시 이동할 URL
            )
        )
        
        apns_config = messaging.APNSConfig(
            payload=messaging.APNSPayload(
                aps=messaging.Aps(
                    alert=messaging.ApsAlert(
                        title=title,
                        body=body
                    ),
                    badge=1,
                    sound='default'
                )
            )
        )
        
        # 메시지 생성 (첫 번째 토큰 사용)
        message = messaging.Message(
            token=fcm_tokens[0],
            notification=messaging.Notification(
                title=title,
                body=body
            ),
            data=data,
            android=android_config,
            webpush=webpush_config,
            apns=apns_config
        )
        
        return message
    
    def _get_user_fcm_tokens(self, user_id: str) -> List[str]:
        """사용자의 FCM 토큰 조회"""
        # TODO: 실제 구현에서는 데이터베이스에서 조회해야 함
        # 여기서는 임시로 빈 리스트 반환
        try:
            from app.core.database_manager import DatabaseManager
            db_manager = DatabaseManager()
            
            query = """
                SELECT fcm_token 
                FROM user_fcm_tokens 
                WHERE user_id = %s 
                  AND is_active = true
                  AND expires_at > CURRENT_TIMESTAMP
                ORDER BY created_at DESC;
            """
            
            results = db_manager.execute_query(query, (user_id,))
            return [row['fcm_token'] for row in results if row['fcm_token']]
        except Exception as e:
            self.logger.error(f"FCM 토큰 조회 실패: {e}")
            return []
    
    def _handle_failed_tokens(self, tokens: List[str], responses: List[messaging.SendResponse]):
        """실패한 토큰 처리"""
        for idx, resp in enumerate(responses):
            if not resp.success:
                token = tokens[idx]
                error = resp.exception
                self.logger.error(f"FCM 토큰 {token} 발송 실패: {error}")
                
                # 토큰이 무효한 경우 DB에서 비활성화
                if error and hasattr(error, 'code'):
                    error_code = error.code
                    if error_code in ['messaging/invalid-registration-token', 
                                     'messaging/registration-token-not-registered']:
                        self._deactivate_token(token)
    
    def _deactivate_token(self, token: str):
        """무효한 FCM 토큰 비활성화"""
        try:
            from app.core.database_manager import DatabaseManager
            db_manager = DatabaseManager()
            
            query = """
                UPDATE user_fcm_tokens 
                SET is_active = false,
                    updated_at = CURRENT_TIMESTAMP
                WHERE fcm_token = %s;
            """
            
            db_manager.execute_query(query, (token,))
            self.logger.info(f"무효한 FCM 토큰 비활성화: {token}")
        except Exception as e:
            self.logger.error(f"FCM 토큰 비활성화 실패: {e}")


async def send_fcm_notification_to_user(user_id: str, 
                                       title: str, 
                                       body: str, 
                                       data: Optional[Dict[str, str]] = None,
                                       url: Optional[str] = None) -> bool:
    """특정 사용자에게 FCM 알림 전송하는 헬퍼 함수"""
    try:
        from app.core.database_manager import DatabaseManager
        db_manager = DatabaseManager()
        
        # 사용자의 FCM 토큰 조회
        query = """
            SELECT fcm_token 
            FROM user_fcm_tokens 
            WHERE user_id = %s 
              AND is_active = true
              AND expires_at > CURRENT_TIMESTAMP
            ORDER BY created_at DESC;
        """
        
        results = db_manager.execute_query(query, (user_id,))
        fcm_tokens = [row['fcm_token'] for row in results if row['fcm_token']]
        
        if not fcm_tokens:
            logging.warning(f"사용자 {user_id}의 활성 FCM 토큰이 없음")
            return False
        
        # 데이터 페이로드 준비
        if data is None:
            data = {}
        
        # URL 추가
        if url:
            data['url'] = url
        
        # 모든 값을 문자열로 변환
        str_data = {k: str(v) if not isinstance(v, str) else v for k, v in data.items()}
        
        # 웹 푸시 설정
        webpush_config = None
        if url:
            webpush_config = messaging.WebpushConfig(
                notification=messaging.WebpushNotification(
                    icon='/pwa-192x192.png',
                    badge='/pwa-64x64.png'
                ),
                fcm_options=messaging.WebpushFCMOptions(link=url)
            )
        
        # 멀티캐스트 메시지 생성 및 전송
        message = messaging.MulticastMessage(
            tokens=fcm_tokens,
            notification=messaging.Notification(
                title=title,
                body=body
            ),
            data=str_data,
            webpush=webpush_config,
            android=messaging.AndroidConfig(
                priority='high',
                notification=messaging.AndroidNotification(
                    icon='weather_icon',
                    color='#007AFF',
                    sound='default'
                )
            )
        )
        
        batch_response = messaging.send_multicast(message)
        logging.info(f"FCM 발송 결과: 성공 {batch_response.success_count}개, 실패 {batch_response.failure_count}개")
        
        # 실패한 토큰 처리
        if batch_response.failure_count > 0:
            for idx, resp in enumerate(batch_response.responses):
                if not resp.success:
                    token = fcm_tokens[idx]
                    error = resp.exception
                    logging.error(f"FCM 토큰 {token} 발송 실패: {error}")
                    
                    # 무효한 토큰 비활성화
                    if hasattr(error, 'code') and error.code in [
                        'messaging/invalid-registration-token',
                        'messaging/registration-token-not-registered'
                    ]:
                        update_query = """
                            UPDATE user_fcm_tokens 
                            SET is_active = false,
                                updated_at = CURRENT_TIMESTAMP
                            WHERE fcm_token = %s;
                        """
                        db_manager.execute_query(update_query, (token,))
        
        return batch_response.success_count > 0
        
    except Exception as e:
        logging.error(f"FCM 알림 전송 실패: {e}")
        return False