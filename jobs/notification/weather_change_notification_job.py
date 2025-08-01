"""여행 플랜 날씨 변화 알림 Job"""
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta, date
import json
from uuid import UUID

from app.core.base_job import BaseJob
from app.core.database_manager import DatabaseManager
from app.core.logger import get_logger
from app.monitoring.notification_channels import (
    NotificationManager, 
    EmailNotificationChannel,
    Alert
)
from app.monitoring.fcm_notification_channel import (
    FCMNotificationChannel, 
    FCMConfig,
    send_fcm_notification_to_user
)
from app.services.weather_comparison_service import WeatherComparisonService
from app.monitoring.monitoring_system import AlertSeverity
from app.collectors.weather_collector import WeatherDataCollector

logger = get_logger(__name__)

class WeatherChangeNotificationJob(BaseJob):
    """여행 플랜의 날씨 변화를 감지하고 알림을 전송하는 Job"""
    
    def __init__(self, config=None):
        from app.core.base_job import JobConfig
        from app.schedulers.advanced_scheduler import BatchJobType
        
        if config is None:
            config = JobConfig(
                job_name="weather_change_notification",
                job_type=BatchJobType.WEATHER_CHANGE_NOTIFICATION,
                schedule_expression="0 9,15,21 * * *",  # 하루 3번 실행
                retry_count=3,
                timeout_minutes=30,
                enabled=True,
            )
        
        super().__init__(config)
        self.name = "WeatherChangeNotificationJob"
        self.description = "여행 플랜 날씨 변화 모니터링 및 알림 전송"
        self.schedule = "0 9,15,21 * * *"  # 하루 3번 실행 (오전 9시, 오후 3시, 오후 9시)
        
        self.db_manager = DatabaseManager()
        self.weather_comparison = WeatherComparisonService()
        self.weather_collector = WeatherDataCollector()
        self.notification_manager = NotificationManager()
        
        # 환경 변수에서 설정 값 가져오기
        import os
        
        # 이메일 채널 설정
        email_channel = EmailNotificationChannel({
            'smtp_host': os.getenv('SMTP_HOST', 'smtp.gmail.com'),
            'smtp_port': int(os.getenv('SMTP_PORT', '587')),
            'smtp_user': os.getenv('SMTP_USER'),
            'smtp_password': os.getenv('SMTP_PASSWORD'),
            'from_email': os.getenv('FROM_EMAIL', 'noreply@weatherflick.com')
        })
        self.notification_manager.register_channel('email', email_channel)
        
        # FCM 채널 설정 (Firebase 설정이 있는 경우)
        firebase_credentials_path = os.getenv('FIREBASE_CREDENTIALS_PATH')
        if firebase_credentials_path:
            try:
                fcm_config = FCMConfig(
                    credentials_path=firebase_credentials_path,
                    project_id=os.getenv('FIREBASE_PROJECT_ID')
                )
                fcm_channel = FCMNotificationChannel(fcm_config)
                self.notification_manager.register_channel('fcm', fcm_channel)
                self.logger.info("FCM 알림 채널 활성화됨")
            except Exception as e:
                self.logger.warning(f"FCM 채널 초기화 실패: {str(e)}. FCM 알림이 비활성화됩니다.")
        
    def execute(self):
        """Job 실행 (BaseJob 인터페이스 구현)"""
        from app.core.base_job import JobResult, JobStatus
        import asyncio
        
        # 비동기 실행을 동기로 래핑
        result_data = asyncio.run(self._execute_async())
        
        # JobResult 객체 생성 및 반환
        result = JobResult(
            job_name=self.config.job_name,
            job_type=self.config.job_type,
            status=JobStatus.COMPLETED if result_data['status'] == 'success' else JobStatus.FAILED,
            start_time=datetime.now(),
            end_time=datetime.now(),
            processed_records=result_data.get('notifications_sent', 0),
            metadata=result_data
        )
        
        return result
    
    async def _execute_async(self) -> Dict[str, Any]:
        """실제 비동기 실행 로직"""
        try:
            self.logger.info("여행 플랜 날씨 변화 모니터링 시작")
            
            # 1. 활성 여행 플랜 조회
            active_plans = await self._get_active_travel_plans()
            self.logger.info(f"{len(active_plans)}개의 활성 여행 플랜 발견")
            
            # 2. 각 플랜에 대해 날씨 변화 체크
            total_notifications = 0
            for plan in active_plans:
                try:
                    notifications_sent = await self._process_travel_plan(plan)
                    total_notifications += notifications_sent
                except Exception as e:
                    self.logger.error(f"플랜 {plan['plan_id']} 처리 중 오류: {str(e)}")
                    continue
            
            self.logger.info(f"총 {total_notifications}개의 알림 전송 완료")
            
            return {
                'status': 'success',
                'plans_processed': len(active_plans),
                'notifications_sent': total_notifications,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            self.logger.error(f"Job 실행 중 오류: {str(e)}")
            raise
    
    async def _get_active_travel_plans(self) -> List[Dict]:
        """활성 여행 플랜 조회 (미래 날짜의 플랜)"""
        query = """
            SELECT 
                tp.plan_id,
                tp.user_id,
                tp.start_date,
                tp.end_date,
                tp.weather_info,
                tp.itinerary,
                u.email,
                u.nickname as user_name,
                unp.weather_alerts,
                unp.email_enabled,
                COALESCE(
                    (SELECT array_agg(DISTINCT d.name) 
                     FROM jsonb_array_elements(tp.itinerary) AS day_data,
                          jsonb_array_elements(day_data->'destinations') AS dest,
                          destinations d
                     WHERE d.destination_id = (dest->>'destination_id')::uuid),
                    ARRAY[]::text[]
                ) as destinations
            FROM travel_plans tp
            JOIN users u ON tp.user_id = u.user_id
            LEFT JOIN user_notification_settings unp ON u.user_id = unp.user_id
            WHERE tp.start_date >= CURRENT_DATE
              AND tp.end_date >= CURRENT_DATE
              AND tp.status IN ('CONFIRMED', 'IN_PROGRESS')
              AND u.is_email_verified = true
              AND COALESCE(unp.weather_alerts, true) = true
              AND COALESCE(unp.email_enabled, true) = true
            ORDER BY tp.start_date;
        """
        
        results = self.db_manager.execute_query(query)
        plans = []
        
        for row in results:
            plan = dict(row)
            # JSONB 필드 파싱
            if plan['weather_info'] and isinstance(plan['weather_info'], str):
                plan['weather_info'] = json.loads(plan['weather_info'])
            if plan['itinerary'] and isinstance(plan['itinerary'], str):
                plan['itinerary'] = json.loads(plan['itinerary'])
            plans.append(plan)
        
        return plans
    
    async def _process_travel_plan(self, plan: Dict) -> int:
        """개별 여행 플랜 처리"""
        notifications_sent = 0
        
        # 1. 최근 알림 전송 여부 확인 (하루에 한 번만)
        if await self._has_recent_notification(plan['plan_id']):
            self.logger.debug(f"플랜 {plan['plan_id']}는 최근 알림이 전송됨")
            return 0
        
        # 2. 현재 날씨 정보 수집
        destinations = plan.get('destinations', [])
        if not destinations:
            return 0
        
        new_weather_info = {}
        for destination in destinations:
            try:
                # 날씨 정보 수집 (start_date부터 end_date까지)
                days_count = (plan['end_date'] - plan['start_date']).days + 1
                weather_data = await self._collect_weather_for_destination(
                    destination, 
                    plan['start_date'],
                    days_count
                )
                
                # 날짜별로 정리
                for day_offset in range(days_count):
                    current_date = plan['start_date'] + timedelta(days=day_offset)
                    date_str = current_date.strftime('%Y-%m-%d')
                    
                    if date_str not in new_weather_info:
                        new_weather_info[date_str] = {}
                    
                    if date_str in weather_data:
                        new_weather_info[date_str][destination] = weather_data[date_str]
                        
            except Exception as e:
                self.logger.error(f"목적지 {destination} 날씨 수집 실패: {str(e)}")
                continue
        
        # 3. 날씨 변화 비교
        old_weather_info = plan.get('weather_info', {})
        changes = self.weather_comparison.compare_weather(
            old_weather_info,
            new_weather_info,
            {
                'min_temperature_change': 5.0,  # 기본값: 5도 이상 변화
                'rain_probability_threshold': 30  # 기본값: 30% 이상 강수확률 변화
            }
        )
        
        # 4. 중요한 변화가 있으면 알림 전송
        if changes:
            plan_info = {
                'destination': ', '.join(destinations),
                'start_date': plan['start_date'].strftime('%Y년 %m월 %d일'),
                'end_date': plan['end_date'].strftime('%Y년 %m월 %d일'),
                'user_name': plan['user_name'] or '고객'
            }
            
            message_data = self.weather_comparison.get_notification_message(changes, plan_info)
            
            # 알림 전송
            success = await self._send_notification(
                plan,
                message_data,
                changes,
                old_weather_info,
                new_weather_info
            )
            
            if success:
                notifications_sent = 1
                
                # 여행 플랜의 날씨 정보 업데이트
                await self._update_plan_weather_info(plan['plan_id'], new_weather_info)
        
        return notifications_sent
    
    async def _collect_weather_for_destination(self, 
                                             destination: str, 
                                             start_date: date,
                                             days: int) -> Dict[str, Dict]:
        """특정 목적지의 날씨 정보 수집"""
        weather_data = {}
        
        # 목적지의 지역 정보 조회
        region_query = """
            SELECT r.region_code, r.nx, r.ny
            FROM destinations d
            JOIN regions r ON d.region_id = r.region_id
            WHERE d.name = %s
            LIMIT 1;
        """
        
        result = self.db_manager.execute_query(region_query, (destination,))
        if not result:
            self.logger.warning(f"목적지 {destination}의 지역 정보를 찾을 수 없음")
            return weather_data
        
        region_info = dict(result[0])
        
        # 날씨 예보 조회
        forecast_query = """
            SELECT 
                forecast_date,
                max_temperature as max_temp,
                min_temperature as min_temp,
                rain_probability,
                weather_description as weather_condition,
                wind_speed
            FROM weather_forecast
            WHERE region_code = %s
              AND forecast_date >= %s
              AND forecast_date < %s
              AND created_at >= CURRENT_TIMESTAMP - INTERVAL '1 day'
            ORDER BY forecast_date, created_at DESC;
        """
        
        end_date = start_date + timedelta(days=days)
        forecasts = self.db_manager.execute_query(
            forecast_query, 
            (region_info['region_code'], start_date, end_date)
        )
        
        for forecast in forecasts:
            date_str = forecast['forecast_date'].strftime('%Y-%m-%d')
            weather_data[date_str] = {
                'max_temp': float(forecast['max_temp']) if forecast['max_temp'] else None,
                'min_temp': float(forecast['min_temp']) if forecast['min_temp'] else None,
                'rain_probability': int(forecast['rain_probability']) if forecast['rain_probability'] else 0,
                'weather_condition': forecast['weather_condition'] or 'unknown',
                'wind_speed': float(forecast['wind_speed']) if forecast['wind_speed'] else 0
            }
        
        return weather_data
    
    async def _has_recent_notification(self, plan_id: UUID) -> bool:
        """최근 24시간 내 알림 전송 여부 확인"""
        query = """
            SELECT COUNT(*) as count
            FROM weather_notifications
            WHERE plan_id = %s
              AND notification_status = 'sent'
              AND created_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours';
        """
        
        result = self.db_manager.execute_query(query, (str(plan_id),))
        return result[0]['count'] > 0
    
    async def _send_notification(self,
                               plan: Dict,
                               message_data: Dict[str, str],
                               changes: List,
                               old_weather: Dict,
                               new_weather: Dict) -> bool:
        """알림 전송"""
        try:
            # 알림 기록 생성
            notification_id = await self._create_notification_record(
                plan, changes, old_weather, new_weather
            )
            
            # 알림 전송
            alert = Alert(
                title=message_data['subject'],
                message=message_data['body'],
                severity=AlertSeverity.INFO,
                source=self.name,
                metadata={
                    'plan_id': str(plan['plan_id']),
                    'user_id': str(plan['user_id']),
                    'notification_id': str(notification_id),
                    'html_body': message_data.get('html_body', '')
                }
            )
            
            # 이메일로 전송
            success = await self.notification_manager.send_alert(
                alert,
                channels=['email'],
                email_to=plan['email']
            )
            
            # FCM 푸시 알림 전송 (별도로 처리)
            import os
            if os.getenv('FIREBASE_CREDENTIALS_PATH'):
                try:
                    fcm_success = await send_fcm_notification_to_user(
                        user_id=str(plan['user_id']),
                        title=message_data['subject'],
                        body=self._create_fcm_body(changes),
                        data={
                            'type': 'weather_change',
                            'plan_id': str(plan['plan_id']),
                            'notification_id': str(notification_id)
                        },
                        url=f"/travel-plans/{plan['plan_id']}"
                    )
                    if fcm_success:
                        self.logger.info(f"FCM 알림 전송 성공: 사용자 {plan['user_id']}")
                except Exception as e:
                    self.logger.error(f"FCM 알림 전송 실패: {str(e)}")
            
            # 전송 결과 업데이트
            if success:
                await self._update_notification_status(notification_id, 'sent')
                self.logger.info(f"알림 전송 성공: {plan['email']}")
            else:
                await self._update_notification_status(notification_id, 'failed')
                self.logger.error(f"알림 전송 실패: {plan['email']}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"알림 전송 중 오류: {str(e)}")
            return False
    
    async def _create_notification_record(self,
                                        plan: Dict,
                                        changes: List,
                                        old_weather: Dict,
                                        new_weather: Dict) -> UUID:
        """알림 기록 생성"""
        weather_changes = [
            {
                'date': change.date.isoformat(),
                'field': change.field,
                'old_value': change.old_value,
                'new_value': change.new_value,
                'change_type': change.change_type,
                'severity': change.severity,
                'description': change.description
            }
            for change in changes
        ]
        
        insert_query = """
            INSERT INTO weather_notifications (
                user_id, plan_id, notification_type, notification_channel,
                weather_changes, previous_weather, current_weather,
                severity, message
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s
            ) RETURNING notification_id;
        """
        
        # 가장 높은 심각도 결정
        severities = [c.severity for c in changes]
        max_severity = 'critical' if 'critical' in severities else \
                      'warning' if 'warning' in severities else 'info'
        
        result = self.db_manager.execute_query(insert_query, (
            str(plan['user_id']),
            str(plan['plan_id']),
            'weather_change',
            'email',
            json.dumps(weather_changes),
            json.dumps(old_weather),
            json.dumps(new_weather),
            max_severity,
            '\n'.join([c.description for c in changes])
        ))
        
        return UUID(result[0]['notification_id'])
    
    async def _update_notification_status(self, 
                                        notification_id: UUID, 
                                        status: str,
                                        error_message: Optional[str] = None):
        """알림 상태 업데이트"""
        update_query = """
            UPDATE weather_notifications
            SET notification_status = %s,
                sent_at = CASE WHEN %s = 'sent' THEN CURRENT_TIMESTAMP ELSE sent_at END,
                error_message = %s,
                retry_count = retry_count + CASE WHEN %s = 'failed' THEN 1 ELSE 0 END
            WHERE notification_id = %s;
        """
        
        self.db_manager.execute_query(update_query, (
            status, status, error_message, status, str(notification_id)
        ))
    
    async def _update_plan_weather_info(self, plan_id: UUID, new_weather_info: Dict):
        """여행 플랜의 날씨 정보 업데이트"""
        update_query = """
            UPDATE travel_plans
            SET weather_info = %s,
                updated_at = CURRENT_TIMESTAMP
            WHERE plan_id = %s;
        """
        
        self.db_manager.execute_query(update_query, (
            json.dumps(new_weather_info),
            str(plan_id)
        ))
    
    def validate(self) -> bool:
        """Job 유효성 검증"""
        import os
        # SMTP 설정 확인
        if not os.getenv('SMTP_USER') or not os.getenv('SMTP_PASSWORD'):
            self.logger.warning("SMTP 설정이 없음. 이메일 전송이 불가능할 수 있습니다.")
            
        return True
    
    def _create_fcm_body(self, changes: List) -> str:
        """FCM 알림용 간단한 본문 생성"""
        if not changes:
            return "여행 일정의 날씨가 변경되었습니다."
        
        # 가장 중요한 변화 1-2개만 표시
        important_changes = []
        for change in changes[:2]:  # 최대 2개
            if change.field == 'rain_probability':
                if change.new_value > change.old_value:
                    important_changes.append("☔ 비 올 확률 증가")
                else:
                    important_changes.append("☀️ 날씨 개선")
            elif change.field in ['max_temp', 'min_temp']:
                temp_diff = abs(change.new_value - change.old_value)
                if temp_diff >= 5:
                    if change.new_value > change.old_value:
                        important_changes.append(f"🌡️ 기온 {temp_diff:.0f}°C 상승")
                    else:
                        important_changes.append(f"❄️ 기온 {temp_diff:.0f}°C 하락")
        
        if important_changes:
            return " | ".join(important_changes)
        else:
            return "날씨 정보가 업데이트되었습니다. 확인해주세요!"