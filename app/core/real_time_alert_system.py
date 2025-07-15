"""
실시간 알람 시스템

데이터베이스 제약조건 위반, 시스템 성능 이상, 배치 작업 실패 등을 실시간으로 감지하고 
알람을 발송하는 종합 알림 시스템입니다.

- 데이터베이스 제약조건 위반 모니터링
- 시스템 성능 임계값 감지
- 배치 작업 상태 실시간 추적
- 다중 채널 알림 (로그, 파일, 외부 시스템)
- 알람 에스컬레이션 및 억제
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable
from dataclasses import dataclass, asdict
from enum import Enum
from collections import deque

from utils.redis_client import RedisClient
from app.core.database_manager import DatabaseManager


class AlertSeverity(Enum):
    """알람 심각도"""
    INFO = "info"
    WARNING = "warning"  
    ERROR = "error"
    CRITICAL = "critical"


class AlertCategory(Enum):
    """알람 카테고리"""
    DATABASE = "database"
    PERFORMANCE = "performance"
    BATCH_JOB = "batch_job"
    SYSTEM = "system"
    SECURITY = "security"
    DATA_QUALITY = "data_quality"


class AlertChannel(Enum):
    """알림 채널"""
    LOG = "log"
    FILE = "file"
    REDIS = "redis"
    SLACK = "slack"
    EMAIL = "email"
    WEBHOOK = "webhook"


@dataclass
class AlertRule:
    """알람 규칙"""
    rule_id: str
    name: str
    category: AlertCategory
    severity: AlertSeverity
    condition: str  # SQL 쿼리 또는 조건식
    threshold: Optional[float] = None
    check_interval_seconds: int = 60
    escalation_time_minutes: int = 15
    suppression_time_minutes: int = 5
    enabled: bool = True
    channels: List[AlertChannel] = None
    
    def __post_init__(self):
        if self.channels is None:
            self.channels = [AlertChannel.LOG, AlertChannel.REDIS]


@dataclass
class Alert:
    """알람 정보"""
    alert_id: str
    rule_id: str
    title: str
    message: str
    severity: AlertSeverity
    category: AlertCategory
    triggered_at: datetime
    resolved_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    escalated_at: Optional[datetime] = None
    suppressed_until: Optional[datetime] = None
    metadata: Dict[str, Any] = None
    notification_sent: bool = False
    escalation_level: int = 0
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    @property
    def is_active(self) -> bool:
        """활성 알람 여부"""
        return self.resolved_at is None and (
            self.suppressed_until is None or 
            datetime.now() > self.suppressed_until
        )
    
    @property
    def duration_minutes(self) -> float:
        """알람 지속 시간 (분)"""
        end_time = self.resolved_at or datetime.now()
        return (end_time - self.triggered_at).total_seconds() / 60


class RealTimeAlertSystem:
    """실시간 알람 시스템"""
    
    def __init__(self, redis_client: RedisClient = None, db_manager: DatabaseManager = None):
        self.redis_client = redis_client or RedisClient()
        self.db_manager = db_manager or DatabaseManager()
        self.logger = logging.getLogger(__name__)
        
        # 알람 규칙 및 상태 관리
        self.alert_rules: Dict[str, AlertRule] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: deque = deque(maxlen=10000)
        
        # 알림 채널 핸들러
        self.notification_handlers: Dict[AlertChannel, Callable] = {}
        self._initialize_notification_handlers()
        
        # 모니터링 상태
        self.is_monitoring = False
        self._monitoring_tasks: List[asyncio.Task] = []
        
        # 통계
        self.alert_stats = {
            "total_triggered": 0,
            "total_resolved": 0,
            "total_escalated": 0,
            "total_suppressed": 0,
            "avg_resolution_time_minutes": 0
        }
        
        # 기본 규칙 초기화
        self._initialize_default_rules()
    
    def _initialize_default_rules(self):
        """기본 알람 규칙 초기화"""
        default_rules = [
            # 데이터베이스 제약조건 위반
            AlertRule(
                rule_id="db_constraint_violation",
                name="데이터베이스 제약조건 위반",
                category=AlertCategory.DATABASE,
                severity=AlertSeverity.ERROR,
                condition="""
                SELECT COUNT(*) as violation_count 
                FROM pg_stat_database_conflicts 
                WHERE confl_deadlock > 0 OR confl_lock > 0
                """,
                threshold=0,
                check_interval_seconds=30,
                channels=[AlertChannel.LOG, AlertChannel.REDIS, AlertChannel.FILE]
            ),
            
            # 연결 풀 고갈
            AlertRule(
                rule_id="db_connection_pool_exhaustion",
                name="데이터베이스 연결 풀 고갈",
                category=AlertCategory.DATABASE,
                severity=AlertSeverity.CRITICAL,
                condition="""
                SELECT 
                    (active + idle + idle_in_transaction + idle_in_transaction_aborted) as total_connections,
                    setting::int as max_connections,
                    ((active + idle + idle_in_transaction + idle_in_transaction_aborted)::float / setting::int) * 100 as usage_percent
                FROM pg_stat_activity 
                CROSS JOIN pg_settings 
                WHERE name = 'max_connections'
                GROUP BY setting
                """,
                threshold=90.0,  # 90% 이상
                check_interval_seconds=60
            ),
            
            # 배치 작업 실패
            AlertRule(
                rule_id="batch_job_failure",
                name="배치 작업 실패",
                category=AlertCategory.BATCH_JOB,
                severity=AlertSeverity.ERROR,
                condition="""
                SELECT COUNT(*) as failed_jobs
                FROM batch_job_logs 
                WHERE status = 'failed' 
                AND created_at > NOW() - INTERVAL '1 hour'
                """,
                threshold=0,
                check_interval_seconds=120
            ),
            
            # 장시간 실행 쿼리
            AlertRule(
                rule_id="long_running_query",
                name="장시간 실행 쿼리",
                category=AlertCategory.PERFORMANCE,
                severity=AlertSeverity.WARNING,
                condition="""
                SELECT COUNT(*) as long_queries
                FROM pg_stat_activity 
                WHERE state = 'active' 
                AND NOW() - query_start > INTERVAL '10 minutes'
                AND query NOT LIKE '%pg_stat_activity%'
                """,
                threshold=0,
                check_interval_seconds=300  # 5분마다
            ),
            
            # 테이블 락 대기
            AlertRule(
                rule_id="table_lock_wait",
                name="테이블 락 대기",
                category=AlertCategory.PERFORMANCE,
                severity=AlertSeverity.WARNING,
                condition="""
                SELECT COUNT(*) as waiting_locks
                FROM pg_stat_activity 
                WHERE wait_event_type = 'Lock' 
                AND state = 'active'
                """,
                threshold=0,
                check_interval_seconds=60
            ),
            
            # 데이터 품질 이상
            AlertRule(
                rule_id="data_quality_anomaly",
                name="데이터 품질 이상",
                category=AlertCategory.DATA_QUALITY,
                severity=AlertSeverity.WARNING,
                condition="""
                SELECT 
                    COUNT(*) as poor_quality_records
                FROM data_transformation_logs 
                WHERE quality_score < 0.8 
                AND completed_at > NOW() - INTERVAL '1 hour'
                """,
                threshold=10,  # 10건 이상
                check_interval_seconds=180
            ),
            
            # 디스크 사용량
            AlertRule(
                rule_id="high_disk_usage",
                name="높은 디스크 사용량",
                category=AlertCategory.SYSTEM,
                severity=AlertSeverity.WARNING,
                condition="""
                SELECT 
                    (pg_database_size(current_database()) / (1024*1024*1024))::numeric(10,2) as db_size_gb
                """,
                threshold=10.0,  # 10GB 이상
                check_interval_seconds=600  # 10분마다
            )
        ]
        
        for rule in default_rules:
            self.add_alert_rule(rule)
    
    def _initialize_notification_handlers(self):
        """알림 채널 핸들러 초기화"""
        self.notification_handlers = {
            AlertChannel.LOG: self._send_log_notification,
            AlertChannel.FILE: self._send_file_notification,
            AlertChannel.REDIS: self._send_redis_notification,
            AlertChannel.SLACK: self._send_slack_notification,
            AlertChannel.EMAIL: self._send_email_notification,
            AlertChannel.WEBHOOK: self._send_webhook_notification
        }
    
    def add_alert_rule(self, rule: AlertRule):
        """알람 규칙 추가"""
        self.alert_rules[rule.rule_id] = rule
        self.logger.info(f"알람 규칙 추가: {rule.name} ({rule.rule_id})")
    
    def remove_alert_rule(self, rule_id: str):
        """알람 규칙 제거"""
        if rule_id in self.alert_rules:
            rule = self.alert_rules.pop(rule_id)
            self.logger.info(f"알람 규칙 제거: {rule.name} ({rule_id})")
    
    def enable_rule(self, rule_id: str):
        """알람 규칙 활성화"""
        if rule_id in self.alert_rules:
            self.alert_rules[rule_id].enabled = True
            self.logger.info(f"알람 규칙 활성화: {rule_id}")
    
    def disable_rule(self, rule_id: str):
        """알람 규칙 비활성화"""
        if rule_id in self.alert_rules:
            self.alert_rules[rule_id].enabled = False
            self.logger.info(f"알람 규칙 비활성화: {rule_id}")
    
    async def start_monitoring(self):
        """실시간 모니터링 시작"""
        if self.is_monitoring:
            self.logger.warning("알람 시스템이 이미 실행 중입니다")
            return
        
        self.is_monitoring = True
        
        # 각 알람 규칙별로 모니터링 태스크 생성
        for rule_id, rule in self.alert_rules.items():
            if rule.enabled:
                task = asyncio.create_task(
                    self._monitor_rule(rule),
                    name=f"alert_monitor_{rule_id}"
                )
                self._monitoring_tasks.append(task)
        
        # 알람 관리 태스크
        cleanup_task = asyncio.create_task(
            self._alert_cleanup_loop(),
            name="alert_cleanup"
        )
        self._monitoring_tasks.append(cleanup_task)
        
        self.logger.info(f"실시간 알람 모니터링 시작: {len(self._monitoring_tasks)}개 태스크")
    
    async def stop_monitoring(self):
        """실시간 모니터링 중지"""
        self.is_monitoring = False
        
        # 모든 모니터링 태스크 취소
        for task in self._monitoring_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self._monitoring_tasks.clear()
        self.logger.info("실시간 알람 모니터링 중지")
    
    async def _monitor_rule(self, rule: AlertRule):
        """특정 규칙 모니터링"""
        try:
            while self.is_monitoring and rule.enabled:
                await self._check_rule_condition(rule)
                await asyncio.sleep(rule.check_interval_seconds)
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error(f"규칙 모니터링 오류 [{rule.rule_id}]: {e}")
            await asyncio.sleep(rule.check_interval_seconds)
            if self.is_monitoring:
                # 재시작 시도
                asyncio.create_task(self._monitor_rule(rule))
    
    async def _check_rule_condition(self, rule: AlertRule):
        """규칙 조건 확인"""
        try:
            if rule.category == AlertCategory.DATABASE:
                # 데이터베이스 쿼리 실행
                result = await self._execute_database_check(rule)
            elif rule.category == AlertCategory.PERFORMANCE:
                # 성능 메트릭 확인
                result = await self._execute_performance_check(rule)
            elif rule.category == AlertCategory.BATCH_JOB:
                # 배치 작업 상태 확인
                result = await self._execute_batch_job_check(rule)
            else:
                # 기타 시스템 체크
                result = await self._execute_system_check(rule)
            
            # 임계값 확인
            should_trigger = self._evaluate_threshold(rule, result)
            
            if should_trigger:
                await self._trigger_alert(rule, result)
            else:
                await self._resolve_alert_if_exists(rule.rule_id)
                
        except Exception as e:
            self.logger.error(f"규칙 조건 확인 실패 [{rule.rule_id}]: {e}")
    
    async def _execute_database_check(self, rule: AlertRule) -> Dict[str, Any]:
        """데이터베이스 체크 실행"""
        try:
            async with self.db_manager:
                result = await self.db_manager.fetch_all(rule.condition)
                
                if result:
                    return result[0] if len(result) == 1 else {"results": result}
                else:
                    return {"count": 0}
                    
        except Exception as e:
            self.logger.error(f"데이터베이스 체크 실행 실패 [{rule.rule_id}]: {e}")
            return {"error": str(e)}
    
    async def _execute_performance_check(self, rule: AlertRule) -> Dict[str, Any]:
        """성능 체크 실행"""
        try:
            # 데이터베이스 성능 메트릭
            if "pg_stat_activity" in rule.condition:
                return await self._execute_database_check(rule)
            
            # Redis 성능 메트릭
            elif "redis" in rule.condition.lower():
                info = await self.redis_client.client.info()
                return {
                    "used_memory_mb": info.get("used_memory", 0) / 1024 / 1024,
                    "connected_clients": info.get("connected_clients", 0),
                    "ops_per_sec": info.get("instantaneous_ops_per_sec", 0)
                }
            
            # 시스템 메트릭 (기본)
            else:
                return {"cpu_percent": 0, "memory_percent": 0}
                
        except Exception as e:
            self.logger.error(f"성능 체크 실행 실패 [{rule.rule_id}]: {e}")
            return {"error": str(e)}
    
    async def _execute_batch_job_check(self, rule: AlertRule) -> Dict[str, Any]:
        """배치 작업 체크 실행"""
        try:
            return await self._execute_database_check(rule)
        except Exception as e:
            self.logger.error(f"배치 작업 체크 실행 실패 [{rule.rule_id}]: {e}")
            return {"error": str(e)}
    
    async def _execute_system_check(self, rule: AlertRule) -> Dict[str, Any]:
        """시스템 체크 실행"""
        try:
            if "pg_database_size" in rule.condition:
                return await self._execute_database_check(rule)
            else:
                return {"status": "ok"}
        except Exception as e:
            self.logger.error(f"시스템 체크 실행 실패 [{rule.rule_id}]: {e}")
            return {"error": str(e)}
    
    def _evaluate_threshold(self, rule: AlertRule, result: Dict[str, Any]) -> bool:
        """임계값 평가"""
        if "error" in result:
            return True  # 오류 발생 시 알람
        
        if rule.threshold is None:
            # 임계값이 없으면 결과에 따라 판단
            for key, value in result.items():
                if isinstance(value, (int, float)) and value > 0:
                    return True
            return False
        
        # 주요 메트릭별 임계값 확인
        metric_keys = [
            "violation_count", "failed_jobs", "long_queries", "waiting_locks",
            "poor_quality_records", "usage_percent", "db_size_gb"
        ]
        
        for key in metric_keys:
            if key in result:
                value = result[key]
                if isinstance(value, (int, float)):
                    return value > rule.threshold
        
        return False
    
    async def _trigger_alert(self, rule: AlertRule, check_result: Dict[str, Any]):
        """알람 발생"""
        alert_id = f"{rule.rule_id}_{int(time.time())}"
        
        # 이미 활성 알람이 있는지 확인
        existing_alert = None
        for alert in self.active_alerts.values():
            if alert.rule_id == rule.rule_id and alert.is_active:
                existing_alert = alert
                break
        
        if existing_alert:
            # 기존 알람 에스컬레이션 확인
            await self._check_escalation(existing_alert, rule)
            return
        
        # 새 알람 생성
        alert = Alert(
            alert_id=alert_id,
            rule_id=rule.rule_id,
            title=rule.name,
            message=self._format_alert_message(rule, check_result),
            severity=rule.severity,
            category=rule.category,
            triggered_at=datetime.now(),
            metadata={
                "check_result": check_result,
                "rule_condition": rule.condition,
                "threshold": rule.threshold
            }
        )
        
        self.active_alerts[alert_id] = alert
        self.alert_history.append(alert)
        
        # 통계 업데이트
        self.alert_stats["total_triggered"] += 1
        
        # 알림 발송
        await self._send_notifications(alert, rule)
        
        self.logger.warning(
            f"🚨 알람 발생: {rule.name} (심각도: {rule.severity.value}, "
            f"카테고리: {rule.category.value})"
        )
    
    def _format_alert_message(self, rule: AlertRule, check_result: Dict[str, Any]) -> str:
        """알람 메시지 포맷"""
        try:
            if "error" in check_result:
                return f"{rule.name}: 체크 실행 중 오류 발생 - {check_result['error']}"
            
            message_parts = [f"{rule.name}이 감지되었습니다."]
            
            # 주요 메트릭 포함
            for key, value in check_result.items():
                if isinstance(value, (int, float)):
                    if key.endswith("_count") or key.endswith("_jobs"):
                        message_parts.append(f"{key}: {value}건")
                    elif key.endswith("_percent"):
                        message_parts.append(f"{key}: {value:.1f}%")
                    elif key.endswith("_gb"):
                        message_parts.append(f"{key}: {value:.2f}GB")
                    else:
                        message_parts.append(f"{key}: {value}")
            
            if rule.threshold:
                message_parts.append(f"임계값: {rule.threshold}")
            
            return " | ".join(message_parts)
            
        except Exception as e:
            return f"{rule.name}: 메시지 포맷 오류 - {str(e)}"
    
    async def _check_escalation(self, alert: Alert, rule: AlertRule):
        """알람 에스컬레이션 확인"""
        if alert.escalated_at:
            return  # 이미 에스컬레이션됨
        
        duration_minutes = alert.duration_minutes
        if duration_minutes >= rule.escalation_time_minutes:
            # 에스컬레이션 실행
            alert.escalated_at = datetime.now()
            alert.escalation_level += 1
            
            # 더 높은 심각도로 알림 재발송
            escalated_severity = self._get_escalated_severity(alert.severity)
            alert.severity = escalated_severity
            
            await self._send_notifications(alert, rule, is_escalation=True)
            
            self.alert_stats["total_escalated"] += 1
            
            self.logger.error(
                f"🔥 알람 에스컬레이션: {alert.title} "
                f"(지속시간: {duration_minutes:.1f}분, 레벨: {alert.escalation_level})"
            )
    
    def _get_escalated_severity(self, current_severity: AlertSeverity) -> AlertSeverity:
        """에스컬레이션된 심각도 반환"""
        escalation_map = {
            AlertSeverity.INFO: AlertSeverity.WARNING,
            AlertSeverity.WARNING: AlertSeverity.ERROR,
            AlertSeverity.ERROR: AlertSeverity.CRITICAL,
            AlertSeverity.CRITICAL: AlertSeverity.CRITICAL  # 최고 수준 유지
        }
        return escalation_map.get(current_severity, AlertSeverity.CRITICAL)
    
    async def _resolve_alert_if_exists(self, rule_id: str):
        """기존 알람 해제 (조건이 해결된 경우)"""
        for alert_id, alert in list(self.active_alerts.items()):
            if alert.rule_id == rule_id and alert.is_active:
                alert.resolved_at = datetime.now()
                
                # 통계 업데이트
                self.alert_stats["total_resolved"] += 1
                resolution_time = alert.duration_minutes
                current_avg = self.alert_stats["avg_resolution_time_minutes"]
                total_resolved = self.alert_stats["total_resolved"]
                
                # 가중 평균 계산
                new_avg = ((current_avg * (total_resolved - 1)) + resolution_time) / total_resolved
                self.alert_stats["avg_resolution_time_minutes"] = new_avg
                
                # 활성 알람에서 제거
                del self.active_alerts[alert_id]
                
                self.logger.info(
                    f"✅ 알람 해제: {alert.title} "
                    f"(지속시간: {resolution_time:.1f}분)"
                )
                
                # 해제 알림 발송
                rule = self.alert_rules.get(rule_id)
                if rule:
                    await self._send_notifications(alert, rule, is_resolution=True)
                
                break
    
    async def _send_notifications(
        self, 
        alert: Alert, 
        rule: AlertRule, 
        is_escalation: bool = False,
        is_resolution: bool = False
    ):
        """알림 발송"""
        try:
            notification_data = {
                "alert": asdict(alert),
                "rule": asdict(rule),
                "is_escalation": is_escalation,
                "is_resolution": is_resolution,
                "timestamp": datetime.now().isoformat()
            }
            
            # 채널별 알림 발송
            for channel in rule.channels:
                handler = self.notification_handlers.get(channel)
                if handler:
                    try:
                        await handler(notification_data)
                    except Exception as e:
                        self.logger.error(f"알림 발송 실패 [{channel.value}]: {e}")
            
            alert.notification_sent = True
            
        except Exception as e:
            self.logger.error(f"알림 발송 처리 실패: {e}")
    
    async def _send_log_notification(self, notification_data: Dict[str, Any]):
        """로그 알림 발송"""
        alert = notification_data["alert"]
        is_escalation = notification_data["is_escalation"]
        is_resolution = notification_data["is_resolution"]
        
        if is_resolution:
            self.logger.info(f"[ALERT RESOLVED] {alert['title']}: {alert['message']}")
        elif is_escalation:
            self.logger.error(f"[ALERT ESCALATED] {alert['title']}: {alert['message']}")
        else:
            level_map = {
                "info": self.logger.info,
                "warning": self.logger.warning,
                "error": self.logger.error,
                "critical": self.logger.critical
            }
            log_func = level_map.get(alert["severity"], self.logger.warning)
            log_func(f"[ALERT] {alert['title']}: {alert['message']}")
    
    async def _send_file_notification(self, notification_data: Dict[str, Any]):
        """파일 알림 발송"""
        try:
            from pathlib import Path
            
            alert = notification_data["alert"]
            timestamp = datetime.now().strftime("%Y%m%d")
            
            # 일별 알람 파일
            alert_file = Path(f"logs/alerts_{timestamp}.jsonl")
            alert_file.parent.mkdir(exist_ok=True)
            
            # JSON Lines 형태로 저장
            with open(alert_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(notification_data, default=str, ensure_ascii=False) + "\n")
                
        except Exception as e:
            self.logger.error(f"파일 알림 저장 실패: {e}")
    
    async def _send_redis_notification(self, notification_data: Dict[str, Any]):
        """Redis 알림 발송"""
        try:
            alert = notification_data["alert"]
            
            # 실시간 알림 채널에 발행
            channel = "weather_flick:alerts:real_time"
            await self.redis_client.client.publish(
                channel, 
                json.dumps(notification_data, default=str, ensure_ascii=False)
            )
            
            # 알림 히스토리 저장
            history_key = f"alerts:history:{datetime.now().strftime('%Y-%m-%d')}"
            await self.redis_client.client.lpush(
                history_key,
                json.dumps(alert, default=str, ensure_ascii=False)
            )
            
            # 최대 1000개 유지
            await self.redis_client.client.ltrim(history_key, 0, 999)
            
            # 24시간 후 만료
            await self.redis_client.client.expire(history_key, 86400)
            
        except Exception as e:
            self.logger.error(f"Redis 알림 발송 실패: {e}")
    
    async def _send_slack_notification(self, notification_data: Dict[str, Any]):
        """Slack 알림 발송 (구현 예시)"""
        # TODO: Slack 웹훅 구현
        self.logger.info("Slack 알림 발송 (미구현)")
    
    async def _send_email_notification(self, notification_data: Dict[str, Any]):
        """이메일 알림 발송 (구현 예시)"""
        # TODO: 이메일 발송 구현
        self.logger.info("이메일 알림 발송 (미구현)")
    
    async def _send_webhook_notification(self, notification_data: Dict[str, Any]):
        """웹훅 알림 발송 (구현 예시)"""
        # TODO: 웹훅 구현
        self.logger.info("웹훅 알림 발송 (미구현)")
    
    async def _alert_cleanup_loop(self):
        """알람 정리 루프"""
        try:
            while self.is_monitoring:
                await self._cleanup_old_alerts()
                await self._update_alert_stats()
                await asyncio.sleep(600)  # 10분마다
                
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error(f"알람 정리 루프 오류: {e}")
    
    async def _cleanup_old_alerts(self):
        """오래된 알람 정리"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            # 해제된 알람 중 24시간 이상 된 것들 제거
            to_remove = []
            for alert_id, alert in self.active_alerts.items():
                if (alert.resolved_at and alert.resolved_at < cutoff_time):
                    to_remove.append(alert_id)
            
            for alert_id in to_remove:
                del self.active_alerts[alert_id]
            
            if to_remove:
                self.logger.info(f"오래된 알람 정리 완료: {len(to_remove)}건")
                
        except Exception as e:
            self.logger.error(f"알람 정리 실패: {e}")
    
    async def _update_alert_stats(self):
        """알람 통계 업데이트"""
        try:
            # Redis에 통계 저장
            stats_key = "alerts:statistics"
            stats_data = {
                **self.alert_stats,
                "active_alerts_count": len(self.active_alerts),
                "total_rules": len(self.alert_rules),
                "enabled_rules": sum(1 for rule in self.alert_rules.values() if rule.enabled),
                "last_updated": datetime.now().isoformat()
            }
            
            await self.redis_client.client.setex(
                stats_key,
                3600,  # 1시간
                json.dumps(stats_data, default=str)
            )
            
        except Exception as e:
            self.logger.error(f"알람 통계 업데이트 실패: {e}")
    
    async def get_alert_status(self) -> Dict[str, Any]:
        """알람 시스템 상태 조회"""
        try:
            return {
                "is_monitoring": self.is_monitoring,
                "active_alerts": len(self.active_alerts),
                "total_rules": len(self.alert_rules),
                "enabled_rules": sum(1 for rule in self.alert_rules.values() if rule.enabled),
                "statistics": self.alert_stats.copy(),
                "active_alerts_detail": [
                    {
                        "alert_id": alert.alert_id,
                        "title": alert.title,
                        "severity": alert.severity.value,
                        "category": alert.category.value,
                        "duration_minutes": alert.duration_minutes,
                        "escalation_level": alert.escalation_level
                    }
                    for alert in self.active_alerts.values()
                ]
            }
        except Exception as e:
            self.logger.error(f"알람 상태 조회 실패: {e}")
            return {"error": str(e)}
    
    async def acknowledge_alert(self, alert_id: str) -> bool:
        """알람 승인 (확인됨 표시)"""
        try:
            if alert_id in self.active_alerts:
                alert = self.active_alerts[alert_id]
                alert.acknowledged_at = datetime.now()
                
                self.logger.info(f"알람 승인: {alert.title} ({alert_id})")
                return True
            else:
                self.logger.warning(f"존재하지 않는 알람: {alert_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"알람 승인 실패 [{alert_id}]: {e}")
            return False
    
    async def suppress_alert(self, alert_id: str, suppress_minutes: int = 60) -> bool:
        """알람 억제 (일시적 비활성화)"""
        try:
            if alert_id in self.active_alerts:
                alert = self.active_alerts[alert_id]
                alert.suppressed_until = datetime.now() + timedelta(minutes=suppress_minutes)
                
                self.alert_stats["total_suppressed"] += 1
                
                self.logger.info(
                    f"알람 억제: {alert.title} ({alert_id}), "
                    f"억제 시간: {suppress_minutes}분"
                )
                return True
            else:
                self.logger.warning(f"존재하지 않는 알람: {alert_id}")
                return False
                
        except Exception as e:
            self.logger.error(f"알람 억제 실패 [{alert_id}]: {e}")
            return False


# 싱글톤 인스턴스
_alert_system: Optional[RealTimeAlertSystem] = None


def get_alert_system() -> RealTimeAlertSystem:
    """실시간 알람 시스템 싱글톤 인스턴스 반환"""
    global _alert_system
    if _alert_system is None:
        _alert_system = RealTimeAlertSystem()
    return _alert_system