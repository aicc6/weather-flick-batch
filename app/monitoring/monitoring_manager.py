"""
통합 모니터링 관리자

모든 모니터링 시스템을 통합 관리하고 조율하는 중앙 관리자입니다.
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass

from app.monitoring.performance_monitor import get_performance_monitor, PerformanceMonitor
from app.monitoring.alert_system import get_alert_system, AlertSystem, AlertSeverity
from app.monitoring.api_performance_tracker import get_api_performance_tracker, APIPerformanceTracker
from app.monitoring.cleanup_monitor import get_cleanup_monitor, CleanupMonitor

logger = logging.getLogger(__name__)


@dataclass
class MonitoringConfig:
    """모니터링 설정"""
    performance_monitoring_enabled: bool = True
    api_tracking_enabled: bool = True
    cleanup_monitoring_enabled: bool = True
    alert_system_enabled: bool = True
    
    # 모니터링 간격 (초)
    performance_check_interval: int = 30
    api_analysis_interval: int = 60
    cleanup_check_interval: int = 300  # 5분
    
    # 자동 이슈 감지
    auto_issue_detection: bool = True
    issue_detection_interval: int = 120  # 2분
    
    # 보고서 생성
    generate_hourly_reports: bool = True
    generate_daily_reports: bool = True


class MonitoringManager:
    """통합 모니터링 관리자"""
    
    def __init__(self, config: Optional[MonitoringConfig] = None):
        """
        모니터링 관리자 초기화
        
        Args:
            config: 모니터링 설정 (None이면 기본 설정 사용)
        """
        self.config = config or MonitoringConfig()
        
        # 모니터링 시스템 인스턴스들
        self.performance_monitor: PerformanceMonitor = get_performance_monitor()
        self.alert_system: AlertSystem = get_alert_system()
        self.api_tracker: APIPerformanceTracker = get_api_performance_tracker()
        self.cleanup_monitor: CleanupMonitor = get_cleanup_monitor()
        
        # 모니터링 상태
        self.is_running = False
        self.monitoring_tasks: List[asyncio.Task] = []
        self.last_report_times = {
            "hourly": None,
            "daily": None
        }
        
        # 통계
        self.manager_stats = {
            "start_time": None,
            "issues_detected": 0,
            "alerts_sent": 0,
            "reports_generated": 0,
            "monitoring_cycles": 0
        }
        
        logger.info("통합 모니터링 관리자 초기화 완료")
    
    async def start_monitoring(self):
        """모니터링 시작"""
        if self.is_running:
            logger.warning("모니터링이 이미 실행 중입니다")
            return
        
        self.is_running = True
        self.manager_stats["start_time"] = datetime.now()
        
        # 성능 모니터링 시작
        if self.config.performance_monitoring_enabled:
            self.performance_monitor.start_monitoring()
        
        # 비동기 모니터링 태스크들 시작
        await self._start_monitoring_tasks()
        
        # 시작 알림
        await self._send_system_alert(
            "모니터링 시스템 시작",
            "Weather Flick 배치 시스템 모니터링이 시작되었습니다.",
            AlertSeverity.INFO
        )
        
        logger.info("🚀 통합 모니터링 시스템 시작됨")
    
    async def stop_monitoring(self):
        """모니터링 중지"""
        if not self.is_running:
            logger.warning("모니터링이 실행되고 있지 않습니다")
            return
        
        self.is_running = False
        
        # 성능 모니터링 중지
        self.performance_monitor.stop_monitoring()
        
        # 비동기 태스크들 취소
        for task in self.monitoring_tasks:
            task.cancel()
        
        # 태스크 완료 대기
        if self.monitoring_tasks:
            await asyncio.gather(*self.monitoring_tasks, return_exceptions=True)
        
        self.monitoring_tasks.clear()
        
        # 종료 알림
        await self._send_system_alert(
            "모니터링 시스템 종료",
            "Weather Flick 배치 시스템 모니터링이 종료되었습니다.",
            AlertSeverity.INFO
        )
        
        logger.info("🛑 통합 모니터링 시스템 종료됨")
    
    async def _start_monitoring_tasks(self):
        """모니터링 태스크들 시작"""
        self.monitoring_tasks = []
        
        # API 성능 분석 태스크
        if self.config.api_tracking_enabled:
            task = asyncio.create_task(self._api_analysis_loop())
            self.monitoring_tasks.append(task)
        
        # 정리 시스템 모니터링 태스크
        if self.config.cleanup_monitoring_enabled:
            task = asyncio.create_task(self._cleanup_monitoring_loop())
            self.monitoring_tasks.append(task)
        
        # 자동 이슈 감지 태스크
        if self.config.auto_issue_detection:
            task = asyncio.create_task(self._issue_detection_loop())
            self.monitoring_tasks.append(task)
        
        # 보고서 생성 태스크
        if self.config.generate_hourly_reports or self.config.generate_daily_reports:
            task = asyncio.create_task(self._report_generation_loop())
            self.monitoring_tasks.append(task)
        
        logger.info(f"모니터링 태스크 {len(self.monitoring_tasks)}개 시작됨")
    
    async def _api_analysis_loop(self):
        """API 성능 분석 루프"""
        while self.is_running:
            try:
                await self._analyze_api_performance()
                await asyncio.sleep(self.config.api_analysis_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"API 성능 분석 오류: {e}")
                await asyncio.sleep(self.config.api_analysis_interval)
    
    async def _cleanup_monitoring_loop(self):
        """정리 시스템 모니터링 루프"""
        while self.is_running:
            try:
                await self._monitor_cleanup_system()
                await asyncio.sleep(self.config.cleanup_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"정리 시스템 모니터링 오류: {e}")
                await asyncio.sleep(self.config.cleanup_check_interval)
    
    async def _issue_detection_loop(self):
        """자동 이슈 감지 루프"""
        while self.is_running:
            try:
                await self._detect_and_handle_issues()
                await asyncio.sleep(self.config.issue_detection_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"이슈 감지 오류: {e}")
                await asyncio.sleep(self.config.issue_detection_interval)
    
    async def _report_generation_loop(self):
        """보고서 생성 루프"""
        while self.is_running:
            try:
                await self._generate_scheduled_reports()
                await asyncio.sleep(60)  # 1분마다 체크
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"보고서 생성 오류: {e}")
                await asyncio.sleep(60)
    
    async def _analyze_api_performance(self):
        """API 성능 분석"""
        try:
            # 전체 성능 요약 가져오기
            overall_performance = self.api_tracker.get_overall_performance()
            
            # 성능 이슈 감지
            performance_issues = self.api_tracker.detect_performance_issues()
            
            # 심각한 이슈가 있으면 알림 전송
            for issue in performance_issues:
                if issue.get("severity") == "critical":
                    await self._send_system_alert(
                        f"API 성능 이슈: {issue['type']}",
                        issue["message"],
                        AlertSeverity.CRITICAL,
                        metadata=issue
                    )
                elif issue.get("severity") == "warning":
                    await self._send_system_alert(
                        f"API 성능 경고: {issue['type']}",
                        issue["message"],
                        AlertSeverity.WARNING,
                        metadata=issue
                    )
            
            # 통계 업데이트
            self.manager_stats["monitoring_cycles"] += 1
            self.manager_stats["issues_detected"] += len(performance_issues)
            
            logger.debug(f"API 성능 분석 완료: {len(performance_issues)}개 이슈 감지")
            
        except Exception as e:
            logger.error(f"API 성능 분석 실패: {e}")
    
    async def _monitor_cleanup_system(self):
        """정리 시스템 모니터링"""
        try:
            # 정리 시스템 건강 상태 체크
            health_result = self.cleanup_monitor.perform_health_check()
            
            overall_health = health_result.get("overall_health", "unknown")
            health_score = health_result.get("health_score", 0)
            
            # 건강 상태가 좋지 않으면 알림
            if overall_health == "critical":
                await self._send_system_alert(
                    "정리 시스템 위험 상태",
                    f"정리 시스템 건강 점수가 위험 수준입니다 (점수: {health_score})",
                    AlertSeverity.CRITICAL,
                    metadata={"health_score": health_score, "health_result": health_result}
                )
            elif overall_health == "unhealthy":
                await self._send_system_alert(
                    "정리 시스템 불안정",
                    f"정리 시스템 상태가 불안정합니다 (점수: {health_score})",
                    AlertSeverity.WARNING,
                    metadata={"health_score": health_score}
                )
            
            logger.debug(f"정리 시스템 모니터링 완료: {overall_health} (점수: {health_score})")
            
        except Exception as e:
            logger.error(f"정리 시스템 모니터링 실패: {e}")
    
    async def _detect_and_handle_issues(self):
        """이슈 감지 및 처리"""
        try:
            # 성능 모니터 요약 가져오기
            performance_summary = self.performance_monitor.get_performance_summary(time_window_minutes=30)
            
            # 최근 알림 확인
            recent_alerts = performance_summary.get("recent_alerts", [])
            critical_alerts = [a for a in recent_alerts if a.get("severity") == "critical"]
            
            # 위험 수준 알림이 많으면 시스템 경고
            if len(critical_alerts) >= 3:
                await self._send_system_alert(
                    "시스템 위험 상태",
                    f"30분 내 {len(critical_alerts)}개의 위험 수준 알림이 발생했습니다.",
                    AlertSeverity.CRITICAL,
                    metadata={
                        "critical_alerts_count": len(critical_alerts),
                        "recent_alerts": recent_alerts
                    }
                )
            
            # 전체 건강 상태 확인
            overall_health = performance_summary.get("overall_health", "healthy")
            if overall_health == "critical":
                await self._send_system_alert(
                    "시스템 전체 상태 위험",
                    "시스템 전체 건강 상태가 위험 수준입니다.",
                    AlertSeverity.CRITICAL,
                    metadata={"performance_summary": performance_summary}
                )
            
            logger.debug(f"이슈 감지 완료: 전체 상태 {overall_health}, 위험 알림 {len(critical_alerts)}개")
            
        except Exception as e:
            logger.error(f"이슈 감지 실패: {e}")
    
    async def _generate_scheduled_reports(self):
        """예약된 보고서 생성"""
        now = datetime.now()
        
        try:
            # 시간별 보고서 생성 체크
            if self.config.generate_hourly_reports:
                last_hourly = self.last_report_times["hourly"]
                if not last_hourly or (now - last_hourly).total_seconds() >= 3600:
                    await self._generate_hourly_report()
                    self.last_report_times["hourly"] = now
            
            # 일별 보고서 생성 체크 (매일 자정)
            if self.config.generate_daily_reports:
                last_daily = self.last_report_times["daily"]
                if not last_daily or (last_daily.date() != now.date() and now.hour == 0):
                    await self._generate_daily_report()
                    self.last_report_times["daily"] = now
                    
        except Exception as e:
            logger.error(f"예약된 보고서 생성 실패: {e}")
    
    async def _generate_hourly_report(self):
        """시간별 보고서 생성"""
        try:
            # 각 시스템에서 데이터 수집
            performance_summary = self.performance_monitor.get_performance_summary(time_window_minutes=60)
            api_performance = self.api_tracker.get_overall_performance()
            alert_stats = self.alert_system.get_alert_statistics()
            
            # 보고서 내용 구성
            report = {
                "report_type": "hourly",
                "generated_at": datetime.now().isoformat(),
                "time_window": "1 hour",
                "performance_summary": performance_summary,
                "api_performance": api_performance,
                "alert_statistics": alert_stats,
                "monitoring_manager_stats": self.manager_stats
            }
            
            # 중요한 지표 요약
            summary_message = self._create_report_summary(report)
            
            # 보고서 알림 전송
            await self._send_system_alert(
                "시간별 모니터링 보고서",
                summary_message,
                AlertSeverity.INFO,
                metadata={"report": report}
            )
            
            self.manager_stats["reports_generated"] += 1
            logger.info("시간별 모니터링 보고서 생성 완료")
            
        except Exception as e:
            logger.error(f"시간별 보고서 생성 실패: {e}")
    
    async def _generate_daily_report(self):
        """일별 보고서 생성"""
        try:
            # 24시간 데이터 수집
            performance_summary = self.performance_monitor.get_performance_summary(time_window_minutes=1440)  # 24시간
            api_performance = self.api_tracker.get_overall_performance()
            alert_stats = self.alert_system.get_alert_statistics()
            
            # 보고서 내용 구성
            report = {
                "report_type": "daily",
                "generated_at": datetime.now().isoformat(),
                "time_window": "24 hours",
                "performance_summary": performance_summary,
                "api_performance": api_performance,
                "alert_statistics": alert_stats,
                "monitoring_manager_stats": self.manager_stats
            }
            
            # 일별 요약 메시지
            summary_message = self._create_daily_report_summary(report)
            
            # 보고서 알림 전송
            await self._send_system_alert(
                "일별 모니터링 보고서",
                summary_message,
                AlertSeverity.INFO,
                metadata={"report": report}
            )
            
            self.manager_stats["reports_generated"] += 1
            logger.info("일별 모니터링 보고서 생성 완료")
            
        except Exception as e:
            logger.error(f"일별 보고서 생성 실패: {e}")
    
    def _create_report_summary(self, report: Dict[str, Any]) -> str:
        """보고서 요약 메시지 생성"""
        api_perf = report.get("api_performance", {}).get("summary", {})
        alert_stats = report.get("alert_statistics", {})
        
        total_calls = api_perf.get("total_api_calls", 0)
        success_rate = api_perf.get("overall_success_rate", 0)
        avg_response_time = api_perf.get("avg_response_time_ms", 0)
        total_alerts = alert_stats.get("total_alerts_sent", 0)
        
        summary = f"""시간별 모니터링 요약:
        
📊 API 성능:
• 총 호출 수: {total_calls:,}개
• 성공률: {success_rate:.1f}%
• 평균 응답시간: {avg_response_time:.0f}ms

🚨 알림:
• 총 알림 수: {total_alerts}개
• 활성 채널: {len(alert_stats.get('active_channels', []))}개

⏱️ 시스템 상태:
• 모니터링 사이클: {self.manager_stats.get('monitoring_cycles', 0)}회
• 감지된 이슈: {self.manager_stats.get('issues_detected', 0)}개"""

        return summary
    
    def _create_daily_report_summary(self, report: Dict[str, Any]) -> str:
        """일별 보고서 요약 메시지 생성"""
        api_perf = report.get("api_performance", {}).get("summary", {})
        
        total_calls = api_perf.get("total_api_calls", 0)
        success_rate = api_perf.get("overall_success_rate", 0)
        avg_response_time = api_perf.get("avg_response_time_ms", 0)
        active_providers = api_perf.get("active_providers", 0)
        
        uptime_hours = self.manager_stats.get("start_time")
        if uptime_hours:
            uptime_hours = (datetime.now() - uptime_hours).total_seconds() / 3600
        else:
            uptime_hours = 0
        
        summary = f"""일별 모니터링 요약:
        
📈 24시간 성과:
• API 호출: {total_calls:,}개
• 전체 성공률: {success_rate:.1f}%
• 평균 응답시간: {avg_response_time:.0f}ms
• 활성 제공자: {active_providers}개

⚡ 시스템 안정성:
• 가동 시간: {uptime_hours:.1f}시간
• 생성된 보고서: {self.manager_stats.get('reports_generated', 0)}개
• 전송된 알림: {self.manager_stats.get('alerts_sent', 0)}개

🎯 다음 24시간 동안 모니터링이 계속됩니다."""

        return summary
    
    async def _send_system_alert(self, title: str, message: str, severity: AlertSeverity, 
                               metadata: Optional[Dict[str, Any]] = None):
        """시스템 알림 전송"""
        try:
            result = self.alert_system.send_system_alert(
                title=title,
                message=message,
                severity=severity,
                source="monitoring_manager",
                metadata=metadata
            )
            
            self.manager_stats["alerts_sent"] += 1
            return result
            
        except Exception as e:
            logger.error(f"시스템 알림 전송 실패: {e}")
            return {}
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """모니터링 상태 반환"""
        uptime = None
        if self.manager_stats["start_time"]:
            uptime = (datetime.now() - self.manager_stats["start_time"]).total_seconds()
        
        return {
            "is_running": self.is_running,
            "config": {
                "performance_monitoring_enabled": self.config.performance_monitoring_enabled,
                "api_tracking_enabled": self.config.api_tracking_enabled,
                "cleanup_monitoring_enabled": self.config.cleanup_monitoring_enabled,
                "alert_system_enabled": self.config.alert_system_enabled,
                "auto_issue_detection": self.config.auto_issue_detection
            },
            "statistics": {
                **self.manager_stats,
                "uptime_seconds": uptime,
                "active_tasks": len(self.monitoring_tasks)
            },
            "subsystem_status": {
                "performance_monitor": self.performance_monitor.monitoring_active,
                "api_tracker": len(self.api_tracker.provider_stats) > 0,
                "alert_system": len(self.alert_system.configs) > 0
            }
        }
    
    async def generate_comprehensive_report(self) -> Dict[str, Any]:
        """종합 모니터링 보고서 생성"""
        try:
            return {
                "report_type": "comprehensive",
                "generated_at": datetime.now().isoformat(),
                "monitoring_status": self.get_monitoring_status(),
                "performance_summary": self.performance_monitor.get_performance_summary(),
                "api_performance": self.api_tracker.get_overall_performance(),
                "real_time_metrics": self.api_tracker.get_real_time_metrics(),
                "alert_statistics": self.alert_system.get_alert_statistics(),
                "detected_issues": self.api_tracker.detect_performance_issues(),
                "cleanup_health": self.cleanup_monitor.perform_health_check()
            }
        except Exception as e:
            logger.error(f"종합 보고서 생성 실패: {e}")
            return {"error": str(e)}
    
    def collect_system_metrics(self) -> Dict[str, Any]:
        """
        시스템 메트릭 수집
        
        Returns:
            Dict[str, Any]: 수집된 시스템 메트릭
        """
        try:
            import psutil
            system_metrics_available = True
        except ImportError:
            system_metrics_available = False
            logger.warning("psutil이 설치되지 않아 시스템 메트릭을 수집할 수 없습니다")
        
        metrics = {
            "timestamp": datetime.now().isoformat(),
            "system_metrics_available": system_metrics_available,
            "monitoring_status": self.get_monitoring_status(),
            "manager_stats": self.manager_stats.copy()
        }
        
        if system_metrics_available:
            try:
                # CPU 메트릭
                cpu_percent = psutil.cpu_percent(interval=1)
                cpu_count = psutil.cpu_count()
                
                # 메모리 메트릭
                memory = psutil.virtual_memory()
                
                # 디스크 메트릭
                disk = psutil.disk_usage('/')
                
                # 프로세스 메트릭
                current_process = psutil.Process()
                process_memory = current_process.memory_info()
                
                metrics.update({
                    "system": {
                        "cpu": {
                            "usage_percent": cpu_percent,
                            "cpu_count": cpu_count,
                            "load_average": psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None
                        },
                        "memory": {
                            "total_gb": round(memory.total / (1024**3), 2),
                            "available_gb": round(memory.available / (1024**3), 2),
                            "used_gb": round(memory.used / (1024**3), 2),
                            "usage_percent": memory.percent,
                            "free_gb": round(memory.free / (1024**3), 2)
                        },
                        "disk": {
                            "total_gb": round(disk.total / (1024**3), 2),
                            "used_gb": round(disk.used / (1024**3), 2),
                            "free_gb": round(disk.free / (1024**3), 2),
                            "usage_percent": round((disk.used / disk.total) * 100, 2)
                        },
                        "process": {
                            "memory_rss_mb": round(process_memory.rss / (1024**2), 2),
                            "memory_vms_mb": round(process_memory.vms / (1024**2), 2),
                            "cpu_percent": current_process.cpu_percent(),
                            "pid": current_process.pid,
                            "create_time": datetime.fromtimestamp(current_process.create_time()).isoformat(),
                            "status": current_process.status()
                        }
                    }
                })
                
                # 네트워크 메트릭 (선택적)
                try:
                    net_io = psutil.net_io_counters()
                    metrics["system"]["network"] = {
                        "bytes_sent": net_io.bytes_sent,
                        "bytes_recv": net_io.bytes_recv,
                        "packets_sent": net_io.packets_sent,
                        "packets_recv": net_io.packets_recv
                    }
                except Exception:
                    # 네트워크 메트릭은 선택적이므로 실패해도 무시
                    pass
                    
            except Exception as e:
                logger.error(f"시스템 메트릭 수집 중 오류: {e}")
                metrics["system_metrics_error"] = str(e)
        
        # 모니터링 시스템 메트릭 추가
        try:
            # 성능 모니터 메트릭
            if hasattr(self.performance_monitor, 'get_performance_summary'):
                metrics["performance_metrics"] = self.performance_monitor.get_performance_summary(time_window_minutes=5)
            
            # API 성능 메트릭
            if hasattr(self.api_tracker, 'get_real_time_metrics'):
                metrics["api_metrics"] = self.api_tracker.get_real_time_metrics()
            
            # 알림 시스템 메트릭
            if hasattr(self.alert_system, 'get_alert_statistics'):
                metrics["alert_metrics"] = self.alert_system.get_alert_statistics()
            
            # 정리 시스템 메트릭
            if hasattr(self.cleanup_monitor, 'get_system_status'):
                try:
                    cleanup_status = self.cleanup_monitor.perform_health_check()
                    metrics["cleanup_metrics"] = {
                        "health_status": cleanup_status.get("overall_health", "unknown"),
                        "health_score": cleanup_status.get("overall_score", 0),
                        "last_check": cleanup_status.get("timestamp", "unknown")
                    }
                except Exception:
                    metrics["cleanup_metrics"] = {"status": "unavailable"}
            
        except Exception as e:
            logger.warning(f"모니터링 시스템 메트릭 수집 중 일부 오류: {e}")
            metrics["monitoring_metrics_warning"] = str(e)
        
        # 수집 완료 로그
        logger.debug(f"시스템 메트릭 수집 완료: {len(metrics)}개 메트릭 카테고리")
        
        return metrics
    
    def get_system_health_summary(self) -> Dict[str, Any]:
        """
        시스템 건강도 요약 정보 반환
        
        Returns:
            Dict[str, Any]: 시스템 건강도 요약
        """
        try:
            metrics = self.collect_system_metrics()
            
            health_summary = {
                "timestamp": datetime.now().isoformat(),
                "overall_status": "healthy",
                "warnings": [],
                "critical_issues": []
            }
            
            # 시스템 리소스 건강도 확인
            if "system" in metrics:
                system = metrics["system"]
                
                # CPU 사용률 체크
                if "cpu" in system:
                    cpu_usage = system["cpu"].get("usage_percent", 0)
                    if cpu_usage > 90:
                        health_summary["critical_issues"].append(f"높은 CPU 사용률: {cpu_usage}%")
                        health_summary["overall_status"] = "critical"
                    elif cpu_usage > 70:
                        health_summary["warnings"].append(f"높은 CPU 사용률: {cpu_usage}%")
                        if health_summary["overall_status"] == "healthy":
                            health_summary["overall_status"] = "warning"
                
                # 메모리 사용률 체크
                if "memory" in system:
                    memory_usage = system["memory"].get("usage_percent", 0)
                    if memory_usage > 90:
                        health_summary["critical_issues"].append(f"높은 메모리 사용률: {memory_usage}%")
                        health_summary["overall_status"] = "critical"
                    elif memory_usage > 80:
                        health_summary["warnings"].append(f"높은 메모리 사용률: {memory_usage}%")
                        if health_summary["overall_status"] == "healthy":
                            health_summary["overall_status"] = "warning"
                
                # 디스크 사용률 체크
                if "disk" in system:
                    disk_usage = system["disk"].get("usage_percent", 0)
                    if disk_usage > 95:
                        health_summary["critical_issues"].append(f"높은 디스크 사용률: {disk_usage}%")
                        health_summary["overall_status"] = "critical"
                    elif disk_usage > 85:
                        health_summary["warnings"].append(f"높은 디스크 사용률: {disk_usage}%")
                        if health_summary["overall_status"] == "healthy":
                            health_summary["overall_status"] = "warning"
            
            # 모니터링 시스템 건강도 체크
            if not self.is_running:
                health_summary["critical_issues"].append("모니터링 시스템이 중지되어 있습니다")
                health_summary["overall_status"] = "critical"
            
            # API 성능 체크
            if "api_metrics" in metrics:
                api_metrics = metrics["api_metrics"]
                if "summary" in api_metrics:
                    success_rate = api_metrics["summary"].get("overall_success_rate", 100)
                    if success_rate < 70:
                        health_summary["critical_issues"].append(f"낮은 API 성공률: {success_rate}%")
                        health_summary["overall_status"] = "critical"
                    elif success_rate < 90:
                        health_summary["warnings"].append(f"낮은 API 성공률: {success_rate}%")
                        if health_summary["overall_status"] == "healthy":
                            health_summary["overall_status"] = "warning"
            
            # 건강도 점수 계산 (100점 만점)
            score = 100
            score -= len(health_summary["warnings"]) * 10
            score -= len(health_summary["critical_issues"]) * 25
            score = max(0, score)
            
            health_summary["health_score"] = score
            health_summary["metrics_collected"] = len(metrics)
            
            return health_summary
            
        except Exception as e:
            logger.error(f"시스템 건강도 요약 생성 실패: {e}")
            return {
                "timestamp": datetime.now().isoformat(),
                "overall_status": "error",
                "error": str(e),
                "health_score": 0
            }


# 전역 모니터링 관리자 인스턴스
_monitoring_manager: Optional[MonitoringManager] = None


def get_monitoring_manager(config: Optional[MonitoringConfig] = None) -> MonitoringManager:
    """전역 모니터링 관리자 인스턴스 반환 (싱글톤)"""
    global _monitoring_manager
    
    if _monitoring_manager is None:
        _monitoring_manager = MonitoringManager(config)
    
    return _monitoring_manager


def reset_monitoring_manager():
    """모니터링 관리자 인스턴스 재설정 (테스트용)"""
    global _monitoring_manager
    
    if _monitoring_manager and _monitoring_manager.is_running:
        # 비동기 중지는 여기서 직접 호출할 수 없으므로 로그만 출력
        logger.warning("모니터링 관리자를 재설정하기 전에 stop_monitoring()을 호출하세요")
    
    _monitoring_manager = None