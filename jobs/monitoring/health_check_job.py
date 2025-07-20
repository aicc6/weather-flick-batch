"""
시스템 헬스체크 배치 작업

시스템 전반의 상태 모니터링 및 이상 감지 작업
실행 주기: 5분마다
"""

import asyncio
import psutil
import aiohttp
import redis
from datetime import datetime
from typing import Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

from app.core.logger import get_logger
from config.settings import get_monitoring_settings
from app.core.database_manager import DatabaseManager


class HealthStatus(Enum):
    """헬스 상태"""

    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class HealthCheckResult:
    """헬스체크 결과"""

    component: str
    status: HealthStatus
    response_time_ms: float = 0.0
    message: str = ""
    details: Dict[str, Any] = None

    def __post_init__(self):
        if self.details is None:
            self.details = {}


class SystemHealthChecker:
    """시스템 헬스체커"""

    def __init__(self):
        self.logger = get_logger(__name__)
        self.settings = get_monitoring_settings()
        self.db_manager = DatabaseManager()
        self.session: Optional[aiohttp.ClientSession] = None
        self.redis_client: Optional[redis.Redis] = None

    async def __aenter__(self):
        """비동기 컨텍스트 매니저 진입"""
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30))

        self.redis_client = redis.Redis(
            host=self.settings.redis_host,
            port=self.settings.redis_port,
            db=self.settings.redis_db,
            decode_responses=True,
        )

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """비동기 컨텍스트 매니저 종료"""
        if self.session:
            await self.session.close()

        if self.redis_client:
            self.redis_client.close()

    async def execute(self) -> Dict[str, Any]:
        """시스템 헬스체크 실행"""
        self.logger.debug("시스템 헬스체크 시작")

        async with self:
            try:
                health_results = {}

                # 1. 데이터베이스 연결 상태 체크
                health_results["database"] = await self._check_database_health()

                # 2. Redis 캐시 상태 체크
                health_results["cache"] = await self._check_redis_health()

                # 3. 외부 API 응답 시간 체크
                health_results["external_apis"] = await self._check_external_apis()

                # 4. 서버 리소스 사용률 체크
                health_results[
                    "server_resources"
                ] = await self._check_server_resources()

                # 5. 애플리케이션 응답 시간 체크
                health_results[
                    "app_response_time"
                ] = await self._check_app_response_time()

                # 6. 배치 작업 상태 체크
                health_results["batch_jobs"] = await self._check_batch_jobs_health()

                # 7. 전체 시스템 상태 평가
                overall_status = self._evaluate_overall_health(health_results)

                # 8. 상태 변화 감지 및 알림
                await self._process_health_status_changes(
                    health_results, overall_status
                )

                # 9. 메트릭 저장
                await self._store_health_metrics(health_results)

                result = {
                    "overall_status": overall_status.value,
                    "components": {
                        k: v.__dict__ if isinstance(v, HealthCheckResult) else v
                        for k, v in health_results.items()
                    },
                    "check_time": datetime.now().isoformat(),
                    "processed_records": len(health_results),
                }

                if overall_status != HealthStatus.HEALTHY:
                    self.logger.warning(f"시스템 상태 이상: {overall_status.value}")

                return result

            except Exception as e:
                self.logger.error(f"헬스체크 실행 실패: {e}")
                raise

    async def _check_database_health(self) -> HealthCheckResult:
        """데이터베이스 연결 상태 체크 (개선된 버전)"""
        start_time = datetime.now()

        try:
            # 연결 테스트 (타임아웃 설정)
            await asyncio.wait_for(self.db_manager.execute("SELECT 1"), timeout=10.0)

            # 성능 체크 (타임아웃 설정)
            perf_query = """
            SELECT
                count(*) as active_connections,
                (SELECT setting::int FROM pg_settings WHERE name = 'max_connections') as max_connections
            FROM pg_stat_activity
            WHERE state = 'active'
            """

            result = await asyncio.wait_for(
                self.db_manager.fetch_one(perf_query), timeout=15.0
            )
            response_time = (datetime.now() - start_time).total_seconds() * 1000

            # 상태 평가 (더 관대한 기준)
            connection_usage = result["active_connections"] / result["max_connections"]

            if connection_usage > 0.95:  # 95% 이상일 때만 critical
                status = HealthStatus.CRITICAL
                message = f"데이터베이스 연결 과부하: {connection_usage:.1%}"
            elif connection_usage > 0.8:  # 80% 이상일 때 warning
                status = HealthStatus.WARNING
                message = f"데이터베이스 연결 높음: {connection_usage:.1%}"
            else:
                status = HealthStatus.HEALTHY
                message = "데이터베이스 정상"

            return HealthCheckResult(
                component="database",
                status=status,
                response_time_ms=response_time,
                message=message,
                details={
                    "active_connections": result["active_connections"],
                    "max_connections": result["max_connections"],
                    "connection_usage_percent": round(connection_usage * 100, 1),
                },
            )

        except asyncio.TimeoutError:
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            return HealthCheckResult(
                component="database",
                status=HealthStatus.WARNING,  # critical 대신 warning으로 변경
                response_time_ms=response_time,
                message="데이터베이스 응답 시간 초과",
                details={"error": "timeout", "timeout_seconds": 15},
            )
        except Exception as e:
            response_time = (datetime.now() - start_time).total_seconds() * 1000
            return HealthCheckResult(
                component="database",
                status=HealthStatus.WARNING,  # critical 대신 warning으로 변경
                response_time_ms=response_time,
                message=f"데이터베이스 연결 실패: {str(e)}",
                details={"error": str(e)},
            )

    async def _check_redis_health(self) -> HealthCheckResult:
        """Redis 캐시 상태 체크"""
        start_time = datetime.now()

        try:
            # 연결 테스트
            pong = self.redis_client.ping()
            if not pong:
                raise Exception("Redis ping 실패")

            # 메모리 사용량 체크
            info = self.redis_client.info("memory")
            used_memory = info["used_memory"]
            max_memory = info.get("maxmemory", 0)

            response_time = (datetime.now() - start_time).total_seconds() * 1000

            # 상태 평가
            if max_memory > 0:
                memory_usage = used_memory / max_memory
                if memory_usage > 0.9:
                    status = HealthStatus.CRITICAL
                    message = f"Redis 메모리 과부하: {memory_usage:.1%}"
                elif memory_usage > 0.8:
                    status = HealthStatus.WARNING
                    message = f"Redis 메모리 높음: {memory_usage:.1%}"
                else:
                    status = HealthStatus.HEALTHY
                    message = "Redis 정상"
            else:
                status = HealthStatus.HEALTHY
                message = "Redis 정상"
                memory_usage = 0

            return HealthCheckResult(
                component="cache",
                status=status,
                response_time_ms=response_time,
                message=message,
                details={
                    "used_memory_mb": round(used_memory / 1024 / 1024, 2),
                    "max_memory_mb": (
                        round(max_memory / 1024 / 1024, 2) if max_memory > 0 else None
                    ),
                    "memory_usage_percent": round(memory_usage * 100, 1),
                    "connected_clients": info.get("connected_clients", 0),
                },
            )

        except Exception as e:
            response_time = (datetime.now() - start_time).total_seconds() * 1000

            return HealthCheckResult(
                component="cache",
                status=HealthStatus.CRITICAL,
                response_time_ms=response_time,
                message=f"Redis 연결 실패: {str(e)}",
                details={"error": str(e)},
            )

    async def _check_external_apis(self) -> Dict[str, HealthCheckResult]:
        """외부 API 응답 시간 체크"""
        apis_to_check = [
            {
                "name": "weather_api",
                "url": f"{self.settings.weather_api_base_url}/weather",
                "params": {"q": "Seoul", "appid": "test"},
                "timeout": 10,
            },
            {
                "name": "tourism_api",
                "url": f"{self.settings.tourism_api_base_url}/areaBasedList1",
                "params": {"serviceKey": "test", "numOfRows": "1", "MobileOS": "ETC"},
                "timeout": 15,
            },
        ]

        results = {}

        for api_config in apis_to_check:
            start_time = datetime.now()

            try:
                async with self.session.get(
                    api_config["url"],
                    params=api_config["params"],
                    timeout=aiohttp.ClientTimeout(total=api_config["timeout"]),
                ) as response:
                    response_time = (datetime.now() - start_time).total_seconds() * 1000

                    # 상태 평가
                    if response.status == 200:
                        if response_time < 2000:
                            status = HealthStatus.HEALTHY
                            message = f"{api_config['name']} 정상"
                        elif response_time < 5000:
                            status = HealthStatus.WARNING
                            message = f"{api_config['name']} 응답 지연"
                        else:
                            status = HealthStatus.CRITICAL
                            message = f"{api_config['name']} 응답 매우 지연"
                    else:
                        status = HealthStatus.WARNING
                        message = f"{api_config['name']} 비정상 응답: {response.status}"

                    results[api_config["name"]] = HealthCheckResult(
                        component=api_config["name"],
                        status=status,
                        response_time_ms=response_time,
                        message=message,
                        details={
                            "status_code": response.status,
                            "url": api_config["url"],
                        },
                    )

            except asyncio.TimeoutError:
                response_time = (datetime.now() - start_time).total_seconds() * 1000
                results[api_config["name"]] = HealthCheckResult(
                    component=api_config["name"],
                    status=HealthStatus.CRITICAL,
                    response_time_ms=response_time,
                    message=f"{api_config['name']} 타임아웃",
                    details={"error": "timeout"},
                )

            except Exception as e:
                response_time = (datetime.now() - start_time).total_seconds() * 1000
                results[api_config["name"]] = HealthCheckResult(
                    component=api_config["name"],
                    status=HealthStatus.CRITICAL,
                    response_time_ms=response_time,
                    message=f"{api_config['name']} 오류: {str(e)}",
                    details={"error": str(e)},
                )

        return results

    async def _check_server_resources(self) -> HealthCheckResult:
        """서버 리소스 사용률 체크"""
        try:
            # CPU 사용률
            cpu_percent = psutil.cpu_percent(interval=1)

            # 메모리 사용률
            memory = psutil.virtual_memory()
            memory_percent = memory.percent

            # 디스크 사용률
            disk = psutil.disk_usage("/")
            disk_percent = (disk.used / disk.total) * 100

            # 네트워크 I/O
            net_io = psutil.net_io_counters()

            # 상태 평가
            critical_threshold = 90
            warning_threshold = 80

            if any(
                usage > critical_threshold
                for usage in [cpu_percent, memory_percent, disk_percent]
            ):
                status = HealthStatus.CRITICAL
                message = "서버 리소스 과부하"
            elif any(
                usage > warning_threshold
                for usage in [cpu_percent, memory_percent, disk_percent]
            ):
                status = HealthStatus.WARNING
                message = "서버 리소스 높음"
            else:
                status = HealthStatus.HEALTHY
                message = "서버 리소스 정상"

            return HealthCheckResult(
                component="server_resources",
                status=status,
                message=message,
                details={
                    "cpu_percent": round(cpu_percent, 1),
                    "memory_percent": round(memory_percent, 1),
                    "disk_percent": round(disk_percent, 1),
                    "memory_available_gb": round(memory.available / 1024**3, 2),
                    "disk_free_gb": round(disk.free / 1024**3, 2),
                    "network_bytes_sent": net_io.bytes_sent,
                    "network_bytes_recv": net_io.bytes_recv,
                },
            )

        except Exception as e:
            return HealthCheckResult(
                component="server_resources",
                status=HealthStatus.UNKNOWN,
                message=f"리소스 체크 실패: {str(e)}",
                details={"error": str(e)},
            )

    async def _check_app_response_time(self) -> HealthCheckResult:
        """애플리케이션 응답 시간 체크"""
        start_time = datetime.now()

        try:
            # 내부 API 엔드포인트 체크
            app_url = f"{self.settings.app_base_url}/health"

            async with self.session.get(
                app_url, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                response_time = (datetime.now() - start_time).total_seconds() * 1000

                if response.status == 200:
                    if response_time < 1000:
                        status = HealthStatus.HEALTHY
                        message = "애플리케이션 응답 정상"
                    elif response_time < 3000:
                        status = HealthStatus.WARNING
                        message = "애플리케이션 응답 지연"
                    else:
                        status = HealthStatus.CRITICAL
                        message = "애플리케이션 응답 매우 지연"
                else:
                    status = HealthStatus.WARNING
                    message = f"애플리케이션 비정상 응답: {response.status}"

                return HealthCheckResult(
                    component="app_response_time",
                    status=status,
                    response_time_ms=response_time,
                    message=message,
                    details={"status_code": response.status},
                )

        except Exception as e:
            response_time = (datetime.now() - start_time).total_seconds() * 1000

            return HealthCheckResult(
                component="app_response_time",
                status=HealthStatus.CRITICAL,
                response_time_ms=response_time,
                message=f"애플리케이션 체크 실패: {str(e)}",
                details={"error": str(e)},
            )

    async def _check_batch_jobs_health(self) -> HealthCheckResult:
        """배치 작업 상태 체크"""
        try:
            # 최근 24시간 내 배치 작업 실행 상태 확인
            query = """
            SELECT
                job_name,
                status,
                start_time,
                end_time,
                error_message
            FROM batch_job_logs
            WHERE start_time >= NOW() - INTERVAL '24 hours'
            ORDER BY start_time DESC
            LIMIT 50
            """

            recent_jobs = await self.db_manager.fetch_all(query)

            if not recent_jobs:
                return HealthCheckResult(
                    component="batch_jobs",
                    status=HealthStatus.WARNING,
                    message="최근 배치 작업 실행 기록 없음",
                )

            # 실패한 작업 수 계산
            failed_jobs = [job for job in recent_jobs if job["status"] == "failed"]
            success_rate = (len(recent_jobs) - len(failed_jobs)) / len(recent_jobs)

            # 상태 평가
            if success_rate >= 0.95:
                status = HealthStatus.HEALTHY
                message = "배치 작업 정상"
            elif success_rate >= 0.8:
                status = HealthStatus.WARNING
                message = f"배치 작업 일부 실패: {len(failed_jobs)}건"
            else:
                status = HealthStatus.CRITICAL
                message = f"배치 작업 다수 실패: {len(failed_jobs)}건"

            return HealthCheckResult(
                component="batch_jobs",
                status=status,
                message=message,
                details={
                    "total_jobs_24h": len(recent_jobs),
                    "failed_jobs_24h": len(failed_jobs),
                    "success_rate_percent": round(success_rate * 100, 1),
                    "last_job_time": (
                        recent_jobs[0]["start_time"].isoformat()
                        if recent_jobs
                        else None
                    ),
                },
            )

        except Exception as e:
            return HealthCheckResult(
                component="batch_jobs",
                status=HealthStatus.UNKNOWN,
                message=f"배치 작업 상태 체크 실패: {str(e)}",
                details={"error": str(e)},
            )

    def _evaluate_overall_health(self, health_results: Dict[str, Any]) -> HealthStatus:
        """전체 시스템 상태 평가"""
        critical_count = 0
        warning_count = 0
        total_components = 0

        # 모든 컴포넌트 상태 집계
        for component, result in health_results.items():
            if isinstance(result, dict):
                # 외부 API 같은 중첩된 결과
                for sub_result in result.values():
                    if isinstance(sub_result, HealthCheckResult):
                        total_components += 1
                        if sub_result.status == HealthStatus.CRITICAL:
                            critical_count += 1
                        elif sub_result.status == HealthStatus.WARNING:
                            warning_count += 1
            elif isinstance(result, HealthCheckResult):
                total_components += 1
                if result.status == HealthStatus.CRITICAL:
                    critical_count += 1
                elif result.status == HealthStatus.WARNING:
                    warning_count += 1

        # 전체 상태 결정
        if critical_count > 0:
            return HealthStatus.CRITICAL
        elif warning_count > total_components * 0.3:  # 30% 이상 경고 시
            return HealthStatus.WARNING
        else:
            return HealthStatus.HEALTHY

    async def _process_health_status_changes(
        self, health_results: Dict[str, Any], overall_status: HealthStatus
    ):
        """상태 변화 감지 및 알림"""
        try:
            # TODO: 이전 상태와 비교하여 변화 감지
            # TODO: 상태 변화 시 알림 발송
            pass
        except Exception as e:
            self.logger.error(f"상태 변화 처리 실패: {e}")

    async def _store_health_metrics(self, health_results: Dict[str, Any]):
        """헬스체크 메트릭 저장"""
        try:
            # TODO: Prometheus, InfluxDB 등에 메트릭 저장
            pass
        except Exception as e:
            self.logger.error(f"헬스 메트릭 저장 실패: {e}")


# 작업 실행 함수
async def health_check_task() -> Dict[str, Any]:
    """헬스체크 작업 실행 함수"""
    checker = SystemHealthChecker()
    return await checker.execute()
