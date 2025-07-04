"""
배치 작업 기본 클래스

모든 배치 작업이 상속받아야 하는 기본 클래스를 정의합니다.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional, List

from config.constants import JobStatus, JobType


@dataclass
class JobConfig:
    """배치 작업 설정"""

    job_name: str
    job_type: JobType
    schedule_expression: str
    retry_count: int = 3
    timeout_minutes: int = 60
    dependencies: List[str] = field(default_factory=list)
    enabled: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class JobResult:
    """작업 실행 결과"""

    job_name: str
    job_type: JobType
    status: JobStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    processed_records: int = 0
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float:
        """실행 시간 (초)"""
        if self.end_time and self.start_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def is_success(self) -> bool:
        """성공 여부"""
        return self.status == JobStatus.COMPLETED

    @property
    def is_failure(self) -> bool:
        """실패 여부"""
        return self.status == JobStatus.FAILED


class BaseJob(ABC):
    """배치 작업 기본 클래스"""

    def __init__(self, config: JobConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.{config.job_name}")
        self._result = None

    @abstractmethod
    def execute(self) -> JobResult:
        """
        작업 실행 로직

        하위 클래스에서 반드시 구현해야 합니다.

        Returns:
            JobResult: 작업 실행 결과
        """
        pass

    def pre_execute(self) -> bool:
        """
        작업 실행 전 처리

        Returns:
            bool: 계속 실행할지 여부
        """
        self.logger.info(f"작업 실행 전 처리 시작: {self.config.job_name}")
        return True

    def post_execute(self, result: JobResult) -> None:
        """
        작업 실행 후 처리

        Args:
            result: 작업 실행 결과
        """
        self.logger.info(
            f"작업 실행 후 처리: {self.config.job_name}, 상태: {result.status.value}"
        )

    def on_failure(self, error: Exception) -> None:
        """
        작업 실패 시 처리

        Args:
            error: 발생한 예외
        """
        self.logger.error(f"작업 실패 처리: {self.config.job_name}, 오류: {str(error)}")

    def run(self) -> JobResult:
        """
        작업 실행 메인 메서드

        Returns:
            JobResult: 작업 실행 결과
        """
        result = JobResult(
            job_name=self.config.job_name,
            job_type=self.config.job_type,
            status=JobStatus.PENDING,
            start_time=datetime.now(),
        )

        try:
            # 작업 실행 전 처리
            if not self.pre_execute():
                result.status = JobStatus.SKIPPED
                result.end_time = datetime.now()
                self.logger.info(f"작업 건너뜀: {self.config.job_name}")
                return result

            # 작업 상태를 실행 중으로 변경
            result.status = JobStatus.RUNNING
            self.logger.info(f"작업 실행 시작: {self.config.job_name}")

            # 실제 작업 실행
            result = self.execute()
            result.status = JobStatus.COMPLETED
            result.end_time = datetime.now()

            self.logger.info(
                f"작업 실행 완료: {self.config.job_name}, "
                f"처리 레코드: {result.processed_records}, "
                f"소요 시간: {result.duration_seconds:.2f}초"
            )

            # 작업 실행 후 처리
            self.post_execute(result)

            # 성공 알림 전송 (중요한 작업만)
            self._send_success_alert(result)

        except Exception as e:
            result.status = JobStatus.FAILED
            result.end_time = datetime.now()
            result.error_message = str(e)

            self.logger.error(
                f"작업 실행 실패: {self.config.job_name}, "
                f"오류: {str(e)}, "
                f"소요 시간: {result.duration_seconds:.2f}초"
            )

            # 실패 처리
            self.on_failure(e)

            # 알림 전송
            self._send_failure_alert(e, result)

        self._result = result
        return result

    @property
    def last_result(self) -> Optional[JobResult]:
        """마지막 실행 결과"""
        return self._result

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.config.job_name})"

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name='{self.config.job_name}', "
            f"type={self.config.job_type.value}, "
            f"enabled={self.config.enabled})"
        )

    def _send_failure_alert(self, exception: Exception, result: JobResult):
        """작업 실패 알림 전송"""
        try:
            from utils.notification import get_notification_manager, AlertLevel

            notification_manager = get_notification_manager()

            metadata = {
                "job_type": self.config.job_type.value
                if hasattr(self.config.job_type, "value")
                else str(self.config.job_type),
                "duration_seconds": result.duration_seconds,
                "processed_records": result.processed_records,
            }

            if result.metadata:
                metadata.update(result.metadata)

            notification_manager.send_job_failure_alert(
                job_name=self.config.job_name,
                error_message=str(exception),
                duration=result.duration_seconds,
                metadata=metadata,
            )
        except Exception as e:
            self.logger.warning(f"알림 전송 실패: {str(e)}")

    def _send_success_alert(self, result: JobResult):
        """작업 성공 알림 전송 (중요한 작업만)"""
        try:
            from utils.notification import get_notification_manager, AlertLevel

            # 백업, 동기화 등 중요한 작업만 성공 알림
            important_jobs = ["database_backup", "tourism_sync", "data_quality_check"]

            if self.config.job_name in important_jobs:
                notification_manager = get_notification_manager()

                title = f"배치 작업 완료: {self.config.job_name}"
                message = f"배치 작업 '{self.config.job_name}'이 성공적으로 완료되었습니다.\n\n"
                message += f"처리 레코드: {result.processed_records}건\n"
                message += f"소요 시간: {result.duration_seconds:.2f}초"

                metadata = {
                    "job_name": self.config.job_name,
                    "job_type": self.config.job_type.value
                    if hasattr(self.config.job_type, "value")
                    else str(self.config.job_type),
                    "processed_records": result.processed_records,
                    "duration_seconds": result.duration_seconds,
                }

                if result.metadata:
                    metadata.update(result.metadata)

                notification_manager.send_alert(
                    title=title,
                    message=message,
                    level=AlertLevel.INFO,
                    source="batch_system",
                    metadata=metadata,
                )
        except Exception as e:
            self.logger.warning(f"성공 알림 전송 실패: {str(e)}")
