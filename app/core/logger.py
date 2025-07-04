"""
로깅 설정 및 관리 모듈

애플리케이션 전체의 로깅을 중앙에서 관리합니다.
"""

import logging
import logging.handlers
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.settings import get_logging_config


class JobLogger:
    """배치 작업용 로거 클래스"""

    def __init__(self, log_dir: str = "logs"):
        self.config = get_logging_config()
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(exist_ok=True)
        self._setup_logging()

    def _setup_logging(self) -> None:
        """로깅 설정"""
        # 기본 로거 설정
        logging.basicConfig(
            level=getattr(logging, self.config.level),
            format=self.config.format,
            handlers=[],
        )

        # 루트 로거 가져오기
        root_logger = logging.getLogger()
        root_logger.handlers.clear()

        # 콘솔 핸들러
        console_handler = logging.StreamHandler()
        console_handler.setLevel(getattr(logging, self.config.level))
        console_formatter = logging.Formatter(self.config.format)
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)

        # 파일 핸들러 (일반 로그)
        log_file = (
            self.log_dir
            / f"{self.config.file_prefix}_{datetime.now().strftime('%Y%m%d')}.log"
        )
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.config.max_bytes,
            backupCount=self.config.backup_count,
            encoding="utf-8",
        )
        file_handler.setLevel(getattr(logging, self.config.level))
        file_formatter = logging.Formatter(self.config.format)
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

        # 에러 로그 파일 핸들러
        error_log_file = (
            self.log_dir
            / f"{self.config.file_prefix}_error_{datetime.now().strftime('%Y%m%d')}.log"
        )
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=self.config.max_bytes,
            backupCount=self.config.backup_count,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_formatter = logging.Formatter(self.config.format)
        error_handler.setFormatter(error_formatter)
        root_logger.addHandler(error_handler)

    def get_logger(self, name: str) -> logging.Logger:
        """특정 이름의 로거 반환"""
        return logging.getLogger(name)

    def log_job_start(self, job_name: str, job_type: str) -> None:
        """작업 시작 로그"""
        logger = self.get_logger(f"job.{job_name}")
        logger.info(f"배치 작업 시작 - 작업명: {job_name}, 타입: {job_type}")

    def log_job_complete(
        self, job_name: str, processed_records: int, duration: float
    ) -> None:
        """작업 완료 로그"""
        logger = self.get_logger(f"job.{job_name}")
        logger.info(
            f"배치 작업 완료 - 작업명: {job_name}, "
            f"처리 레코드: {processed_records}, 소요시간: {duration:.2f}초"
        )

    def log_job_failure(
        self, job_name: str, error_message: str, duration: float
    ) -> None:
        """작업 실패 로그"""
        logger = self.get_logger(f"job.{job_name}")
        logger.error(
            f"배치 작업 실패 - 작업명: {job_name}, "
            f"오류: {error_message}, 소요시간: {duration:.2f}초"
        )

    def log_api_call(
        self,
        api_name: str,
        endpoint: str,
        status_code: Optional[int] = None,
        duration: Optional[float] = None,
    ) -> None:
        """API 호출 로그"""
        logger = self.get_logger("api")
        message = f"API 호출 - {api_name}: {endpoint}"
        if status_code:
            message += f", 상태코드: {status_code}"
        if duration:
            message += f", 응답시간: {duration:.3f}초"
        logger.info(message)

    def log_data_quality(
        self,
        table_name: str,
        record_count: int,
        quality_score: float,
        issues: list = None,
    ) -> None:
        """데이터 품질 로그"""
        logger = self.get_logger("data_quality")
        message = f"데이터 품질 검사 - 테이블: {table_name}, 레코드: {record_count}, 품질점수: {quality_score:.2f}"
        if issues:
            message += f", 이슈: {', '.join(issues)}"
        logger.info(message)

    def log_performance_metric(
        self, metric_name: str, value: float, unit: str = ""
    ) -> None:
        """성능 메트릭 로그"""
        logger = self.get_logger("performance")
        logger.info(f"성능 메트릭 - {metric_name}: {value} {unit}")


# 전역 로거 인스턴스
_logger_instance = None


def get_logger_instance() -> JobLogger:
    """전역 로거 인스턴스 반환"""
    global _logger_instance
    if _logger_instance is None:
        _logger_instance = JobLogger()
    return _logger_instance


def get_logger(name: str) -> logging.Logger:
    """특정 이름의 로거 반환 (편의 함수)"""
    return get_logger_instance().get_logger(name)
