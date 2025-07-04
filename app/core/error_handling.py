"""
통합 오류 처리 프레임워크

프로젝트 전체에서 일관된 오류 처리와 로깅을 제공합니다.
"""

import sys
import traceback
import functools
import time
from datetime import datetime
from enum import Enum
from typing import Dict, Any, Optional, Callable, Type
from dataclasses import dataclass, field


class ErrorSeverity(Enum):
    """오류 심각도 수준"""

    CRITICAL = "critical"  # 시스템 중단 수준
    HIGH = "high"  # 주요 기능 영향
    MEDIUM = "medium"  # 일부 기능 영향
    LOW = "low"  # 경미한 문제
    INFO = "info"  # 정보성 메시지


class ErrorCategory(Enum):
    """오류 카테고리"""

    API_ERROR = "api"  # API 호출 관련
    DATABASE_ERROR = "database"  # 데이터베이스 관련
    NETWORK_ERROR = "network"  # 네트워크 관련
    VALIDATION_ERROR = "validation"  # 데이터 검증 관련
    CONFIGURATION_ERROR = "config"  # 설정 관련
    BUSINESS_LOGIC_ERROR = "business"  # 비즈니스 로직 관련
    SYSTEM_ERROR = "system"  # 시스템 관련
    EXTERNAL_SERVICE_ERROR = "external"  # 외부 서비스 관련


@dataclass
class ErrorContext:
    """오류 컨텍스트 정보"""

    error_id: str = ""
    operation: str = ""
    module: str = ""
    function: str = ""
    parameters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    user_message: str = ""
    technical_message: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환 (JSON 직렬화용)"""
        return {
            "error_id": self.error_id,
            "operation": self.operation,
            "module": self.module,
            "function": self.function,
            "parameters": self._sanitize_parameters(self.parameters),
            "metadata": self.metadata,
            "timestamp": self.timestamp.isoformat(),
            "user_message": self.user_message,
            "technical_message": self.technical_message,
        }

    def _sanitize_parameters(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """민감 정보 제거"""
        sanitized = {}
        sensitive_keys = {
            "api_key",
            "password",
            "token",
            "secret",
            "auth",
            "key",
            "serviceKey",
            "apiKey",
            "authToken",
            "accessToken",
        }

        for key, value in params.items():
            if any(sensitive_key in key.lower() for sensitive_key in sensitive_keys):
                if isinstance(value, str) and len(value) > 0:
                    sanitized[key] = (
                        f"{value[:3]}***{value[-3:]}" if len(value) > 6 else "***"
                    )
                else:
                    sanitized[key] = "***"
            else:
                sanitized[key] = value

        return sanitized


class WeatherFlickError(Exception):
    """프로젝트 기본 예외 클래스"""

    def __init__(
        self,
        message: str,
        error_code: str = "WF_UNKNOWN",
        category: ErrorCategory = ErrorCategory.SYSTEM_ERROR,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        context: Optional[ErrorContext] = None,
        cause: Optional[Exception] = None,
    ):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.category = category
        self.severity = severity
        self.context = context or ErrorContext()
        self.cause = cause

        # 고유 오류 ID 생성
        if not self.context.error_id:
            self.context.error_id = self._generate_error_id()

    def _generate_error_id(self) -> str:
        """고유 오류 ID 생성"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"{self.error_code}_{timestamp}_{id(self) % 10000:04d}"

    def to_dict(self) -> Dict[str, Any]:
        """딕셔너리로 변환"""
        return {
            "error_id": self.context.error_id,
            "error_code": self.error_code,
            "message": self.message,
            "category": self.category.value,
            "severity": self.severity.value,
            "context": self.context.to_dict(),
            "cause": str(self.cause) if self.cause else None,
            "traceback": traceback.format_exc() if sys.exc_info()[0] else None,
        }


# ========== 특화된 예외 클래스들 ==========


class APIError(WeatherFlickError):
    """API 관련 오류"""

    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        endpoint: str = "",
        **kwargs,
    ):
        super().__init__(
            message,
            error_code=f"WF_API_{status_code or 'ERROR'}",
            category=ErrorCategory.API_ERROR,
            **kwargs,
        )
        self.status_code = status_code
        self.endpoint = endpoint
        if self.context:
            self.context.metadata.update(
                {"status_code": status_code, "endpoint": endpoint}
            )


class DatabaseError(WeatherFlickError):
    """데이터베이스 관련 오류"""

    def __init__(self, message: str, table_name: str = "", query: str = "", **kwargs):
        super().__init__(
            message,
            error_code="WF_DB_ERROR",
            category=ErrorCategory.DATABASE_ERROR,
            **kwargs,
        )
        self.table_name = table_name
        self.query = query
        if self.context:
            self.context.metadata.update(
                {
                    "table_name": table_name,
                    "query": query[:200] + "..." if len(query) > 200 else query,
                }
            )


class NetworkError(WeatherFlickError):
    """네트워크 관련 오류"""

    def __init__(
        self, message: str, url: str = "", timeout: Optional[float] = None, **kwargs
    ):
        super().__init__(
            message,
            error_code="WF_NETWORK_ERROR",
            category=ErrorCategory.NETWORK_ERROR,
            **kwargs,
        )
        self.url = url
        self.timeout = timeout
        if self.context:
            self.context.metadata.update({"url": url, "timeout": timeout})


class ValidationError(WeatherFlickError):
    """데이터 검증 관련 오류"""

    def __init__(
        self, message: str, field_name: str = "", field_value: Any = None, **kwargs
    ):
        super().__init__(
            message,
            error_code="WF_VALIDATION_ERROR",
            category=ErrorCategory.VALIDATION_ERROR,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )
        self.field_name = field_name
        self.field_value = field_value
        if self.context:
            self.context.metadata.update(
                {
                    "field_name": field_name,
                    "field_value": str(field_value)[:100] if field_value else None,
                }
            )


class ConfigurationError(WeatherFlickError):
    """설정 관련 오류"""

    def __init__(self, message: str, config_key: str = "", **kwargs):
        super().__init__(
            message,
            error_code="WF_CONFIG_ERROR",
            category=ErrorCategory.CONFIGURATION_ERROR,
            severity=ErrorSeverity.HIGH,
            **kwargs,
        )
        self.config_key = config_key
        if self.context:
            self.context.metadata.update({"config_key": config_key})


class BusinessLogicError(WeatherFlickError):
    """비즈니스 로직 관련 오류"""

    def __init__(self, message: str, operation: str = "", **kwargs):
        super().__init__(
            message,
            error_code="WF_BUSINESS_ERROR",
            category=ErrorCategory.BUSINESS_LOGIC_ERROR,
            severity=ErrorSeverity.MEDIUM,
            **kwargs,
        )
        self.operation = operation
        if self.context:
            self.context.operation = operation


class ExternalServiceError(WeatherFlickError):
    """외부 서비스 관련 오류"""

    def __init__(self, message: str, service_name: str = "", **kwargs):
        super().__init__(
            message,
            error_code=f"WF_EXT_{service_name.upper()}_ERROR"
            if service_name
            else "WF_EXT_ERROR",
            category=ErrorCategory.EXTERNAL_SERVICE_ERROR,
            **kwargs,
        )
        self.service_name = service_name
        if self.context:
            self.context.metadata.update({"service_name": service_name})


# ========== 오류 처리 유틸리티 ==========


class RetryConfig:
    """재시도 설정"""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        retry_on: tuple = (Exception,),
        stop_on: tuple = (),
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.retry_on = retry_on
        self.stop_on = stop_on

    def calculate_delay(self, attempt: int) -> float:
        """재시도 지연 시간 계산 (지수 백오프)"""
        delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        return min(delay, self.max_delay)


def with_retry(
    retry_config: Optional[RetryConfig] = None, context: Optional[ErrorContext] = None
):
    """재시도 데코레이터"""
    if retry_config is None:
        retry_config = RetryConfig()

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None

            for attempt in range(1, retry_config.max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except retry_config.stop_on as e:
                    # 재시도 중단 예외
                    if context:
                        context.metadata.update(
                            {"stop_exception": str(e), "attempt": attempt}
                        )
                    raise
                except retry_config.retry_on as e:
                    last_exception = e

                    if attempt == retry_config.max_attempts:
                        # 마지막 시도에서도 실패
                        if context:
                            context.metadata.update(
                                {
                                    "final_attempt": attempt,
                                    "total_attempts": retry_config.max_attempts,
                                }
                            )
                        raise

                    # 재시도 대기
                    delay = retry_config.calculate_delay(attempt)
                    if context:
                        context.metadata.update(
                            {"retry_attempt": attempt, "retry_delay": delay}
                        )

                    time.sleep(delay)

            # 이 코드에 도달할 일은 없지만 안전장치
            if last_exception:
                raise last_exception

        return wrapper

    return decorator


def error_handler(
    error_mapping: Optional[Dict[Type[Exception], Type[WeatherFlickError]]] = None,
    default_severity: ErrorSeverity = ErrorSeverity.MEDIUM,
    context: Optional[ErrorContext] = None,
    reraise: bool = True,
):
    """통합 오류 처리 데코레이터"""
    if error_mapping is None:
        error_mapping = {}

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            error_context = context or ErrorContext()
            error_context.function = func.__name__
            error_context.module = func.__module__

            try:
                return func(*args, **kwargs)
            except WeatherFlickError:
                # 이미 처리된 WeatherFlick 오류는 그대로 전파
                raise
            except Exception as e:
                # 다른 예외를 WeatherFlick 오류로 변환
                error_context.technical_message = str(e)

                # 매핑된 예외 타입 찾기
                mapped_error_type = None
                for source_type, target_type in error_mapping.items():
                    if isinstance(e, source_type):
                        mapped_error_type = target_type
                        break

                if mapped_error_type:
                    weather_error = mapped_error_type(
                        f"{func.__name__} 실행 중 오류 발생: {str(e)}",
                        severity=default_severity,
                        context=error_context,
                        cause=e,
                    )
                else:
                    weather_error = WeatherFlickError(
                        f"{func.__name__} 실행 중 예상치 못한 오류 발생: {str(e)}",
                        severity=default_severity,
                        context=error_context,
                        cause=e,
                    )

                if reraise:
                    raise weather_error from e
                else:
                    return None

        return wrapper

    return decorator


# ========== 오류 코드 상수 ==========


class ErrorCodes:
    """표준 오류 코드"""

    # API 관련
    API_KEY_MISSING = "WF_API_001"
    API_KEY_INVALID = "WF_API_002"
    API_RATE_LIMIT = "WF_API_003"
    API_TIMEOUT = "WF_API_004"
    API_RESPONSE_INVALID = "WF_API_005"

    # 데이터베이스 관련
    DB_CONNECTION_FAILED = "WF_DB_001"
    DB_QUERY_FAILED = "WF_DB_002"
    DB_TRANSACTION_FAILED = "WF_DB_003"
    DB_TABLE_NOT_FOUND = "WF_DB_004"
    DB_CONSTRAINT_VIOLATION = "WF_DB_005"

    # 네트워크 관련
    NETWORK_TIMEOUT = "WF_NET_001"
    NETWORK_CONNECTION_FAILED = "WF_NET_002"
    NETWORK_DNS_FAILED = "WF_NET_003"

    # 데이터 검증 관련
    VALIDATION_REQUIRED_FIELD = "WF_VAL_001"
    VALIDATION_INVALID_FORMAT = "WF_VAL_002"
    VALIDATION_OUT_OF_RANGE = "WF_VAL_003"

    # 설정 관련
    CONFIG_FILE_NOT_FOUND = "WF_CFG_001"
    CONFIG_INVALID_FORMAT = "WF_CFG_002"
    CONFIG_REQUIRED_MISSING = "WF_CFG_003"

    # 비즈니스 로직 관련
    BUSINESS_INVALID_OPERATION = "WF_BIZ_001"
    BUSINESS_STATE_CONFLICT = "WF_BIZ_002"
    BUSINESS_RESOURCE_NOT_FOUND = "WF_BIZ_003"


# ========== 편의 함수들 ==========


def create_api_error(
    message: str, status_code: Optional[int] = None, **kwargs
) -> APIError:
    """API 오류 생성 편의 함수"""
    return APIError(message, status_code=status_code, **kwargs)


def create_database_error(
    message: str, table_name: str = "", **kwargs
) -> DatabaseError:
    """데이터베이스 오류 생성 편의 함수"""
    return DatabaseError(message, table_name=table_name, **kwargs)


def create_validation_error(
    message: str, field_name: str = "", **kwargs
) -> ValidationError:
    """검증 오류 생성 편의 함수"""
    return ValidationError(message, field_name=field_name, **kwargs)


def handle_exception(
    e: Exception,
    context: Optional[ErrorContext] = None,
    severity: ErrorSeverity = ErrorSeverity.MEDIUM,
) -> WeatherFlickError:
    """일반 예외를 WeatherFlick 오류로 변환"""
    if isinstance(e, WeatherFlickError):
        return e

    error_context = context or ErrorContext()
    error_context.technical_message = str(e)

    # 예외 타입별 매핑
    if isinstance(e, (ConnectionError, TimeoutError)):
        return NetworkError(str(e), context=error_context, cause=e)
    elif isinstance(e, ValueError):
        return ValidationError(str(e), context=error_context, cause=e)
    elif isinstance(e, PermissionError):
        return ConfigurationError(str(e), context=error_context, cause=e)
    else:
        return WeatherFlickError(
            str(e), context=error_context, severity=severity, cause=e
        )
