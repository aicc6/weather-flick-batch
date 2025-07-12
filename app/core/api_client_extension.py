"""
API 클라이언트 확장

기존 API 클라이언트에 선택적 저장 기능을 통합하는 확장 모듈입니다.
"""

import time
import logging
from typing import Dict, Any, Optional, Callable, Union
from datetime import datetime
import json

from .selective_storage_manager import StorageRequest, get_storage_manager
from .async_storage_queue import get_async_storage_queue

logger = logging.getLogger(__name__)


class APIClientStorageExtension:
    """API 클라이언트 저장 기능 확장"""
    
    def __init__(self, 
                 provider_name: str,
                 enable_async_storage: bool = True,
                 default_priority: int = 2):
        """
        저장 확장 초기화
        
        Args:
            provider_name: API 제공자 이름 (KMA, KTO, etc.)
            enable_async_storage: 비동기 저장 활성화 여부
            default_priority: 기본 저장 우선순위
        """
        self.provider_name = provider_name
        self.enable_async_storage = enable_async_storage
        self.default_priority = default_priority
        
        self.storage_manager = get_storage_manager()
        if enable_async_storage:
            self.async_queue = get_async_storage_queue()
        else:
            self.async_queue = None
        
        # 저장 통계
        self.stats = {
            "api_calls": 0,
            "storage_attempts": 0,
            "storage_successes": 0,
            "async_queued": 0,
            "sync_stored": 0,
        }
        
        logger.info(f"API 저장 확장 초기화: {provider_name} "
                   f"(비동기: {enable_async_storage})")
    
    def capture_api_call(self,
                        endpoint: str,
                        request_params: Dict[str, Any],
                        response_data: Union[Dict, str, bytes],
                        status_code: int,
                        execution_time_ms: float,
                        request_id: Optional[str] = None,
                        priority: Optional[int] = None,
                        force_sync: bool = False,
                        storage_callback: Optional[Callable] = None) -> Dict[str, Any]:
        """
        API 호출 결과를 캡처하고 저장 처리
        
        Args:
            endpoint: API 엔드포인트
            request_params: 요청 파라미터
            response_data: 응답 데이터
            status_code: HTTP 상태 코드
            execution_time_ms: 실행 시간 (밀리초)
            request_id: 요청 ID (선택)
            priority: 저장 우선순위 (선택)
            force_sync: 동기 저장 강제 (선택)
            storage_callback: 저장 완료 콜백 (선택)
        
        Returns:
            저장 처리 결과
        """
        self.stats["api_calls"] += 1
        
        try:
            # 응답 데이터 정규화
            normalized_response = self._normalize_response_data(response_data)
            response_size = self._calculate_response_size(normalized_response)
            
            # 저장 요청 생성
            storage_request = StorageRequest(
                provider=self.provider_name,
                endpoint=endpoint,
                request_params=request_params,
                response_data=normalized_response,
                response_size_bytes=response_size,
                status_code=status_code,
                execution_time_ms=execution_time_ms,
                created_at=datetime.now(),
                request_id=request_id
            )
            
            # 저장 우선순위 결정
            storage_priority = priority if priority is not None else self.default_priority
            
            # 저장 처리 방식 결정
            if force_sync or not self.enable_async_storage or not self.async_queue:
                return self._handle_sync_storage(storage_request)
            else:
                return self._handle_async_storage(storage_request, storage_priority, storage_callback)
            
        except Exception as e:
            logger.error(f"API 호출 캡처 오류: {e}")
            return {
                "capture_success": False,
                "error": str(e),
                "storage_attempted": False
            }
    
    def _normalize_response_data(self, response_data: Union[Dict, str, bytes]) -> Dict[str, Any]:
        """응답 데이터를 JSON 형태로 정규화"""
        try:
            if isinstance(response_data, dict):
                return response_data
            elif isinstance(response_data, str):
                # JSON 문자열 파싱 시도
                try:
                    return json.loads(response_data)
                except json.JSONDecodeError:
                    return {"raw_text": response_data}
            elif isinstance(response_data, bytes):
                # 바이트 데이터 디코딩 시도
                try:
                    text_data = response_data.decode('utf-8')
                    return json.loads(text_data)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    return {"raw_bytes": response_data.hex()}
            else:
                return {"raw_data": str(response_data)}
        except Exception as e:
            logger.warning(f"응답 데이터 정규화 실패: {e}")
            return {"normalization_error": str(e), "raw_data": str(response_data)}
    
    def _calculate_response_size(self, response_data: Dict[str, Any]) -> int:
        """응답 데이터 크기 계산 (바이트)"""
        try:
            json_str = json.dumps(response_data, ensure_ascii=False)
            return len(json_str.encode('utf-8'))
        except Exception as e:
            logger.warning(f"응답 크기 계산 실패: {e}")
            return 0
    
    def _handle_sync_storage(self, storage_request: StorageRequest) -> Dict[str, Any]:
        """동기 저장 처리"""
        self.stats["storage_attempts"] += 1
        self.stats["sync_stored"] += 1
        
        try:
            result = self.storage_manager.process_storage_request(storage_request)
            
            if result.get("storage_success", False):
                self.stats["storage_successes"] += 1
            
            return {
                "capture_success": True,
                "storage_mode": "sync",
                "storage_attempted": result["should_store"],
                "storage_success": result.get("storage_success", False),
                "decision_reason": result["decision_reason"],
                "process_time_ms": result["process_time_ms"],
                "storage_metadata": result.get("storage_metadata", {})
            }
            
        except Exception as e:
            logger.error(f"동기 저장 처리 오류: {e}")
            return {
                "capture_success": True,
                "storage_mode": "sync",
                "storage_attempted": False,
                "storage_success": False,
                "error": str(e)
            }
    
    def _handle_async_storage(self, 
                            storage_request: StorageRequest,
                            priority: int,
                            callback: Optional[Callable]) -> Dict[str, Any]:
        """비동기 저장 처리"""
        self.stats["storage_attempts"] += 1
        
        try:
            # 저장 여부 미리 결정 (큐 오버헤드 방지)
            should_store, reason, metadata = self.storage_manager.should_store_response(storage_request)
            
            if not should_store:
                return {
                    "capture_success": True,
                    "storage_mode": "async",
                    "storage_attempted": False,
                    "decision_reason": reason,
                    "queue_added": False
                }
            
            # 비동기 큐에 추가
            queue_success = self.async_queue.enqueue(
                storage_request=storage_request,
                priority=priority,
                callback=self._create_storage_callback(callback)
            )
            
            if queue_success:
                self.stats["async_queued"] += 1
                return {
                    "capture_success": True,
                    "storage_mode": "async",
                    "storage_attempted": True,
                    "queue_added": True,
                    "decision_reason": reason,
                    "storage_metadata": metadata,
                    "priority": priority
                }
            else:
                # 큐 추가 실패 시 동기 저장으로 폴백
                logger.warning(f"비동기 큐 추가 실패, 동기 저장으로 폴백: "
                             f"{storage_request.provider}/{storage_request.endpoint}")
                return self._handle_sync_storage(storage_request)
            
        except Exception as e:
            logger.error(f"비동기 저장 처리 오류: {e}")
            return {
                "capture_success": True,
                "storage_mode": "async",
                "storage_attempted": False,
                "queue_added": False,
                "error": str(e)
            }
    
    def _create_storage_callback(self, user_callback: Optional[Callable]) -> Callable:
        """저장 완료 콜백 생성"""
        def storage_callback(result: Dict[str, Any]):
            try:
                # 내부 통계 업데이트
                if result.get("storage_success", False):
                    self.stats["storage_successes"] += 1
                
                # 사용자 콜백 실행
                if user_callback:
                    user_callback(result)
                    
            except Exception as e:
                logger.error(f"저장 콜백 실행 오류: {e}")
        
        return storage_callback
    
    def get_statistics(self) -> Dict[str, Any]:
        """저장 확장 통계 반환"""
        api_calls = self.stats["api_calls"]
        storage_attempts = self.stats["storage_attempts"]
        
        return {
            **self.stats,
            "provider": self.provider_name,
            "async_enabled": self.enable_async_storage,
            "storage_attempt_rate": round(
                storage_attempts / max(api_calls, 1) * 100, 2
            ) if api_calls > 0 else 0,
            "storage_success_rate": round(
                self.stats["storage_successes"] / max(storage_attempts, 1) * 100, 2
            ) if storage_attempts > 0 else 0,
            "async_usage_rate": round(
                self.stats["async_queued"] / max(storage_attempts, 1) * 100, 2
            ) if storage_attempts > 0 else 0
        }
    
    def reset_statistics(self):
        """통계 초기화"""
        for key in self.stats:
            self.stats[key] = 0
        logger.info(f"API 저장 확장 통계 초기화: {self.provider_name}")


class StorageEnabledAPIClient:
    """저장 기능이 통합된 API 클라이언트 기본 클래스"""
    
    def __init__(self, provider_name: str, enable_async_storage: bool = True):
        """
        저장 기능 통합 API 클라이언트 초기화
        
        Args:
            provider_name: API 제공자 이름
            enable_async_storage: 비동기 저장 활성화 여부
        """
        self.provider_name = provider_name
        self.storage_extension = APIClientStorageExtension(
            provider_name=provider_name,
            enable_async_storage=enable_async_storage
        )
        
        logger.info(f"저장 기능 통합 API 클라이언트 초기화: {provider_name}")
    
    def execute_api_call_with_storage(self,
                                    endpoint: str,
                                    api_call_func: Callable,
                                    *args,
                                    request_params: Optional[Dict] = None,
                                    priority: Optional[int] = None,
                                    force_sync: bool = False,
                                    storage_callback: Optional[Callable] = None,
                                    **kwargs) -> tuple:
        """
        저장 기능과 함께 API 호출 실행
        
        Args:
            endpoint: API 엔드포인트
            api_call_func: 실제 API 호출 함수
            *args: API 함수 위치 인수
            request_params: 요청 파라미터 (로깅용)
            priority: 저장 우선순위
            force_sync: 동기 저장 강제
            storage_callback: 저장 완료 콜백
            **kwargs: API 함수 키워드 인수
        
        Returns:
            (api_result, storage_result) 튜플
        """
        start_time = time.time()
        
        try:
            # API 호출 실행
            api_result = api_call_func(*args, **kwargs)
            
            # 실행 시간 계산
            execution_time_ms = (time.time() - start_time) * 1000
            
            # 응답 데이터 추출
            if isinstance(api_result, dict):
                response_data = api_result
                status_code = 200
            elif hasattr(api_result, 'status_code'):
                response_data = getattr(api_result, 'json', lambda: {})()
                status_code = api_result.status_code
            else:
                response_data = {"result": str(api_result)}
                status_code = 200
            
            # 저장 처리
            storage_result = self.storage_extension.capture_api_call(
                endpoint=endpoint,
                request_params=request_params or kwargs,
                response_data=response_data,
                status_code=status_code,
                execution_time_ms=execution_time_ms,
                priority=priority,
                force_sync=force_sync,
                storage_callback=storage_callback
            )
            
            return api_result, storage_result
            
        except Exception as e:
            # 오류 발생 시에도 저장 시도
            execution_time_ms = (time.time() - start_time) * 1000
            
            storage_result = self.storage_extension.capture_api_call(
                endpoint=endpoint,
                request_params=request_params or kwargs,
                response_data={"error": str(e)},
                status_code=500,
                execution_time_ms=execution_time_ms,
                priority=1,  # 오류는 높은 우선순위
                force_sync=force_sync,
                storage_callback=storage_callback
            )
            
            # 원래 예외 재발생
            raise e
    
    def get_storage_statistics(self) -> Dict[str, Any]:
        """저장 통계 조회"""
        return self.storage_extension.get_statistics()


# 유틸리티 함수들

def create_storage_enabled_client(provider_name: str, 
                                enable_async_storage: bool = True) -> StorageEnabledAPIClient:
    """저장 기능 통합 클라이언트 생성"""
    return StorageEnabledAPIClient(provider_name, enable_async_storage)


def wrap_existing_api_function(provider_name: str, 
                              endpoint: str,
                              api_function: Callable,
                              enable_async_storage: bool = True) -> Callable:
    """기존 API 함수에 저장 기능 래핑"""
    
    storage_extension = APIClientStorageExtension(
        provider_name=provider_name,
        enable_async_storage=enable_async_storage
    )
    
    def wrapped_function(*args, **kwargs):
        """래핑된 API 함수"""
        start_time = time.time()
        
        try:
            # 원본 함수 실행
            result = api_function(*args, **kwargs)
            
            # 저장 처리
            execution_time_ms = (time.time() - start_time) * 1000
            
            storage_result = storage_extension.capture_api_call(
                endpoint=endpoint,
                request_params=kwargs,
                response_data=result,
                status_code=200,
                execution_time_ms=execution_time_ms
            )
            
            return result
            
        except Exception as e:
            # 오류 시에도 저장
            execution_time_ms = (time.time() - start_time) * 1000
            
            storage_extension.capture_api_call(
                endpoint=endpoint,
                request_params=kwargs,
                response_data={"error": str(e)},
                status_code=500,
                execution_time_ms=execution_time_ms,
                priority=1
            )
            
            raise e
    
    # 원본 함수의 메타데이터 보존
    wrapped_function.__name__ = f"storage_enabled_{api_function.__name__}"
    wrapped_function.__doc__ = f"Storage-enabled version of {api_function.__name__}"
    wrapped_function._original_function = api_function
    wrapped_function._storage_extension = storage_extension
    
    return wrapped_function