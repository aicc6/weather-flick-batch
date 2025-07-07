"""
선택적 저장 매니저

API 응답의 저장 여부를 정책에 따라 결정하고 관리하는 핵심 모듈입니다.
"""

import logging
import time
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from dataclasses import dataclass

from .api_storage_policy_engine import get_policy_engine
from .database_manager import DatabaseManager
from .database_manager_extension import extend_database_manager

logger = logging.getLogger(__name__)


@dataclass
class StorageRequest:
    """저장 요청 데이터 구조"""
    provider: str
    endpoint: str
    request_url: str
    request_params: Dict[str, Any]
    response_data: Dict[str, Any]
    response_size_bytes: int
    status_code: int
    execution_time_ms: float
    created_at: datetime
    request_id: Optional[str] = None
    additional_metadata: Optional[Dict[str, Any]] = None


class SelectiveStorageManager:
    """선택적 저장 관리자"""
    
    def __init__(self):
        """저장 관리자 초기화"""
        self.policy_engine = get_policy_engine()
        self.db_manager = extend_database_manager(DatabaseManager().sync_manager)
        
        # 성능 통계
        self.stats = {
            "total_requests": 0,
            "storage_decisions": 0,
            "storage_executions": 0,
            "storage_failures": 0,
            "decision_time_ms": 0,
            "storage_time_ms": 0,
        }
        
        logger.info("선택적 저장 매니저 초기화 완료")
    
    def should_store_response(self, storage_request: StorageRequest) -> Tuple[bool, str, Dict[str, Any]]:
        """
        API 응답 저장 여부 결정
        
        Args:
            storage_request: 저장 요청 데이터
        
        Returns:
            (저장여부, 결정사유, 저장메타데이터)
        """
        start_time = time.time()
        self.stats["total_requests"] += 1
        
        try:
            # 정책 엔진을 통한 저장 결정
            should_store, reason = self.policy_engine.should_store(
                provider=storage_request.provider,
                endpoint=storage_request.endpoint,
                response_size_bytes=storage_request.response_size_bytes,
                status_code=storage_request.status_code,
                additional_context={
                    "execution_time_ms": storage_request.execution_time_ms,
                    "request_id": storage_request.request_id
                }
            )
            
            # 저장 메타데이터 생성
            storage_metadata = {}
            if should_store:
                storage_metadata = self.policy_engine.get_storage_metadata(
                    storage_request.provider,
                    storage_request.endpoint
                )
            
            self.stats["storage_decisions"] += 1
            decision_time = (time.time() - start_time) * 1000
            self.stats["decision_time_ms"] += decision_time
            
            logger.debug(f"저장 결정 완료: {storage_request.provider}/{storage_request.endpoint} "
                        f"-> {should_store} ({decision_time:.2f}ms)")
            
            return should_store, reason, storage_metadata
            
        except Exception as e:
            logger.error(f"저장 결정 오류: {e}")
            # 오류 시 기본적으로 저장하지 않음
            return False, f"결정 오류: {str(e)}", {}
    
    def store_api_response(self, storage_request: StorageRequest, 
                          storage_metadata: Dict[str, Any]) -> bool:
        """
        API 응답 데이터를 데이터베이스에 저장
        
        Args:
            storage_request: 저장 요청 데이터
            storage_metadata: 저장 메타데이터
        
        Returns:
            저장 성공 여부
        """
        start_time = time.time()
        
        try:
            # 저장 데이터 준비 (JSON 직렬화)
            import json
            from datetime import datetime
            
            def json_serial(obj):
                """JSON serialization for datetime objects"""
                if isinstance(obj, datetime):
                    return obj.isoformat()
                raise TypeError(f"Type {type(obj)} not serializable")
            
            insert_data = {
                "api_provider": storage_request.provider,
                "endpoint": storage_request.endpoint,
                "request_url": storage_request.request_url,
                "request_params": json.dumps(storage_request.request_params, default=json_serial),
                "raw_response": json.dumps(storage_request.response_data, default=json_serial),
                "response_status": storage_request.status_code,
                "response_size": storage_request.response_size_bytes,
                "execution_time_ms": storage_request.execution_time_ms,
                "created_at": storage_request.created_at,
                "storage_metadata": json.dumps(storage_metadata, default=json_serial)
            }
            
            # 추가 메타데이터 병합
            if storage_request.additional_metadata:
                combined_metadata = storage_metadata.copy()
                combined_metadata.update(storage_request.additional_metadata)
                insert_data["storage_metadata"] = json.dumps(combined_metadata, default=json_serial)
            
            # 데이터베이스에 저장
            insert_query = """
            INSERT INTO api_raw_data (
                api_provider, endpoint, request_url, request_params, 
                raw_response, response_status, response_size, 
                execution_time_ms, created_at, storage_metadata
            ) VALUES (
                %(api_provider)s, %(endpoint)s, %(request_url)s, %(request_params)s,
                %(raw_response)s, %(response_status)s, %(response_size)s,
                %(execution_time_ms)s, %(created_at)s, %(storage_metadata)s
            )
            """
            
            self.db_manager.execute_query(insert_query, insert_data)
            
            # 성능 통계 업데이트
            storage_time = (time.time() - start_time) * 1000
            self.stats["storage_executions"] += 1
            self.stats["storage_time_ms"] += storage_time
            
            logger.debug(f"API 응답 저장 완료: {storage_request.provider}/{storage_request.endpoint} "
                        f"({storage_time:.2f}ms)")
            
            return True
            
        except Exception as e:
            self.stats["storage_failures"] += 1
            logger.error(f"API 응답 저장 실패: {e}")
            return False
    
    def process_storage_request(self, storage_request: StorageRequest) -> Dict[str, Any]:
        """
        저장 요청 전체 처리 (결정 + 저장)
        
        Args:
            storage_request: 저장 요청 데이터
        
        Returns:
            처리 결과 딕셔너리
        """
        process_start = time.time()
        
        # 1. 저장 여부 결정
        should_store, reason, storage_metadata = self.should_store_response(storage_request)
        
        result = {
            "should_store": should_store,
            "decision_reason": reason,
            "storage_success": False,
            "process_time_ms": 0,
            "storage_metadata": storage_metadata
        }
        
        # 2. 저장 실행 (필요한 경우)
        if should_store:
            storage_success = self.store_api_response(storage_request, storage_metadata)
            result["storage_success"] = storage_success
            
            if not storage_success:
                logger.warning(f"저장 실패하였으나 정책상 저장 대상: "
                             f"{storage_request.provider}/{storage_request.endpoint}")
        
        # 3. 전체 처리 시간 기록
        total_time = (time.time() - process_start) * 1000
        result["process_time_ms"] = total_time
        
        return result
    
    def get_statistics(self) -> Dict[str, Any]:
        """저장 관리자 통계 반환"""
        total_requests = self.stats["total_requests"]
        
        if total_requests == 0:
            return self.stats
        
        avg_decision_time = self.stats["decision_time_ms"] / max(self.stats["storage_decisions"], 1)
        avg_storage_time = self.stats["storage_time_ms"] / max(self.stats["storage_executions"], 1)
        
        return {
            **self.stats,
            "avg_decision_time_ms": round(avg_decision_time, 2),
            "avg_storage_time_ms": round(avg_storage_time, 2),
            "storage_success_rate": round(
                (self.stats["storage_executions"] - self.stats["storage_failures"]) / 
                max(self.stats["storage_executions"], 1) * 100, 2
            ),
            "decision_rate": round(self.stats["storage_decisions"] / total_requests * 100, 2)
        }
    
    def reset_statistics(self):
        """통계 초기화"""
        for key in self.stats:
            self.stats[key] = 0
        logger.info("선택적 저장 매니저 통계가 초기화되었습니다")
    
    def bulk_process_requests(self, storage_requests: List[StorageRequest]) -> List[Dict[str, Any]]:
        """
        여러 저장 요청을 배치로 처리
        
        Args:
            storage_requests: 저장 요청 리스트
        
        Returns:
            처리 결과 리스트
        """
        results = []
        
        logger.info(f"배치 저장 처리 시작: {len(storage_requests)}개 요청")
        
        for request in storage_requests:
            try:
                result = self.process_storage_request(request)
                results.append(result)
            except Exception as e:
                logger.error(f"배치 처리 중 오류: {e}")
                results.append({
                    "should_store": False,
                    "decision_reason": f"처리 오류: {str(e)}",
                    "storage_success": False,
                    "process_time_ms": 0,
                    "storage_metadata": {}
                })
        
        # 배치 처리 통계
        successful_stores = sum(1 for r in results if r["storage_success"])
        total_decisions = sum(1 for r in results if r["should_store"])
        
        logger.info(f"배치 저장 처리 완료: {successful_stores}/{total_decisions} 저장 성공")
        
        return results


# 전역 저장 관리자 인스턴스
_storage_manager: Optional[SelectiveStorageManager] = None


def get_storage_manager() -> SelectiveStorageManager:
    """전역 저장 관리자 인스턴스 반환 (싱글톤)"""
    global _storage_manager
    
    if _storage_manager is None:
        _storage_manager = SelectiveStorageManager()
    
    return _storage_manager


def reset_storage_manager():
    """저장 관리자 인스턴스 재설정 (테스트용)"""
    global _storage_manager
    _storage_manager = None