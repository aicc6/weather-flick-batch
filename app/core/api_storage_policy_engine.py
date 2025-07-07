"""
API 저장 정책 엔진

API 응답의 저장 여부를 결정하고 관련 정책을 관리하는 핵심 엔진입니다.
"""

import os
import json
import logging
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
from dataclasses import asdict

from config.api_storage_policy import (
    API_STORAGE_POLICIES,
    GLOBAL_STORAGE_CONFIG,
    ProviderConfig,
    EndpointConfig,
    StoragePolicy,
    should_store_response,
    get_ttl_days,
    get_priority
)

logger = logging.getLogger(__name__)


class APIStoragePolicyEngine:
    """API 저장 정책을 관리하고 저장 결정을 내리는 엔진"""
    
    def __init__(self, config_override: Optional[Dict[str, Any]] = None):
        """
        정책 엔진 초기화
        
        Args:
            config_override: 설정 오버라이드 (테스트용)
        """
        self.policies = API_STORAGE_POLICIES.copy()
        self.global_config = GLOBAL_STORAGE_CONFIG.copy()
        
        if config_override:
            self.global_config.update(config_override)
        
        # 런타임 통계
        self.stats = {
            "decisions_made": 0,
            "storage_approved": 0,
            "storage_rejected": 0,
            "errors_stored": 0,
            "size_rejected": 0,
            "policy_disabled": 0,
        }
        
        logger.info("API 저장 정책 엔진 초기화 완료")
    
    def should_store(self, provider: str, endpoint: str, response_size_bytes: int = 0, 
                    status_code: int = 200, additional_context: Optional[Dict] = None) -> Tuple[bool, str]:
        """
        API 응답 저장 여부 결정
        
        Args:
            provider: API 제공자 (KMA, KTO, etc.)
            endpoint: API 엔드포인트
            response_size_bytes: 응답 크기 (바이트)
            status_code: HTTP 상태 코드
            additional_context: 추가 컨텍스트 정보
        
        Returns:
            (저장여부, 결정사유)
        """
        self.stats["decisions_made"] += 1
        
        try:
            # 전역 저장 비활성화 체크
            if not self.global_config["storage_enabled"]:
                self.stats["policy_disabled"] += 1
                return False, "전역 저장 기능이 비활성화됨"
            
            # 제공자 설정 조회
            provider_config = self._get_provider_config(provider)
            if not provider_config or not provider_config.enabled:
                self.stats["policy_disabled"] += 1
                return False, f"제공자 '{provider}' 저장 정책이 비활성화됨"
            
            # 응답 크기 계산 (MB)
            response_size_mb = response_size_bytes / (1024 * 1024)
            
            # 오류 응답 처리
            if status_code >= 400:
                if self._should_store_error(provider, endpoint, provider_config):
                    self.stats["errors_stored"] += 1
                    self.stats["storage_approved"] += 1
                    return True, f"오류 응답 저장 정책에 따라 저장 (상태코드: {status_code})"
                else:
                    self.stats["storage_rejected"] += 1
                    return False, f"오류 응답 저장 정책에 따라 저장 안함 (상태코드: {status_code})"
            
            # 엔드포인트별 세부 검사
            endpoint_config = self._get_endpoint_config(provider, endpoint)
            
            if endpoint_config:
                # 엔드포인트 레벨 저장 비활성화
                if not endpoint_config.store:
                    self.stats["storage_rejected"] += 1
                    return False, f"엔드포인트 '{endpoint}' 저장 비활성화"
                
                # 엔드포인트 레벨 크기 제한
                if (endpoint_config.max_response_size_mb and 
                    response_size_mb > endpoint_config.max_response_size_mb):
                    self.stats["size_rejected"] += 1
                    return False, f"응답 크기 초과 (현재: {response_size_mb:.2f}MB, 제한: {endpoint_config.max_response_size_mb}MB)"
            
            # 제공자 레벨 크기 제한
            elif response_size_mb > provider_config.max_response_size_mb:
                self.stats["size_rejected"] += 1
                return False, f"응답 크기 초과 (현재: {response_size_mb:.2f}MB, 제한: {provider_config.max_response_size_mb}MB)"
            
            # 제공자 기본 정책 확인
            if provider_config.default_policy == StoragePolicy.NEVER:
                self.stats["storage_rejected"] += 1
                return False, "제공자 기본 정책이 저장 안함으로 설정됨"
            
            # 긴급 정리 모드 확인
            if self._is_emergency_cleanup_needed():
                priority = get_priority(provider, endpoint)
                if priority > 1:  # 높은 우선순위(1)만 저장
                    self.stats["storage_rejected"] += 1
                    return False, f"긴급 정리 모드 - 낮은 우선순위 ({priority}) 저장 중단"
            
            # 모든 조건 통과 - 저장 승인
            self.stats["storage_approved"] += 1
            return True, "저장 정책 조건을 모두 만족함"
            
        except Exception as e:
            logger.error(f"저장 정책 결정 중 오류: {e}")
            self.stats["storage_rejected"] += 1
            return False, f"정책 결정 오류: {str(e)}"
    
    def get_storage_metadata(self, provider: str, endpoint: str) -> Dict[str, Any]:
        """
        저장 메타데이터 생성
        
        Args:
            provider: API 제공자
            endpoint: API 엔드포인트
        
        Returns:
            저장 메타데이터 딕셔너리
        """
        try:
            ttl_days = get_ttl_days(provider, endpoint)
            priority = get_priority(provider, endpoint)
            
            # TTL 만료 시점 계산
            expires_at = datetime.now() + timedelta(days=ttl_days)
            
            # 압축 설정 확인
            compression_enabled = self._should_compress(provider, endpoint)
            
            return {
                "ttl_days": ttl_days,
                "priority": priority,
                "expires_at": expires_at,
                "compression_enabled": compression_enabled,
                "policy_version": "1.0",
                "created_by": "api_storage_policy_engine",
                "created_at": datetime.now()
            }
            
        except Exception as e:
            logger.error(f"저장 메타데이터 생성 실패: {e}")
            return {}
    
    def get_cleanup_candidates(self, target_size_reduction_mb: float = 0) -> List[Dict[str, Any]]:
        """
        정리 대상 후보 조회
        
        Args:
            target_size_reduction_mb: 목표 용량 감소량 (MB)
        
        Returns:
            정리 대상 목록 (우선순위 순)
        """
        # 실제 구현에서는 데이터베이스 쿼리를 통해 정리 대상을 선별
        # 여기서는 정책 기반 우선순위 로직만 제공
        
        cleanup_criteria = [
            # 1순위: 만료된 데이터
            {"condition": "expires_at < NOW()", "priority": 1},
            
            # 2순위: 낮은 우선순위 + 오래된 데이터
            {"condition": "priority >= 3 AND created_at < NOW() - INTERVAL '30 days'", "priority": 2},
            
            # 3순위: 대용량 응답 + 중간 우선순위
            {"condition": "response_size > 10*1024*1024 AND priority >= 2", "priority": 3},
            
            # 4순위: 긴급 모드 - 중간 우선순위 데이터
            {"condition": "priority >= 2", "priority": 4, "emergency_only": True},
        ]
        
        return cleanup_criteria
    
    def _get_provider_config(self, provider: str) -> Optional[ProviderConfig]:
        """제공자 설정 조회"""
        return self.policies.get(provider.upper())
    
    def _get_endpoint_config(self, provider: str, endpoint: str) -> Optional[EndpointConfig]:
        """엔드포인트 설정 조회"""
        provider_config = self._get_provider_config(provider)
        if not provider_config:
            return None
        
        return provider_config.endpoints.get(endpoint)
    
    def _should_store_error(self, provider: str, endpoint: str, provider_config: ProviderConfig) -> bool:
        """오류 응답 저장 여부 결정"""
        endpoint_config = self._get_endpoint_config(provider, endpoint)
        
        if endpoint_config:
            return endpoint_config.store_on_error
        
        return provider_config.store_errors
    
    def _should_compress(self, provider: str, endpoint: str) -> bool:
        """압축 적용 여부 결정"""
        provider_config = self._get_provider_config(provider)
        if not provider_config:
            return False
        
        endpoint_config = self._get_endpoint_config(provider, endpoint)
        if endpoint_config:
            return endpoint_config.compression
        
        return provider_config.compression_enabled
    
    def _is_emergency_cleanup_needed(self) -> bool:
        """긴급 정리 모드 필요 여부 확인"""
        # 실제 구현에서는 디스크 사용률을 확인
        # 여기서는 설정값만 반환
        return False  # 실제로는 시스템 상태를 확인
    
    def get_statistics(self) -> Dict[str, Any]:
        """정책 엔진 통계 반환"""
        total_decisions = self.stats["decisions_made"]
        
        if total_decisions == 0:
            return self.stats
        
        return {
            **self.stats,
            "approval_rate": round(self.stats["storage_approved"] / total_decisions * 100, 2),
            "rejection_rate": round(self.stats["storage_rejected"] / total_decisions * 100, 2),
            "error_storage_rate": round(self.stats["errors_stored"] / total_decisions * 100, 2),
        }
    
    def reset_statistics(self):
        """통계 초기화"""
        for key in self.stats:
            self.stats[key] = 0
        logger.info("정책 엔진 통계가 초기화되었습니다")
    
    def reload_policies(self, new_policies: Optional[Dict[str, ProviderConfig]] = None):
        """정책 재로딩"""
        try:
            if new_policies:
                self.policies = new_policies
            else:
                # 설정 파일에서 다시 로드
                from config.api_storage_policy import API_STORAGE_POLICIES
                self.policies = API_STORAGE_POLICIES.copy()
            
            logger.info("API 저장 정책이 재로딩되었습니다")
            
        except Exception as e:
            logger.error(f"정책 재로딩 실패: {e}")
            raise
    
    def validate_configuration(self) -> Tuple[bool, List[str]]:
        """설정 유효성 검증"""
        errors = []
        
        try:
            # 전역 설정 검증
            if not isinstance(self.global_config.get("storage_enabled"), bool):
                errors.append("global_config.storage_enabled는 boolean이어야 합니다")
            
            # 제공자별 설정 검증
            for provider_name, config in self.policies.items():
                if not isinstance(config, ProviderConfig):
                    errors.append(f"제공자 '{provider_name}' 설정이 ProviderConfig 타입이 아닙니다")
                    continue
                
                # TTL 검증
                if config.default_ttl_days <= 0:
                    errors.append(f"제공자 '{provider_name}'의 TTL이 0 이하입니다")
                
                # 크기 제한 검증
                if config.max_response_size_mb <= 0:
                    errors.append(f"제공자 '{provider_name}'의 최대 응답 크기가 0 이하입니다")
                
                # 엔드포인트 설정 검증
                for endpoint_name, endpoint_config in config.endpoints.items():
                    if endpoint_config.ttl_days <= 0:
                        errors.append(f"제공자 '{provider_name}', 엔드포인트 '{endpoint_name}'의 TTL이 0 이하입니다")
            
            is_valid = len(errors) == 0
            
            if is_valid:
                logger.info("API 저장 정책 설정 검증 통과")
            else:
                logger.error(f"API 저장 정책 설정 검증 실패: {len(errors)}개 오류 발견")
            
            return is_valid, errors
            
        except Exception as e:
            errors.append(f"설정 검증 중 예외 발생: {str(e)}")
            return False, errors


# 전역 정책 엔진 인스턴스
_policy_engine: Optional[APIStoragePolicyEngine] = None


def get_policy_engine() -> APIStoragePolicyEngine:
    """전역 정책 엔진 인스턴스 반환 (싱글톤)"""
    global _policy_engine
    
    if _policy_engine is None:
        _policy_engine = APIStoragePolicyEngine()
    
    return _policy_engine


def reset_policy_engine():
    """정책 엔진 인스턴스 재설정 (테스트용)"""
    global _policy_engine
    _policy_engine = None