"""
아카이빙 정책 관리 시스템

API 원본 데이터의 아카이빙 및 백업 정책을 관리합니다.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class ArchivalTrigger(Enum):
    """아카이빙 트리거 타입"""
    AGE_BASED = "age_based"  # 나이 기반
    SIZE_BASED = "size_based"  # 크기 기반
    USAGE_BASED = "usage_based"  # 사용빈도 기반
    MANUAL = "manual"  # 수동


class CompressionType(Enum):
    """압축 타입"""
    NONE = "none"
    GZIP = "gzip"
    BZIP2 = "bzip2"
    LZMA = "lzma"


class StorageLocation(Enum):
    """저장 위치"""
    LOCAL_DISK = "local_disk"
    CLOUD_STORAGE = "cloud_storage"
    TAPE_BACKUP = "tape_backup"
    DISTRIBUTED_STORAGE = "distributed_storage"


@dataclass
class ArchivalRule:
    """아카이빙 규칙"""
    rule_id: str
    name: str
    description: str
    trigger: ArchivalTrigger
    condition: Dict[str, Any]  # 트리거 조건
    target_location: StorageLocation
    compression: CompressionType
    retention_days: int  # 보존 기간 (일)
    enabled: bool = True
    priority: int = 0  # 우선순위 (높을수록 먼저 실행)
    created_at: datetime = field(default_factory=datetime.now)
    last_applied: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ArchivalPolicy:
    """아카이빙 정책"""
    policy_id: str
    name: str
    description: str
    api_provider: str  # KTO, KMA, etc.
    endpoint_pattern: Optional[str] = None  # 특정 엔드포인트만 적용
    rules: List[ArchivalRule] = field(default_factory=list)
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


class ArchivalPolicyManager:
    """아카이빙 정책 관리자"""
    
    def __init__(self):
        """정책 관리자 초기화"""
        self.policies: Dict[str, ArchivalPolicy] = {}
        self.default_policies = self._create_default_policies()
        
        # 기본 정책 로드
        for policy in self.default_policies:
            self.policies[policy.policy_id] = policy
        
        logger.info(f"아카이빙 정책 관리자 초기화 완료 (기본 정책 {len(self.default_policies)}개)")
    
    def _create_default_policies(self) -> List[ArchivalPolicy]:
        """기본 아카이빙 정책 생성"""
        policies = []
        
        # 1. KTO API 데이터 정책
        kto_policy = ArchivalPolicy(
            policy_id="kto_standard",
            name="KTO API 표준 아카이빙 정책",
            description="한국관광공사 API 데이터의 표준 아카이빙 정책",
            api_provider="KTO"
        )
        
        # KTO 30일 경과 데이터 압축 아카이빙
        kto_age_rule = ArchivalRule(
            rule_id="kto_age_30d",
            name="KTO 30일 경과 데이터 아카이빙",
            description="30일 이상 경과한 KTO API 원본 데이터를 압축하여 로컬 저장소로 이동",
            trigger=ArchivalTrigger.AGE_BASED,
            condition={"max_age_days": 30},
            target_location=StorageLocation.LOCAL_DISK,
            compression=CompressionType.GZIP,
            retention_days=365,  # 1년 보존
            priority=1
        )
        kto_policy.rules.append(kto_age_rule)
        
        # KTO 대용량 데이터 즉시 압축
        kto_size_rule = ArchivalRule(
            rule_id="kto_size_large",
            name="KTO 대용량 데이터 즉시 압축",
            description="100MB 이상의 KTO API 데이터를 즉시 압축 저장",
            trigger=ArchivalTrigger.SIZE_BASED,
            condition={"max_size_mb": 100},
            target_location=StorageLocation.LOCAL_DISK,
            compression=CompressionType.GZIP,
            retention_days=180,  # 6개월 보존
            priority=2
        )
        kto_policy.rules.append(kto_size_rule)
        
        policies.append(kto_policy)
        
        # 2. KMA API 데이터 정책
        kma_policy = ArchivalPolicy(
            policy_id="kma_standard",
            name="KMA API 표준 아카이빙 정책",
            description="기상청 API 데이터의 표준 아카이빙 정책",
            api_provider="KMA"
        )
        
        # KMA 7일 경과 데이터 압축 (날씨 데이터는 빠른 아카이빙)
        kma_age_rule = ArchivalRule(
            rule_id="kma_age_7d",
            name="KMA 7일 경과 데이터 아카이빙",
            description="7일 이상 경과한 KMA API 원본 데이터를 압축하여 저장",
            trigger=ArchivalTrigger.AGE_BASED,
            condition={"max_age_days": 7},
            target_location=StorageLocation.LOCAL_DISK,
            compression=CompressionType.BZIP2,  # 더 강한 압축
            retention_days=730,  # 2년 보존
            priority=1
        )
        kma_policy.rules.append(kma_age_rule)
        
        # KMA 사용빈도 낮은 데이터 장기 보관
        kma_usage_rule = ArchivalRule(
            rule_id="kma_usage_low",
            name="KMA 저사용 데이터 장기 보관",
            description="30일간 접근되지 않은 KMA 데이터를 장기 보관소로 이동",
            trigger=ArchivalTrigger.USAGE_BASED,
            condition={"max_unused_days": 30},
            target_location=StorageLocation.DISTRIBUTED_STORAGE,
            compression=CompressionType.LZMA,  # 최대 압축
            retention_days=1095,  # 3년 보존
            priority=0
        )
        kma_policy.rules.append(kma_usage_rule)
        
        policies.append(kma_policy)
        
        # 3. 일반 API 데이터 정책
        general_policy = ArchivalPolicy(
            policy_id="general_standard",
            name="일반 API 표준 아카이빙 정책",
            description="기타 API 데이터의 표준 아카이빙 정책",
            api_provider="*"  # 모든 제공자
        )
        
        # 일반 60일 경과 데이터 아카이빙
        general_age_rule = ArchivalRule(
            rule_id="general_age_60d",
            name="일반 60일 경과 데이터 아카이빙",
            description="60일 이상 경과한 API 원본 데이터를 압축하여 저장",
            trigger=ArchivalTrigger.AGE_BASED,
            condition={"max_age_days": 60},
            target_location=StorageLocation.LOCAL_DISK,
            compression=CompressionType.GZIP,
            retention_days=365,
            priority=0
        )
        general_policy.rules.append(general_age_rule)
        
        policies.append(general_policy)
        
        return policies
    
    def add_policy(self, policy: ArchivalPolicy):
        """정책 추가"""
        policy.updated_at = datetime.now()
        self.policies[policy.policy_id] = policy
        logger.info(f"아카이빙 정책 추가: {policy.name}")
    
    def update_policy(self, policy_id: str, updates: Dict[str, Any]):
        """정책 업데이트"""
        if policy_id not in self.policies:
            raise ValueError(f"정책을 찾을 수 없습니다: {policy_id}")
        
        policy = self.policies[policy_id]
        for key, value in updates.items():
            if hasattr(policy, key):
                setattr(policy, key, value)
        
        policy.updated_at = datetime.now()
        logger.info(f"아카이빙 정책 업데이트: {policy.name}")
    
    def remove_policy(self, policy_id: str):
        """정책 제거"""
        if policy_id in self.policies:
            policy_name = self.policies[policy_id].name
            del self.policies[policy_id]
            logger.info(f"아카이빙 정책 제거: {policy_name}")
        else:
            logger.warning(f"제거할 정책을 찾을 수 없습니다: {policy_id}")
    
    def get_applicable_policies(self, api_provider: str, endpoint: str = None) -> List[ArchivalPolicy]:
        """특정 API 제공자와 엔드포인트에 적용 가능한 정책 반환"""
        applicable_policies = []
        
        for policy in self.policies.values():
            if not policy.enabled:
                continue
            
            # API 제공자 확인
            if policy.api_provider != "*" and policy.api_provider != api_provider:
                continue
            
            # 엔드포인트 패턴 확인
            if policy.endpoint_pattern and endpoint:
                import re
                if not re.match(policy.endpoint_pattern, endpoint):
                    continue
            
            applicable_policies.append(policy)
        
        return applicable_policies
    
    def get_archival_rules(self, api_provider: str, endpoint: str = None) -> List[ArchivalRule]:
        """특정 API에 적용할 아카이빙 규칙 반환 (우선순위 순)"""
        applicable_policies = self.get_applicable_policies(api_provider, endpoint)
        
        all_rules = []
        for policy in applicable_policies:
            all_rules.extend(policy.rules)
        
        # 우선순위 순으로 정렬 (높은 우선순위가 먼저)
        return sorted(all_rules, key=lambda x: x.priority, reverse=True)
    
    def evaluate_archival_condition(self, rule: ArchivalRule, data_metadata: Dict[str, Any]) -> bool:
        """아카이빙 조건 평가"""
        if not rule.enabled:
            return False
        
        condition = rule.condition
        
        if rule.trigger == ArchivalTrigger.AGE_BASED:
            max_age_days = condition.get("max_age_days", 30)
            created_at = data_metadata.get("created_at")
            
            if created_at:
                if isinstance(created_at, str):
                    created_at = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                
                age_days = (datetime.now() - created_at).days
                return age_days >= max_age_days
        
        elif rule.trigger == ArchivalTrigger.SIZE_BASED:
            max_size_mb = condition.get("max_size_mb", 100)
            data_size_bytes = data_metadata.get("data_size_bytes", 0)
            
            size_mb = data_size_bytes / (1024 * 1024)
            return size_mb >= max_size_mb
        
        elif rule.trigger == ArchivalTrigger.USAGE_BASED:
            max_unused_days = condition.get("max_unused_days", 30)
            last_accessed = data_metadata.get("last_accessed")
            
            if last_accessed:
                if isinstance(last_accessed, str):
                    last_accessed = datetime.fromisoformat(last_accessed.replace('Z', '+00:00'))
                
                unused_days = (datetime.now() - last_accessed).days
                return unused_days >= max_unused_days
        
        elif rule.trigger == ArchivalTrigger.MANUAL:
            # 수동 트리거는 명시적으로 호출되어야 함
            return data_metadata.get("manual_archive_requested", False)
        
        return False
    
    def get_policy_statistics(self) -> Dict[str, Any]:
        """정책 통계 반환"""
        total_policies = len(self.policies)
        enabled_policies = sum(1 for p in self.policies.values() if p.enabled)
        total_rules = sum(len(p.rules) for p in self.policies.values())
        enabled_rules = sum(
            len([r for r in p.rules if r.enabled]) 
            for p in self.policies.values()
        )
        
        # 제공자별 정책 수
        provider_counts = {}
        for policy in self.policies.values():
            provider = policy.api_provider
            provider_counts[provider] = provider_counts.get(provider, 0) + 1
        
        # 트리거별 규칙 수
        trigger_counts = {}
        for policy in self.policies.values():
            for rule in policy.rules:
                trigger = rule.trigger.value
                trigger_counts[trigger] = trigger_counts.get(trigger, 0) + 1
        
        return {
            "total_policies": total_policies,
            "enabled_policies": enabled_policies,
            "total_rules": total_rules,
            "enabled_rules": enabled_rules,
            "policies_by_provider": provider_counts,
            "rules_by_trigger": trigger_counts,
            "compression_types": list(CompressionType),
            "storage_locations": list(StorageLocation)
        }
    
    def get_all_policies(self) -> Dict[str, ArchivalPolicy]:
        """모든 정책 반환"""
        return self.policies.copy()


# 전역 아카이빙 정책 관리자 인스턴스
_archival_policy_manager: Optional[ArchivalPolicyManager] = None


def get_archival_policy_manager() -> ArchivalPolicyManager:
    """전역 아카이빙 정책 관리자 인스턴스 반환 (싱글톤)"""
    global _archival_policy_manager
    
    if _archival_policy_manager is None:
        _archival_policy_manager = ArchivalPolicyManager()
    
    return _archival_policy_manager


def reset_archival_policy_manager():
    """아카이빙 정책 관리자 인스턴스 재설정 (테스트용)"""
    global _archival_policy_manager
    _archival_policy_manager = None