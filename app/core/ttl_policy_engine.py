"""
TTL (Time To Live) 정책 엔진

저장된 API 데이터의 생명주기를 관리하고 자동 정리 정책을 수행하는 엔진입니다.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass
from enum import Enum

from .database_manager import DatabaseManager
from .database_manager_extension import extend_database_manager

logger = logging.getLogger(__name__)


class CleanupPriority(Enum):
    """정리 우선순위"""
    EXPIRED = 1          # 만료된 데이터
    LOW_PRIORITY = 2     # 낮은 우선순위 + 오래된 데이터
    LARGE_SIZE = 3       # 대용량 + 중간 우선순위
    EMERGENCY = 4        # 긴급 모드 - 중간 우선순위


@dataclass
class CleanupCandidate:
    """정리 대상 후보"""
    id: int
    api_provider: str
    endpoint: str
    created_at: datetime
    response_size: int
    priority: int
    cleanup_priority: CleanupPriority
    estimated_space_mb: float
    reason: str


@dataclass
class CleanupResult:
    """정리 작업 결과"""
    total_candidates: int
    deleted_records: int
    space_freed_mb: float
    execution_time_sec: float
    errors: List[str]
    cleanup_summary: Dict[str, Any]


class TTLPolicyEngine:
    """TTL 정책 관리 및 자동 정리 엔진"""
    
    def __init__(self, dry_run: bool = False):
        """
        TTL 정책 엔진 초기화
        
        Args:
            dry_run: 실제 삭제하지 않고 시뮬레이션만 수행
        """
        self.db_manager = extend_database_manager(DatabaseManager().sync_manager)
        self.dry_run = dry_run
        
        # 정리 통계
        self.stats = {
            "cleanup_runs": 0,
            "total_deleted": 0,
            "total_space_freed_mb": 0.0,
            "avg_execution_time_sec": 0.0,
            "last_cleanup": None,
        }
        
        logger.info(f"TTL 정책 엔진 초기화 완료 (dry_run: {dry_run})")
    
    def identify_cleanup_candidates(self, 
                                  target_space_mb: Optional[float] = None,
                                  emergency_mode: bool = False) -> List[CleanupCandidate]:
        """
        정리 대상 후보들을 식별하고 우선순위 정렬
        
        Args:
            target_space_mb: 목표 확보 공간 (MB)
            emergency_mode: 긴급 정리 모드
        
        Returns:
            우선순위별 정리 대상 후보 리스트
        """
        logger.info(f"정리 대상 식별 시작 (목표: {target_space_mb}MB, 긴급: {emergency_mode})")
        
        candidates = []
        
        # 1. 만료된 데이터 식별
        expired_candidates = self._identify_expired_data()
        candidates.extend(expired_candidates)
        
        # 2. 낮은 우선순위 + 오래된 데이터
        low_priority_candidates = self._identify_low_priority_old_data()
        candidates.extend(low_priority_candidates)
        
        # 3. 대용량 + 중간 우선순위 데이터
        large_size_candidates = self._identify_large_size_data()
        candidates.extend(large_size_candidates)
        
        # 4. 긴급 모드: 중간 우선순위 데이터
        if emergency_mode:
            emergency_candidates = self._identify_emergency_cleanup_data()
            candidates.extend(emergency_candidates)
        
        # 우선순위별 정렬
        candidates.sort(key=lambda x: (x.cleanup_priority.value, -x.estimated_space_mb))
        
        # 목표 공간 달성 시까지만 선택
        if target_space_mb:
            candidates = self._select_by_target_space(candidates, target_space_mb)
        
        logger.info(f"정리 대상 식별 완료: {len(candidates)}개 후보")
        return candidates
    
    def _identify_expired_data(self) -> List[CleanupCandidate]:
        """만료된 데이터 식별"""
        query = """
        SELECT 
            id, api_provider, endpoint, created_at, response_size,
            COALESCE((storage_metadata->>'priority')::int, 2) as priority
        FROM api_raw_data 
        WHERE created_at < NOW() - INTERVAL '1 day' * (
            CASE 
                WHEN api_provider = 'KMA' AND endpoint = 'fct_shrt_reg' THEN 180
                WHEN api_provider = 'KMA' AND endpoint IN ('getUltraSrtNcst') THEN 30
                WHEN api_provider = 'KMA' AND endpoint IN ('getUltraSrtFcst', 'getVilageFcst') THEN 60
                WHEN api_provider = 'KTO' AND endpoint = 'areaCode2' THEN 365
                WHEN api_provider = 'KTO' AND endpoint = 'ldongCode2' THEN 365
                WHEN api_provider = 'KTO' AND endpoint IN ('areaBasedList2', 'detailCommon2', 'detailIntro2') THEN 180
                WHEN api_provider = 'KTO' AND endpoint = 'detailImage2' THEN 90
                WHEN api_provider = 'WEATHER' THEN 30
                ELSE 90
            END
        )
        ORDER BY created_at ASC
        """
        
        try:
            rows = self.db_manager.fetch_all(query)
            candidates = []
            
            for row in rows:
                candidate = CleanupCandidate(
                    id=row['id'],
                    api_provider=row['api_provider'],
                    endpoint=row['endpoint'],
                    created_at=row['created_at'],
                    response_size=row['response_size'] or 0,
                    priority=row['priority'],
                    cleanup_priority=CleanupPriority.EXPIRED,
                    estimated_space_mb=(row['response_size'] or 0) / (1024 * 1024),
                    reason="TTL 만료"
                )
                candidates.append(candidate)
            
            logger.info(f"만료된 데이터 {len(candidates)}개 식별")
            return candidates
            
        except Exception as e:
            logger.error(f"만료 데이터 식별 오류: {e}")
            return []
    
    def _identify_low_priority_old_data(self) -> List[CleanupCandidate]:
        """낮은 우선순위 + 오래된 데이터 식별"""
        query = """
        SELECT 
            id, api_provider, endpoint, created_at, response_size,
            COALESCE((storage_metadata->>'priority')::int, 2) as priority
        FROM api_raw_data 
        WHERE COALESCE((storage_metadata->>'priority')::int, 2) >= 3
          AND created_at < NOW() - INTERVAL '30 days'
          AND created_at >= NOW() - INTERVAL '1 day' * (
              CASE 
                  WHEN api_provider = 'KMA' AND endpoint = 'fct_shrt_reg' THEN 180
                  WHEN api_provider = 'KMA' AND endpoint IN ('getUltraSrtNcst') THEN 30
                  WHEN api_provider = 'KMA' AND endpoint IN ('getUltraSrtFcst', 'getVilageFcst') THEN 60
                  WHEN api_provider = 'KTO' AND endpoint = 'areaCode2' THEN 365
                  WHEN api_provider = 'KTO' AND endpoint = 'ldongCode2' THEN 365
                  WHEN api_provider = 'KTO' AND endpoint IN ('areaBasedList2', 'detailCommon2', 'detailIntro2') THEN 180
                  WHEN api_provider = 'KTO' AND endpoint = 'detailImage2' THEN 90
                  WHEN api_provider = 'WEATHER' THEN 30
                  ELSE 90
              END
          )
        ORDER BY priority DESC, created_at ASC
        """
        
        try:
            rows = self.db_manager.fetch_all(query)
            candidates = []
            
            for row in rows:
                candidate = CleanupCandidate(
                    id=row['id'],
                    api_provider=row['api_provider'],
                    endpoint=row['endpoint'],
                    created_at=row['created_at'],
                    response_size=row['response_size'] or 0,
                    priority=row['priority'],
                    cleanup_priority=CleanupPriority.LOW_PRIORITY,
                    estimated_space_mb=(row['response_size'] or 0) / (1024 * 1024),
                    reason="낮은 우선순위 + 30일 이상"
                )
                candidates.append(candidate)
            
            logger.info(f"낮은 우선순위 오래된 데이터 {len(candidates)}개 식별")
            return candidates
            
        except Exception as e:
            logger.error(f"낮은 우선순위 데이터 식별 오류: {e}")
            return []
    
    def _identify_large_size_data(self) -> List[CleanupCandidate]:
        """대용량 + 중간 우선순위 데이터 식별"""
        query = """
        SELECT 
            id, api_provider, endpoint, created_at, response_size,
            COALESCE((storage_metadata->>'priority')::int, 2) as priority
        FROM api_raw_data 
        WHERE response_size > 10 * 1024 * 1024  -- 10MB 이상
          AND COALESCE((storage_metadata->>'priority')::int, 2) >= 2
          AND created_at < NOW() - INTERVAL '7 days'
        ORDER BY response_size DESC, priority DESC
        """
        
        try:
            rows = self.db_manager.fetch_all(query)
            candidates = []
            
            for row in rows:
                candidate = CleanupCandidate(
                    id=row['id'],
                    api_provider=row['api_provider'],
                    endpoint=row['endpoint'],
                    created_at=row['created_at'],
                    response_size=row['response_size'] or 0,
                    priority=row['priority'],
                    cleanup_priority=CleanupPriority.LARGE_SIZE,
                    estimated_space_mb=(row['response_size'] or 0) / (1024 * 1024),
                    reason="대용량 응답 (10MB 이상)"
                )
                candidates.append(candidate)
            
            logger.info(f"대용량 데이터 {len(candidates)}개 식별")
            return candidates
            
        except Exception as e:
            logger.error(f"대용량 데이터 식별 오류: {e}")
            return []
    
    def _identify_emergency_cleanup_data(self) -> List[CleanupCandidate]:
        """긴급 정리 데이터 식별 (중간 우선순위 포함)"""
        query = """
        SELECT 
            id, api_provider, endpoint, created_at, response_size,
            COALESCE((storage_metadata->>'priority')::int, 2) as priority
        FROM api_raw_data 
        WHERE COALESCE((storage_metadata->>'priority')::int, 2) >= 2
          AND created_at < NOW() - INTERVAL '3 days'
        ORDER BY priority DESC, created_at ASC
        """
        
        try:
            rows = self.db_manager.fetch_all(query)
            candidates = []
            
            for row in rows:
                candidate = CleanupCandidate(
                    id=row['id'],
                    api_provider=row['api_provider'],
                    endpoint=row['endpoint'],
                    created_at=row['created_at'],
                    response_size=row['response_size'] or 0,
                    priority=row['priority'],
                    cleanup_priority=CleanupPriority.EMERGENCY,
                    estimated_space_mb=(row['response_size'] or 0) / (1024 * 1024),
                    reason="긴급 정리 모드"
                )
                candidates.append(candidate)
            
            logger.info(f"긴급 정리 데이터 {len(candidates)}개 식별")
            return candidates
            
        except Exception as e:
            logger.error(f"긴급 정리 데이터 식별 오류: {e}")
            return []
    
    def _select_by_target_space(self, candidates: List[CleanupCandidate], 
                               target_space_mb: float) -> List[CleanupCandidate]:
        """목표 공간까지 후보 선택"""
        selected = []
        accumulated_space = 0.0
        
        for candidate in candidates:
            selected.append(candidate)
            accumulated_space += candidate.estimated_space_mb
            
            if accumulated_space >= target_space_mb:
                break
        
        logger.info(f"목표 공간 {target_space_mb}MB 달성을 위해 {len(selected)}개 선택 "
                   f"(예상 확보: {accumulated_space:.2f}MB)")
        
        return selected
    
    def execute_cleanup(self, candidates: List[CleanupCandidate], 
                       batch_size: int = 1000) -> CleanupResult:
        """
        정리 작업 실행
        
        Args:
            candidates: 정리 대상 후보 리스트
            batch_size: 배치 삭제 크기
        
        Returns:
            정리 작업 결과
        """
        start_time = datetime.now()
        
        logger.info(f"정리 작업 시작: {len(candidates)}개 대상 "
                   f"(dry_run: {self.dry_run})")
        
        deleted_count = 0
        space_freed = 0.0
        errors = []
        
        # 배치별로 삭제 실행
        for i in range(0, len(candidates), batch_size):
            batch = candidates[i:i + batch_size]
            batch_ids = [c.id for c in batch]
            batch_space = sum(c.estimated_space_mb for c in batch)
            
            try:
                if not self.dry_run:
                    delete_query = "DELETE FROM api_raw_data WHERE id = ANY(%s)"
                    self.db_manager.execute_query(delete_query, [batch_ids])
                
                deleted_count += len(batch)
                space_freed += batch_space
                
                logger.debug(f"배치 {i//batch_size + 1} 완료: {len(batch)}개 삭제, "
                           f"{batch_space:.2f}MB 확보")
                
            except Exception as e:
                error_msg = f"배치 {i//batch_size + 1} 삭제 오류: {e}"
                errors.append(error_msg)
                logger.error(error_msg)
        
        # 실행 시간 계산
        execution_time = (datetime.now() - start_time).total_seconds()
        
        # 정리 요약 생성
        cleanup_summary = self._generate_cleanup_summary(candidates, deleted_count)
        
        # 통계 업데이트
        self.stats["cleanup_runs"] += 1
        self.stats["total_deleted"] += deleted_count
        self.stats["total_space_freed_mb"] += space_freed
        self.stats["avg_execution_time_sec"] = (
            (self.stats["avg_execution_time_sec"] + execution_time) / 2
        )
        self.stats["last_cleanup"] = datetime.now()
        
        result = CleanupResult(
            total_candidates=len(candidates),
            deleted_records=deleted_count,
            space_freed_mb=space_freed,
            execution_time_sec=execution_time,
            errors=errors,
            cleanup_summary=cleanup_summary
        )
        
        logger.info(f"정리 작업 완료: {deleted_count}/{len(candidates)}개 삭제, "
                   f"{space_freed:.2f}MB 확보, {execution_time:.2f}초 소요")
        
        return result
    
    def _generate_cleanup_summary(self, candidates: List[CleanupCandidate], 
                                 deleted_count: int) -> Dict[str, Any]:
        """정리 작업 요약 생성"""
        summary = {
            "by_priority": {},
            "by_provider": {},
            "by_reason": {},
            "size_distribution": {
                "small": 0,   # < 1MB
                "medium": 0,  # 1MB - 10MB
                "large": 0    # > 10MB
            }
        }
        
        for candidate in candidates[:deleted_count]:
            # 우선순위별
            priority_key = candidate.cleanup_priority.name
            if priority_key not in summary["by_priority"]:
                summary["by_priority"][priority_key] = {"count": 0, "space_mb": 0.0}
            summary["by_priority"][priority_key]["count"] += 1
            summary["by_priority"][priority_key]["space_mb"] += candidate.estimated_space_mb
            
            # 제공자별
            if candidate.api_provider not in summary["by_provider"]:
                summary["by_provider"][candidate.api_provider] = {"count": 0, "space_mb": 0.0}
            summary["by_provider"][candidate.api_provider]["count"] += 1
            summary["by_provider"][candidate.api_provider]["space_mb"] += candidate.estimated_space_mb
            
            # 사유별
            if candidate.reason not in summary["by_reason"]:
                summary["by_reason"][candidate.reason] = {"count": 0, "space_mb": 0.0}
            summary["by_reason"][candidate.reason]["count"] += 1
            summary["by_reason"][candidate.reason]["space_mb"] += candidate.estimated_space_mb
            
            # 크기별
            if candidate.estimated_space_mb < 1:
                summary["size_distribution"]["small"] += 1
            elif candidate.estimated_space_mb < 10:
                summary["size_distribution"]["medium"] += 1
            else:
                summary["size_distribution"]["large"] += 1
        
        return summary
    
    def get_storage_usage_stats(self) -> Dict[str, Any]:
        """현재 스토리지 사용량 통계"""
        query = """
        SELECT 
            COUNT(*) as total_records,
            ROUND(SUM(response_size)::numeric / (1024*1024), 2) as total_size_mb,
            ROUND(AVG(response_size)::numeric, 2) as avg_size_bytes,
            MIN(created_at) as oldest_record,
            MAX(created_at) as newest_record,
            COUNT(CASE WHEN created_at < NOW() - INTERVAL '90 days' THEN 1 END) as old_records_90d,
            COUNT(CASE WHEN response_size > 10*1024*1024 THEN 1 END) as large_records_10mb
        FROM api_raw_data
        """
        
        try:
            result = self.db_manager.fetch_one(query)
            
            # 제공자별 통계
            provider_query = """
            SELECT 
                api_provider,
                COUNT(*) as count,
                ROUND(SUM(response_size)::numeric / (1024*1024), 2) as size_mb
            FROM api_raw_data
            GROUP BY api_provider
            ORDER BY size_mb DESC
            """
            
            provider_stats = self.db_manager.fetch_all(provider_query)
            
            return {
                "overall": dict(result) if result else {},
                "by_provider": [dict(row) for row in provider_stats],
                "cleanup_potential": {
                    "old_records_90d": result['old_records_90d'] if result else 0,
                    "large_records_10mb": result['large_records_10mb'] if result else 0
                }
            }
            
        except Exception as e:
            logger.error(f"스토리지 사용량 통계 조회 오류: {e}")
            return {}
    
    def get_statistics(self) -> Dict[str, Any]:
        """TTL 엔진 통계 반환"""
        return {
            **self.stats,
            "dry_run_mode": self.dry_run
        }
    
    def reset_statistics(self):
        """통계 초기화"""
        for key in self.stats:
            if key != "last_cleanup":
                self.stats[key] = 0 if isinstance(self.stats[key], (int, float)) else None
        logger.info("TTL 정책 엔진 통계가 초기화되었습니다")


# 전역 TTL 정책 엔진 인스턴스
_ttl_engine: Optional[TTLPolicyEngine] = None


def get_ttl_engine(dry_run: bool = False) -> TTLPolicyEngine:
    """전역 TTL 정책 엔진 인스턴스 반환 (싱글톤)"""
    global _ttl_engine
    
    if _ttl_engine is None or _ttl_engine.dry_run != dry_run:
        _ttl_engine = TTLPolicyEngine(dry_run=dry_run)
    
    return _ttl_engine


def reset_ttl_engine():
    """TTL 엔진 인스턴스 재설정 (테스트용)"""
    global _ttl_engine
    _ttl_engine = None