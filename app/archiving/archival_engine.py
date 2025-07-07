"""
통합 아카이빙 엔진

아카이빙 정책과 백업 관리자를 통합하여 자동화된 아카이빙을 수행합니다.
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from enum import Enum

from app.core.database_connection_pool import get_connection_pool
from app.archiving.archival_policies import (
    get_archival_policy_manager, ArchivalRule, ArchivalTrigger
)
from app.archiving.backup_manager import (
    get_backup_manager, BackupManager, BackupRecord, BackupStatus
)

logger = logging.getLogger(__name__)


class ArchivalTaskStatus(Enum):
    """아카이빙 작업 상태"""
    PENDING = "pending"
    ANALYZING = "analyzing"
    BACKING_UP = "backing_up"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ArchivalTask:
    """아카이빙 작업"""
    task_id: str
    data_id: str
    api_provider: str
    endpoint: str
    rule: ArchivalRule
    status: ArchivalTaskStatus = ArchivalTaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    backup_record: Optional[BackupRecord] = None
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ArchivalSummary:
    """아카이빙 요약"""
    total_candidates: int = 0
    processed_items: int = 0
    successful_backups: int = 0
    failed_backups: int = 0
    skipped_items: int = 0
    total_original_size_mb: float = 0.0
    total_compressed_size_mb: float = 0.0
    average_compression_ratio: float = 0.0
    processing_time_seconds: float = 0.0


class ArchivalEngine:
    """통합 아카이빙 엔진"""
    
    def __init__(self, backup_manager: Optional[BackupManager] = None):
        """아카이빙 엔진 초기화"""
        self.policy_manager = get_archival_policy_manager()
        self.backup_manager = backup_manager or get_backup_manager()
        self.db_pool = get_connection_pool()
        
        # 작업 큐 및 상태 관리
        self.active_tasks: Dict[str, ArchivalTask] = {}
        self.completed_tasks: List[ArchivalTask] = []
        self.max_concurrent_tasks = 5
        self.task_semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        
        # 통계
        self.engine_stats = {
            "total_runs": 0,
            "total_items_processed": 0,
            "total_backups_created": 0,
            "total_data_archived_mb": 0.0,
            "last_run_time": None,
            "average_processing_time_seconds": 0.0
        }
        
        logger.info("아카이빙 엔진 초기화 완료")
    
    async def run_archival_process(self, api_provider: str = None, 
                                 endpoint: str = None, 
                                 dry_run: bool = False) -> ArchivalSummary:
        """아카이빙 프로세스 실행"""
        start_time = datetime.now()
        summary = ArchivalSummary()
        
        try:
            logger.info(f"아카이빙 프로세스 시작 (제공자: {api_provider}, 엔드포인트: {endpoint}, 드라이런: {dry_run})")
            
            # 1. 아카이빙 후보 데이터 식별
            candidates = await self._identify_archival_candidates(api_provider, endpoint)
            summary.total_candidates = len(candidates)
            
            if not candidates:
                logger.info("아카이빙할 데이터가 없습니다")
                return summary
            
            logger.info(f"아카이빙 후보 {len(candidates)}개 식별됨")
            
            # 2. 아카이빙 작업 생성
            tasks = await self._create_archival_tasks(candidates)
            
            # 3. 작업 실행
            if not dry_run:
                results = await self._execute_archival_tasks(tasks)
                summary = self._compile_archival_summary(results, start_time)
            else:
                logger.info(f"드라이런 모드: {len(tasks)}개 작업이 생성되었지만 실행되지 않았습니다")
                summary.processed_items = len(tasks)
            
            # 4. 통계 업데이트
            self._update_engine_statistics(summary)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"아카이빙 프로세스 완료 (처리 시간: {processing_time:.2f}초)")
            
        except Exception as e:
            logger.error(f"아카이빙 프로세스 실패: {e}")
            raise
        
        return summary
    
    async def _identify_archival_candidates(self, api_provider: str = None, 
                                          endpoint: str = None) -> List[Dict[str, Any]]:
        """아카이빙 후보 데이터 식별"""
        candidates = []
        
        # 데이터베이스에서 원본 API 데이터 조회
        query = """
            SELECT 
                id,
                api_provider,
                endpoint,
                created_at,
                last_accessed_at,
                data_size_bytes,
                request_url,
                response_status_code,
                response_data
            FROM api_raw_data
            WHERE 1=1
        """
        params = []
        
        if api_provider:
            query += " AND api_provider = %s"
            params.append(api_provider)
        
        if endpoint:
            query += " AND endpoint = %s"
            params.append(endpoint)
        
        query += " ORDER BY created_at ASC"
        
        async with self.db_pool.get_async_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                rows = await cursor.fetchall()
                
                columns = [desc[0] for desc in cursor.description]
                for row in rows:
                    candidate = dict(zip(columns, row))
                    candidates.append(candidate)
        
        logger.debug(f"데이터베이스에서 {len(candidates)}개 후보 조회됨")
        return candidates
    
    async def _create_archival_tasks(self, candidates: List[Dict[str, Any]]) -> List[ArchivalTask]:
        """아카이빙 작업 생성"""
        tasks = []
        
        for candidate in candidates:
            api_provider = candidate["api_provider"]
            endpoint = candidate["endpoint"]
            data_id = str(candidate["id"])
            
            # 적용 가능한 아카이빙 규칙 가져오기
            rules = self.policy_manager.get_archival_rules(api_provider, endpoint)
            
            # 각 규칙에 대해 조건 평가
            for rule in rules:
                # 데이터 메타데이터 구성
                data_metadata = {
                    "created_at": candidate["created_at"],
                    "last_accessed": candidate.get("last_accessed_at"),
                    "data_size_bytes": candidate.get("data_size_bytes", 0)
                }
                
                # 아카이빙 조건 확인
                if self.policy_manager.evaluate_archival_condition(rule, data_metadata):
                    task_id = f"{data_id}_{rule.rule_id}_{int(datetime.now().timestamp())}"
                    
                    task = ArchivalTask(
                        task_id=task_id,
                        data_id=data_id,
                        api_provider=api_provider,
                        endpoint=endpoint,
                        rule=rule,
                        metadata={
                            "candidate_data": candidate,
                            "rule_name": rule.name
                        }
                    )
                    
                    tasks.append(task)
                    logger.debug(f"아카이빙 작업 생성: {task_id} (규칙: {rule.name})")
                    break  # 첫 번째 매칭 규칙만 적용
        
        logger.info(f"총 {len(tasks)}개 아카이빙 작업 생성됨")
        return tasks
    
    async def _execute_archival_tasks(self, tasks: List[ArchivalTask]) -> List[ArchivalTask]:
        """아카이빙 작업 실행"""
        # 동시 실행 작업들
        concurrent_tasks = []
        
        for task in tasks:
            self.active_tasks[task.task_id] = task
            concurrent_task = asyncio.create_task(self._execute_single_task(task))
            concurrent_tasks.append(concurrent_task)
        
        # 모든 작업 완료 대기
        await asyncio.gather(*concurrent_tasks, return_exceptions=True)
        
        # 완료된 작업들을 completed_tasks로 이동
        for task in tasks:
            if task.task_id in self.active_tasks:
                del self.active_tasks[task.task_id]
            self.completed_tasks.append(task)
        
        return tasks
    
    async def _execute_single_task(self, task: ArchivalTask):
        """단일 아카이빙 작업 실행"""
        async with self.task_semaphore:
            task.started_at = datetime.now()
            task.status = ArchivalTaskStatus.ANALYZING
            
            try:
                # 후보 데이터 가져오기
                candidate_data = task.metadata["candidate_data"]
                response_data = candidate_data.get("response_data", {})
                
                if not response_data:
                    task.status = ArchivalTaskStatus.SKIPPED
                    task.error_message = "응답 데이터가 없음"
                    task.completed_at = datetime.now()
                    return
                
                # 백업 실행
                task.status = ArchivalTaskStatus.BACKING_UP
                backup_record = await self.backup_manager.backup_data(
                    data_id=task.data_id,
                    api_provider=task.api_provider,
                    endpoint=task.endpoint,
                    data=response_data,
                    rule=task.rule
                )
                
                task.backup_record = backup_record
                
                if backup_record.status == BackupStatus.COMPLETED:
                    task.status = ArchivalTaskStatus.COMPLETED
                    
                    # 원본 데이터베이스에서 아카이빙 표시 또는 삭제
                    await self._mark_data_as_archived(task.data_id, backup_record.backup_id)
                    
                else:
                    task.status = ArchivalTaskStatus.FAILED
                    task.error_message = backup_record.error_message
                
                task.completed_at = datetime.now()
                
                logger.info(f"아카이빙 작업 완료: {task.task_id} (상태: {task.status.value})")
                
            except Exception as e:
                task.status = ArchivalTaskStatus.FAILED
                task.error_message = str(e)
                task.completed_at = datetime.now()
                logger.error(f"아카이빙 작업 실패: {task.task_id}, 오류: {e}")
    
    async def _mark_data_as_archived(self, data_id: str, backup_id: str):
        """데이터를 아카이빙됨으로 표시"""
        update_query = """
            UPDATE api_raw_data 
            SET 
                archived_at = %s,
                backup_id = %s,
                is_archived = true
            WHERE id = %s
        """
        
        async with self.db_pool.get_async_connection() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(update_query, (datetime.now(), backup_id, data_id))
                await conn.commit()
        
        logger.debug(f"데이터 {data_id}를 아카이빙됨으로 표시 (백업 ID: {backup_id})")
    
    def _compile_archival_summary(self, tasks: List[ArchivalTask], start_time: datetime) -> ArchivalSummary:
        """아카이빙 요약 컴파일"""
        summary = ArchivalSummary()
        summary.processed_items = len(tasks)
        
        total_original_bytes = 0
        total_compressed_bytes = 0
        
        for task in tasks:
            if task.status == ArchivalTaskStatus.COMPLETED and task.backup_record:
                summary.successful_backups += 1
                total_original_bytes += task.backup_record.original_size_bytes
                total_compressed_bytes += task.backup_record.compressed_size_bytes
            elif task.status == ArchivalTaskStatus.FAILED:
                summary.failed_backups += 1
            elif task.status == ArchivalTaskStatus.SKIPPED:
                summary.skipped_items += 1
        
        # 크기 통계 (MB 단위)
        summary.total_original_size_mb = total_original_bytes / (1024 * 1024)
        summary.total_compressed_size_mb = total_compressed_bytes / (1024 * 1024)
        
        # 압축률 계산
        if total_original_bytes > 0:
            summary.average_compression_ratio = (
                1 - total_compressed_bytes / total_original_bytes
            ) * 100
        
        # 처리 시간
        summary.processing_time_seconds = (datetime.now() - start_time).total_seconds()
        
        return summary
    
    def _update_engine_statistics(self, summary: ArchivalSummary):
        """엔진 통계 업데이트"""
        self.engine_stats["total_runs"] += 1
        self.engine_stats["total_items_processed"] += summary.processed_items
        self.engine_stats["total_backups_created"] += summary.successful_backups
        self.engine_stats["total_data_archived_mb"] += summary.total_compressed_size_mb
        self.engine_stats["last_run_time"] = datetime.now()
        
        # 평균 처리 시간 업데이트
        if self.engine_stats["total_runs"] > 0:
            current_avg = self.engine_stats["average_processing_time_seconds"]
            new_avg = (
                (current_avg * (self.engine_stats["total_runs"] - 1) + summary.processing_time_seconds) /
                self.engine_stats["total_runs"]
            )
            self.engine_stats["average_processing_time_seconds"] = new_avg
    
    async def restore_archived_data(self, data_id: str) -> Optional[Dict[str, Any]]:
        """아카이빙된 데이터 복원"""
        try:
            # 데이터베이스에서 백업 ID 조회
            query = """
                SELECT backup_id, api_provider, endpoint
                FROM api_raw_data
                WHERE id = %s AND is_archived = true
            """
            
            backup_id = None
            async with self.db_pool.get_async_connection() as conn:
                async with conn.cursor() as cursor:
                    await cursor.execute(query, (data_id,))
                    row = await cursor.fetchone()
                    if row:
                        backup_id, api_provider, endpoint = row
            
            if not backup_id:
                logger.warning(f"아카이빙된 데이터를 찾을 수 없습니다: {data_id}")
                return None
            
            # 백업에서 데이터 복원
            restored_data = await self.backup_manager.restore_data(backup_id)
            
            if restored_data:
                logger.info(f"아카이빙된 데이터 복원 완료: {data_id}")
            
            return restored_data
            
        except Exception as e:
            logger.error(f"아카이빙된 데이터 복원 실패: {data_id}, 오류: {e}")
            return None
    
    def get_archival_statistics(self) -> Dict[str, Any]:
        """아카이빙 통계 반환"""
        active_task_count = len(self.active_tasks)
        completed_task_count = len(self.completed_tasks)
        
        # 최근 완료된 작업들의 상태별 집계
        recent_tasks = self.completed_tasks[-100:]  # 최근 100개
        status_counts = {}
        for status in ArchivalTaskStatus:
            status_counts[status.value] = sum(
                1 for task in recent_tasks if task.status == status
            )
        
        return {
            "engine_statistics": self.engine_stats,
            "active_tasks": active_task_count,
            "completed_tasks": completed_task_count,
            "recent_task_status_distribution": status_counts,
            "backup_statistics": self.backup_manager.get_backup_statistics(),
            "policy_statistics": self.policy_manager.get_policy_statistics()
        }
    
    def get_active_tasks(self) -> List[ArchivalTask]:
        """현재 진행 중인 작업 목록"""
        return list(self.active_tasks.values())
    
    def get_recent_tasks(self, limit: int = 50) -> List[ArchivalTask]:
        """최근 완료된 작업 목록"""
        return self.completed_tasks[-limit:]


# 전역 아카이빙 엔진 인스턴스
_archival_engine: Optional[ArchivalEngine] = None


def get_archival_engine(backup_manager: Optional[BackupManager] = None) -> ArchivalEngine:
    """전역 아카이빙 엔진 인스턴스 반환 (싱글톤)"""
    global _archival_engine
    
    if _archival_engine is None:
        _archival_engine = ArchivalEngine(backup_manager)
    
    return _archival_engine


def reset_archival_engine():
    """아카이빙 엔진 인스턴스 재설정 (테스트용)"""
    global _archival_engine
    _archival_engine = None