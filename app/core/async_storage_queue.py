"""
비동기 저장 큐 시스템

API 응답 데이터를 백그라운드에서 비동기적으로 저장하여 
메인 로직의 성능 영향을 최소화하는 시스템입니다.
"""

import asyncio
import logging
import time
import threading
from typing import Dict, Any, Optional, List, Callable
from datetime import datetime
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field

from .selective_storage_manager import StorageRequest, get_storage_manager

logger = logging.getLogger(__name__)


@dataclass
class QueuedStorageRequest:
    """큐에 저장되는 저장 요청"""
    storage_request: StorageRequest
    priority: int = 2  # 1: 높음, 2: 중간, 3: 낮음
    retry_count: int = 0
    max_retries: int = 3
    created_at: datetime = field(default_factory=datetime.now)
    callback: Optional[Callable] = None


class AsyncStorageQueue:
    """비동기 저장 큐 관리자"""
    
    def __init__(self, 
                 queue_size: int = 1000,
                 worker_count: int = 2,
                 batch_size: int = 50,
                 flush_interval_seconds: int = 10):
        """
        비동기 저장 큐 초기화
        
        Args:
            queue_size: 큐 최대 크기
            worker_count: 워커 스레드 수
            batch_size: 배치 처리 크기
            flush_interval_seconds: 강제 플러시 간격 (초)
        """
        self.queue_size = queue_size
        self.worker_count = worker_count
        self.batch_size = batch_size
        self.flush_interval = flush_interval_seconds
        
        # 우선순위별 큐 (우선순위가 낮을수록 먼저 처리)
        self.high_priority_queue = Queue(maxsize=queue_size // 3)
        self.medium_priority_queue = Queue(maxsize=queue_size // 3) 
        self.low_priority_queue = Queue(maxsize=queue_size // 3)
        
        # 워커 스레드 관리
        self.workers = []
        self.executor = ThreadPoolExecutor(max_workers=worker_count)
        self.running = False
        self.storage_manager = get_storage_manager()
        
        # 통계
        self.stats = {
            "queued_requests": 0,
            "processed_requests": 0,
            "failed_requests": 0,
            "queue_full_rejections": 0,
            "avg_queue_time_ms": 0,
            "avg_processing_time_ms": 0,
            "current_queue_size": 0,
        }
        
        logger.info(f"비동기 저장 큐 초기화: 큐크기={queue_size}, 워커={worker_count}, "
                   f"배치크기={batch_size}, 플러시간격={flush_interval_seconds}초")
    
    def start(self):
        """비동기 저장 큐 시작"""
        if self.running:
            logger.warning("이미 실행 중인 비동기 저장 큐입니다")
            return
        
        self.running = True
        
        # 워커 스레드 시작
        for i in range(self.worker_count):
            worker_thread = threading.Thread(
                target=self._worker_loop,
                name=f"AsyncStorageWorker-{i}",
                daemon=True
            )
            worker_thread.start()
            self.workers.append(worker_thread)
        
        # 플러시 스레드 시작
        flush_thread = threading.Thread(
            target=self._flush_loop,
            name="AsyncStorageFlush",
            daemon=True
        )
        flush_thread.start()
        self.workers.append(flush_thread)
        
        logger.info(f"비동기 저장 큐 시작 완료: {len(self.workers)}개 워커 스레드")
    
    def stop(self, timeout: int = 30):
        """비동기 저장 큐 중지"""
        if not self.running:
            return
        
        logger.info("비동기 저장 큐 중지 시작...")
        self.running = False
        
        # 남은 작업 처리 대기
        start_time = time.time()
        while (self._get_total_queue_size() > 0 and 
               time.time() - start_time < timeout):
            time.sleep(0.1)
        
        # 워커 스레드 종료 대기
        for worker in self.workers:
            if worker.is_alive():
                worker.join(timeout=5)
        
        self.executor.shutdown(wait=True)
        logger.info("비동기 저장 큐 중지 완료")
    
    def enqueue(self, storage_request: StorageRequest, 
                priority: int = 2, 
                callback: Optional[Callable] = None) -> bool:
        """
        저장 요청을 큐에 추가
        
        Args:
            storage_request: 저장 요청 데이터
            priority: 우선순위 (1: 높음, 2: 중간, 3: 낮음)
            callback: 완료 콜백 함수
        
        Returns:
            큐 추가 성공 여부
        """
        if not self.running:
            logger.error("비동기 저장 큐가 실행되지 않았습니다")
            return False
        
        queued_request = QueuedStorageRequest(
            storage_request=storage_request,
            priority=priority,
            callback=callback
        )
        
        try:
            # 우선순위에 따라 적절한 큐에 추가
            target_queue = self._get_queue_by_priority(priority)
            target_queue.put(queued_request, block=False)
            
            self.stats["queued_requests"] += 1
            self.stats["current_queue_size"] = self._get_total_queue_size()
            
            logger.debug(f"저장 요청 큐 추가: {storage_request.provider}/{storage_request.endpoint} "
                        f"(우선순위: {priority})")
            
            return True
            
        except Exception as e:
            self.stats["queue_full_rejections"] += 1
            logger.warning(f"큐 추가 실패: {e}")
            return False
    
    def _get_queue_by_priority(self, priority: int) -> Queue:
        """우선순위에 따른 큐 반환"""
        if priority == 1:
            return self.high_priority_queue
        elif priority == 2:
            return self.medium_priority_queue
        else:
            return self.low_priority_queue
    
    def _get_total_queue_size(self) -> int:
        """전체 큐 크기 반환"""
        return (self.high_priority_queue.qsize() + 
                self.medium_priority_queue.qsize() + 
                self.low_priority_queue.qsize())
    
    def _get_next_request(self, timeout: float = 1.0) -> Optional[QueuedStorageRequest]:
        """우선순위에 따라 다음 요청 가져오기"""
        # 우선순위 순으로 큐 확인
        queues = [
            self.high_priority_queue,
            self.medium_priority_queue,
            self.low_priority_queue
        ]
        
        for queue in queues:
            try:
                return queue.get(block=False)
            except Empty:
                continue
        
        # 모든 큐가 비어있으면 잠시 대기
        try:
            return self.medium_priority_queue.get(timeout=timeout)
        except Empty:
            return None
    
    def _worker_loop(self):
        """워커 스레드 메인 루프"""
        worker_name = threading.current_thread().name
        logger.info(f"저장 워커 시작: {worker_name}")
        
        batch_requests = []
        last_flush_time = time.time()
        
        while self.running or self._get_total_queue_size() > 0:
            try:
                # 요청 가져오기
                queued_request = self._get_next_request(timeout=1.0)
                if queued_request is None:
                    # 타임아웃 시 배치 처리
                    if batch_requests:
                        self._process_batch(batch_requests)
                        batch_requests = []
                        last_flush_time = time.time()
                    continue
                
                batch_requests.append(queued_request)
                
                # 배치 크기 도달 또는 플러시 시간 초과 시 처리
                current_time = time.time()
                should_flush = (
                    len(batch_requests) >= self.batch_size or
                    current_time - last_flush_time >= self.flush_interval
                )
                
                if should_flush:
                    self._process_batch(batch_requests)
                    batch_requests = []
                    last_flush_time = current_time
                
            except Exception as e:
                logger.error(f"워커 루프 오류 ({worker_name}): {e}")
                time.sleep(1)
        
        # 종료 시 남은 배치 처리
        if batch_requests:
            self._process_batch(batch_requests)
        
        logger.info(f"저장 워커 종료: {worker_name}")
    
    def _process_batch(self, batch_requests: List[QueuedStorageRequest]):
        """배치 요청 처리"""
        if not batch_requests:
            return
        
        start_time = time.time()
        
        # StorageRequest 리스트 추출
        storage_requests = [req.storage_request for req in batch_requests]
        
        try:
            # 배치 처리
            results = self.storage_manager.bulk_process_requests(storage_requests)
            
            # 결과 처리 및 콜백 실행
            for i, (queued_req, result) in enumerate(zip(batch_requests, results)):
                self.stats["processed_requests"] += 1
                
                if not result.get("storage_success", False) and result.get("should_store", False):
                    # 저장 실패 시 재시도 확인
                    if queued_req.retry_count < queued_req.max_retries:
                        queued_req.retry_count += 1
                        self._retry_request(queued_req)
                    else:
                        self.stats["failed_requests"] += 1
                        logger.error(f"저장 요청 최종 실패: "
                                   f"{queued_req.storage_request.provider}/"
                                   f"{queued_req.storage_request.endpoint}")
                
                # 콜백 실행
                if queued_req.callback:
                    try:
                        queued_req.callback(result)
                    except Exception as e:
                        logger.error(f"콜백 실행 오류: {e}")
            
            # 성능 통계 업데이트
            processing_time = (time.time() - start_time) * 1000
            self.stats["avg_processing_time_ms"] = (
                (self.stats["avg_processing_time_ms"] + processing_time) / 2
            )
            
            logger.debug(f"배치 처리 완료: {len(batch_requests)}개 요청 "
                        f"({processing_time:.2f}ms)")
            
        except Exception as e:
            self.stats["failed_requests"] += len(batch_requests)
            logger.error(f"배치 처리 오류: {e}")
    
    def _retry_request(self, queued_request: QueuedStorageRequest):
        """실패한 요청 재시도"""
        try:
            # 우선순위를 낮춰서 재시도
            retry_priority = min(queued_request.priority + 1, 3)
            target_queue = self._get_queue_by_priority(retry_priority)
            target_queue.put(queued_request, block=False)
            
            logger.debug(f"저장 요청 재시도: {queued_request.storage_request.provider}/"
                        f"{queued_request.storage_request.endpoint} "
                        f"({queued_request.retry_count}/{queued_request.max_retries})")
            
        except Exception as e:
            self.stats["failed_requests"] += 1
            logger.error(f"재시도 큐 추가 실패: {e}")
    
    def _flush_loop(self):
        """주기적 플러시 루프"""
        logger.info("플러시 루프 시작")
        
        while self.running:
            try:
                time.sleep(self.flush_interval)
                
                # 큐 상태 통계 업데이트
                self.stats["current_queue_size"] = self._get_total_queue_size()
                
                # 필요시 강제 플러시 로직 추가 가능
                
            except Exception as e:
                logger.error(f"플러시 루프 오류: {e}")
        
        logger.info("플러시 루프 종료")
    
    def get_statistics(self) -> Dict[str, Any]:
        """큐 통계 반환"""
        total_processed = self.stats["processed_requests"]
        
        return {
            **self.stats,
            "queue_sizes": {
                "high_priority": self.high_priority_queue.qsize(),
                "medium_priority": self.medium_priority_queue.qsize(),
                "low_priority": self.low_priority_queue.qsize(),
                "total": self._get_total_queue_size()
            },
            "success_rate": round(
                (total_processed - self.stats["failed_requests"]) / 
                max(total_processed, 1) * 100, 2
            ) if total_processed > 0 else 0,
            "queue_utilization": round(
                self._get_total_queue_size() / self.queue_size * 100, 2
            ),
            "worker_count": len([w for w in self.workers if w.is_alive()]),
            "running": self.running
        }
    
    def health_check(self) -> Dict[str, Any]:
        """시스템 상태 검사"""
        stats = self.get_statistics()
        
        # 건강 상태 평가
        is_healthy = (
            self.running and
            stats["queue_utilization"] < 90 and
            stats["success_rate"] > 95 and
            stats["worker_count"] == self.worker_count
        )
        
        return {
            "healthy": is_healthy,
            "status": "healthy" if is_healthy else "warning",
            "checks": {
                "queue_running": self.running,
                "queue_not_full": stats["queue_utilization"] < 90,
                "high_success_rate": stats["success_rate"] > 95,
                "all_workers_alive": stats["worker_count"] == self.worker_count
            },
            "statistics": stats
        }


# 전역 비동기 저장 큐 인스턴스
_async_queue: Optional[AsyncStorageQueue] = None


def get_async_storage_queue() -> AsyncStorageQueue:
    """전역 비동기 저장 큐 인스턴스 반환 (싱글톤)"""
    global _async_queue
    
    if _async_queue is None:
        _async_queue = AsyncStorageQueue()
    
    return _async_queue


def initialize_async_storage(queue_size: int = 1000,
                           worker_count: int = 2,
                           batch_size: int = 50,
                           flush_interval_seconds: int = 10) -> AsyncStorageQueue:
    """비동기 저장 시스템 초기화 및 시작"""
    global _async_queue
    
    if _async_queue is not None:
        _async_queue.stop()
    
    _async_queue = AsyncStorageQueue(
        queue_size=queue_size,
        worker_count=worker_count,
        batch_size=batch_size,
        flush_interval_seconds=flush_interval_seconds
    )
    
    _async_queue.start()
    return _async_queue


def shutdown_async_storage(timeout: int = 30):
    """비동기 저장 시스템 종료"""
    global _async_queue
    
    if _async_queue is not None:
        _async_queue.stop(timeout=timeout)
        _async_queue = None