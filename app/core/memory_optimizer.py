"""
메모리 사용량 최적화 모듈

대용량 데이터 처리 시 메모리 효율성을 높이는 기능들을 제공합니다.
"""

import gc
import psutil
import threading
import time
import logging
from typing import Iterator, List, Dict, Any, Optional, Callable
from dataclasses import dataclass
from contextlib import contextmanager
import weakref
from collections import defaultdict


@dataclass
class MemoryConfig:
    """메모리 최적화 설정"""
    
    # 메모리 임계값 (MB)
    warning_threshold_mb: int = 500
    critical_threshold_mb: int = 1000
    
    # 청크 처리 설정
    default_chunk_size: int = 100
    adaptive_chunking: bool = True
    
    # 가비지 컬렉션 설정
    gc_frequency: int = 100  # N회 처리마다 GC 실행
    auto_gc: bool = True
    
    # 모니터링 설정
    monitor_interval: int = 30  # 초
    enable_monitoring: bool = True


class MemoryMonitor:
    """메모리 사용량 모니터링"""
    
    def __init__(self, config: MemoryConfig = None):
        self.config = config or MemoryConfig()
        self.logger = logging.getLogger(__name__)
        
        # 모니터링 상태
        self._monitoring = False
        self._monitor_thread = None
        self._shutdown_event = threading.Event()
        
        # 통계
        self.stats = {
            'peak_memory_mb': 0,
            'current_memory_mb': 0,
            'gc_collections': 0,
            'warning_count': 0,
            'critical_count': 0,
            'memory_history': []
        }
        
        # 알림 콜백
        self._warning_callbacks = []
        self._critical_callbacks = []
    
    def get_current_memory_mb(self) -> float:
        """현재 메모리 사용량 조회 (MB)"""
        process = psutil.Process()
        memory_mb = process.memory_info().rss / 1024 / 1024
        
        self.stats['current_memory_mb'] = memory_mb
        if memory_mb > self.stats['peak_memory_mb']:
            self.stats['peak_memory_mb'] = memory_mb
            
        return memory_mb
    
    def check_memory_threshold(self) -> str:
        """메모리 임계값 확인"""
        current_mb = self.get_current_memory_mb()
        
        if current_mb >= self.config.critical_threshold_mb:
            self.stats['critical_count'] += 1
            self._trigger_critical_callbacks(current_mb)
            return 'critical'
        elif current_mb >= self.config.warning_threshold_mb:
            self.stats['warning_count'] += 1
            self._trigger_warning_callbacks(current_mb)
            return 'warning'
        else:
            return 'normal'
    
    def add_warning_callback(self, callback: Callable[[float], None]):
        """경고 임계값 도달 시 콜백 추가"""
        self._warning_callbacks.append(callback)
    
    def add_critical_callback(self, callback: Callable[[float], None]):
        """위험 임계값 도달 시 콜백 추가"""
        self._critical_callbacks.append(callback)
    
    def _trigger_warning_callbacks(self, memory_mb: float):
        """경고 콜백 실행"""
        self.logger.warning(f"⚠️ 메모리 사용량 경고: {memory_mb:.1f}MB")
        for callback in self._warning_callbacks:
            try:
                callback(memory_mb)
            except Exception as e:
                self.logger.error(f"경고 콜백 실행 실패: {e}")
    
    def _trigger_critical_callbacks(self, memory_mb: float):
        """위험 콜백 실행"""
        self.logger.error(f"🚨 메모리 사용량 위험: {memory_mb:.1f}MB")
        for callback in self._critical_callbacks:
            try:
                callback(memory_mb)
            except Exception as e:
                self.logger.error(f"위험 콜백 실행 실패: {e}")
    
    def start_monitoring(self):
        """메모리 모니터링 시작"""
        if self._monitoring or not self.config.enable_monitoring:
            return
        
        self._monitoring = True
        self._shutdown_event.clear()
        
        def monitor_worker():
            """모니터링 작업"""
            while not self._shutdown_event.wait(self.config.monitor_interval):
                try:
                    current_mb = self.get_current_memory_mb()
                    self.stats['memory_history'].append({
                        'timestamp': time.time(),
                        'memory_mb': current_mb
                    })
                    
                    # 히스토리 제한 (최대 100개)
                    if len(self.stats['memory_history']) > 100:
                        self.stats['memory_history'] = self.stats['memory_history'][-100:]
                    
                    self.check_memory_threshold()
                    
                except Exception as e:
                    self.logger.error(f"메모리 모니터링 오류: {e}")
        
        self._monitor_thread = threading.Thread(
            target=monitor_worker,
            daemon=True,
            name="memory-monitor"
        )
        self._monitor_thread.start()
        self.logger.info("메모리 모니터링 시작")
    
    def stop_monitoring(self):
        """메모리 모니터링 중지"""
        if not self._monitoring:
            return
        
        self._monitoring = False
        self._shutdown_event.set()
        
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        
        self.logger.info("메모리 모니터링 중지")
    
    def get_memory_stats(self) -> Dict[str, Any]:
        """메모리 통계 조회"""
        current_mb = self.get_current_memory_mb()
        
        return {
            'current_memory_mb': current_mb,
            'peak_memory_mb': self.stats['peak_memory_mb'],
            'warning_threshold_mb': self.config.warning_threshold_mb,
            'critical_threshold_mb': self.config.critical_threshold_mb,
            'memory_status': self.check_memory_threshold(),
            'gc_collections': self.stats['gc_collections'],
            'warning_count': self.stats['warning_count'],
            'critical_count': self.stats['critical_count'],
            'history_points': len(self.stats['memory_history'])
        }


class MemoryOptimizer:
    """메모리 최적화 도구"""
    
    def __init__(self, config: MemoryConfig = None):
        self.config = config or MemoryConfig()
        self.monitor = MemoryMonitor(config)
        self.logger = logging.getLogger(__name__)
        
        # GC 카운터
        self._gc_counter = 0
        
        # 약한 참조 추적
        self._tracked_objects = weakref.WeakSet()
        
        # 메모리 최적화 콜백 등록
        self.monitor.add_warning_callback(self._on_memory_warning)
        self.monitor.add_critical_callback(self._on_memory_critical)
    
    def chunk_iterator(
        self, 
        data: List[Any], 
        chunk_size: Optional[int] = None
    ) -> Iterator[List[Any]]:
        """메모리 효율적인 청크 이터레이터"""
        
        if chunk_size is None:
            chunk_size = self._calculate_adaptive_chunk_size(len(data))
        
        self.logger.debug(f"청크 크기: {chunk_size}, 총 항목: {len(data)}")
        
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i + chunk_size]
            yield chunk
            
            # 메모리 체크 및 GC
            self._maybe_collect_garbage()
    
    def streaming_processor(
        self,
        data_source: Iterator[Any],
        processor: Callable[[Any], Any],
        batch_size: int = None
    ) -> Iterator[Any]:
        """스트리밍 데이터 처리기"""
        
        batch_size = batch_size or self.config.default_chunk_size
        batch = []
        
        for item in data_source:
            batch.append(item)
            
            if len(batch) >= batch_size:
                # 배치 처리
                for processed_item in self._process_batch(batch, processor):
                    yield processed_item
                
                batch.clear()
                self._maybe_collect_garbage()
        
        # 마지막 배치 처리
        if batch:
            for processed_item in self._process_batch(batch, processor):
                yield processed_item
    
    def _process_batch(self, batch: List[Any], processor: Callable[[Any], Any]) -> Iterator[Any]:
        """배치 처리"""
        for item in batch:
            try:
                result = processor(item)
                if result is not None:
                    yield result
            except Exception as e:
                self.logger.error(f"배치 처리 오류: {e}")
                continue
    
    def _calculate_adaptive_chunk_size(self, total_items: int) -> int:
        """적응형 청크 크기 계산"""
        if not self.config.adaptive_chunking:
            return self.config.default_chunk_size
        
        # 현재 메모리 상태에 따라 청크 크기 조정
        memory_status = self.monitor.check_memory_threshold()
        base_chunk_size = self.config.default_chunk_size
        
        if memory_status == 'critical':
            # 위험 상태: 청크 크기 50% 감소
            chunk_size = max(10, base_chunk_size // 2)
        elif memory_status == 'warning':
            # 경고 상태: 청크 크기 25% 감소
            chunk_size = max(20, int(base_chunk_size * 0.75))
        else:
            # 정상 상태: 기본 크기 또는 약간 증가
            chunk_size = min(total_items, base_chunk_size)
        
        return chunk_size
    
    def _maybe_collect_garbage(self):
        """조건부 가비지 컬렉션"""
        if not self.config.auto_gc:
            return
        
        self._gc_counter += 1
        if self._gc_counter >= self.config.gc_frequency:
            self._force_garbage_collection()
            self._gc_counter = 0
    
    def _force_garbage_collection(self):
        """강제 가비지 컬렉션"""
        before_mb = self.monitor.get_current_memory_mb()
        
        # 모든 세대 GC 실행
        collected = gc.collect()
        
        after_mb = self.monitor.get_current_memory_mb()
        freed_mb = before_mb - after_mb
        
        self.monitor.stats['gc_collections'] += 1
        
        if freed_mb > 5:  # 5MB 이상 해제된 경우만 로그
            self.logger.info(
                f"가비지 컬렉션 완료: {collected}개 객체, "
                f"{freed_mb:.1f}MB 해제 ({before_mb:.1f}MB → {after_mb:.1f}MB)"
            )
    
    def _on_memory_warning(self, memory_mb: float):
        """메모리 경고 처리"""
        # 가비지 컬렉션 실행
        self._force_garbage_collection()
        
        # 추가 최적화 수행
        self._cleanup_tracked_objects()
    
    def _on_memory_critical(self, memory_mb: float):
        """메모리 위험 처리"""
        # 강제 가비지 컬렉션
        self._force_garbage_collection()
        
        # 추적된 객체 정리
        self._cleanup_tracked_objects()
        
        # 메모리 상태 로깅
        self.logger.error(f"메모리 사용량이 위험 수준에 도달했습니다: {memory_mb:.1f}MB")
    
    def _cleanup_tracked_objects(self):
        """추적된 객체 정리"""
        # WeakSet이므로 자동으로 정리되지만, 명시적으로 GC 유도
        try:
            # 약한 참조 객체들 정리
            for obj in list(self._tracked_objects):
                if hasattr(obj, 'cleanup'):
                    obj.cleanup()
        except Exception as e:
            self.logger.error(f"추적 객체 정리 오류: {e}")
    
    def track_object(self, obj: Any):
        """객체 추적 추가"""
        self._tracked_objects.add(obj)
    
    @contextmanager
    def memory_context(self, context_name: str = "operation"):
        """메모리 사용량 추적 컨텍스트"""
        start_mb = self.monitor.get_current_memory_mb()
        self.logger.debug(f"{context_name} 시작: {start_mb:.1f}MB")
        
        try:
            yield self
        finally:
            end_mb = self.monitor.get_current_memory_mb()
            used_mb = end_mb - start_mb
            
            if used_mb > 0:
                self.logger.debug(f"{context_name} 완료: {end_mb:.1f}MB (+{used_mb:.1f}MB)")
            else:
                self.logger.debug(f"{context_name} 완료: {end_mb:.1f}MB ({used_mb:.1f}MB)")
    
    def start_monitoring(self):
        """메모리 모니터링 시작"""
        self.monitor.start_monitoring()
    
    def stop_monitoring(self):
        """메모리 모니터링 중지"""
        self.monitor.stop_monitoring()
    
    def get_memory_report(self) -> Dict[str, Any]:
        """메모리 사용량 보고서"""
        stats = self.monitor.get_memory_stats()
        
        return {
            'memory_stats': stats,
            'gc_stats': {
                'collections': self.monitor.stats['gc_collections'],
                'tracked_objects': len(self._tracked_objects)
            },
            'recommendations': self._get_optimization_recommendations(stats)
        }
    
    def _get_optimization_recommendations(self, stats: Dict[str, Any]) -> List[str]:
        """최적화 권장사항"""
        recommendations = []
        
        if stats['current_memory_mb'] > stats['warning_threshold_mb']:
            recommendations.append("메모리 사용량이 높습니다. 청크 크기를 줄이세요.")
        
        if stats['gc_collections'] > 100:
            recommendations.append("GC 빈도가 높습니다. 객체 생성을 줄이세요.")
        
        if stats['critical_count'] > 0:
            recommendations.append("메모리 위험 상황이 발생했습니다. 배치 크기를 줄이세요.")
        
        if not recommendations:
            recommendations.append("메모리 사용량이 최적화되어 있습니다.")
        
        return recommendations


# 싱글톤 인스턴스
_memory_optimizer = None


def get_memory_optimizer(config: MemoryConfig = None) -> MemoryOptimizer:
    """메모리 최적화기 인스턴스 반환"""
    global _memory_optimizer
    if _memory_optimizer is None:
        _memory_optimizer = MemoryOptimizer(config)
    return _memory_optimizer


def reset_memory_optimizer():
    """메모리 최적화기 리셋"""
    global _memory_optimizer
    if _memory_optimizer:
        _memory_optimizer.stop_monitoring()
        _memory_optimizer = None