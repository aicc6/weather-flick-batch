"""
동시 API 호출 관리자

API 호출 병렬 처리 및 속도 제한을 효율적으로 관리합니다.
"""

import asyncio
import time
import logging
from typing import List, Dict, Callable
from dataclasses import dataclass
from enum import Enum
from datetime import datetime

from app.core.multi_api_key_manager import get_api_key_manager, APIProvider


class APICallPriority(Enum):
    """API 호출 우선순위"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


@dataclass
class APICallTask:
    """API 호출 작업 정의"""
    
    task_id: str
    api_provider: APIProvider
    endpoint: str
    params: Dict
    callback: Callable
    priority: APICallPriority = APICallPriority.MEDIUM
    max_retries: int = 3
    retry_delay: float = 1.0
    timeout: float = 30.0
    
    def __post_init__(self):
        self.created_at = datetime.utcnow()
        self.attempts = 0
        self.last_error = None


@dataclass
class ConcurrencyConfig:
    """동시 실행 설정"""
    
    # API 제공자별 동시 실행 제한
    max_concurrent_kto: int = 5        # KTO API 동시 호출 수
    max_concurrent_kma: int = 3        # KMA API 동시 호출 수
    max_concurrent_total: int = 8      # 전체 동시 호출 수
    
    # 속도 제한 설정
    min_delay_between_calls: float = 0.1    # 최소 호출 간격
    adaptive_delay: bool = True             # 적응형 지연 시간
    
    # 배치 처리 설정
    batch_size: int = 50               # 배치당 작업 수
    queue_timeout: int = 300           # 큐 대기 타임아웃 (초)
    
    # 에러 처리 설정
    circuit_breaker_threshold: int = 5  # 연속 실패 임계값
    circuit_breaker_reset_time: int = 60  # 회로 차단기 리셋 시간 (초)


class AdaptiveRateLimiter:
    """적응형 속도 제한기"""
    
    def __init__(self, initial_delay: float = 0.1):
        self.current_delay = initial_delay
        self.min_delay = 0.05
        self.max_delay = 2.0
        self.success_count = 0
        self.error_count = 0
        self.last_adjustment = time.time()
        
    async def wait_and_adjust(self, success: bool, api_provider: APIProvider):
        """성공/실패에 따라 지연 시간 조정"""
        
        if success:
            self.success_count += 1
            self.error_count = 0
            
            # 연속 성공 시 지연 시간 감소
            if self.success_count >= 5:
                self.current_delay = max(self.min_delay, self.current_delay * 0.9)
                self.success_count = 0
                
        else:
            self.error_count += 1
            self.success_count = 0
            
            # 에러 발생 시 지연 시간 증가
            self.current_delay = min(self.max_delay, self.current_delay * 1.5)
        
        # API 제공자별 추가 지연 (KTO는 더 보수적으로)
        provider_multiplier = 1.2 if api_provider == APIProvider.KTO else 1.0
        actual_delay = self.current_delay * provider_multiplier
        
        await asyncio.sleep(actual_delay)


class CircuitBreaker:
    """회로 차단기 패턴 구현"""
    
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        
    def is_call_allowed(self) -> bool:
        """호출 허용 여부 확인"""
        
        if self.state == "CLOSED":
            return True
            
        if self.state == "OPEN":
            if time.time() - self.last_failure_time >= self.reset_timeout:
                self.state = "HALF_OPEN"
                return True
            return False
            
        # HALF_OPEN 상태에서는 제한적 호출 허용
        return True
    
    def record_success(self):
        """성공 기록"""
        self.failure_count = 0
        self.state = "CLOSED"
    
    def record_failure(self):
        """실패 기록"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = "OPEN"


class ConcurrentAPIManager:
    """동시 API 호출 관리자"""
    
    def __init__(self, config: ConcurrencyConfig = None):
        self.config = config or ConcurrencyConfig()
        self.logger = logging.getLogger(__name__)
        
        # 세마포어 설정
        self.kto_semaphore = asyncio.Semaphore(self.config.max_concurrent_kto)
        self.kma_semaphore = asyncio.Semaphore(self.config.max_concurrent_kma)
        self.total_semaphore = asyncio.Semaphore(self.config.max_concurrent_total)
        
        # 속도 제한기
        self.rate_limiters = {
            APIProvider.KTO: AdaptiveRateLimiter(0.2),
            APIProvider.KMA: AdaptiveRateLimiter(0.1),
        }
        
        # 회로 차단기
        self.circuit_breakers = {
            APIProvider.KTO: CircuitBreaker(self.config.circuit_breaker_threshold, 
                                          self.config.circuit_breaker_reset_time),
            APIProvider.KMA: CircuitBreaker(self.config.circuit_breaker_threshold,
                                          self.config.circuit_breaker_reset_time),
        }
        
        # API 키 매니저
        self.key_manager = get_api_key_manager()
        
        # 통계
        self.stats = {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'circuit_breaker_trips': 0,
            'average_response_time': 0.0,
            'concurrent_peaks': {
                'kto': 0,
                'kma': 0,
                'total': 0
            }
        }
        
    async def execute_single_call(self, task: APICallTask) -> Dict:
        """단일 API 호출 실행"""
        
        provider = task.api_provider
        circuit_breaker = self.circuit_breakers.get(provider)
        
        # 회로 차단기 확인
        if circuit_breaker and not circuit_breaker.is_call_allowed():
            self.logger.warning(f"회로 차단기 작동: {provider.value} API 호출 차단")
            self.stats['circuit_breaker_trips'] += 1
            return {
                'success': False,
                'error': 'Circuit breaker is open',
                'task_id': task.task_id
            }
        
        # 세마포어 획득
        provider_semaphore = (self.kto_semaphore if provider == APIProvider.KTO 
                             else self.kma_semaphore)
        
        async with self.total_semaphore, provider_semaphore:
            # 동시 실행 통계 업데이트
            current_total = self.config.max_concurrent_total - self.total_semaphore._value
            current_provider = (self.config.max_concurrent_kto - self.kto_semaphore._value 
                              if provider == APIProvider.KTO 
                              else self.config.max_concurrent_kma - self.kma_semaphore._value)
            
            self.stats['concurrent_peaks']['total'] = max(
                self.stats['concurrent_peaks']['total'], current_total
            )
            
            if provider == APIProvider.KTO:
                self.stats['concurrent_peaks']['kto'] = max(
                    self.stats['concurrent_peaks']['kto'], current_provider
                )
            else:
                self.stats['concurrent_peaks']['kma'] = max(
                    self.stats['concurrent_peaks']['kma'], current_provider
                )
            
            # 속도 제한 적용
            rate_limiter = self.rate_limiters.get(provider)
            if rate_limiter:
                await rate_limiter.wait_and_adjust(True, provider)  # 초기값 True
            
            # API 호출 실행
            start_time = time.time()
            
            try:
                self.stats['total_calls'] += 1
                
                # 실제 API 호출
                result = await asyncio.wait_for(
                    task.callback(task.endpoint, task.params),
                    timeout=task.timeout
                )
                
                duration = time.time() - start_time
                
                # 성공 처리
                if circuit_breaker:
                    circuit_breaker.record_success()
                
                if rate_limiter:
                    await rate_limiter.wait_and_adjust(True, provider)
                
                self.stats['successful_calls'] += 1
                self._update_average_response_time(duration)
                
                self.logger.debug(f"API 호출 성공: {task.task_id} ({duration:.2f}s)")
                
                return {
                    'success': True,
                    'data': result,
                    'task_id': task.task_id,
                    'duration': duration
                }
                
            except asyncio.TimeoutError:
                self.logger.error(f"API 호출 타임아웃: {task.task_id}")
                error_msg = f"Timeout after {task.timeout}s"
                
            except Exception as e:
                self.logger.error(f"API 호출 실패: {task.task_id} - {e}")
                error_msg = str(e)
            
            # 실패 처리
            duration = time.time() - start_time
            
            if circuit_breaker:
                circuit_breaker.record_failure()
            
            if rate_limiter:
                await rate_limiter.wait_and_adjust(False, provider)
            
            self.stats['failed_calls'] += 1
            self._update_average_response_time(duration)
            
            return {
                'success': False,
                'error': error_msg,
                'task_id': task.task_id,
                'duration': duration
            }
    
    async def execute_batch(self, tasks: List[APICallTask]) -> List[Dict]:
        """배치 API 호출 실행"""
        
        self.logger.info(f"배치 API 호출 시작: {len(tasks)}개 작업")
        
        # 우선순위별 정렬
        sorted_tasks = sorted(tasks, key=lambda t: t.priority.value, reverse=True)
        
        # 비동기 작업 생성
        async_tasks = [self.execute_single_call(task) for task in sorted_tasks]
        
        # 병렬 실행
        start_time = time.time()
        results = await asyncio.gather(*async_tasks, return_exceptions=True)
        total_duration = time.time() - start_time
        
        # 예외 처리된 결과 정리
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    'success': False,
                    'error': str(result),
                    'task_id': sorted_tasks[i].task_id,
                    'duration': 0
                })
            else:
                processed_results.append(result)
        
        # 통계 정보
        successful_count = sum(1 for r in processed_results if r['success'])
        
        self.logger.info(
            f"배치 API 호출 완료: {successful_count}/{len(tasks)} 성공 "
            f"({total_duration:.2f}s, 평균 {total_duration/len(tasks):.2f}s/call)"
        )
        
        return processed_results
    
    def _update_average_response_time(self, duration: float):
        """평균 응답 시간 업데이트"""
        total_calls = self.stats['total_calls']
        current_avg = self.stats['average_response_time']
        
        # 이동 평균 계산
        self.stats['average_response_time'] = (
            (current_avg * (total_calls - 1) + duration) / total_calls
        )
    
    def get_performance_stats(self) -> Dict:
        """성능 통계 조회"""
        
        return {
            'total_calls': self.stats['total_calls'],
            'successful_calls': self.stats['successful_calls'],
            'failed_calls': self.stats['failed_calls'],
            'success_rate': (
                self.stats['successful_calls'] / self.stats['total_calls'] * 100
                if self.stats['total_calls'] > 0 else 0
            ),
            'average_response_time': self.stats['average_response_time'],
            'circuit_breaker_trips': self.stats['circuit_breaker_trips'],
            'concurrent_peaks': self.stats['concurrent_peaks'],
            'rate_limiter_status': {
                provider.value: {
                    'current_delay': limiter.current_delay,
                    'success_count': limiter.success_count,
                    'error_count': limiter.error_count
                }
                for provider, limiter in self.rate_limiters.items()
            },
            'circuit_breaker_status': {
                provider.value: {
                    'state': breaker.state,
                    'failure_count': breaker.failure_count
                }
                for provider, breaker in self.circuit_breakers.items()
            }
        }
    
    def reset_stats(self):
        """통계 초기화"""
        self.stats = {
            'total_calls': 0,
            'successful_calls': 0,
            'failed_calls': 0,
            'circuit_breaker_trips': 0,
            'average_response_time': 0.0,
            'concurrent_peaks': {
                'kto': 0,
                'kma': 0,
                'total': 0
            }
        }


# 싱글톤 인스턴스
_concurrent_api_manager = None


def get_concurrent_api_manager(config: ConcurrencyConfig = None) -> ConcurrentAPIManager:
    """동시 API 호출 관리자 인스턴스 반환"""
    global _concurrent_api_manager
    if _concurrent_api_manager is None:
        _concurrent_api_manager = ConcurrentAPIManager(config)
    return _concurrent_api_manager


def reset_concurrent_api_manager():
    """동시 API 호출 관리자 싱글톤 인스턴스 리셋"""
    global _concurrent_api_manager
    _concurrent_api_manager = None