"""
API 작업 관리자

스마트 스케줄러를 사용하여 API 호출을 관리하는 고수준 인터페이스
"""

import time
from typing import Dict, List, Optional, Callable
from datetime import datetime
import threading

from app.core.smart_scheduler import SmartAPIScheduler, APIStatus
from app.core.logger import get_logger


class APITaskManager:
    """API 작업 관리자"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.smart_scheduler = SmartAPIScheduler()
        
        # API별 우선순위 설정
        self.api_priorities = {
            'kto_api': 1,          # 한국관광공사 API (높은 우선순위)
            'weather_api': 2,      # 기상청 API
            'database': 3,         # 데이터베이스 작업
            'backup': 4,           # 백업 작업
            'cleanup': 5           # 정리 작업
        }
        
        # 작업 결과 저장
        self.task_results: Dict[str, any] = {}
        self.task_lock = threading.Lock()
        
        self.logger.info("API 작업 관리자 초기화 완료")
    
    def start(self):
        """작업 관리자 시작"""
        self.smart_scheduler.start()
        self.logger.info("API 작업 관리자 시작됨")
    
    def stop(self):
        """작업 관리자 중지"""
        self.smart_scheduler.stop()
        self.logger.info("API 작업 관리자 중지됨")
    
    def add_kto_task(self, task_func: Callable, task_name: str = None) -> bool:
        """한국관광공사 API 작업 추가"""
        task_name = task_name or f"kto_task_{int(time.time())}"
        
        def wrapped_task():
            try:
                result = task_func()
                with self.task_lock:
                    self.task_results[task_name] = result
                return result
            except Exception as e:
                self.logger.error(f"KTO API 작업 실패 ({task_name}): {e}")
                
                # API 한도 초과 감지
                if self._is_rate_limit_error(str(e)):
                    self.smart_scheduler.set_api_status('kto_api', APIStatus.RATE_LIMITED)
                    self.logger.warning("KTO API 한도 초과 감지 - 다른 작업을 우선 실행합니다")
                else:
                    self.smart_scheduler.set_api_status('kto_api', APIStatus.ERROR)
                
                with self.task_lock:
                    self.task_results[task_name] = None
                return False
        
        return self.smart_scheduler.add_task(
            api_name='kto_api',
            task_func=wrapped_task,
            priority=self.api_priorities['kto_api']
        )
    
    def add_weather_task(self, task_func: Callable, task_name: str = None) -> bool:
        """기상청 API 작업 추가"""
        task_name = task_name or f"weather_task_{int(time.time())}"
        
        def wrapped_task():
            try:
                result = task_func()
                with self.task_lock:
                    self.task_results[task_name] = result
                return result
            except Exception as e:
                self.logger.error(f"기상청 API 작업 실패 ({task_name}): {e}")
                
                # API 한도 초과 감지
                if self._is_rate_limit_error(str(e)):
                    self.smart_scheduler.set_api_status('weather_api', APIStatus.RATE_LIMITED)
                    self.logger.warning("기상청 API 한도 초과 감지 - 다른 작업을 우선 실행합니다")
                else:
                    self.smart_scheduler.set_api_status('weather_api', APIStatus.ERROR)
                
                with self.task_lock:
                    self.task_results[task_name] = None
                return False
        
        return self.smart_scheduler.add_task(
            api_name='weather_api',
            task_func=wrapped_task,
            priority=self.api_priorities['weather_api']
        )
    
    def add_database_task(self, task_func: Callable, task_name: str = None) -> bool:
        """데이터베이스 작업 추가"""
        task_name = task_name or f"db_task_{int(time.time())}"
        
        def wrapped_task():
            try:
                result = task_func()
                with self.task_lock:
                    self.task_results[task_name] = result
                return result
            except Exception as e:
                self.logger.error(f"데이터베이스 작업 실패 ({task_name}): {e}")
                self.smart_scheduler.set_api_status('database', APIStatus.ERROR)
                
                with self.task_lock:
                    self.task_results[task_name] = None
                return False
        
        return self.smart_scheduler.add_task(
            api_name='database',
            task_func=wrapped_task,
            priority=self.api_priorities['database']
        )
    
    def add_maintenance_task(self, task_func: Callable, task_type: str = 'cleanup', 
                           task_name: str = None) -> bool:
        """유지보수 작업 추가 (백업, 정리 등)"""
        task_name = task_name or f"{task_type}_task_{int(time.time())}"
        priority = self.api_priorities.get(task_type, 5)
        
        def wrapped_task():
            try:
                result = task_func()
                with self.task_lock:
                    self.task_results[task_name] = result
                return result
            except Exception as e:
                self.logger.error(f"유지보수 작업 실패 ({task_name}): {e}")
                
                with self.task_lock:
                    self.task_results[task_name] = None
                return False
        
        return self.smart_scheduler.add_task(
            api_name=task_type,
            task_func=wrapped_task,
            priority=priority
        )
    
    def _is_rate_limit_error(self, error_message: str) -> bool:
        """API 한도 초과 오류인지 확인"""
        rate_limit_keywords = [
            'rate limit',
            'quota exceeded',
            'too many requests',
            'limit exceeded',
            '429',
            'throttle',
            'rate exceeded'
        ]
        
        error_lower = error_message.lower()
        return any(keyword in error_lower for keyword in rate_limit_keywords)
    
    def get_task_result(self, task_name: str) -> any:
        """작업 결과 조회"""
        with self.task_lock:
            return self.task_results.get(task_name)
    
    def wait_for_task(self, task_name: str, timeout: int = 300) -> any:
        """작업 완료 대기"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            result = self.get_task_result(task_name)
            if result is not None:
                return result
            time.sleep(1)
        
        self.logger.warning(f"작업 대기 시간 초과: {task_name}")
        return None
    
    def get_api_status(self, api_name: str) -> str:
        """API 상태 조회"""
        status = self.smart_scheduler.api_status.get(api_name, APIStatus.AVAILABLE)
        return status.value
    
    def is_api_available(self, api_name: str) -> bool:
        """API 사용 가능 여부 확인"""
        return self.smart_scheduler.is_api_available(api_name)
    
    def get_status_report(self) -> Dict:
        """전체 상태 보고서"""
        scheduler_status = self.smart_scheduler.get_status_report()
        
        with self.task_lock:
            task_count = len(self.task_results)
            completed_tasks = sum(1 for result in self.task_results.values() if result is not None)
        
        return {
            **scheduler_status,
            'total_tasks_executed': task_count,
            'completed_tasks': completed_tasks,
            'failed_tasks': task_count - completed_tasks,
            'api_priorities': self.api_priorities
        }
    
    def force_reset_api(self, api_name: str):
        """API 상태 강제 리셋"""
        self.smart_scheduler.set_api_status(api_name, APIStatus.AVAILABLE)
        self.logger.info(f"API 상태 강제 리셋: {api_name}")
    
    def get_queue_info(self) -> Dict:
        """큐 정보 조회"""
        return {
            'queue_size': self.smart_scheduler.task_queue.qsize(),
            'running_tasks': len(self.smart_scheduler.running_tasks),
            'max_concurrent': self.smart_scheduler.max_concurrent_tasks
        }


# 전역 인스턴스
_task_manager_instance = None
_task_manager_lock = threading.Lock()


def get_task_manager() -> APITaskManager:
    """API 작업 관리자 싱글톤 인스턴스 반환"""
    global _task_manager_instance
    
    if _task_manager_instance is None:
        with _task_manager_lock:
            if _task_manager_instance is None:
                _task_manager_instance = APITaskManager()
    
    return _task_manager_instance