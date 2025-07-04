"""
스마트 API 스케줄러

API 한도 초과 시 다른 API를 우선 실행하는 지능형 스케줄링 시스템
"""

import time
import threading
from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass
from queue import PriorityQueue
import logging

from app.core.logger import get_logger


class APIStatus(Enum):
    """API 상태"""

    AVAILABLE = "available"
    RATE_LIMITED = "rate_limited"
    ERROR = "error"
    MAINTENANCE = "maintenance"


@dataclass
class APITask:
    """API 작업 정의"""

    api_name: str
    task_func: Callable
    priority: int  # 낮을수록 높은 우선순위
    retry_count: int = 0
    max_retries: int = 3
    delay_seconds: int = 0
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now()

    def __lt__(self, other):
        # 우선순위 큐를 위한 비교 연산자
        if self.priority != other.priority:
            return self.priority < other.priority
        return self.created_at < other.created_at


class SmartAPIScheduler:
    """스마트 API 스케줄러"""

    def __init__(self):
        self.logger = get_logger(__name__)

        # API 상태 추적
        self.api_status: Dict[str, APIStatus] = {}
        self.api_last_error: Dict[str, datetime] = {}
        self.api_rate_limit_reset: Dict[str, datetime] = {}
        self.api_retry_delays: Dict[str, int] = {}

        # 작업 큐
        self.task_queue = PriorityQueue()
        self.running_tasks: Dict[str, threading.Thread] = {}

        # 스케줄러 설정
        self.max_concurrent_tasks = 3
        self.rate_limit_recovery_time = 300  # 5분
        self.error_recovery_time = 60  # 1분

        # 실행 상태
        self.is_running = False
        self.scheduler_thread = None

        self.logger.info("스마트 API 스케줄러 초기화 완료")

    def add_task(
        self,
        api_name: str,
        task_func: Callable,
        priority: int = 5,
        max_retries: int = 3,
    ) -> bool:
        """작업 추가"""
        try:
            task = APITask(
                api_name=api_name,
                task_func=task_func,
                priority=priority,
                max_retries=max_retries,
            )

            self.task_queue.put(task)
            self.logger.info(f"작업 추가됨: {api_name} (우선순위: {priority})")
            return True

        except Exception as e:
            self.logger.error(f"작업 추가 실패: {e}")
            return False

    def set_api_status(
        self, api_name: str, status: APIStatus, reset_time: Optional[datetime] = None
    ):
        """API 상태 설정"""
        self.api_status[api_name] = status

        if status == APIStatus.RATE_LIMITED:
            if reset_time:
                self.api_rate_limit_reset[api_name] = reset_time
            else:
                # 기본 5분 후 복구
                self.api_rate_limit_reset[api_name] = datetime.now() + timedelta(
                    seconds=self.rate_limit_recovery_time
                )
            self.logger.warning(
                f"API 한도 초과: {api_name}, 복구 시간: {self.api_rate_limit_reset[api_name]}"
            )

        elif status == APIStatus.ERROR:
            self.api_last_error[api_name] = datetime.now()
            self.logger.error(f"API 오류 상태: {api_name}")

        self.logger.info(f"API 상태 변경: {api_name} -> {status.value}")

    def is_api_available(self, api_name: str) -> bool:
        """API 사용 가능 여부 확인"""
        status = self.api_status.get(api_name, APIStatus.AVAILABLE)

        if status == APIStatus.AVAILABLE:
            return True

        if status == APIStatus.RATE_LIMITED:
            reset_time = self.api_rate_limit_reset.get(api_name)
            if reset_time and datetime.now() >= reset_time:
                self.api_status[api_name] = APIStatus.AVAILABLE
                self.logger.info(f"API 한도 제한 해제: {api_name}")
                return True
            return False

        if status == APIStatus.ERROR:
            last_error = self.api_last_error.get(api_name)
            if last_error and datetime.now() >= last_error + timedelta(
                seconds=self.error_recovery_time
            ):
                self.api_status[api_name] = APIStatus.AVAILABLE
                self.logger.info(f"API 오류 상태 해제: {api_name}")
                return True
            return False

        return False

    def get_next_available_task(self) -> Optional[APITask]:
        """다음 실행 가능한 작업 조회"""
        temp_tasks = []

        while not self.task_queue.empty():
            task = self.task_queue.get()

            if self.is_api_available(task.api_name):
                # 실행 가능한 작업 발견
                for temp_task in temp_tasks:
                    self.task_queue.put(temp_task)
                return task
            else:
                temp_tasks.append(task)

        # 실행 가능한 작업이 없으면 모든 작업을 다시 큐에 넣음
        for temp_task in temp_tasks:
            self.task_queue.put(temp_task)

        return None

    def execute_task(self, task: APITask):
        """작업 실행"""
        try:
            self.logger.info(f"작업 실행 시작: {task.api_name}")

            # 작업 실행
            result = task.task_func()

            if result:
                self.logger.info(f"작업 실행 완료: {task.api_name}")
                # 성공 시 API 상태를 정상으로 설정
                self.set_api_status(task.api_name, APIStatus.AVAILABLE)
            else:
                raise Exception("작업 실행 결과가 False")

        except Exception as e:
            self.logger.error(f"작업 실행 실패: {task.api_name}, 오류: {e}")

            # 특정 오류 패턴에 따른 처리
            error_msg = str(e).lower()

            if any(
                keyword in error_msg
                for keyword in ["rate limit", "quota", "limit exceed"]
            ):
                # API 한도 초과
                self.set_api_status(task.api_name, APIStatus.RATE_LIMITED)
            else:
                # 일반 오류
                self.set_api_status(task.api_name, APIStatus.ERROR)

            # 재시도 로직
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.delay_seconds = min(60 * task.retry_count, 300)  # 최대 5분

                self.logger.info(
                    f"작업 재시도 예약: {task.api_name} "
                    f"({task.retry_count}/{task.max_retries}), "
                    f"지연: {task.delay_seconds}초"
                )

                # 지연 후 재추가
                threading.Timer(
                    task.delay_seconds, lambda: self.task_queue.put(task)
                ).start()
            else:
                self.logger.error(f"작업 최대 재시도 초과: {task.api_name}")

        finally:
            # 실행 중인 작업 목록에서 제거
            if task.api_name in self.running_tasks:
                del self.running_tasks[task.api_name]

    def scheduler_loop(self):
        """스케줄러 메인 루프"""
        self.logger.info("스마트 스케줄러 루프 시작")

        while self.is_running:
            try:
                # 현재 실행 중인 작업 수 확인
                if len(self.running_tasks) >= self.max_concurrent_tasks:
                    time.sleep(1)
                    continue

                # 다음 실행 가능한 작업 조회
                next_task = self.get_next_available_task()

                if next_task is None:
                    # 실행 가능한 작업이 없음
                    time.sleep(5)
                    continue

                # 작업 실행
                task_thread = threading.Thread(
                    target=self.execute_task,
                    args=(next_task,),
                    name=f"API-{next_task.api_name}",
                )

                self.running_tasks[next_task.api_name] = task_thread
                task_thread.start()

                # 짧은 지연으로 다음 작업 처리
                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"스케줄러 루프 오류: {e}")
                time.sleep(5)

        self.logger.info("스마트 스케줄러 루프 종료")

    def start(self):
        """스케줄러 시작"""
        if self.is_running:
            self.logger.warning("스케줄러가 이미 실행 중입니다")
            return

        self.is_running = True
        self.scheduler_thread = threading.Thread(
            target=self.scheduler_loop, name="SmartAPIScheduler"
        )
        self.scheduler_thread.start()

        self.logger.info("스마트 API 스케줄러 시작됨")

    def stop(self):
        """스케줄러 중지"""
        if not self.is_running:
            return

        self.is_running = False

        # 실행 중인 작업들이 완료될 때까지 대기
        for task_name, thread in self.running_tasks.items():
            self.logger.info(f"작업 완료 대기 중: {task_name}")
            thread.join(timeout=30)

        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=10)

        self.logger.info("스마트 API 스케줄러 중지됨")

    def get_status_report(self) -> Dict:
        """상태 보고서 생성"""
        return {
            "is_running": self.is_running,
            "task_queue_size": self.task_queue.qsize(),
            "running_tasks": list(self.running_tasks.keys()),
            "api_status": {
                name: status.value for name, status in self.api_status.items()
            },
            "rate_limit_resets": {
                name: reset_time.isoformat()
                for name, reset_time in self.api_rate_limit_reset.items()
            },
        }
