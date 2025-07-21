"""
배치 시스템용 타임존 처리 유틸리티
외부 API 연동, 데이터 수집, 스케줄링 등에 특화된 시간 처리
"""

import pytz
from datetime import datetime, timezone, timedelta
from typing import Optional, Union, Dict, Any
import logging

logger = logging.getLogger(__name__)

# 한국 표준시 타임존
KST = pytz.timezone('Asia/Seoul')
UTC = pytz.UTC


class BatchTimezoneUtils:
    """배치 시스템을 위한 타임존 유틸리티 클래스"""
    
    @staticmethod
    def get_collection_timestamp() -> datetime:
        """데이터 수집 시점의 UTC 타임스탬프"""
        return datetime.now(timezone.utc)
    
    @staticmethod
    def get_kst_date_for_api() -> str:
        """외부 API용 KST 기준 날짜 (YYYYMMDD 형식)"""
        kst_now = datetime.now(KST)
        return kst_now.strftime('%Y%m%d')
    
    @staticmethod
    def get_kst_date_range_for_api(days_ahead: int = 7) -> list:
        """외부 API용 KST 기준 날짜 범위"""
        kst_now = datetime.now(KST)
        date_range = []
        
        for i in range(days_ahead):
            date = kst_now + timedelta(days=i)
            date_range.append(date.strftime('%Y%m%d'))
        
        return date_range
    
    @staticmethod
    def format_api_date(dt: datetime) -> str:
        """외부 API용 날짜 형식 (YYYYMMDD)"""
        if dt.tzinfo is None:
            # naive datetime을 KST로 가정
            dt = KST.localize(dt)
        
        # KST 기준으로 날짜 계산
        kst_dt = dt.astimezone(KST)
        return kst_dt.strftime('%Y%m%d')
    
    @staticmethod
    def parse_api_timestamp(timestamp: Union[int, str], source_tz: str = 'UTC') -> datetime:
        """
        외부 API 타임스탬프를 UTC datetime으로 변환
        
        Args:
            timestamp: Unix 타임스탬프 또는 문자열
            source_tz: 소스 타임존 (기본값: UTC)
        
        Returns:
            UTC datetime 객체
        """
        try:
            if isinstance(timestamp, str):
                timestamp = int(timestamp)
            
            # Unix 타임스탬프를 UTC datetime으로 변환
            dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
            
            return dt
            
        except (ValueError, TypeError) as e:
            logger.error(f"타임스탬프 파싱 오류: {timestamp}, 오류: {e}")
            return datetime.now(timezone.utc)
    
    @staticmethod
    def create_batch_log_entry(
        job_name: str, 
        status: str, 
        details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """배치 작업 로그 엔트리 생성"""
        
        current_time = BatchTimezoneUtils.get_collection_timestamp()
        
        log_entry = {
            "job_name": job_name,
            "status": status,
            "timestamp_utc": current_time.isoformat(),
            "timestamp_kst": current_time.astimezone(KST).isoformat(),
            "date_kst": current_time.astimezone(KST).strftime('%Y-%m-%d'),
            "details": details or {}
        }
        
        return log_entry
    
    @staticmethod
    def calculate_next_run_time(
        schedule_expression: str, 
        base_time: Optional[datetime] = None
    ) -> datetime:
        """
        스케줄 표현식을 기반으로 다음 실행 시간 계산
        
        Args:
            schedule_expression: 스케줄 표현식 (예: "daily_09:00", "hourly", "weekly_mon_14:00")
            base_time: 기준 시간 (기본값: 현재 KST 시간)
        
        Returns:
            다음 실행 시간 (UTC)
        """
        if base_time is None:
            base_time = datetime.now(KST)
        elif base_time.tzinfo is None:
            base_time = KST.localize(base_time)
        else:
            base_time = base_time.astimezone(KST)
        
        try:
            if schedule_expression.startswith('daily_'):
                # 예: "daily_09:00"
                time_part = schedule_expression.split('_')[1]
                hour, minute = map(int, time_part.split(':'))
                
                next_run = base_time.replace(hour=hour, minute=minute, second=0, microsecond=0)
                
                # 이미 지난 시간이면 다음 날로 설정
                if next_run <= base_time:
                    next_run += timedelta(days=1)
                    
            elif schedule_expression == 'hourly':
                # 매시간 정각
                next_run = base_time.replace(minute=0, second=0, microsecond=0)
                next_run += timedelta(hours=1)
                
            elif schedule_expression.startswith('weekly_'):
                # 예: "weekly_mon_14:00"
                parts = schedule_expression.split('_')
                day_name = parts[1]
                time_part = parts[2]
                
                # 요일 매핑
                day_mapping = {
                    'mon': 0, 'tue': 1, 'wed': 2, 'thu': 3,
                    'fri': 4, 'sat': 5, 'sun': 6
                }
                
                target_weekday = day_mapping.get(day_name, 0)
                hour, minute = map(int, time_part.split(':'))
                
                # 이번 주 해당 요일 계산
                days_ahead = target_weekday - base_time.weekday()
                if days_ahead <= 0:  # 이미 지났으면 다음 주
                    days_ahead += 7
                
                next_run = base_time + timedelta(days=days_ahead)
                next_run = next_run.replace(hour=hour, minute=minute, second=0, microsecond=0)
                
            else:
                # 기본값: 1시간 후
                logger.warning(f"알 수 없는 스케줄 표현식: {schedule_expression}")
                next_run = base_time + timedelta(hours=1)
            
            # UTC로 변환하여 반환
            return next_run.astimezone(timezone.utc)
            
        except Exception as e:
            logger.error(f"스케줄 계산 오류: {schedule_expression}, 오류: {e}")
            # 오류 시 1시간 후로 설정
            return (base_time + timedelta(hours=1)).astimezone(timezone.utc)
    
    @staticmethod
    def is_business_hours(dt: Optional[datetime] = None) -> bool:
        """업무 시간인지 확인 (KST 기준 09:00-18:00)"""
        if dt is None:
            dt = datetime.now(KST)
        elif dt.tzinfo is None:
            dt = KST.localize(dt)
        else:
            dt = dt.astimezone(KST)
        
        # 주말 제외
        if dt.weekday() >= 5:  # 토요일(5), 일요일(6)
            return False
        
        # 업무 시간 확인
        return 9 <= dt.hour < 18
    
    @staticmethod
    def get_safe_api_call_time() -> datetime:
        """
        API 호출에 안전한 시간 반환
        업무 시간을 피해서 새벽 시간대 반환
        """
        kst_now = datetime.now(KST)
        
        # 새벽 3시로 설정 (API 서버 부하가 적은 시간)
        safe_time = kst_now.replace(hour=3, minute=0, second=0, microsecond=0)
        
        # 이미 지난 시간이면 다음 날로
        if safe_time <= kst_now:
            safe_time += timedelta(days=1)
        
        return safe_time.astimezone(timezone.utc)
    
    @staticmethod
    def format_duration(start_time: datetime, end_time: datetime) -> str:
        """실행 시간을 사람이 읽기 쉬운 형식으로 포맷팅"""
        
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        
        duration = end_time - start_time
        total_seconds = duration.total_seconds()
        
        if total_seconds < 60:
            return f"{total_seconds:.1f}초"
        elif total_seconds < 3600:
            minutes = total_seconds / 60
            return f"{minutes:.1f}분"
        else:
            hours = total_seconds / 3600
            return f"{hours:.1f}시간"


class ExternalApiTimezoneHelper:
    """외부 API 연동을 위한 타임존 헬퍼"""
    
    @staticmethod
    def format_for_kma_api(dt: Optional[datetime] = None) -> dict:
        """기상청 API용 날짜/시간 포맷"""
        if dt is None:
            dt = datetime.now(KST)
        elif dt.tzinfo is None:
            dt = KST.localize(dt)
        else:
            dt = dt.astimezone(KST)
        
        return {
            "base_date": dt.strftime('%Y%m%d'),
            "base_time": dt.strftime('%H%M'),
            "fcst_date": dt.strftime('%Y%m%d'),
            "fcst_time": dt.strftime('%H%M')
        }
    
    @staticmethod
    def format_for_tour_api(dt: Optional[datetime] = None) -> dict:
        """한국관광공사 API용 날짜/시간 포맷"""
        if dt is None:
            dt = datetime.now(KST)
        elif dt.tzinfo is None:
            dt = KST.localize(dt)
        else:
            dt = dt.astimezone(KST)
        
        return {
            "eventStartDate": dt.strftime('%Y%m%d'),
            "eventEndDate": (dt + timedelta(days=30)).strftime('%Y%m%d'),
            "modifiedtime": dt.strftime('%Y%m%d')
        }
    
    @staticmethod
    def standardize_api_response_time(response_time: Any, source_format: str = 'timestamp') -> datetime:
        """외부 API 응답 시간을 UTC datetime으로 표준화"""
        
        try:
            if source_format == 'timestamp':
                # Unix 타임스탬프
                if isinstance(response_time, str):
                    response_time = int(response_time)
                return datetime.fromtimestamp(response_time, tz=timezone.utc)
            
            elif source_format == 'kma_datetime':
                # 기상청 API 형식: "20250120 1400"
                if isinstance(response_time, str):
                    if len(response_time) == 13:  # "20250120 1400"
                        date_part = response_time[:8]
                        time_part = response_time[9:]
                        dt_str = f"{date_part} {time_part}"
                        dt = datetime.strptime(dt_str, '%Y%m%d %H%M')
                        # KST로 가정하고 UTC로 변환
                        kst_dt = KST.localize(dt)
                        return kst_dt.astimezone(timezone.utc)
            
            elif source_format == 'iso_string':
                # ISO 형식 문자열
                if isinstance(response_time, str):
                    return datetime.fromisoformat(response_time.replace('Z', '+00:00'))
            
            else:
                logger.warning(f"알 수 없는 시간 형식: {source_format}")
                return datetime.now(timezone.utc)
        
        except Exception as e:
            logger.error(f"API 응답 시간 파싱 오류: {response_time}, 형식: {source_format}, 오류: {e}")
            return datetime.now(timezone.utc)


# 배치 작업 로깅 데코레이터
def log_batch_execution(job_name: str):
    """배치 작업 실행을 로깅하는 데코레이터"""
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = BatchTimezoneUtils.get_collection_timestamp()
            
            try:
                logger.info(f"[배치 시작] {job_name} - 시작 시간: {start_time.isoformat()}")
                
                result = func(*args, **kwargs)
                
                end_time = BatchTimezoneUtils.get_collection_timestamp()
                duration = BatchTimezoneUtils.format_duration(start_time, end_time)
                
                logger.info(f"[배치 완료] {job_name} - 완료 시간: {end_time.isoformat()}, 소요시간: {duration}")
                
                return result
                
            except Exception as e:
                end_time = BatchTimezoneUtils.get_collection_timestamp()
                duration = BatchTimezoneUtils.format_duration(start_time, end_time)
                
                logger.error(f"[배치 오류] {job_name} - 오류 시간: {end_time.isoformat()}, 소요시간: {duration}, 오류: {str(e)}")
                
                raise
        
        return wrapper
    return decorator


# 스케줄러 설정을 위한 헬퍼 함수들
def get_scheduler_timezone() -> str:
    """스케줄러 타임존 반환"""
    return "Asia/Seoul"


def create_cron_expression(hour: int, minute: int = 0) -> str:
    """KST 기준 크론 표현식 생성"""
    return f"{minute} {hour} * * *"


def get_batch_job_schedule_config() -> dict:
    """배치 작업 스케줄 설정 반환"""
    return {
        "timezone": get_scheduler_timezone(),
        "weather_update": create_cron_expression(6, 0),    # 매일 오전 6시
        "tour_data_sync": create_cron_expression(3, 30),   # 매일 오전 3시 30분
        "cleanup_old_data": create_cron_expression(2, 0),  # 매일 오전 2시
        "health_check": create_cron_expression(0, 0),      # 매시 정각
    }