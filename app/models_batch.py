"""
배치 작업 관련 모델
batch API에서 사용하는 모델들
"""

from datetime import datetime
from sqlalchemy import Column, String, Float, Integer, DateTime, Text, ForeignKey, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import func
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class BatchJobExecution(Base):
    """
    배치 작업 실행 내역 테이블
    사용처: weather-flick-batch
    설명: 배치 API에서 관리하는 배치 작업 실행 내역
    """
    __tablename__ = "batch_job_executions"
    
    # Primary Key
    id = Column(String, primary_key=True, index=True)
    
    # 작업 정보
    job_type = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, index=True, default="PENDING")
    parameters = Column(JSONB)
    
    # 진행 상황
    progress = Column(Float, default=0.0)
    current_step = Column(String)
    total_steps = Column(Integer)
    
    # 실행 정보
    created_at = Column(DateTime, server_default=func.now(), index=True)
    created_by = Column(String, nullable=False, default="system")
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    
    # 결과 정보
    error_message = Column(Text)
    result_summary = Column(JSONB)
    
    # 재시도 정보
    retry_status = Column(String)  # 재시도 상태
    retry_count = Column(Integer, default=0)  # 재시도 횟수
    
    # 인덱스
    __table_args__ = (
        # 복합 인덱스 추가 가능
    )


class BatchJobDetail(Base):
    """
    배치 작업 상세 로그 테이블
    사용처: weather-flick-batch
    설명: 배치 작업 실행 중 발생하는 상세 로그
    """
    __tablename__ = "batch_job_details"
    
    # Primary Key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Foreign Key
    job_id = Column(String, ForeignKey("batch_job_executions.id", ondelete="CASCADE"), nullable=False, index=True)
    
    # 로그 정보
    log_level = Column(String, nullable=False, default="INFO")  # INFO, WARNING, ERROR
    message = Column(Text)
    details = Column(JSONB)
    
    # 시간 정보
    created_at = Column(DateTime, server_default=func.now(), index=True)
    
    # 인덱스
    __table_args__ = (
        # job_id와 created_at 복합 인덱스
    )