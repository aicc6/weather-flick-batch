"""
성능 모니터링 관련 테이블 생성 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from app.models import Base
from app.api.config import settings

def create_performance_tables():
    """성능 모니터링 관련 테이블 생성"""
    # 데이터베이스 엔진 생성
    engine = create_engine(settings.DATABASE_URL)
    
    # 테이블 생성
    print("성능 모니터링 관련 테이블 생성 중...")
    Base.metadata.create_all(bind=engine)
    print("테이블 생성 완료!")
    
    # 생성된 테이블 확인
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    print("\n현재 데이터베이스 테이블:")
    for table in sorted(tables):
        print(f"  - {table}")
    
    # 성능 모니터링 관련 테이블만 필터링
    performance_tables = [t for t in tables if any(keyword in t for keyword in 
                         ['performance', 'alert', 'metric'])]
    print(f"\n성능 모니터링 관련 테이블 ({len(performance_tables)}개):")
    for table in performance_tables:
        print(f"  - {table}")

    # 새로 생성된 테이블들
    new_tables = [
        'batch_job_performance_metrics',
        'system_performance_metrics', 
        'performance_alerts',
        'alert_rules',
        'performance_reports'
    ]
    
    print(f"\n새로 추가된 성능 모니터링 테이블:")
    for table in new_tables:
        if table in tables:
            print(f"  ✅ {table}")
        else:
            print(f"  ❌ {table} (생성되지 않음)")

if __name__ == "__main__":
    create_performance_tables()