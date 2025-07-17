"""
알림 관련 테이블 생성 스크립트
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine
from app.models import Base
from app.api.config import settings

def create_notification_tables():
    """알림 관련 테이블 생성"""
    # 데이터베이스 엔진 생성
    engine = create_engine(settings.DATABASE_URL)
    
    # 테이블 생성
    print("알림 관련 테이블 생성 중...")
    Base.metadata.create_all(bind=engine)
    print("테이블 생성 완료!")
    
    # 생성된 테이블 확인
    from sqlalchemy import inspect
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    
    print("\n현재 데이터베이스 테이블:")
    for table in sorted(tables):
        print(f"  - {table}")
    
    notification_tables = [t for t in tables if 'notification' in t]
    print(f"\n알림 관련 테이블 ({len(notification_tables)}개):")
    for table in notification_tables:
        print(f"  - {table}")

if __name__ == "__main__":
    create_notification_tables()