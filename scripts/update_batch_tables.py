"""
배치 테이블 업데이트 스크립트 - 재시도 필드 추가
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from app.api.config import settings

def update_batch_tables():
    """배치 테이블에 재시도 필드 추가"""
    engine = create_engine(settings.DATABASE_URL)
    
    with engine.begin() as conn:
        try:
            # retry_status 컬럼 추가
            print("retry_status 컬럼 추가 중...")
            conn.execute(text("""
                ALTER TABLE batch_job_executions 
                ADD COLUMN IF NOT EXISTS retry_status VARCHAR
            """))
            
            # retry_count 컬럼 추가
            print("retry_count 컬럼 추가 중...")
            conn.execute(text("""
                ALTER TABLE batch_job_executions 
                ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0
            """))
            
            print("테이블 업데이트 완료!")
            
            # 업데이트된 컬럼 확인
            result = conn.execute(text("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = 'batch_job_executions' 
                AND column_name IN ('retry_status', 'retry_count')
                ORDER BY column_name
            """))
            
            print("\n추가된 컬럼:")
            for row in result:
                print(f"  - {row[0]}: {row[1]}")
                
        except Exception as e:
            print(f"오류 발생: {e}")
            raise

if __name__ == "__main__":
    update_batch_tables()