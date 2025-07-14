#!/usr/bin/env python3
"""
알림 테이블 마이그레이션 실행 스크립트
"""
import sys
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from app.core.database_manager import DatabaseManager
from app.core.logger import get_logger

logger = get_logger(__name__)

def run_migration():
    """마이그레이션 실행"""
    db_manager = DatabaseManager()
    
    migration_file = project_root / "database" / "migrations" / "010_create_notification_tables.sql"
    
    if not migration_file.exists():
        logger.error(f"마이그레이션 파일을 찾을 수 없습니다: {migration_file}")
        return False
    
    try:
        logger.info("알림 테이블 마이그레이션 시작...")
        
        # SQL 파일 읽기
        with open(migration_file, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        
        # 트랜잭션으로 실행
        with db_manager.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql_content)
                conn.commit()
        
        logger.info("마이그레이션 성공!")
        
        # 생성된 테이블 확인
        check_query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
              AND table_name IN ('weather_notifications', 'user_notification_preferences')
            ORDER BY table_name;
        """
        
        tables = db_manager.execute_query(check_query)
        logger.info(f"생성된 테이블: {[t['table_name'] for t in tables]}")
        
        return True
        
    except Exception as e:
        logger.error(f"마이그레이션 실행 중 오류: {str(e)}")
        return False

if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)