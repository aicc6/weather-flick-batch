#!/usr/bin/env python3
"""
데이터베이스 마이그레이션 실행 스크립트

배치 시스템의 데이터베이스 스키마를 최신 상태로 업데이트합니다.
"""

import sys
import os
from pathlib import Path
import psycopg2
from psycopg2.extras import RealDictCursor
import logging

# 프로젝트 루트를 sys.path에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config.settings import get_app_settings

# 로깅 설정
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_database_connection():
    """데이터베이스 연결 생성"""
    settings = get_app_settings()

    try:
        conn = psycopg2.connect(
            host=settings.database.host,
            port=settings.database.port,
            database=settings.database.database,
            user=settings.database.user,
            password=settings.database.password,
        )
        return conn
    except Exception as e:
        logger.error(f"데이터베이스 연결 실패: {e}")
        raise


def check_migration_table(conn):
    """마이그레이션 로그 테이블 확인 및 생성"""
    with conn.cursor() as cursor:
        # migration_log 테이블이 있는지 확인
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'migration_log'
            );
        """)

        if not cursor.fetchone()[0]:
            logger.info("migration_log 테이블 생성 중...")
            cursor.execute("""
                CREATE TABLE migration_log (
                    id SERIAL PRIMARY KEY,
                    migration_name VARCHAR(255) UNIQUE NOT NULL,
                    applied_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    description TEXT,
                    status VARCHAR(50) DEFAULT 'SUCCESS'
                );
            """)
            conn.commit()
            logger.info("migration_log 테이블 생성 완료")


def apply_migration(conn, migration_file: str):
    """마이그레이션 파일 적용"""
    migration_path = project_root / "database" / "migrations" / migration_file

    if not migration_path.exists():
        logger.error(f"마이그레이션 파일을 찾을 수 없습니다: {migration_path}")
        return False

    migration_name = migration_path.stem

    # 이미 적용된 마이그레이션인지 확인
    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT COUNT(*) FROM migration_log WHERE migration_name = %s",
            (migration_name,),
        )
        if cursor.fetchone()[0] > 0:
            logger.info(f"마이그레이션 {migration_name}은 이미 적용되었습니다.")
            return True

    try:
        # 마이그레이션 SQL 파일 읽기
        with open(migration_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        logger.info(f"마이그레이션 적용 중: {migration_name}")

        # SQL 실행
        with conn.cursor() as cursor:
            cursor.execute(sql_content)

        # 마이그레이션 로그 기록
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO migration_log (migration_name, description, status)
                VALUES (%s, %s, %s)
            """,
                (migration_name, f"마이그레이션 {migration_name} 적용", "SUCCESS"),
            )

        conn.commit()
        logger.info(f"마이그레이션 {migration_name} 적용 완료")
        return True

    except Exception as e:
        logger.error(f"마이그레이션 {migration_name} 적용 실패: {e}")

        # 실패 로그 기록
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO migration_log (migration_name, description, status)
                    VALUES (%s, %s, %s)
                """,
                    (
                        migration_name,
                        f"마이그레이션 {migration_name} 실패: {str(e)}",
                        "FAILED",
                    ),
                )
            conn.commit()
        except:
            pass

        return False


def main():
    """메인 함수"""
    logger.info("🚀 배치 시스템 데이터베이스 마이그레이션 시작")

    try:
        # 데이터베이스 연결
        conn = get_database_connection()
        logger.info("데이터베이스 연결 성공")

        # 마이그레이션 로그 테이블 확인
        check_migration_table(conn)

        # 적용할 마이그레이션 파일
        migration_files = ["015_add_retry_status_to_batch_jobs.sql"]

        success_count = 0
        total_count = len(migration_files)

        for migration_file in migration_files:
            if apply_migration(conn, migration_file):
                success_count += 1

        logger.info(f"마이그레이션 완료: {success_count}/{total_count} 성공")

        if success_count == total_count:
            logger.info("✅ 모든 마이그레이션이 성공적으로 적용되었습니다.")
            logger.info("이제 배치 작업을 다시 실행할 수 있습니다.")
        else:
            logger.error("❌ 일부 마이그레이션이 실패했습니다.")
            sys.exit(1)

    except Exception as e:
        logger.error(f"마이그레이션 실행 중 오류 발생: {e}")
        sys.exit(1)
    finally:
        if "conn" in locals():
            conn.close()


if __name__ == "__main__":
    main()
