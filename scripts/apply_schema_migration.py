#!/usr/bin/env python3
"""
스키마 마이그레이션 실행 스크립트
작성일: 2025-07-06
목적: 003_schema_enhancement_phase1.sql 마이그레이션 안전 실행
"""

import sys
import os
import psycopg2
import logging
from pathlib import Path
from typing import Optional, Dict, Any
import subprocess
from datetime import datetime

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent.parent))

from config.settings import DatabaseConfig, get_settings
from app.core.logger_config import get_logger

logger = get_logger(__name__)

class SchemaMigrationRunner:
    """스키마 마이그레이션 안전 실행기"""
    
    def __init__(self):
        self.settings = get_settings()
        self.db_config = self.settings.database
        self.migration_file = Path(__file__).parent.parent / "database/migrations/003_schema_enhancement_phase1_fixed.sql"
        
    def create_backup(self) -> Optional[str]:
        """데이터베이스 백업 생성"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_file = f"backup_before_schema_migration_{timestamp}.sql"
            backup_path = Path(__file__).parent.parent / "data" / "backups" / backup_file
            
            # 백업 디렉토리 생성
            backup_path.parent.mkdir(parents=True, exist_ok=True)
            
            # pg_dump 명령 구성
            dump_command = [
                "pg_dump",
                f"--host={self.db_config.host}",
                f"--port={self.db_config.port}",
                f"--username={self.db_config.user}",
                f"--dbname={self.db_config.name}",
                "--verbose",
                "--clean",
                "--no-owner",
                "--no-privileges",
                f"--file={backup_path}"
            ]
            
            logger.info(f"데이터베이스 백업 시작: {backup_path}")
            
            # 환경 변수로 비밀번호 설정
            env = os.environ.copy()
            env['PGPASSWORD'] = self.db_config.password
            
            result = subprocess.run(
                dump_command,
                env=env,
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                logger.info(f"데이터베이스 백업 완료: {backup_path}")
                return str(backup_path)
            else:
                logger.error(f"백업 실패: {result.stderr}")
                return None
                
        except Exception as e:
            logger.error(f"백업 생성 중 오류: {e}")
            return None
    
    def validate_migration_file(self) -> bool:
        """마이그레이션 파일 유효성 검사"""
        try:
            if not self.migration_file.exists():
                logger.error(f"마이그레이션 파일이 존재하지 않음: {self.migration_file}")
                return False
            
            with open(self.migration_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 기본 유효성 검사
            required_elements = [
                "-- 스키마 확장 1단계",
                "DO $$",
                "content_images",
                "content_detail_info",
                "ALTER TABLE",
                "CREATE INDEX"
            ]
            
            for element in required_elements:
                if element not in content:
                    logger.error(f"마이그레이션 파일에 필수 요소 누락: {element}")
                    return False
            
            logger.info("마이그레이션 파일 유효성 검사 통과")
            return True
            
        except Exception as e:
            logger.error(f"마이그레이션 파일 검증 중 오류: {e}")
            return False
    
    def check_database_connection(self) -> bool:
        """데이터베이스 연결 확인"""
        try:
            conn = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.name,
                user=self.db_config.user,
                password=self.db_config.password
            )
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                logger.info(f"데이터베이스 연결 확인: {version}")
            
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"데이터베이스 연결 실패: {e}")
            return False
    
    def check_existing_schema(self) -> Dict[str, Any]:
        """기존 스키마 상태 확인"""
        try:
            conn = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.name,
                user=self.db_config.user,
                password=self.db_config.password
            )
            
            schema_info = {}
            
            with conn.cursor() as cursor:
                # 기존 테이블 목록 확인
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name;
                """)
                schema_info['existing_tables'] = [row[0] for row in cursor.fetchall()]
                
                # 각 테이블의 컬럼 수 확인
                table_columns = {}
                for table in schema_info['existing_tables']:
                    if table in ['tourist_attractions', 'cultural_facilities', 'festivals_events', 
                               'travel_courses', 'leisure_sports', 'accommodations', 
                               'shopping', 'restaurants']:
                        cursor.execute("""
                            SELECT COUNT(*) 
                            FROM information_schema.columns 
                            WHERE table_name = %s;
                        """, (table,))
                        table_columns[table] = cursor.fetchone()[0]
                
                schema_info['table_columns'] = table_columns
                
                # 새 테이블 존재 여부 확인
                new_tables = ['content_images', 'content_detail_info']
                existing_new_tables = []
                for table in new_tables:
                    if table in schema_info['existing_tables']:
                        existing_new_tables.append(table)
                
                schema_info['existing_new_tables'] = existing_new_tables
            
            conn.close()
            logger.info(f"기존 스키마 정보: {schema_info}")
            return schema_info
            
        except Exception as e:
            logger.error(f"스키마 상태 확인 중 오류: {e}")
            return {}
    
    def execute_migration(self) -> bool:
        """마이그레이션 실행"""
        try:
            conn = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.name,
                user=self.db_config.user,
                password=self.db_config.password
            )
            
            with open(self.migration_file, 'r', encoding='utf-8') as f:
                migration_sql = f.read()
            
            logger.info("마이그레이션 실행 시작")
            
            with conn.cursor() as cursor:
                # 트랜잭션으로 실행
                cursor.execute(migration_sql)
                conn.commit()
            
            logger.info("마이그레이션 실행 완료")
            conn.close()
            return True
            
        except Exception as e:
            logger.error(f"마이그레이션 실행 중 오류: {e}")
            if 'conn' in locals():
                conn.rollback()
                conn.close()
            return False
    
    def verify_migration(self) -> bool:
        """마이그레이션 결과 검증"""
        try:
            conn = psycopg2.connect(
                host=self.db_config.host,
                port=self.db_config.port,
                database=self.db_config.name,
                user=self.db_config.user,
                password=self.db_config.password
            )
            
            verification_passed = True
            
            with conn.cursor() as cursor:
                # 새 테이블 생성 확인
                new_tables = ['content_images', 'content_detail_info']
                for table in new_tables:
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM information_schema.tables 
                        WHERE table_name = %s;
                    """, (table,))
                    
                    if cursor.fetchone()[0] == 0:
                        logger.error(f"새 테이블 {table}이 생성되지 않음")
                        verification_passed = False
                    else:
                        logger.info(f"새 테이블 {table} 생성 확인")
                
                # 기존 테이블 새 필드 확인
                target_tables = ['tourist_attractions', 'cultural_facilities', 'festivals_events']
                new_columns = ['homepage', 'booktour', 'createdtime', 'modifiedtime', 
                             'telname', 'faxno', 'zipcode', 'mlevel', 
                             'detail_intro_info', 'detail_additional_info']
                
                for table in target_tables:
                    cursor.execute("""
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_name = %s;
                    """, (table,))
                    
                    if cursor.fetchone():
                        for column in new_columns:
                            cursor.execute("""
                                SELECT COUNT(*) 
                                FROM information_schema.columns 
                                WHERE table_name = %s AND column_name = %s;
                            """, (table, column))
                            
                            if cursor.fetchone()[0] == 0:
                                logger.warning(f"테이블 {table}에 컬럼 {column}이 추가되지 않음")
                            else:
                                logger.info(f"테이블 {table}에 컬럼 {column} 추가 확인")
                
                # 인덱스 생성 확인
                cursor.execute("""
                    SELECT indexname 
                    FROM pg_indexes 
                    WHERE schemaname = 'public'
                    AND (indexname LIKE 'idx_content_images_%' 
                         OR indexname LIKE 'idx_content_detail_info_%');
                """)
                
                indexes = [row[0] for row in cursor.fetchall()]
                expected_indexes = [
                    'idx_content_images_content_id',
                    'idx_content_images_content_type',
                    'idx_content_detail_info_content_id'
                ]
                
                for expected_index in expected_indexes:
                    if expected_index in indexes:
                        logger.info(f"인덱스 {expected_index} 생성 확인")
                    else:
                        logger.warning(f"인덱스 {expected_index}가 생성되지 않음")
            
            conn.close()
            
            if verification_passed:
                logger.info("마이그레이션 검증 완료")
            else:
                logger.warning("마이그레이션 검증에서 일부 문제 발견")
            
            return verification_passed
            
        except Exception as e:
            logger.error(f"마이그레이션 검증 중 오류: {e}")
            return False
    
    def run_migration(self) -> bool:
        """전체 마이그레이션 프로세스 실행"""
        logger.info("=== 스키마 마이그레이션 시작 ===")
        
        # 1. 사전 검사
        logger.info("1. 사전 검사 시작")
        if not self.check_database_connection():
            logger.error("데이터베이스 연결 실패로 마이그레이션 중단")
            return False
        
        if not self.validate_migration_file():
            logger.error("마이그레이션 파일 검증 실패로 마이그레이션 중단")
            return False
        
        # 2. 기존 스키마 상태 확인
        logger.info("2. 기존 스키마 상태 확인")
        schema_info = self.check_existing_schema()
        
        # 3. 백업 생성
        logger.info("3. 데이터베이스 백업 생성")
        backup_file = self.create_backup()
        if not backup_file:
            logger.error("백업 생성 실패로 마이그레이션 중단")
            return False
        
        # 4. 마이그레이션 실행
        logger.info("4. 마이그레이션 실행")
        if not self.execute_migration():
            logger.error("마이그레이션 실행 실패")
            logger.info(f"백업 파일을 사용하여 복원 가능: {backup_file}")
            return False
        
        # 5. 결과 검증
        logger.info("5. 마이그레이션 결과 검증")
        if not self.verify_migration():
            logger.warning("마이그레이션 검증에서 일부 문제 발견")
        
        logger.info("=== 스키마 마이그레이션 완료 ===")
        logger.info(f"백업 파일 위치: {backup_file}")
        
        return True

def main():
    """메인 실행 함수"""
    migration_runner = SchemaMigrationRunner()
    
    # 사용자 확인
    print("=== Weather Flick Batch 스키마 마이그레이션 ===")
    print("이 작업은 다음을 수행합니다:")
    print("1. 기존 테이블에 새로운 필드들 추가")
    print("2. content_images, content_detail_info 테이블 생성")
    print("3. 관련 인덱스 생성")
    print("4. 데이터 품질 임계값 설정")
    print("")
    
    confirm = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
    if confirm != 'y':
        print("마이그레이션이 취소되었습니다.")
        return
    
    success = migration_runner.run_migration()
    
    if success:
        print("\n✅ 마이그레이션이 성공적으로 완료되었습니다!")
        print("이제 다음 단계를 진행할 수 있습니다:")
        print("1. 코드베이스 업데이트 (database_manager.py, unified_kto_client.py 등)")
        print("2. 새로운 API 수집 로직 구현")
        print("3. 데이터 품질 검증 테스트")
    else:
        print("\n❌ 마이그레이션이 실패했습니다.")
        print("로그를 확인하고 백업을 사용하여 복원을 고려하세요.")
        sys.exit(1)

if __name__ == "__main__":
    main()