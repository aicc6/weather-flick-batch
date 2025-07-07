#!/usr/bin/env python3
"""
간단한 마이그레이션 실행 스크립트
작성일: 2025-07-06
목적: 의존성 없이 직접 SQL 실행
"""

import os
import sys
import psycopg2
from pathlib import Path

# .env 파일 로드 시도 (선택사항)
try:
    from dotenv import load_dotenv
    # 프로젝트 루트에서 .env 파일 찾기
    project_root = Path(__file__).parent.parent
    env_file = project_root / '.env'
    
    if env_file.exists():
        load_dotenv(env_file)
        print(f"✅ .env 파일 로드 완료: {env_file}")
    else:
        print(f"ℹ️  .env 파일이 없습니다: {env_file}")
        print("환경 변수에서 직접 값을 가져옵니다.")
except ImportError:
    print("⚠️  python-dotenv가 설치되지 않았습니다. 환경 변수에서 직접 값을 가져옵니다.")
except Exception as e:
    print(f"⚠️  .env 파일 로드 중 오류: {e}")
    print("환경 변수에서 직접 값을 가져옵니다.")

def get_db_connection():
    """데이터베이스 연결 생성"""
    try:
        conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', 5432),
            database=os.getenv('DB_NAME', 'weather_flick'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD')
        )
        return conn
    except Exception as e:
        print(f"데이터베이스 연결 실패: {e}")
        return None

def run_migration():
    """마이그레이션 실행"""
    migration_file = Path(__file__).parent.parent / "database/migrations/003_schema_enhancement_phase1_fixed.sql"
    
    if not migration_file.exists():
        print(f"마이그레이션 파일이 존재하지 않습니다: {migration_file}")
        return False
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        print("마이그레이션 실행 시작...")
        
        with open(migration_file, 'r', encoding='utf-8') as f:
            migration_sql = f.read()
        
        with conn.cursor() as cursor:
            cursor.execute(migration_sql)
            conn.commit()
        
        print("✅ 마이그레이션 실행 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 마이그레이션 실행 실패: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def verify_migration():
    """마이그레이션 결과 확인"""
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        with conn.cursor() as cursor:
            # 새 테이블 확인
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name IN ('content_images', 'content_detail_info')
                ORDER BY table_name;
            """)
            new_tables = [row[0] for row in cursor.fetchall()]
            
            print(f"새로 생성된 테이블: {new_tables}")
            
            # 기존 테이블의 새 컬럼 확인
            test_table = 'tourist_attractions'
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                AND table_name = %s 
                AND column_name IN ('homepage', 'booktour', 'createdtime', 'modifiedtime', 'detail_intro_info')
                ORDER BY column_name;
            """, (test_table,))
            new_columns = [row[0] for row in cursor.fetchall()]
            
            print(f"테이블 {test_table}에 추가된 컬럼: {new_columns}")
            
        return True
        
    except Exception as e:
        print(f"검증 중 오류: {e}")
        return False
    finally:
        conn.close()

def main():
    print("=== Weather Flick Batch 스키마 마이그레이션 (간단 버전) ===")
    print()
    
    # 환경 변수 확인 및 기본값 제공
    required_env_vars = {
        'DB_HOST': 'localhost',
        'DB_USER': 'postgres', 
        'DB_PASSWORD': None,  # 필수
        'DB_NAME': 'weather_flick'
    }
    
    missing_vars = []
    current_config = {}
    
    for var, default_value in required_env_vars.items():
        value = os.getenv(var, default_value)
        if not value:
            missing_vars.append(var)
        else:
            current_config[var] = value
    
    # 현재 설정 표시
    print("현재 데이터베이스 설정:")
    for var, value in current_config.items():
        if 'PASSWORD' in var:
            print(f"  {var}: {'*' * len(value) if value else 'NOT SET'}")
        else:
            print(f"  {var}: {value}")
    print()
    
    if missing_vars:
        print(f"❌ 필요한 환경 변수가 없습니다: {missing_vars}")
        print()
        print("해결 방법:")
        print("1. .env 파일 생성:")
        print("   cp .env.example .env")
        print("   # .env 파일을 편집하여 실제 값 입력")
        print()
        print("2. 환경 변수 직접 설정:")
        for var in missing_vars:
            print(f"   export {var}=your_value_here")
        print()
        return
    
    # 사용자 확인
    print("이 스크립트는 다음을 수행합니다:")
    print("1. 기존 테이블에 새로운 필드들 추가")
    print("2. content_images, content_detail_info 테이블 생성")
    print("3. 관련 인덱스 생성")
    print()
    
    confirm = input("계속 진행하시겠습니까? (y/N): ").strip().lower()
    if confirm != 'y':
        print("마이그레이션이 취소되었습니다.")
        return
    
    # 마이그레이션 실행
    if run_migration():
        print()
        print("마이그레이션 결과 확인 중...")
        verify_migration()
        print()
        print("✅ 마이그레이션이 성공적으로 완료되었습니다!")
        print()
        print("다음 단계:")
        print("1. 코드베이스 업데이트 (database_manager.py, unified_kto_client.py 등)")
        print("2. 새로운 API 수집 로직 구현")
        print("3. 테스트 실행")
    else:
        print()
        print("❌ 마이그레이션이 실패했습니다.")
        print("오류 메시지를 확인하고 문제를 해결한 후 다시 시도하세요.")
        sys.exit(1)

if __name__ == "__main__":
    main()