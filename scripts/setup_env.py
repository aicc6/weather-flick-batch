#!/usr/bin/env python3
"""
환경 설정 도구
작성일: 2025-07-06
목적: .env 파일 생성 및 데이터베이스 연결 테스트
"""

import os
import sys
import shutil
from pathlib import Path

def create_env_file():
    """환경 파일 생성"""
    project_root = Path(__file__).parent.parent
    env_example = project_root / '.env.example'
    env_file = project_root / '.env'
    
    print("=== Weather Flick Batch 환경 설정 ===")
    print()
    
    if env_file.exists():
        print(f"⚠️  .env 파일이 이미 존재합니다: {env_file}")
        overwrite = input("덮어쓰시겠습니까? (y/N): ").strip().lower()
        if overwrite != 'y':
            print("환경 설정을 취소했습니다.")
            return False
    
    if not env_example.exists():
        print(f"❌ .env.example 파일이 없습니다: {env_example}")
        print("먼저 .env.example 파일을 생성해주세요.")
        return False
    
    # .env.example을 .env로 복사
    shutil.copy(env_example, env_file)
    print(f"✅ .env 파일을 생성했습니다: {env_file}")
    print()
    
    # 사용자에게 편집 안내
    print("다음 단계:")
    print(f"1. .env 파일을 편집하세요: {env_file}")
    print("2. 특히 다음 값들을 실제 값으로 변경하세요:")
    print("   - DB_PASSWORD=your_password_here")
    print("   - KTO_API_KEY=your_kto_api_key_here")
    print("   - KMA_API_KEY=your_kma_api_key_here")
    print()
    
    return True

def test_database_connection():
    """데이터베이스 연결 테스트"""
    try:
        import psycopg2
    except ImportError:
        print("❌ psycopg2가 설치되지 않았습니다.")
        print("설치 명령: pip install psycopg2-binary")
        return False
    
    # .env 파일 로드 시도
    try:
        from dotenv import load_dotenv
        project_root = Path(__file__).parent.parent
        env_file = project_root / '.env'
        
        if env_file.exists():
            load_dotenv(env_file)
            print(f"✅ .env 파일 로드 완료")
        else:
            print("⚠️  .env 파일이 없습니다. 환경 변수를 사용합니다.")
    except ImportError:
        print("⚠️  python-dotenv가 설치되지 않았습니다.")
        print("설치 명령: pip install python-dotenv")
    
    # 데이터베이스 연결 테스트
    db_config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 5432)),
        'database': os.getenv('DB_NAME', 'weather_flick'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD')
    }
    
    print()
    print("데이터베이스 연결 테스트...")
    print(f"Host: {db_config['host']}:{db_config['port']}")
    print(f"Database: {db_config['database']}")
    print(f"User: {db_config['user']}")
    
    if not db_config['password']:
        print("❌ DB_PASSWORD가 설정되지 않았습니다.")
        return False
    
    try:
        conn = psycopg2.connect(**db_config)
        
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"✅ 데이터베이스 연결 성공!")
            print(f"PostgreSQL 버전: {version.split(',')[0]}")
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        print()
        print("확인사항:")
        print("1. PostgreSQL 서버가 실행 중인지 확인")
        print("2. 데이터베이스가 존재하는지 확인")
        print("3. 사용자 권한이 올바른지 확인")
        print("4. .env 파일의 설정값이 정확한지 확인")
        return False

def check_dependencies():
    """필요한 의존성 확인"""
    required_packages = {
        'psycopg2': 'psycopg2-binary',
        'dotenv': 'python-dotenv'
    }
    
    missing_packages = []
    
    for package, install_name in required_packages.items():
        try:
            if package == 'psycopg2':
                import psycopg2
            elif package == 'dotenv':
                from dotenv import load_dotenv
        except ImportError:
            missing_packages.append(install_name)
    
    if missing_packages:
        print("❌ 누락된 패키지:")
        for package in missing_packages:
            print(f"   - {package}")
        print()
        print("설치 명령:")
        print(f"pip install {' '.join(missing_packages)}")
        return False
    
    print("✅ 모든 필수 패키지가 설치되어 있습니다.")
    return True

def main():
    print("=== Weather Flick Batch 환경 설정 도구 ===")
    print()
    
    # 의존성 확인
    if not check_dependencies():
        print()
        print("먼저 필요한 패키지를 설치해주세요.")
        return
    
    print()
    
    # 메뉴 선택
    while True:
        print("원하는 작업을 선택하세요:")
        print("1. .env 파일 생성")
        print("2. 데이터베이스 연결 테스트")
        print("3. 전체 설정 및 테스트")
        print("0. 종료")
        print()
        
        choice = input("선택 (0-3): ").strip()
        
        if choice == '0':
            print("설정 도구를 종료합니다.")
            break
        elif choice == '1':
            create_env_file()
        elif choice == '2':
            test_database_connection()
        elif choice == '3':
            if create_env_file():
                print()
                print("이제 .env 파일을 편집한 후 데이터베이스 연결을 테스트해보세요.")
                print("편집이 완료되면 아무 키나 누르세요...")
                input()
                test_database_connection()
        else:
            print("잘못된 선택입니다.")
        
        print()

if __name__ == "__main__":
    main()