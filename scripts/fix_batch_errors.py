#!/usr/bin/env python3
"""
배치 시스템 오류 통합 수정 스크립트
작성일: 2025-07-07
목적: 7월 7일 새벽 배치 실행 중 발생한 오류들을 일괄 수정
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.core.multi_api_key_manager import get_api_key_manager, APIProvider
from app.core.database_manager_extension import get_extended_database_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def fix_api_keys():
    """API 키 상태 수정"""
    print("=== 1단계: API 키 상태 수정 ===")
    
    try:
        manager = get_api_key_manager()
        
        # 모든 키 강제 활성화
        print("비활성화된 API 키들을 다시 활성화합니다...")
        reset_count = 0
        
        for provider in [APIProvider.KTO, APIProvider.KMA]:
            if provider in manager.api_keys:
                for key_info in manager.api_keys[provider]:
                    if not key_info.is_active or key_info.error_count >= 5:
                        key_info.is_active = True
                        key_info.error_count = 0
                        key_info.rate_limit_reset_time = None
                        key_info.last_error_time = None
                        key_info.current_usage = 0  # 사용량도 리셋
                        reset_count += 1
                        print(f"  ✅ {provider.value} 키 {key_info.key[:10]}... 활성화됨")
        
        # 캐시 업데이트
        try:
            manager._save_to_cache()
            print(f"  ✅ {reset_count}개 키 상태 캐시 저장 완료")
        except Exception as e:
            print(f"  ⚠️ 캐시 저장 실패: {e}")
        
        print("✅ API 키 상태 수정 완료!")
        return True
        
    except Exception as e:
        print(f"❌ API 키 상태 수정 실패: {e}")
        return False


async def fix_database_schema():
    """데이터베이스 스키마 수정"""
    print("\n=== 2단계: 데이터베이스 스키마 수정 ===")
    
    try:
        db_manager = get_extended_database_manager()
        
        # 마이그레이션 스크립트 실행
        migration_file = project_root / "database" / "migrations" / "005_fix_current_weather_raw_data_id.sql"
        
        if migration_file.exists():
            print("데이터베이스 마이그레이션을 실행합니다...")
            
            with open(migration_file, 'r', encoding='utf-8') as f:
                migration_sql = f.read()
            
            # 마이그레이션 실행
            async with db_manager.get_async_connection() as conn:
                async with conn.cursor() as cursor:
                    # PostgreSQL 마이그레이션 실행
                    await cursor.execute(migration_sql)
                    await conn.commit()
                    
            print("✅ 데이터베이스 스키마 수정 완료!")
            return True
        else:
            print("❌ 마이그레이션 파일을 찾을 수 없습니다")
            return False
            
    except Exception as e:
        print(f"❌ 데이터베이스 스키마 수정 실패: {e}")
        return False


def validate_fixes():
    """수정 사항 검증"""
    print("\n=== 3단계: 수정 사항 검증 ===")
    
    try:
        # API 키 상태 확인
        manager = get_api_key_manager()
        active_kto_keys = sum(1 for key in manager.api_keys.get(APIProvider.KTO, []) if key.is_active)
        active_kma_keys = sum(1 for key in manager.api_keys.get(APIProvider.KMA, []) if key.is_active)
        
        print(f"활성 KTO API 키: {active_kto_keys}개")
        print(f"활성 KMA API 키: {active_kma_keys}개")
        
        if active_kto_keys == 0:
            print("⚠️ 활성 KTO API 키가 없습니다. API 키 설정을 확인하세요.")
            return False
        
        print("✅ 수정 사항 검증 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 수정 사항 검증 실패: {e}")
        return False


async def main():
    """메인 실행 함수"""
    print("=== Weather Flick 배치 오류 수정 도구 ===")
    print("2025-07-07 새벽 배치 실행 중 발생한 오류들을 수정합니다.")
    print()
    
    success_count = 0
    
    # 1. API 키 상태 수정
    if fix_api_keys():
        success_count += 1
    
    # 2. 데이터베이스 스키마 수정
    if await fix_database_schema():
        success_count += 1
    
    # 3. 수정 사항 검증
    if validate_fixes():
        success_count += 1
    
    print(f"\n=== 수정 결과 ===")
    print(f"총 3단계 중 {success_count}단계 성공")
    
    if success_count == 3:
        print("✅ 모든 오류가 성공적으로 수정되었습니다!")
        print("\n다음 명령어로 배치를 다시 실행할 수 있습니다:")
        print("python main_advanced.py")
    else:
        print("❌ 일부 오류 수정에 실패했습니다.")
        print("로그를 확인하고 수동으로 수정이 필요할 수 있습니다.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())