#!/usr/bin/env python3
"""
Weather Forecasts 테이블 forecast_date 컬럼 타입 최적화 스크립트

이 스크립트는 다음 작업을 수행합니다:
1. 현재 데이터 검증
2. 마이그레이션 실행
3. 코드 호환성 확인
4. 결과 검증

실행 방법:
python scripts/apply_forecast_date_migration.py

주의사항:
- 데이터베이스 백업을 권장합니다
- 배치 작업이 실행되지 않는 시간에 수행하세요
"""

import sys
import os
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from app.core.database_manager import DatabaseManager
from app.core.logger import get_logger


class ForecastDateMigration:
    """forecast_date 컬럼 타입 마이그레이션 관리자"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.db_manager = DatabaseManager()
        
    def validate_current_data(self) -> bool:
        """현재 데이터 검증"""
        try:
            # 유효하지 않은 날짜 형식 확인
            invalid_query = """
            SELECT forecast_date, COUNT(*) as count
            FROM weather_forecasts 
            WHERE NOT (forecast_date ~ '^\\d{8}$' AND LENGTH(forecast_date) = 8)
            GROUP BY forecast_date
            """
            
            invalid_data = self.db_manager.fetch_all(invalid_query)
            
            if invalid_data:
                self.logger.error(f"유효하지 않은 날짜 형식 발견: {len(invalid_data)}건")
                for row in invalid_data:
                    self.logger.error(f"  - {row['forecast_date']}: {row['count']}건")
                return False
                
            # 총 레코드 수 확인
            total_query = "SELECT COUNT(*) as total FROM weather_forecasts"
            total_count = self.db_manager.fetch_one(total_query)['total']
            
            self.logger.info(f"데이터 검증 완료: 총 {total_count}건의 유효한 데이터")
            return True
            
        except Exception as e:
            self.logger.error(f"데이터 검증 실패: {e}")
            return False
    
    def apply_migration(self) -> bool:
        """마이그레이션 실행 (단계별 처리)"""
        try:
            migration_file = project_root / "database" / "migrations" / "003_optimize_forecast_date_type.sql"
            
            if not migration_file.exists():
                self.logger.error(f"마이그레이션 파일을 찾을 수 없습니다: {migration_file}")
                return False
                
            with open(migration_file, 'r', encoding='utf-8') as f:
                migration_content = f.read()
            
            # SQL을 단계별로 분리
            sql_parts = migration_content.split('-- 6단계: 성능 최적화를 위한 인덱스 생성 (트랜잭션 외부)')
            
            if len(sql_parts) != 2:
                self.logger.error("마이그레이션 SQL 파싱 실패")
                return False
            
            main_migration = sql_parts[0].strip()
            index_creation = sql_parts[1].strip()
            
            self.logger.info("1단계: 컬럼 타입 변경 실행...")
            
            # 1단계: 메인 마이그레이션 (트랜잭션 내)
            self.db_manager.execute_update(main_migration)
            
            self.logger.info("1단계 완료. 2단계: 인덱스 생성 실행...")
            
            # 2단계: 인덱스 생성 (각각 개별 실행)
            index_statements = [
                "CREATE INDEX idx_weather_forecasts_region_date ON weather_forecasts(region_code, forecast_date);",
                "CREATE INDEX idx_weather_forecasts_date_time ON weather_forecasts(forecast_date, forecast_time);",
                "ANALYZE weather_forecasts;"
            ]
            
            for statement in index_statements:
                if statement.strip():
                    try:
                        self.db_manager.execute_update(statement)
                        self.logger.info(f"인덱스 생성 완료: {statement.split()[2] if 'CREATE INDEX' in statement else 'ANALYZE'}")
                    except Exception as e:
                        self.logger.warning(f"인덱스 생성 실패 (계속 진행): {e}")
            
            self.logger.info("마이그레이션 실행 완료")
            return True
            
        except Exception as e:
            self.logger.error(f"마이그레이션 실행 실패: {e}")
            return False
    
    def verify_migration(self) -> bool:
        """마이그레이션 결과 검증"""
        try:
            # 컬럼 타입 확인
            column_query = """
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns 
            WHERE table_name = 'weather_forecasts' 
                AND column_name = 'forecast_date'
            """
            
            column_info = self.db_manager.fetch_one(column_query)
            
            if not column_info:
                self.logger.error("forecast_date 컬럼을 찾을 수 없습니다")
                return False
                
            if column_info['data_type'] != 'date':
                self.logger.error(f"예상하지 못한 데이터 타입: {column_info['data_type']} (예상: date)")
                return False
                
            # 인덱스 확인
            index_query = """
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'weather_forecasts' 
                AND indexname LIKE '%forecast%'
            """
            
            indexes = self.db_manager.fetch_all(index_query)
            
            expected_indexes = ['idx_weather_forecasts_region_date', 'idx_weather_forecasts_date_time']
            existing_indexes = [idx['indexname'] for idx in indexes]
            
            missing_indexes = [idx for idx in expected_indexes if idx not in existing_indexes]
            
            if missing_indexes:
                self.logger.warning(f"일부 인덱스가 생성되지 않았습니다: {missing_indexes}")
            
            # 데이터 샘플 확인
            sample_query = """
            SELECT forecast_date, forecast_time, region_code
            FROM weather_forecasts 
            ORDER BY forecast_date DESC 
            LIMIT 5
            """
            
            samples = self.db_manager.fetch_all(sample_query)
            
            self.logger.info("마이그레이션 검증 완료:")
            self.logger.info(f"  - 컬럼 타입: {column_info['data_type']}")
            self.logger.info(f"  - NULL 허용: {column_info['is_nullable']}")
            self.logger.info(f"  - 생성된 인덱스: {len(existing_indexes)}개")
            self.logger.info(f"  - 샘플 데이터: {len(samples)}건")
            
            return True
            
        except Exception as e:
            self.logger.error(f"마이그레이션 검증 실패: {e}")
            return False
    
    def run_migration(self) -> bool:
        """전체 마이그레이션 프로세스 실행"""
        self.logger.info("=== Forecast Date 마이그레이션 시작 ===")
        
        # 1단계: 데이터 검증
        if not self.validate_current_data():
            self.logger.error("데이터 검증 실패. 마이그레이션을 중단합니다.")
            return False
        
        # 2단계: 마이그레이션 실행
        if not self.apply_migration():
            self.logger.error("마이그레이션 실행 실패.")
            return False
        
        # 3단계: 결과 검증
        if not self.verify_migration():
            self.logger.error("마이그레이션 검증 실패.")
            return False
        
        self.logger.info("=== Forecast Date 마이그레이션 완료 ===")
        return True


def main():
    """메인 실행 함수"""
    migration = ForecastDateMigration()
    
    try:
        success = migration.run_migration()
        
        if success:
            print("✅ 마이그레이션이 성공적으로 완료되었습니다.")
            print("📋 다음 단계:")
            print("  1. 배치 작업 재시작")
            print("  2. 로그 모니터링")
            print("  3. 성능 개선 확인")
            return 0
        else:
            print("❌ 마이그레이션이 실패했습니다.")
            print("📋 확인 사항:")
            print("  1. 데이터베이스 연결 상태")
            print("  2. 권한 설정")
            print("  3. 로그 파일 확인")
            return 1
            
    except Exception as e:
        print(f"❌ 예상하지 못한 오류 발생: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)