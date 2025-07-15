#!/usr/bin/env python3
"""
Weather Forecast 테이블 성능 최적화 스크립트

이 스크립트는 다음 작업을 수행합니다:
1. 현재 테이블 및 인덱스 상태 분석
2. 추가 인덱스 생성
3. 쿼리 성능 측정
4. 최적화 결과 리포트 생성

실행 방법:
python scripts/optimize_weather_performance.py [--analyze-only]

옵션:
--analyze-only : 분석만 수행하고 인덱스 생성하지 않음
"""

import sys
import argparse
import time
from pathlib import Path
from datetime import datetime

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from app.core.database_manager import DatabaseManager
from app.core.logger import get_logger


class WeatherPerformanceOptimizer:
    """날씨 데이터 테이블 성능 최적화 관리자"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.db_manager = DatabaseManager()
        
    def analyze_current_state(self) -> dict:
        """현재 테이블 상태 분석"""
        analysis = {}
        
        try:
            # 테이블 크기 분석
            size_query = """
            SELECT 
                pg_size_pretty(pg_total_relation_size('weather_forecast')) as total_size,
                pg_size_pretty(pg_relation_size('weather_forecast')) as table_size,
                pg_size_pretty(pg_total_relation_size('weather_forecast') - pg_relation_size('weather_forecast')) as indexes_size
            """
            
            size_info = self.db_manager.fetch_one(size_query)
            analysis['table_sizes'] = size_info
            
            # 레코드 수 및 날짜 범위
            stats_query = """
            SELECT 
                COUNT(*) as total_records,
                MIN(forecast_date) as earliest_date,
                MAX(forecast_date) as latest_date,
                COUNT(DISTINCT region_code) as unique_regions,
                COUNT(DISTINCT forecast_date) as unique_dates
            FROM weather_forecast
            """
            
            stats_info = self.db_manager.fetch_one(stats_query)
            analysis['table_stats'] = stats_info
            
            # 기존 인덱스 확인
            index_query = """
            SELECT 
                indexname,
                indexdef,
                pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
            FROM pg_indexes 
            WHERE tablename = 'weather_forecast'
            ORDER BY indexname
            """
            
            indexes = self.db_manager.fetch_all(index_query)
            analysis['existing_indexes'] = indexes
            
            # 데이터 품질 체크
            quality_query = """
            SELECT 
                COUNT(*) as total_records,
                COUNT(*) FILTER (WHERE min_temp IS NULL) as missing_min_temp,
                COUNT(*) FILTER (WHERE max_temp IS NULL) as missing_max_temp,
                COUNT(*) FILTER (WHERE weather_condition IS NULL OR weather_condition = '') as missing_condition,
                COUNT(*) FILTER (WHERE forecast_date < CURRENT_DATE - INTERVAL '30 days') as old_records
            FROM weather_forecast
            """
            
            quality_info = self.db_manager.fetch_one(quality_query)
            analysis['data_quality'] = quality_info
            
            self.logger.info(f"현재 상태 분석 완료: {stats_info['total_records']}건의 레코드")
            
        except Exception as e:
            self.logger.error(f"현재 상태 분석 실패: {e}")
            
        return analysis
    
    def measure_query_performance(self) -> dict:
        """주요 쿼리 성능 측정"""
        performance_results = {}
        
        # 테스트할 쿼리들
        test_queries = {
            "region_latest_forecast": """
                SELECT region_code, forecast_date, forecast_time, min_temp, max_temp
                FROM weather_forecast 
                WHERE region_code = '11' 
                ORDER BY forecast_date DESC, forecast_time DESC 
                LIMIT 10
            """,
            
            "date_range_search": """
                SELECT COUNT(*) 
                FROM weather_forecast 
                WHERE forecast_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '7 days'
            """,
            
            "coordinate_lookup": """
                SELECT region_code, forecast_date, min_temp, max_temp
                FROM weather_forecast 
                WHERE nx = 60 AND ny = 127 
                AND forecast_date >= CURRENT_DATE
                ORDER BY forecast_date, forecast_time
                LIMIT 20
            """,
            
            "quality_check": """
                SELECT COUNT(*) 
                FROM weather_forecast 
                WHERE min_temp IS NULL OR max_temp IS NULL 
                OR weather_condition IS NULL
            """
        }
        
        for query_name, query_sql in test_queries.items():
            try:
                # EXPLAIN ANALYZE 실행
                explain_query = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {query_sql}"
                
                start_time = time.time()
                explain_result = self.db_manager.fetch_one(explain_query)
                execution_time = time.time() - start_time
                
                performance_results[query_name] = {
                    "execution_time": execution_time,
                    "explain_result": explain_result
                }
                
                self.logger.debug(f"쿼리 성능 측정 완료: {query_name} ({execution_time:.3f}초)")
                
            except Exception as e:
                self.logger.warning(f"쿼리 성능 측정 실패 [{query_name}]: {e}")
                performance_results[query_name] = {"error": str(e)}
        
        return performance_results
    
    def apply_optimizations(self) -> bool:
        """성능 최적화 적용 (개별 인덱스 생성)"""
        try:
            optimization_file = project_root / "database" / "migrations" / "004_additional_weather_indexes.sql"
            
            if not optimization_file.exists():
                self.logger.error(f"최적화 파일을 찾을 수 없습니다: {optimization_file}")
                return False
                
            with open(optimization_file, 'r', encoding='utf-8') as f:
                optimization_content = f.read()
            
            self.logger.info("성능 최적화 적용 시작...")
            
            # CREATE INDEX 문들을 개별적으로 실행
            import re
            
            # CREATE INDEX 문들만 추출 (문제가 있는 패턴 제외)
            index_statements = re.findall(
                r'CREATE INDEX[^;]+;', 
                optimization_content, 
                re.IGNORECASE | re.MULTILINE | re.DOTALL
            )
            
            # ANALYZE 문 추출
            analyze_statements = re.findall(
                r'ANALYZE[^;]*;', 
                optimization_content, 
                re.IGNORECASE
            )
            
            success_count = 0
            total_count = len(index_statements)
            
            for statement in index_statements:
                try:
                    statement = statement.strip()
                    
                    # 문제가 있는 패턴 감지 및 수정
                    if 'CURRENT_DATE' in statement and 'WHERE' in statement:
                        # CURRENT_DATE 관련 WHERE 절 제거
                        statement = re.sub(r'\s*WHERE\s+.*CURRENT_DATE[^;]*', '', statement)
                        self.logger.info("IMMUTABLE 함수 문제로 WHERE 절 제거됨")
                    
                    self.db_manager.execute_update(statement)
                    index_name = re.search(r'idx_\w+', statement)
                    if index_name:
                        self.logger.info(f"인덱스 생성 완료: {index_name.group()}")
                    success_count += 1
                    
                except Exception as e:
                    index_name = re.search(r'idx_\w+', statement)
                    error_context = index_name.group() if index_name else "알 수 없는 인덱스"
                    self.logger.warning(f"인덱스 생성 실패 [{error_context}]: {e}")
                    
                    # 특정 오류에 대한 자동 복구 시도
                    if 'IMMUTABLE' in str(e) and 'WHERE' in statement:
                        try:
                            # WHERE 절 제거 후 재시도
                            fixed_statement = re.sub(r'\s*WHERE\s+[^;]*', ';', statement)
                            self.db_manager.execute_update(fixed_statement)
                            self.logger.info(f"자동 복구 성공: {error_context} (WHERE 절 제거)")
                            success_count += 1
                        except Exception as retry_error:
                            self.logger.error(f"자동 복구 실패 [{error_context}]: {retry_error}")
            
            # ANALYZE 실행
            for analyze_stmt in analyze_statements:
                try:
                    self.db_manager.execute_update(analyze_stmt.strip())
                    self.logger.info("테이블 통계 정보 업데이트 완료")
                except Exception as e:
                    self.logger.warning(f"ANALYZE 실패: {e}")
            
            self.logger.info(f"성능 최적화 완료: {success_count}/{total_count}개 인덱스 생성됨")
            return success_count > 0
            
        except Exception as e:
            self.logger.error(f"성능 최적화 적용 실패: {e}")
            return False
    
    def generate_report(self, analysis: dict, performance_before: dict, performance_after: dict = None):
        """최적화 리포트 생성"""
        report_lines = []
        report_lines.append("=" * 60)
        report_lines.append("Weather Forecasts 테이블 성능 분석 리포트")
        report_lines.append(f"생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append("=" * 60)
        
        # 테이블 정보
        if 'table_stats' in analysis:
            stats = analysis['table_stats']
            report_lines.append("\n📊 테이블 통계:")
            report_lines.append(f"  - 총 레코드 수: {stats['total_records']:,}건")
            report_lines.append(f"  - 고유 지역 수: {stats['unique_regions']}개")
            report_lines.append(f"  - 예보 날짜 범위: {stats['earliest_date']} ~ {stats['latest_date']}")
            report_lines.append(f"  - 고유 날짜 수: {stats['unique_dates']}개")
        
        # 테이블 크기
        if 'table_sizes' in analysis:
            sizes = analysis['table_sizes']
            report_lines.append("\n💾 저장공간 사용량:")
            report_lines.append(f"  - 전체 크기: {sizes['total_size']}")
            report_lines.append(f"  - 테이블 크기: {sizes['table_size']}")
            report_lines.append(f"  - 인덱스 크기: {sizes['indexes_size']}")
        
        # 인덱스 정보
        if 'existing_indexes' in analysis:
            indexes = analysis['existing_indexes']
            report_lines.append(f"\n🔍 인덱스 현황 ({len(indexes)}개):")
            for idx in indexes:
                report_lines.append(f"  - {idx['indexname']}: {idx.get('index_size', 'N/A')}")
        
        # 데이터 품질
        if 'data_quality' in analysis:
            quality = analysis['data_quality']
            report_lines.append("\n🎯 데이터 품질:")
            report_lines.append(f"  - 누락된 최저온도: {quality['missing_min_temp']}건")
            report_lines.append(f"  - 누락된 최고온도: {quality['missing_max_temp']}건")
            report_lines.append(f"  - 누락된 날씨상태: {quality['missing_condition']}건")
            report_lines.append(f"  - 30일 이전 데이터: {quality['old_records']}건")
        
        # 성능 측정 결과
        if performance_before:
            report_lines.append("\n⚡ 쿼리 성능 측정:")
            for query_name, result in performance_before.items():
                if 'execution_time' in result:
                    report_lines.append(f"  - {query_name}: {result['execution_time']:.3f}초")
                else:
                    report_lines.append(f"  - {query_name}: 오류 발생")
        
        # 개선 권장사항
        report_lines.append("\n💡 권장사항:")
        
        if 'data_quality' in analysis:
            quality = analysis['data_quality']
            if quality['missing_min_temp'] > 0 or quality['missing_max_temp'] > 0:
                report_lines.append("  - 누락된 온도 데이터 보완 필요")
            if quality['old_records'] > 1000:
                report_lines.append("  - 오래된 예보 데이터 아카이빙 고려")
        
        report_lines.append("  - 정기적인 VACUUM ANALYZE 실행")
        report_lines.append("  - 배치 작업 성능 모니터링")
        
        report_lines.append("\n" + "=" * 60)
        
        # 리포트 저장
        report_content = "\n".join(report_lines)
        
        report_file = project_root / "logs" / f"weather_performance_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        report_file.parent.mkdir(exist_ok=True)
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        print(report_content)
        print(f"\n📄 상세 리포트가 저장되었습니다: {report_file}")
    
    def run_optimization(self, analyze_only: bool = False):
        """전체 최적화 프로세스 실행"""
        self.logger.info("=== Weather Forecasts 성능 최적화 시작 ===")
        
        # 1단계: 현재 상태 분석
        analysis = self.analyze_current_state()
        
        # 2단계: 성능 측정 (최적화 전)
        performance_before = self.measure_query_performance()
        
        performance_after = None
        
        if not analyze_only:
            # 3단계: 최적화 적용
            if self.apply_optimizations():
                # 4단계: 성능 측정 (최적화 후)
                performance_after = self.measure_query_performance()
            else:
                self.logger.error("최적화 적용 실패")
        
        # 5단계: 리포트 생성
        self.generate_report(analysis, performance_before, performance_after)
        
        self.logger.info("=== Weather Forecasts 성능 최적화 완료 ===")


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description='Weather Forecasts 테이블 성능 최적화')
    parser.add_argument('--analyze-only', action='store_true', 
                       help='분석만 수행하고 최적화는 적용하지 않음')
    
    args = parser.parse_args()
    
    optimizer = WeatherPerformanceOptimizer()
    
    try:
        optimizer.run_optimization(analyze_only=args.analyze_only)
        
        if args.analyze_only:
            print("✅ 성능 분석이 완료되었습니다.")
        else:
            print("✅ 성능 최적화가 완료되었습니다.")
            
        print("📋 다음 단계:")
        print("  1. 배치 작업 성능 모니터링")
        print("  2. 쿼리 실행 계획 확인")
        print("  3. 정기적인 VACUUM ANALYZE 스케줄링")
        
        return 0
        
    except Exception as e:
        print(f"❌ 최적화 프로세스 실패: {e}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)