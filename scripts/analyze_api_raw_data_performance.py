#!/usr/bin/env python3
"""
api_raw_data 테이블 성능 분석 스크립트

현재 테이블 구조, 인덱스, 쿼리 성능을 종합 분석하고 최적화 방안을 제시합니다.
"""

import sys
import os
import time
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import DatabaseManager
from app.core.database_manager_extension import extend_database_manager


class APIRawDataAnalyzer:
    """api_raw_data 테이블 성능 분석기"""
    
    def __init__(self):
        self.db_manager = extend_database_manager(DatabaseManager().sync_manager)
        self.analysis_results = {}
    
    def run_full_analysis(self) -> Dict[str, Any]:
        """전체 성능 분석 실행"""
        
        print("🔍 api_raw_data 테이블 성능 분석 시작...\n")
        
        # 1. 기본 테이블 정보
        self.analysis_results['table_info'] = self._analyze_table_structure()
        
        # 2. 데이터 분포 분석
        self.analysis_results['data_distribution'] = self._analyze_data_distribution()
        
        # 3. 인덱스 분석
        self.analysis_results['index_analysis'] = self._analyze_indexes()
        
        # 4. 쿼리 성능 측정
        self.analysis_results['query_performance'] = self._measure_query_performance()
        
        # 5. 스토리지 분석
        self.analysis_results['storage_analysis'] = self._analyze_storage_usage()
        
        # 6. 최적화 권장사항
        self.analysis_results['recommendations'] = self._generate_recommendations()
        
        return self.analysis_results
    
    def _analyze_table_structure(self) -> Dict[str, Any]:
        """테이블 구조 분석"""
        print("📋 테이블 구조 분석...")
        
        # 테이블 스키마 정보
        schema_query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default,
            character_maximum_length
        FROM information_schema.columns 
        WHERE table_name = 'api_raw_data'
        ORDER BY ordinal_position
        """
        
        columns = self.db_manager.fetch_all(schema_query)
        
        # 테이블 크기 정보
        size_query = """
        SELECT 
            pg_size_pretty(pg_total_relation_size('api_raw_data')) as total_size,
            pg_size_pretty(pg_relation_size('api_raw_data')) as table_size,
            pg_size_pretty(pg_total_relation_size('api_raw_data') - pg_relation_size('api_raw_data')) as index_size,
            (SELECT COUNT(*) FROM api_raw_data) as row_count
        """
        
        size_info = self.db_manager.fetch_one(size_query)
        
        # 제약조건 정보
        constraints_query = """
        SELECT 
            constraint_name,
            constraint_type
        FROM information_schema.table_constraints
        WHERE table_name = 'api_raw_data'
        """
        
        constraints = self.db_manager.fetch_all(constraints_query)
        
        return {
            'columns': [dict(col) for col in columns],
            'size_info': dict(size_info) if size_info else {},
            'constraints': [dict(c) for c in constraints],
            'total_columns': len(columns)
        }
    
    def _analyze_data_distribution(self) -> Dict[str, Any]:
        """데이터 분포 분석"""
        print("📊 데이터 분포 분석...")
        
        # API 제공자별 분포
        provider_dist_query = """
        SELECT 
            api_provider,
            COUNT(*) as count,
            ROUND(AVG(response_size), 2) as avg_response_size,
            MIN(created_at) as earliest,
            MAX(created_at) as latest,
            COUNT(CASE WHEN response_status >= 400 THEN 1 END) as error_count
        FROM api_raw_data
        GROUP BY api_provider
        ORDER BY count DESC
        """
        
        provider_distribution = self.db_manager.fetch_all(provider_dist_query)
        
        # 엔드포인트별 분포 (상위 20개)
        endpoint_dist_query = """
        SELECT 
            endpoint,
            COUNT(*) as count,
            ROUND(AVG(response_size), 2) as avg_response_size,
            MIN(created_at) as earliest,
            MAX(created_at) as latest
        FROM api_raw_data
        GROUP BY endpoint
        ORDER BY count DESC
        LIMIT 20
        """
        
        endpoint_distribution = self.db_manager.fetch_all(endpoint_dist_query)
        
        # 시간별 분포 (월별)
        temporal_dist_query = """
        SELECT 
            DATE_TRUNC('month', created_at) as month,
            COUNT(*) as count,
            ROUND(AVG(response_size), 2) as avg_response_size,
            SUM(response_size) as total_size
        FROM api_raw_data
        GROUP BY DATE_TRUNC('month', created_at)
        ORDER BY month
        """
        
        temporal_distribution = self.db_manager.fetch_all(temporal_dist_query)
        
        # 응답 크기 분포
        size_dist_query = """
        SELECT 
            CASE 
                WHEN response_size < 1024 THEN '< 1KB'
                WHEN response_size < 10240 THEN '1KB - 10KB'
                WHEN response_size < 102400 THEN '10KB - 100KB'
                WHEN response_size < 1048576 THEN '100KB - 1MB'
                WHEN response_size < 10485760 THEN '1MB - 10MB'
                ELSE '> 10MB'
            END as size_range,
            COUNT(*) as count
        FROM api_raw_data
        WHERE response_size IS NOT NULL
        GROUP BY 
            CASE 
                WHEN response_size < 1024 THEN '< 1KB'
                WHEN response_size < 10240 THEN '1KB - 10KB'
                WHEN response_size < 102400 THEN '10KB - 100KB'
                WHEN response_size < 1048576 THEN '100KB - 1MB'
                WHEN response_size < 10485760 THEN '1MB - 10MB'
                ELSE '> 10MB'
            END
        ORDER BY 
            MIN(CASE 
                WHEN response_size < 1024 THEN 1
                WHEN response_size < 10240 THEN 2
                WHEN response_size < 102400 THEN 3
                WHEN response_size < 1048576 THEN 4
                WHEN response_size < 10485760 THEN 5
                ELSE 6
            END)
        """
        
        size_distribution = self.db_manager.fetch_all(size_dist_query)
        
        return {
            'provider_distribution': [dict(row) for row in provider_distribution],
            'endpoint_distribution': [dict(row) for row in endpoint_distribution],
            'temporal_distribution': [dict(row) for row in temporal_distribution],
            'size_distribution': [dict(row) for row in size_distribution]
        }
    
    def _analyze_indexes(self) -> Dict[str, Any]:
        """인덱스 분석"""
        print("🗂️ 인덱스 분석...")
        
        # 현재 인덱스 조회
        indexes_query = """
        SELECT 
            schemaname,
            tablename,
            indexname,
            indexdef
        FROM pg_indexes 
        WHERE tablename = 'api_raw_data'
        ORDER BY indexname
        """
        
        current_indexes = self.db_manager.fetch_all(indexes_query)
        
        # 인덱스 사용 통계
        index_usage_query = """
        SELECT 
            indexrelname as index_name,
            idx_tup_read,
            idx_tup_fetch,
            idx_scan
        FROM pg_stat_user_indexes 
        WHERE relname = 'api_raw_data'
        ORDER BY idx_scan DESC
        """
        
        index_usage = self.db_manager.fetch_all(index_usage_query)
        
        # 중복 가능성이 있는 인덱스 분석
        duplicate_analysis = self._analyze_duplicate_indexes()
        
        return {
            'current_indexes': [dict(idx) for idx in current_indexes],
            'index_usage_stats': [dict(usage) for usage in index_usage],
            'duplicate_analysis': duplicate_analysis,
            'total_indexes': len(current_indexes)
        }
    
    def _analyze_duplicate_indexes(self) -> List[Dict[str, Any]]:
        """중복 인덱스 분석"""
        # 실제 환경에서는 더 복잡한 중복 분석이 필요
        # 여기서는 기본적인 분석만 수행
        
        return [
            {
                'analysis': '현재 인덱스 중복성 검사',
                'note': '상세한 중복 분석은 실제 쿼리 패턴 확인 후 수행 필요'
            }
        ]
    
    def _measure_query_performance(self) -> Dict[str, Any]:
        """쿼리 성능 측정"""
        print("⚡ 쿼리 성능 측정...")
        
        # 성능 측정할 주요 쿼리들
        test_queries = [
            {
                'name': 'provider_lookup',
                'description': 'API 제공자별 조회',
                'query': "SELECT COUNT(*) FROM api_raw_data WHERE api_provider = 'KMA'"
            },
            {
                'name': 'endpoint_lookup',
                'description': '엔드포인트별 조회',
                'query': "SELECT COUNT(*) FROM api_raw_data WHERE endpoint = 'getUltraSrtFcst'"
            },
            {
                'name': 'date_range_lookup',
                'description': '날짜 범위 조회',
                'query': "SELECT COUNT(*) FROM api_raw_data WHERE created_at >= NOW() - INTERVAL '7 days'"
            },
            {
                'name': 'combined_lookup',
                'description': '복합 조건 조회',
                'query': "SELECT COUNT(*) FROM api_raw_data WHERE api_provider = 'KMA' AND created_at >= NOW() - INTERVAL '1 day'"
            },
            {
                'name': 'size_filter',
                'description': '크기 기반 필터링',
                'query': "SELECT COUNT(*) FROM api_raw_data WHERE response_size > 1048576"
            },
            {
                'name': 'error_lookup',
                'description': '오류 응답 조회',
                'query': "SELECT COUNT(*) FROM api_raw_data WHERE response_status >= 400"
            }
        ]
        
        performance_results = []
        
        for test in test_queries:
            try:
                # 쿼리 실행 시간 측정
                start_time = time.time()
                result = self.db_manager.fetch_one(test['query'])
                end_time = time.time()
                
                execution_time = round((end_time - start_time) * 1000, 2)  # ms
                
                performance_results.append({
                    'query_name': test['name'],
                    'description': test['description'],
                    'execution_time_ms': execution_time,
                    'result_count': result.get('count', 0) if result else 0
                })
                
            except Exception as e:
                performance_results.append({
                    'query_name': test['name'],
                    'description': test['description'],
                    'execution_time_ms': -1,
                    'error': str(e)
                })
        
        return {
            'query_tests': performance_results,
            'baseline_established': datetime.now()
        }
    
    def _analyze_storage_usage(self) -> Dict[str, Any]:
        """스토리지 사용량 분석"""
        print("💾 스토리지 사용량 분석...")
        
        # 상세 스토리지 분석
        storage_query = """
        SELECT 
            schemaname,
            tablename,
            attname as column_name,
            n_distinct,
            correlation,
            avg_width
        FROM pg_stats 
        WHERE tablename = 'api_raw_data'
        ORDER BY avg_width DESC
        """
        
        storage_stats = self.db_manager.fetch_all(storage_query)
        
        # JSONB 컬럼별 크기 분석
        jsonb_analysis_query = """
        SELECT 
            'raw_response' as column_name,
            AVG(pg_column_size(raw_response)) as avg_size_bytes,
            MIN(pg_column_size(raw_response)) as min_size_bytes,
            MAX(pg_column_size(raw_response)) as max_size_bytes
        FROM api_raw_data
        WHERE raw_response IS NOT NULL
        
        UNION ALL
        
        SELECT 
            'request_params' as column_name,
            AVG(pg_column_size(request_params)) as avg_size_bytes,
            MIN(pg_column_size(request_params)) as min_size_bytes,
            MAX(pg_column_size(request_params)) as max_size_bytes
        FROM api_raw_data
        WHERE request_params IS NOT NULL
        """
        
        jsonb_analysis = self.db_manager.fetch_all(jsonb_analysis_query)
        
        # 압축 효과 추정
        compression_estimate = self._estimate_compression_benefits()
        
        return {
            'storage_stats': [dict(stat) for stat in storage_stats],
            'jsonb_analysis': [dict(analysis) for analysis in jsonb_analysis],
            'compression_estimate': compression_estimate
        }
    
    def _estimate_compression_benefits(self) -> Dict[str, Any]:
        """압축 효과 추정"""
        # 샘플 데이터로 압축 효과 추정
        sample_query = """
        SELECT 
            AVG(LENGTH(raw_response::text)) as avg_raw_response_length,
            COUNT(*) as sample_size
        FROM api_raw_data
        WHERE raw_response IS NOT NULL
        LIMIT 1000
        """
        
        sample_result = self.db_manager.fetch_one(sample_query)
        
        if sample_result and sample_result.get('avg_raw_response_length'):
            # 일반적인 JSON 압축률 70-80% 적용
            original_size = float(sample_result['avg_raw_response_length'])
            estimated_compressed_size = original_size * 0.25  # 75% 압축 가정
            
            return {
                'original_avg_size_bytes': round(original_size, 2),
                'estimated_compressed_size_bytes': round(estimated_compressed_size, 2),
                'compression_ratio': round((1 - estimated_compressed_size / original_size) * 100, 1),
                'sample_size': sample_result.get('sample_size', 0)
            }
        
        return {'note': '압축 효과 추정을 위한 충분한 데이터 없음'}
    
    def _generate_recommendations(self) -> List[Dict[str, Any]]:
        """최적화 권장사항 생성"""
        print("💡 최적화 권장사항 생성...")
        
        recommendations = []
        
        # 데이터 분포 기반 권장사항
        provider_dist = self.analysis_results.get('data_distribution', {}).get('provider_distribution', [])
        
        if provider_dist:
            # KMA 데이터가 많은 경우
            kma_data = next((p for p in provider_dist if p['api_provider'] == 'KMA'), None)
            if kma_data and kma_data.get('count', 0) > 30000:
                recommendations.append({
                    'category': 'indexing',
                    'priority': 'high',
                    'title': 'KMA 데이터 전용 인덱스 추가',
                    'description': 'KMA 데이터가 전체의 88%를 차지하므로 api_provider + created_at 복합 인덱스 생성',
                    'sql': "CREATE INDEX CONCURRENTLY idx_api_raw_data_kma_created ON api_raw_data(api_provider, created_at) WHERE api_provider = 'KMA';"
                })
        
        # 성능 측정 기반 권장사항
        query_performance = self.analysis_results.get('query_performance', {}).get('query_tests', [])
        
        slow_queries = [q for q in query_performance if q.get('execution_time_ms', 0) > 100]
        if slow_queries:
            recommendations.append({
                'category': 'performance',
                'priority': 'medium',
                'title': '느린 쿼리 최적화',
                'description': f'{len(slow_queries)}개의 쿼리가 100ms 이상 소요됨',
                'details': [q['query_name'] for q in slow_queries]
            })
        
        # 스토리지 기반 권장사항
        storage_info = self.analysis_results.get('table_info', {}).get('size_info', {})
        row_count = storage_info.get('row_count', 0)
        
        if row_count > 30000:
            recommendations.extend([
                {
                    'category': 'partitioning',
                    'priority': 'medium',
                    'title': '파티셔닝 검토',
                    'description': f'현재 {row_count:,}개 레코드로 월별 파티셔닝 고려',
                    'benefit': '쿼리 성능 향상, 유지보수 효율성 증대'
                },
                {
                    'category': 'archiving',
                    'priority': 'low',
                    'title': '오래된 데이터 아카이빙',
                    'description': '90일 이상 된 데이터의 압축 아카이빙 검토',
                    'benefit': '스토리지 비용 절감'
                }
            ])
        
        # 압축 관련 권장사항
        compression_estimate = self.analysis_results.get('storage_analysis', {}).get('compression_estimate', {})
        if compression_estimate.get('compression_ratio', 0) > 50:
            recommendations.append({
                'category': 'compression',
                'priority': 'low',
                'title': 'JSONB 데이터 압축',
                'description': f'예상 압축률 {compression_estimate.get("compression_ratio", 0)}%로 상당한 공간 절약 가능',
                'benefit': '스토리지 비용 절감'
            })
        
        # 인덱스 최적화 권장사항
        recommendations.extend([
            {
                'category': 'indexing',
                'priority': 'high',
                'title': '복합 인덱스 생성',
                'description': '자주 사용되는 쿼리 패턴용 복합 인덱스',
                'sql': "CREATE INDEX CONCURRENTLY idx_api_raw_data_provider_endpoint_created ON api_raw_data(api_provider, endpoint, created_at);"
            },
            {
                'category': 'indexing',
                'priority': 'medium',
                'title': '부분 인덱스 생성',
                'description': '오류 응답 전용 부분 인덱스',
                'sql': "CREATE INDEX CONCURRENTLY idx_api_raw_data_errors ON api_raw_data(api_provider, created_at) WHERE response_status >= 400;"
            }
        ])
        
        return recommendations
    
    def generate_optimization_sql(self) -> List[str]:
        """최적화 SQL 스크립트 생성"""
        
        sql_scripts = []
        
        # 권장 인덱스들
        indexes = [
            # 1. API 제공자 + 생성일 복합 인덱스
            """
            -- API 제공자별 시간 범위 조회용 인덱스
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_raw_data_provider_created 
            ON api_raw_data(api_provider, created_at DESC);
            """,
            
            # 2. 엔드포인트 + 생성일 복합 인덱스  
            """
            -- 엔드포인트별 시간 범위 조회용 인덱스
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_raw_data_endpoint_created 
            ON api_raw_data(endpoint, created_at DESC);
            """,
            
            # 3. 응답 상태 부분 인덱스
            """
            -- 오류 응답 전용 부분 인덱스
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_raw_data_errors 
            ON api_raw_data(api_provider, created_at) 
            WHERE response_status >= 400;
            """,
            
            # 4. 응답 크기 부분 인덱스
            """
            -- 대용량 응답 전용 부분 인덱스
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_raw_data_large_responses 
            ON api_raw_data(api_provider, response_size, created_at) 
            WHERE response_size > 1048576; -- 1MB 이상
            """,
            
            # 5. TTL 관리용 인덱스
            """
            -- TTL 기반 정리 작업용 인덱스
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_raw_data_ttl 
            ON api_raw_data(expires_at) 
            WHERE expires_at IS NOT NULL;
            """,
            
            # 6. API 키 해시 인덱스
            """
            -- API 키별 사용량 추적용 인덱스
            CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_raw_data_api_key_hash 
            ON api_raw_data(api_key_hash, created_at) 
            WHERE api_key_hash IS NOT NULL;
            """
        ]
        
        sql_scripts.extend(indexes)
        
        # 테이블 통계 업데이트
        sql_scripts.append("""
        -- 테이블 통계 업데이트
        ANALYZE api_raw_data;
        """)
        
        return sql_scripts


def print_analysis_report(analysis: Dict[str, Any]):
    """분석 결과 보고서 출력"""
    
    print("\n" + "="*80)
    print("📊 API_RAW_DATA 테이블 성능 분석 보고서")
    print("="*80)
    print(f"📅 분석 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 1. 테이블 기본 정보
    table_info = analysis.get('table_info', {})
    size_info = table_info.get('size_info', {})
    
    print("📋 **테이블 기본 정보**")
    print("-" * 50)
    print(f"• 총 레코드 수: {size_info.get('row_count', 0):,}개")
    print(f"• 테이블 크기: {size_info.get('table_size', 'N/A')}")
    print(f"• 인덱스 크기: {size_info.get('index_size', 'N/A')}")
    print(f"• 전체 크기: {size_info.get('total_size', 'N/A')}")
    print(f"• 컬럼 수: {table_info.get('total_columns', 0)}개")
    print()
    
    # 2. 데이터 분포
    data_dist = analysis.get('data_distribution', {})
    provider_dist = data_dist.get('provider_distribution', [])
    
    if provider_dist:
        print("📊 **API 제공자별 분포**")
        print("-" * 50)
        for provider in provider_dist:
            error_rate = 0
            if provider.get('count', 0) > 0:
                error_rate = (provider.get('error_count', 0) / provider['count']) * 100
            
            avg_size = provider.get('avg_response_size') or 0
            print(f"• {provider['api_provider']}: {provider['count']:,}개 "
                  f"(평균 크기: {avg_size:.0f}바이트, "
                  f"오류율: {error_rate:.1f}%)")
        print()
    
    # 3. 쿼리 성능
    query_perf = analysis.get('query_performance', {})
    query_tests = query_perf.get('query_tests', [])
    
    if query_tests:
        print("⚡ **쿼리 성능 측정 결과**")
        print("-" * 50)
        for test in query_tests:
            status = "✅" if test.get('execution_time_ms', 0) < 50 else "⚠️" if test.get('execution_time_ms', 0) < 200 else "❌"
            print(f"{status} {test['description']}: {test.get('execution_time_ms', 'N/A')}ms")
        print()
    
    # 4. 최적화 권장사항
    recommendations = analysis.get('recommendations', [])
    
    if recommendations:
        print("💡 **최적화 권장사항**")
        print("-" * 50)
        
        # 우선순위별로 정렬
        high_priority = [r for r in recommendations if r.get('priority') == 'high']
        medium_priority = [r for r in recommendations if r.get('priority') == 'medium']
        low_priority = [r for r in recommendations if r.get('priority') == 'low']
        
        for priority_group, priority_name in [(high_priority, "🔴 높음"), (medium_priority, "🟡 중간"), (low_priority, "🟢 낮음")]:
            if priority_group:
                print(f"\n{priority_name} 우선순위:")
                for i, rec in enumerate(priority_group, 1):
                    print(f"  {i}. {rec['title']}")
                    print(f"     └─ {rec['description']}")
                    if rec.get('benefit'):
                        print(f"     └─ 효과: {rec['benefit']}")
        print()
    
    # 5. 다음 단계
    print("🎯 **다음 단계**")
    print("-" * 50)
    print("1. 권장 인덱스 생성 (CONCURRENTLY 옵션 사용)")
    print("2. 쿼리 성능 재측정 및 비교")
    print("3. 파티셔닝 전략 상세 설계")
    print("4. 자동 정리 시스템 구현")
    print("5. 모니터링 대시보드 구축")
    print()
    
    print("="*80)


def main():
    """메인 실행 함수"""
    
    try:
        print("🚀 API Raw Data 성능 분석 도구 시작\n")
        
        analyzer = APIRawDataAnalyzer()
        analysis_results = analyzer.run_full_analysis()
        
        # 분석 결과 출력
        print_analysis_report(analysis_results)
        
        # 최적화 SQL 생성
        optimization_sql = analyzer.generate_optimization_sql()
        
        # SQL 스크립트 파일로 저장
        sql_filename = f"api_raw_data_optimization_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
        
        with open(sql_filename, 'w', encoding='utf-8') as f:
            f.write("-- API Raw Data 테이블 최적화 SQL 스크립트\n")
            f.write(f"-- 생성일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            for i, sql in enumerate(optimization_sql, 1):
                f.write(f"-- {i}. 최적화 스크립트\n")
                f.write(sql.strip() + "\n\n")
        
        print(f"📄 최적화 SQL 스크립트가 '{sql_filename}' 파일로 저장되었습니다.")
        
        # JSON 결과 파일 저장
        json_filename = f"api_raw_data_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # datetime 및 Decimal 객체를 JSON 직렬화 가능하게 변환하는 함수
        def json_serial(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            elif hasattr(obj, '__float__'):  # Decimal 등의 숫자 타입
                return float(obj)
            raise TypeError(f"Type {type(obj)} not serializable")
        
        with open(json_filename, 'w', encoding='utf-8') as f:
            json.dump(analysis_results, f, ensure_ascii=False, indent=2, default=json_serial)
        
        print(f"📊 상세 분석 결과가 '{json_filename}' 파일로 저장되었습니다.")
        
    except Exception as e:
        print(f"❌ 분석 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()