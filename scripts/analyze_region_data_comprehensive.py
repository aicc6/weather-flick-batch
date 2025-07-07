#!/usr/bin/env python3
"""
지역정보 종합 분석 스크립트

데이터베이스에 수집된 모든 지역정보를 상세히 분석합니다.
"""

import sys
import os
import json
from datetime import datetime
from typing import Dict, List, Any
from collections import defaultdict, Counter

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database_manager import DatabaseManager
from app.core.database_manager_extension import extend_database_manager


class RegionDataAnalyzer:
    """지역정보 종합 분석기"""
    
    def __init__(self):
        self.db_manager = extend_database_manager(DatabaseManager().sync_manager)
    
    def analyze_all_region_data(self) -> Dict[str, Any]:
        """모든 지역정보 데이터 종합 분석"""
        
        analysis = {
            "summary": self._get_data_summary(),
            "regions_analysis": self._analyze_regions_table(),
            "weather_regions_analysis": self._analyze_weather_regions_table(),
            "unified_regions_analysis": self._analyze_unified_regions_table(),
            "integration_quality": self._analyze_integration_quality(),
            "coverage_analysis": self._analyze_geographical_coverage(),
            "data_completeness": self._analyze_data_completeness(),
            "recommendations": self._generate_recommendations()
        }
        
        return analysis
    
    def _get_data_summary(self) -> Dict[str, Any]:
        """데이터 요약"""
        
        summary = {}
        
        tables = ['regions', 'weather_regions', 'unified_regions', 'legal_dong_codes']
        
        for table in tables:
            try:
                count_query = f"SELECT COUNT(*) as count FROM {table}"
                result = self.db_manager.fetch_one(count_query)
                summary[table] = result.get('count', 0) if result else 0
            except:
                summary[table] = 0
        
        # 총 통합 지역 수
        summary['total_integrated_regions'] = summary.get('unified_regions', 0)
        
        # 중복 제거율 계산
        total_source = summary.get('regions', 0) + summary.get('weather_regions', 0)
        if total_source > 0:
            summary['deduplication_rate'] = round(
                (1 - summary.get('unified_regions', 0) / total_source) * 100, 2
            )
        
        return summary
    
    def _analyze_regions_table(self) -> Dict[str, Any]:
        """regions 테이블 상세 분석"""
        
        analysis = {}
        
        # 기본 통계
        stats_query = """
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coordinates,
            COUNT(CASE WHEN parent_region_code IS NOT NULL THEN 1 END) as with_parent,
            AVG(region_level) as avg_level,
            MIN(region_level) as min_level,
            MAX(region_level) as max_level
        FROM regions
        """
        
        stats = self.db_manager.fetch_one(stats_query)
        analysis['basic_stats'] = dict(stats) if stats else {}
        
        # 레벨별 분포
        level_query = """
        SELECT region_level, COUNT(*) as count
        FROM regions
        GROUP BY region_level
        ORDER BY region_level
        """
        
        levels = self.db_manager.fetch_all(level_query)
        analysis['level_distribution'] = {str(row['region_level']): row['count'] for row in levels}
        
        # 좌표 정보 현황
        coord_query = """
        SELECT 
            CASE 
                WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 'complete'
                WHEN latitude IS NULL AND longitude IS NULL THEN 'missing'
                ELSE 'partial'
            END as coord_status,
            COUNT(*) as count
        FROM regions
        GROUP BY coord_status
        """
        
        coords = self.db_manager.fetch_all(coord_query)
        analysis['coordinate_status'] = {row['coord_status']: row['count'] for row in coords}
        
        # 상위 지역별 하위 지역 수
        hierarchy_query = """
        SELECT 
            COALESCE(parent_region_code, 'ROOT') as parent,
            COUNT(*) as child_count
        FROM regions
        GROUP BY parent_region_code
        ORDER BY child_count DESC
        LIMIT 10
        """
        
        hierarchy = self.db_manager.fetch_all(hierarchy_query)
        analysis['hierarchy_stats'] = [dict(row) for row in hierarchy]
        
        return analysis
    
    def _analyze_weather_regions_table(self) -> Dict[str, Any]:
        """weather_regions 테이블 상세 분석"""
        
        analysis = {}
        
        # 기본 통계
        stats_query = """
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN grid_x IS NOT NULL AND grid_y IS NOT NULL THEN 1 END) as with_grid,
            COUNT(CASE WHEN is_active = true THEN 1 END) as active_count,
            AVG(latitude) as avg_lat,
            AVG(longitude) as avg_lon,
            MIN(created_at) as earliest_data,
            MAX(created_at) as latest_data
        FROM weather_regions
        """
        
        stats = self.db_manager.fetch_one(stats_query)
        analysis['basic_stats'] = dict(stats) if stats else {}
        
        # 지역 코드 패턴 분석
        pattern_query = """
        SELECT 
            CASE 
                WHEN region_code ~ '^[0-9]+$' THEN 'numeric'
                WHEN region_code ~ '^[0-9]+[A-Z][0-9]+$' THEN 'forecast_code'
                ELSE 'other'
            END as code_pattern,
            COUNT(*) as count
        FROM weather_regions
        WHERE region_code IS NOT NULL
        GROUP BY code_pattern
        """
        
        patterns = self.db_manager.fetch_all(pattern_query)
        analysis['code_patterns'] = {row['code_pattern']: row['count'] for row in patterns}
        
        # 격자 좌표 분포
        grid_query = """
        SELECT 
            CASE 
                WHEN grid_x BETWEEN 1 AND 50 THEN '1-50'
                WHEN grid_x BETWEEN 51 AND 100 THEN '51-100'
                WHEN grid_x BETWEEN 101 AND 150 THEN '101-150'
                ELSE 'other'
            END as grid_x_range,
            COUNT(*) as count
        FROM weather_regions
        WHERE grid_x IS NOT NULL
        GROUP BY grid_x_range
        ORDER BY grid_x_range
        """
        
        grids = self.db_manager.fetch_all(grid_query)
        analysis['grid_distribution'] = {row['grid_x_range']: row['count'] for row in grids}
        
        # 최근 추가된 예보구역 (모의 데이터)
        recent_query = """
        SELECT region_code, region_name, created_at
        FROM weather_regions
        WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
        ORDER BY created_at DESC
        LIMIT 10
        """
        
        recent = self.db_manager.fetch_all(recent_query)
        analysis['recent_additions'] = [dict(row) for row in recent]
        
        return analysis
    
    def _analyze_unified_regions_table(self) -> Dict[str, Any]:
        """unified_regions 테이블 상세 분석"""
        
        analysis = {}
        
        # 기본 통계
        stats_query = """
        SELECT 
            COUNT(*) as total_count,
            COUNT(CASE WHEN parent_region_id IS NOT NULL THEN 1 END) as with_parent,
            COUNT(CASE WHEN center_latitude IS NOT NULL AND center_longitude IS NOT NULL THEN 1 END) as with_coordinates,
            COUNT(CASE WHEN administrative_code IS NOT NULL THEN 1 END) as with_admin_code,
            COUNT(CASE WHEN region_name_en IS NOT NULL THEN 1 END) as with_english_name,
            AVG(region_level) as avg_level
        FROM unified_regions
        """
        
        stats = self.db_manager.fetch_one(stats_query)
        analysis['basic_stats'] = dict(stats) if stats else {}
        
        # 레벨별 분포
        level_query = """
        SELECT region_level, COUNT(*) as count
        FROM unified_regions
        GROUP BY region_level
        ORDER BY region_level
        """
        
        levels = self.db_manager.fetch_all(level_query)
        analysis['level_distribution'] = {str(row['region_level']): row['count'] for row in levels}
        
        # 행정구역 코드 패턴
        admin_code_query = """
        SELECT 
            CASE 
                WHEN administrative_code IS NULL THEN 'missing'
                WHEN LENGTH(administrative_code) = 2 THEN 'sido_level'
                WHEN LENGTH(administrative_code) = 5 THEN 'sigungu_level'
                WHEN LENGTH(administrative_code) > 5 THEN 'dong_level'
                ELSE 'other'
            END as admin_code_type,
            COUNT(*) as count
        FROM unified_regions
        GROUP BY admin_code_type
        """
        
        admin_codes = self.db_manager.fetch_all(admin_code_query)
        analysis['admin_code_distribution'] = {row['admin_code_type']: row['count'] for row in admin_codes}
        
        # 최상위 지역 (레벨 1)
        top_level_query = """
        SELECT region_code, region_name, region_name_full, administrative_code
        FROM unified_regions
        WHERE region_level = 1
        ORDER BY region_name
        """
        
        top_regions = self.db_manager.fetch_all(top_level_query)
        analysis['top_level_regions'] = [dict(row) for row in top_regions]
        
        # 부모-자식 관계 통계
        hierarchy_query = """
        SELECT 
            COUNT(CASE WHEN parent_region_id IS NULL THEN 1 END) as root_regions,
            COUNT(CASE WHEN parent_region_id IS NOT NULL THEN 1 END) as child_regions
        FROM unified_regions
        """
        
        hierarchy = self.db_manager.fetch_one(hierarchy_query)
        analysis['hierarchy_stats'] = dict(hierarchy) if hierarchy else {}
        
        return analysis
    
    def _analyze_integration_quality(self) -> Dict[str, Any]:
        """통합 품질 분석"""
        
        analysis = {}
        
        # 지역명 일치도 분석
        name_match_query = """
        SELECT 
            COUNT(CASE WHEN r.region_name = ur.region_name THEN 1 END) as exact_matches,
            COUNT(CASE WHEN SIMILARITY(r.region_name, ur.region_name) > 0.8 THEN 1 END) as similar_matches,
            COUNT(*) as total_comparisons
        FROM regions r
        CROSS JOIN unified_regions ur
        WHERE r.region_name IS NOT NULL AND ur.region_name IS NOT NULL
        """
        
        try:
            name_matches = self.db_manager.fetch_one(name_match_query)
            analysis['name_matching'] = dict(name_matches) if name_matches else {}
        except:
            # SIMILARITY 함수가 없는 경우 단순 비교
            simple_match_query = """
            SELECT 
                COUNT(CASE WHEN r.region_name = ur.region_name THEN 1 END) as exact_matches,
                COUNT(*) as total_comparisons
            FROM regions r
            CROSS JOIN unified_regions ur
            WHERE r.region_name IS NOT NULL AND ur.region_name IS NOT NULL
            """
            name_matches = self.db_manager.fetch_one(simple_match_query)
            analysis['name_matching'] = dict(name_matches) if name_matches else {}
        
        # 좌표 정보 품질
        coord_quality_query = """
        SELECT 
            COUNT(CASE WHEN center_latitude BETWEEN 33 AND 39 AND center_longitude BETWEEN 124 AND 132 THEN 1 END) as valid_korea_coords,
            COUNT(CASE WHEN center_latitude IS NOT NULL AND center_longitude IS NOT NULL THEN 1 END) as total_coords,
            COUNT(*) as total_regions
        FROM unified_regions
        """
        
        coord_quality = self.db_manager.fetch_one(coord_quality_query)
        analysis['coordinate_quality'] = dict(coord_quality) if coord_quality else {}
        
        # 중복 지역 분석
        duplicate_query = """
        SELECT region_name, COUNT(*) as count
        FROM unified_regions
        GROUP BY region_name
        HAVING COUNT(*) > 1
        ORDER BY count DESC
        LIMIT 10
        """
        
        duplicates = self.db_manager.fetch_all(duplicate_query)
        analysis['duplicate_names'] = [dict(row) for row in duplicates]
        
        return analysis
    
    def _analyze_geographical_coverage(self) -> Dict[str, Any]:
        """지리적 커버리지 분석"""
        
        analysis = {}
        
        # 시도별 통합 지역 수
        sido_coverage_query = """
        SELECT 
            CASE 
                WHEN administrative_code IN ('11', '26', '27', '28', '29', '30', '31', '36') THEN 
                    CASE administrative_code
                        WHEN '11' THEN '서울특별시'
                        WHEN '26' THEN '부산광역시'
                        WHEN '27' THEN '대구광역시'
                        WHEN '28' THEN '인천광역시'
                        WHEN '29' THEN '광주광역시'
                        WHEN '30' THEN '대전광역시'
                        WHEN '31' THEN '울산광역시'
                        WHEN '36' THEN '세종특별자치시'
                    END
                WHEN administrative_code = '41' THEN '경기도'
                WHEN administrative_code = '42' THEN '강원특별자치도'
                WHEN administrative_code = '43' THEN '충청북도'
                WHEN administrative_code = '44' THEN '충청남도'
                WHEN administrative_code = '45' THEN '전북특별자치도'
                WHEN administrative_code = '46' THEN '전라남도'
                WHEN administrative_code = '47' THEN '경상북도'
                WHEN administrative_code = '48' THEN '경상남도'
                WHEN administrative_code IN ('49', '50') THEN '제주특별자치도'
                ELSE '기타'
            END as sido_name,
            COUNT(*) as region_count
        FROM unified_regions
        WHERE administrative_code IS NOT NULL
        GROUP BY sido_name
        ORDER BY region_count DESC
        """
        
        sido_coverage = self.db_manager.fetch_all(sido_coverage_query)
        analysis['sido_coverage'] = [dict(row) for row in sido_coverage]
        
        # 좌표 범위 분석
        coord_range_query = """
        SELECT 
            MIN(center_latitude) as min_lat,
            MAX(center_latitude) as max_lat,
            MIN(center_longitude) as min_lon,
            MAX(center_longitude) as max_lon,
            AVG(center_latitude) as avg_lat,
            AVG(center_longitude) as avg_lon
        FROM unified_regions
        WHERE center_latitude IS NOT NULL AND center_longitude IS NOT NULL
        """
        
        coord_range = self.db_manager.fetch_one(coord_range_query)
        analysis['coordinate_range'] = dict(coord_range) if coord_range else {}
        
        return analysis
    
    def _analyze_data_completeness(self) -> Dict[str, Any]:
        """데이터 완성도 분석"""
        
        analysis = {}
        
        # 필수 필드 완성도 (unified_regions 기준)
        completeness_query = """
        SELECT 
            COUNT(*) as total_regions,
            COUNT(region_code) * 100.0 / COUNT(*) as region_code_completeness,
            COUNT(region_name) * 100.0 / COUNT(*) as region_name_completeness,
            COUNT(center_latitude) * 100.0 / COUNT(*) as coordinate_completeness,
            COUNT(administrative_code) * 100.0 / COUNT(*) as admin_code_completeness,
            COUNT(region_name_full) * 100.0 / COUNT(*) as full_name_completeness,
            COUNT(region_name_en) * 100.0 / COUNT(*) as english_name_completeness
        FROM unified_regions
        """
        
        completeness = self.db_manager.fetch_one(completeness_query)
        analysis['field_completeness'] = dict(completeness) if completeness else {}
        
        # 데이터 소스별 기여도
        source_query = """
        SELECT 
            CASE 
                WHEN region_code ~ '^[0-9]+[A-Z][0-9]+$' THEN 'forecast_regions'
                WHEN LENGTH(region_code) <= 4 THEN 'original_regions'
                WHEN region_code LIKE 'CITY-%' THEN 'city_data'
                ELSE 'other'
            END as data_source,
            COUNT(*) as count
        FROM unified_regions
        GROUP BY data_source
        """
        
        sources = self.db_manager.fetch_all(source_query)
        analysis['data_source_distribution'] = {row['data_source']: row['count'] for row in sources}
        
        return analysis
    
    def _generate_recommendations(self) -> List[str]:
        """개선 권장사항 생성"""
        
        recommendations = []
        
        # 기본 분석 데이터 조회
        unified_count_query = "SELECT COUNT(*) as count FROM unified_regions"
        unified_result = self.db_manager.fetch_one(unified_count_query)
        unified_count = unified_result.get('count', 0) if unified_result else 0
        
        coord_missing_query = """
        SELECT COUNT(*) as count FROM unified_regions 
        WHERE center_latitude IS NULL OR center_longitude IS NULL
        """
        coord_missing_result = self.db_manager.fetch_one(coord_missing_query)
        coord_missing = coord_missing_result.get('count', 0) if coord_missing_result else 0
        
        if coord_missing > 0:
            recommendations.append(
                f"좌표 정보가 누락된 {coord_missing}개 지역에 대한 좌표 보완 필요"
            )
        
        if unified_count > 300:
            recommendations.append(
                "통합 지역 수가 많으므로 중복 데이터 재검토 및 정리 권장"
            )
        
        # 영문명 누락 확인
        english_missing_query = """
        SELECT COUNT(*) as count FROM unified_regions 
        WHERE region_name_en IS NULL AND region_level = 1
        """
        english_missing_result = self.db_manager.fetch_one(english_missing_query)
        english_missing = english_missing_result.get('count', 0) if english_missing_result else 0
        
        if english_missing > 0:
            recommendations.append(
                f"최상위 지역 {english_missing}개의 영문명 추가 필요"
            )
        
        recommendations.extend([
            "기상청 API 실제 연동을 통한 예보구역 데이터 갱신 필요",
            "지역별 날씨 격자 좌표 (nx, ny) 정보 보완 권장",
            "정기적인 지역정보 동기화 배치 작업 설정 필요",
            "지역 계층 구조 검증 및 부모-자식 관계 정리"
        ])
        
        return recommendations


def print_analysis_report(analysis: Dict[str, Any]):
    """분석 결과 보고서 출력"""
    
    print("🌏 " + "="*80)
    print("🌏 지역정보 데이터베이스 종합 분석 보고서")
    print("🌏 " + "="*80)
    print(f"📅 분석 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 1. 데이터 요약
    summary = analysis.get('summary', {})
    print("📊 **데이터 요약**")
    print("-" * 50)
    print(f"• 기본 지역정보 (regions): {summary.get('regions', 0):,}개")
    print(f"• 기상 예보구역 (weather_regions): {summary.get('weather_regions', 0):,}개")
    print(f"• 통합 지역정보 (unified_regions): {summary.get('unified_regions', 0):,}개")
    print(f"• 법정동 코드 (legal_dong_codes): {summary.get('legal_dong_codes', 0):,}개")
    if 'deduplication_rate' in summary:
        print(f"• 중복 제거율: {summary['deduplication_rate']}%")
    print()
    
    # 2. Regions 테이블 분석
    regions_analysis = analysis.get('regions_analysis', {})
    if regions_analysis:
        print("🏛️ **기본 지역정보 (regions) 분석**")
        print("-" * 50)
        
        basic_stats = regions_analysis.get('basic_stats', {})
        print(f"• 총 지역 수: {basic_stats.get('total_count', 0):,}개")
        print(f"• 좌표 정보 보유: {basic_stats.get('with_coordinates', 0):,}개")
        print(f"• 상위 지역 연결: {basic_stats.get('with_parent', 0):,}개")
        print(f"• 평균 레벨: {basic_stats.get('avg_level', 0):.1f}")
        
        level_dist = regions_analysis.get('level_distribution', {})
        if level_dist:
            print("• 레벨별 분포:")
            for level, count in level_dist.items():
                level_name = {
                    '1': '시도급', '2': '시군구급', '3': '읍면동급'
                }.get(level, f'레벨{level}')
                print(f"  - {level_name}: {count:,}개")
        
        coord_status = regions_analysis.get('coordinate_status', {})
        if coord_status:
            print("• 좌표 정보 상태:")
            for status, count in coord_status.items():
                status_name = {
                    'complete': '완전', 'missing': '누락', 'partial': '부분'
                }.get(status, status)
                print(f"  - {status_name}: {count:,}개")
        print()
    
    # 3. Weather Regions 테이블 분석
    weather_analysis = analysis.get('weather_regions_analysis', {})
    if weather_analysis:
        print("🌤️ **기상 예보구역 (weather_regions) 분석**")
        print("-" * 50)
        
        basic_stats = weather_analysis.get('basic_stats', {})
        print(f"• 총 예보구역 수: {basic_stats.get('total_count', 0):,}개")
        print(f"• 격자 좌표 보유: {basic_stats.get('with_grid', 0):,}개")
        print(f"• 활성 상태: {basic_stats.get('active_count', 0):,}개")
        
        if basic_stats.get('avg_lat') and basic_stats.get('avg_lon'):
            print(f"• 평균 좌표: ({float(basic_stats['avg_lat']):.4f}, {float(basic_stats['avg_lon']):.4f})")
        
        code_patterns = weather_analysis.get('code_patterns', {})
        if code_patterns:
            print("• 지역 코드 패턴:")
            for pattern, count in code_patterns.items():
                pattern_name = {
                    'numeric': '숫자형', 'forecast_code': '예보구역코드', 'other': '기타'
                }.get(pattern, pattern)
                print(f"  - {pattern_name}: {count:,}개")
        
        recent_additions = weather_analysis.get('recent_additions', [])
        if recent_additions:
            print("• 최근 추가된 예보구역 (상위 5개):")
            for region in recent_additions[:5]:
                print(f"  - {region['region_code']}: {region['region_name']}")
        print()
    
    # 4. Unified Regions 테이블 분석
    unified_analysis = analysis.get('unified_regions_analysis', {})
    if unified_analysis:
        print("🗺️ **통합 지역정보 (unified_regions) 분석**")
        print("-" * 50)
        
        basic_stats = unified_analysis.get('basic_stats', {})
        print(f"• 총 통합지역 수: {basic_stats.get('total_count', 0):,}개")
        print(f"• 부모 지역 연결: {basic_stats.get('with_parent', 0):,}개")
        print(f"• 좌표 정보 보유: {basic_stats.get('with_coordinates', 0):,}개")
        print(f"• 행정구역코드 보유: {basic_stats.get('with_admin_code', 0):,}개")
        print(f"• 영문명 보유: {basic_stats.get('with_english_name', 0):,}개")
        
        level_dist = unified_analysis.get('level_distribution', {})
        if level_dist:
            print("• 레벨별 분포:")
            for level, count in level_dist.items():
                level_name = {
                    '1': '최상위 (시도)', '2': '중간 (시군구)', '3': '하위 (읍면동)'
                }.get(level, f'레벨{level}')
                print(f"  - {level_name}: {count:,}개")
        
        top_regions = unified_analysis.get('top_level_regions', [])
        if top_regions:
            print("• 최상위 지역 (시도급):")
            for region in top_regions[:10]:  # 상위 10개만 표시
                admin_code = region.get('administrative_code', 'N/A')
                print(f"  - {region['region_name']} ({admin_code})")
        print()
    
    # 5. 통합 품질 분석
    integration_quality = analysis.get('integration_quality', {})
    if integration_quality:
        print("🔍 **데이터 통합 품질 분석**")
        print("-" * 50)
        
        coord_quality = integration_quality.get('coordinate_quality', {})
        if coord_quality:
            total_coords = coord_quality.get('total_coords', 0)
            valid_coords = coord_quality.get('valid_korea_coords', 0)
            if total_coords > 0:
                validity_rate = (valid_coords / total_coords) * 100
                print(f"• 좌표 유효성: {valid_coords:,}/{total_coords:,} ({validity_rate:.1f}%)")
        
        duplicates = integration_quality.get('duplicate_names', [])
        if duplicates:
            print("• 중복 지역명 (상위 5개):")
            for dup in duplicates[:5]:
                print(f"  - '{dup['region_name']}': {dup['count']}개")
        print()
    
    # 6. 지리적 커버리지
    coverage = analysis.get('coverage_analysis', {})
    if coverage:
        print("🗾 **지리적 커버리지 분석**")
        print("-" * 50)
        
        sido_coverage = coverage.get('sido_coverage', [])
        if sido_coverage:
            print("• 시도별 통합지역 수:")
            for sido in sido_coverage[:10]:  # 상위 10개
                print(f"  - {sido['sido_name']}: {sido['region_count']:,}개")
        
        coord_range = coverage.get('coordinate_range', {})
        if coord_range and coord_range.get('min_lat'):
            print("• 좌표 범위:")
            print(f"  - 위도: {float(coord_range['min_lat']):.4f} ~ {float(coord_range['max_lat']):.4f}")
            print(f"  - 경도: {float(coord_range['min_lon']):.4f} ~ {float(coord_range['max_lon']):.4f}")
        print()
    
    # 7. 데이터 완성도
    completeness = analysis.get('data_completeness', {})
    if completeness:
        print("📈 **데이터 완성도 분석**")
        print("-" * 50)
        
        field_completeness = completeness.get('field_completeness', {})
        if field_completeness:
            print("• 필드별 완성도:")
            fields = {
                'region_code_completeness': '지역코드',
                'region_name_completeness': '지역명',
                'coordinate_completeness': '좌표정보',
                'admin_code_completeness': '행정구역코드',
                'full_name_completeness': '전체명',
                'english_name_completeness': '영문명'
            }
            
            for field, name in fields.items():
                if field in field_completeness:
                    rate = float(field_completeness[field])
                    print(f"  - {name}: {rate:.1f}%")
        
        source_dist = completeness.get('data_source_distribution', {})
        if source_dist:
            print("• 데이터 소스별 분포:")
            source_names = {
                'forecast_regions': '기상 예보구역',
                'original_regions': '기존 지역정보',
                'city_data': '도시 데이터',
                'other': '기타'
            }
            for source, count in source_dist.items():
                name = source_names.get(source, source)
                print(f"  - {name}: {count:,}개")
        print()
    
    # 8. 권장사항
    recommendations = analysis.get('recommendations', [])
    if recommendations:
        print("💡 **개선 권장사항**")
        print("-" * 50)
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
        print()
    
    print("🌏 " + "="*80)
    print("🌏 분석 완료")
    print("🌏 " + "="*80)


def main():
    """메인 실행 함수"""
    
    try:
        print("🔍 지역정보 데이터베이스 종합 분석 시작...\n")
        
        analyzer = RegionDataAnalyzer()
        analysis_result = analyzer.analyze_all_region_data()
        
        # 분석 결과 출력
        print_analysis_report(analysis_result)
        
        # JSON 파일로 상세 결과 저장
        output_file = f"region_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # datetime 객체를 문자열로 변환하는 함수
        def json_serial(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Type {type(obj)} not serializable")
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(analysis_result, f, ensure_ascii=False, indent=2, default=json_serial)
        
        print(f"\n📄 상세 분석 결과가 '{output_file}' 파일로 저장되었습니다.")
        
    except Exception as e:
        print(f"❌ 분석 중 오류 발생: {e}")


if __name__ == "__main__":
    main()