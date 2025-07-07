#!/usr/bin/env python3
"""
지역정보 통합 스크립트

기상청 예보구역 데이터와 기존 지역정보를 통합하여
통일된 지역 데이터베이스를 구축하는 스크립트입니다.

Usage:
    python scripts/integrate_region_data.py --mode full
    python scripts/integrate_region_data.py --mode forecast_only
    python scripts/integrate_region_data.py --test
"""

import sys
import os
import argparse
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import json
import uuid

# 프로젝트 루트 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import get_app_settings
from app.core.database_manager import DatabaseManager
from app.core.database_manager_extension import extend_database_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RegionDataIntegrator:
    """지역정보 통합기"""
    
    def __init__(self):
        self.db_manager = extend_database_manager(DatabaseManager().sync_manager)
        
    def analyze_current_data(self) -> Dict[str, Any]:
        """현재 지역 데이터 현황 분석"""
        try:
            analysis = {}
            
            # 각 테이블별 데이터 현황
            tables = ['regions', 'weather_regions', 'unified_regions', 'legal_dong_codes']
            
            for table in tables:
                if table == 'weather_regions':
                    query = f"""
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(CASE WHEN region_code IS NOT NULL THEN 1 END) as with_code,
                        COUNT(CASE WHEN region_name IS NOT NULL THEN 1 END) as with_name,
                        COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coordinates
                    FROM {table}
                    """
                elif table == 'unified_regions':
                    query = f"""
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(CASE WHEN region_code IS NOT NULL THEN 1 END) as with_code,
                        COUNT(CASE WHEN region_name IS NOT NULL THEN 1 END) as with_name,
                        COUNT(CASE WHEN center_latitude IS NOT NULL AND center_longitude IS NOT NULL THEN 1 END) as with_coordinates
                    FROM {table}
                    """
                elif table == 'legal_dong_codes':
                    query = f"""
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(CASE WHEN code IS NOT NULL THEN 1 END) as with_code,
                        COUNT(CASE WHEN name IS NOT NULL THEN 1 END) as with_name
                    FROM {table}
                    """
                else:  # regions
                    query = f"""
                    SELECT 
                        COUNT(*) as total_count,
                        COUNT(CASE WHEN region_code IS NOT NULL THEN 1 END) as with_code,
                        COUNT(CASE WHEN region_name IS NOT NULL THEN 1 END) as with_name
                    FROM {table}
                    """
                
                result = self.db_manager.fetch_one(query)
                analysis[table] = dict(result) if result else {}
            
            # 중복 데이터 분석
            analysis['duplicates'] = self._analyze_duplicates()
            
            # 매핑 가능성 분석
            analysis['mapping_potential'] = self._analyze_mapping_potential()
            
            logger.info(f"현재 데이터 분석 완료: {analysis}")
            return analysis
            
        except Exception as e:
            logger.error(f"데이터 분석 실패: {e}")
            return {}
    
    def _analyze_duplicates(self) -> Dict[str, Any]:
        """중복 데이터 분석"""
        try:
            duplicates = {}
            
            # 지역명 기반 중복 확인
            query = """
            SELECT 
                r.region_name,
                wr.region_name as weather_name,
                ur.region_name as unified_name,
                r.region_code,
                wr.region_code as weather_code,
                ur.region_code as unified_code
            FROM regions r
            FULL OUTER JOIN weather_regions wr ON r.region_name = wr.region_name
            FULL OUTER JOIN unified_regions ur ON r.region_name = ur.region_name
            WHERE r.region_name IS NOT NULL 
               OR wr.region_name IS NOT NULL 
               OR ur.region_name IS NOT NULL
            """
            
            results = self.db_manager.fetch_all(query)
            duplicates['name_matches'] = len([r for r in results if sum([
                1 for x in [r.get('region_name'), r.get('weather_name'), r.get('unified_name')] 
                if x is not None
            ]) > 1])
            
            # 좌표 기반 중복 확인 (100m 이내)
            query = """
            SELECT COUNT(*) as nearby_count
            FROM weather_regions wr1
            JOIN weather_regions wr2 ON (
                wr1.id != wr2.id 
                AND ABS(wr1.latitude - wr2.latitude) < 0.001 
                AND ABS(wr1.longitude - wr2.longitude) < 0.001
            )
            """
            
            result = self.db_manager.fetch_one(query)
            duplicates['coordinate_proximity'] = result.get('nearby_count', 0) if result else 0
            
            return duplicates
            
        except Exception as e:
            logger.error(f"중복 분석 실패: {e}")
            return {}
    
    def _analyze_mapping_potential(self) -> Dict[str, Any]:
        """매핑 가능성 분석"""
        try:
            mapping = {}
            
            # 행정구역 코드 매핑 가능성
            query = """
            SELECT 
                COUNT(CASE WHEN r.region_code = ur.administrative_code THEN 1 END) as direct_matches,
                COUNT(CASE WHEN LEFT(r.region_code, 2) = LEFT(ur.administrative_code, 2) THEN 1 END) as partial_matches,
                COUNT(*) as total_comparisons
            FROM regions r
            CROSS JOIN unified_regions ur
            WHERE r.region_code IS NOT NULL AND ur.administrative_code IS NOT NULL
            """
            
            result = self.db_manager.fetch_one(query)
            mapping['administrative_code'] = dict(result) if result else {}
            
            # 지역명 유사도 분석
            query = """
            SELECT 
                COUNT(CASE WHEN r.region_name = wr.region_name THEN 1 END) as exact_name_matches,
                COUNT(CASE WHEN r.region_name LIKE '%' || wr.region_name || '%' 
                           OR wr.region_name LIKE '%' || r.region_name || '%' THEN 1 END) as partial_name_matches,
                COUNT(*) as total_name_comparisons
            FROM regions r
            CROSS JOIN weather_regions wr
            WHERE r.region_name IS NOT NULL AND wr.region_name IS NOT NULL
            """
            
            result = self.db_manager.fetch_one(query)
            mapping['name_similarity'] = dict(result) if result else {}
            
            return mapping
            
        except Exception as e:
            logger.error(f"매핑 분석 실패: {e}")
            return {}
    
    def create_unified_region_mapping(self) -> List[Dict[str, Any]]:
        """통합 지역 매핑 생성"""
        try:
            logger.info("통합 지역 매핑 생성 시작")
            
            mappings = []
            
            # 1. 기존 regions 테이블 기반 매핑
            regions_mapping = self._create_regions_based_mapping()
            mappings.extend(regions_mapping)
            
            # 2. weather_regions 테이블 기반 매핑
            weather_mapping = self._create_weather_regions_based_mapping()
            mappings.extend(weather_mapping)
            
            # 3. 새로운 예보구역 데이터 매핑
            forecast_mapping = self._create_forecast_regions_mapping()
            mappings.extend(forecast_mapping)
            
            # 4. 중복 제거 및 우선순위 적용
            unified_mappings = self._merge_and_deduplicate_mappings(mappings)
            
            logger.info(f"통합 매핑 생성 완료: {len(unified_mappings)}개")
            return unified_mappings
            
        except Exception as e:
            logger.error(f"통합 매핑 생성 실패: {e}")
            return []
    
    def _create_regions_based_mapping(self) -> List[Dict[str, Any]]:
        """기존 regions 테이블 기반 매핑 생성"""
        try:
            query = """
            SELECT 
                region_code,
                region_name,
                parent_region_code,
                region_level,
                latitude,
                longitude,
                created_at,
                updated_at
            FROM regions
            ORDER BY region_level, region_code
            """
            
            results = self.db_manager.fetch_all(query)
            mappings = []
            
            for row in results:
                mapping = {
                    "region_id": str(uuid.uuid4()),
                    "region_code": row.get("region_code"),
                    "region_name": row.get("region_name"),
                    "region_name_full": row.get("region_name"),
                    "region_name_en": None,
                    "parent_region_id": None,  # 나중에 매핑
                    "region_level": row.get("region_level", 0),
                    "center_latitude": row.get("latitude"),
                    "center_longitude": row.get("longitude"),
                    "boundary_data": None,
                    "administrative_code": row.get("region_code"),
                    "forecast_region_code": None,
                    "grid_x": None,
                    "grid_y": None,
                    "is_active": True,
                    "source_table": "regions",
                    "source_priority": 1,
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at")
                }
                mappings.append(mapping)
            
            logger.info(f"regions 기반 매핑 생성: {len(mappings)}개")
            return mappings
            
        except Exception as e:
            logger.error(f"regions 매핑 생성 실패: {e}")
            return []
    
    def _create_weather_regions_based_mapping(self) -> List[Dict[str, Any]]:
        """weather_regions 테이블 기반 매핑 생성"""
        try:
            query = """
            SELECT 
                region_code,
                region_name,
                latitude,
                longitude,
                grid_x,
                grid_y,
                is_active,
                created_at,
                updated_at
            FROM weather_regions
            WHERE is_active = true
            ORDER BY region_code
            """
            
            results = self.db_manager.fetch_all(query)
            mappings = []
            
            for row in results:
                mapping = {
                    "region_id": str(uuid.uuid4()),
                    "region_code": row.get("region_code"),
                    "region_name": row.get("region_name"),
                    "region_name_full": row.get("region_name"),
                    "region_name_en": None,
                    "parent_region_id": None,
                    "region_level": self._determine_region_level_from_code(row.get("region_code")),
                    "center_latitude": row.get("latitude"),
                    "center_longitude": row.get("longitude"),
                    "boundary_data": None,
                    "administrative_code": self._map_to_administrative_code(row.get("region_code")),
                    "forecast_region_code": row.get("region_code"),
                    "grid_x": row.get("grid_x"),
                    "grid_y": row.get("grid_y"),
                    "is_active": row.get("is_active", True),
                    "source_table": "weather_regions",
                    "source_priority": 2,
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at")
                }
                mappings.append(mapping)
            
            logger.info(f"weather_regions 기반 매핑 생성: {len(mappings)}개")
            return mappings
            
        except Exception as e:
            logger.error(f"weather_regions 매핑 생성 실패: {e}")
            return []
    
    def _create_forecast_regions_mapping(self) -> List[Dict[str, Any]]:
        """새로 수집된 예보구역 데이터 매핑 생성"""
        try:
            # 새로 수집된 예보구역 데이터가 weather_regions에 저장되었다고 가정
            # 기존 데이터와 구분하기 위해 최근 수집 데이터만 선택
            query = """
            SELECT 
                region_code,
                region_name,
                latitude,
                longitude,
                grid_x,
                grid_y,
                is_active,
                created_at,
                updated_at
            FROM weather_regions
            WHERE created_at >= CURRENT_DATE - INTERVAL '1 day'
            ORDER BY region_code
            """
            
            results = self.db_manager.fetch_all(query)
            mappings = []
            
            for row in results:
                mapping = {
                    "region_id": str(uuid.uuid4()),
                    "region_code": row.get("region_code"),
                    "region_name": row.get("region_name"),
                    "region_name_full": row.get("region_name"),
                    "region_name_en": None,
                    "parent_region_id": None,
                    "region_level": self._determine_region_level_from_forecast_code(row.get("region_code")),
                    "center_latitude": row.get("latitude"),
                    "center_longitude": row.get("longitude"),
                    "boundary_data": None,
                    "administrative_code": self._forecast_to_administrative_code(row.get("region_code")),
                    "forecast_region_code": row.get("region_code"),
                    "grid_x": row.get("grid_x"),
                    "grid_y": row.get("grid_y"),
                    "is_active": row.get("is_active", True),
                    "source_table": "forecast_regions",
                    "source_priority": 3,
                    "created_at": row.get("created_at"),
                    "updated_at": row.get("updated_at")
                }
                mappings.append(mapping)
            
            logger.info(f"예보구역 기반 매핑 생성: {len(mappings)}개")
            return mappings
            
        except Exception as e:
            logger.error(f"예보구역 매핑 생성 실패: {e}")
            return []
    
    def _determine_region_level_from_code(self, region_code: str) -> int:
        """지역 코드를 기반으로 레벨 결정"""
        if not region_code:
            return 0
        
        if len(region_code) <= 2:
            return 1  # 시도
        elif len(region_code) <= 5:
            return 2  # 시군구
        else:
            return 3  # 읍면동
    
    def _determine_region_level_from_forecast_code(self, forecast_code: str) -> int:
        """예보구역 코드를 기반으로 레벨 결정"""
        if not forecast_code:
            return 0
        
        if forecast_code.endswith("00000"):
            return 1  # 시도
        elif forecast_code.endswith("000"):
            return 2  # 시군구
        else:
            return 3  # 읍면동
    
    def _map_to_administrative_code(self, region_code: str) -> Optional[str]:
        """지역 코드를 행정구역 코드로 매핑"""
        if not region_code:
            return None
        
        # 이미 행정구역 코드 형태인 경우
        if region_code.isdigit() and len(region_code) <= 5:
            return region_code
        
        return region_code
    
    def _forecast_to_administrative_code(self, forecast_code: str) -> Optional[str]:
        """예보구역 코드를 행정구역 코드로 변환"""
        if not forecast_code:
            return None
        
        # 기상청 예보구역 코드 → 행정구역 코드 매핑 테이블
        mapping = {
            "11B00000": "11",     # 서울
            "26000000": "26",     # 부산
            "27000000": "27",     # 대구
            "28000000": "28",     # 인천
            "29000000": "29",     # 광주
            "30000000": "30",     # 대전
            "31000000": "31",     # 울산
            "36000000": "36",     # 세종
            "41000000": "41",     # 경기
            "42000000": "42",     # 강원
            "43000000": "43",     # 충북
            "44000000": "44",     # 충남
            "45000000": "45",     # 전북
            "46000000": "46",     # 전남
            "47000000": "47",     # 경북
            "48000000": "48",     # 경남
            "49000000": "49",     # 제주
        }
        
        return mapping.get(forecast_code, forecast_code[:2] if len(forecast_code) >= 2 else None)
    
    def _merge_and_deduplicate_mappings(self, mappings: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """매핑 데이터 병합 및 중복 제거"""
        try:
            # 지역명과 코드 기준으로 그룹화
            region_groups = {}
            
            for mapping in mappings:
                # 그룹 키 생성 (지역명 + 행정구역코드)
                group_key = f"{mapping.get('region_name', '')}_{mapping.get('administrative_code', '')}"
                
                if group_key not in region_groups:
                    region_groups[group_key] = []
                
                region_groups[group_key].append(mapping)
            
            # 각 그룹에서 우선순위가 높은 매핑 선택
            unified_mappings = []
            
            for group_key, group_mappings in region_groups.items():
                if len(group_mappings) == 1:
                    unified_mappings.append(group_mappings[0])
                else:
                    # 우선순위와 데이터 완성도를 고려하여 최적 매핑 선택
                    best_mapping = self._select_best_mapping(group_mappings)
                    unified_mappings.append(best_mapping)
            
            logger.info(f"매핑 병합 완료: {len(mappings)} → {len(unified_mappings)}")
            return unified_mappings
            
        except Exception as e:
            logger.error(f"매핑 병합 실패: {e}")
            return mappings
    
    def _select_best_mapping(self, mappings: List[Dict[str, Any]]) -> Dict[str, Any]:
        """여러 매핑 중 최적 매핑 선택"""
        # 우선순위 점수 계산
        def calculate_score(mapping):
            score = 0
            
            # 소스 우선순위 (낮을수록 높은 우선순위)
            score += (4 - mapping.get('source_priority', 4)) * 10
            
            # 데이터 완성도
            if mapping.get('center_latitude') and mapping.get('center_longitude'):
                score += 5
            if mapping.get('grid_x') and mapping.get('grid_y'):
                score += 3
            if mapping.get('forecast_region_code'):
                score += 2
            if mapping.get('administrative_code'):
                score += 1
            
            return score
        
        # 점수 기준으로 정렬하여 최고점 반환
        scored_mappings = [(calculate_score(m), m) for m in mappings]
        scored_mappings.sort(key=lambda x: x[0], reverse=True)
        
        best_mapping = scored_mappings[0][1]
        
        # 다른 매핑들의 유용한 정보 병합
        for _, mapping in scored_mappings[1:]:
            if not best_mapping.get('center_latitude') and mapping.get('center_latitude'):
                best_mapping['center_latitude'] = mapping.get('center_latitude')
                best_mapping['center_longitude'] = mapping.get('center_longitude')
            
            if not best_mapping.get('grid_x') and mapping.get('grid_x'):
                best_mapping['grid_x'] = mapping.get('grid_x')
                best_mapping['grid_y'] = mapping.get('grid_y')
            
            if not best_mapping.get('forecast_region_code') and mapping.get('forecast_region_code'):
                best_mapping['forecast_region_code'] = mapping.get('forecast_region_code')
        
        return best_mapping
    
    def save_unified_regions(self, unified_mappings: List[Dict[str, Any]]) -> int:
        """통합 지역 데이터를 데이터베이스에 저장"""
        try:
            logger.info("통합 지역 데이터 저장 시작")
            
            # 기존 unified_regions 테이블 백업 (선택사항)
            self._backup_existing_unified_regions()
            
            saved_count = 0
            
            for mapping in unified_mappings:
                try:
                    success = self._upsert_unified_region(mapping)
                    if success:
                        saved_count += 1
                        
                except Exception as e:
                    logger.error(f"통합 지역 저장 실패 {mapping.get('region_code')}: {e}")
                    continue
            
            # 부모-자식 관계 설정
            self._update_parent_child_relationships(unified_mappings)
            
            logger.info(f"통합 지역 데이터 저장 완료: {saved_count}/{len(unified_mappings)}")
            return saved_count
            
        except Exception as e:
            logger.error(f"통합 지역 데이터 저장 실패: {e}")
            return 0
    
    def _backup_existing_unified_regions(self):
        """기존 unified_regions 데이터 백업"""
        try:
            backup_table = f"unified_regions_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            query = f"""
            CREATE TABLE {backup_table} AS 
            SELECT * FROM unified_regions
            """
            
            self.db_manager.execute_update(query)
            logger.info(f"기존 데이터 백업 완료: {backup_table}")
            
        except Exception as e:
            logger.warning(f"데이터 백업 실패: {e}")
    
    def _upsert_unified_region(self, mapping: Dict[str, Any]) -> bool:
        """통합 지역 데이터 UPSERT"""
        try:
            query = """
            INSERT INTO unified_regions (
                region_id, region_code, region_name, region_name_full, region_name_en,
                parent_region_id, region_level, center_latitude, center_longitude,
                boundary_data, administrative_code, is_active, created_at, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (region_code) DO UPDATE SET
                region_name = EXCLUDED.region_name,
                region_name_full = EXCLUDED.region_name_full,
                region_name_en = EXCLUDED.region_name_en,
                parent_region_id = EXCLUDED.parent_region_id,
                region_level = EXCLUDED.region_level,
                center_latitude = EXCLUDED.center_latitude,
                center_longitude = EXCLUDED.center_longitude,
                boundary_data = EXCLUDED.boundary_data,
                administrative_code = EXCLUDED.administrative_code,
                is_active = EXCLUDED.is_active,
                updated_at = CURRENT_TIMESTAMP
            """
            
            # boundary_data JSON 직렬화
            boundary_data = mapping.get('boundary_data')
            if boundary_data and not isinstance(boundary_data, str):
                boundary_data = json.dumps(boundary_data)
            
            current_time = datetime.now()
            
            params = (
                mapping.get('region_id'),
                mapping.get('region_code'),
                mapping.get('region_name'),
                mapping.get('region_name_full'),
                mapping.get('region_name_en'),
                mapping.get('parent_region_id'),
                mapping.get('region_level'),
                mapping.get('center_latitude'),
                mapping.get('center_longitude'),
                boundary_data,
                mapping.get('administrative_code'),
                mapping.get('is_active', True),
                mapping.get('created_at', current_time),
                mapping.get('updated_at', current_time)
            )
            
            self.db_manager.execute_update(query, params)
            return True
            
        except Exception as e:
            logger.error(f"통합 지역 UPSERT 실패: {e}")
            return False
    
    def _update_parent_child_relationships(self, unified_mappings: List[Dict[str, Any]]):
        """부모-자식 관계 업데이트"""
        try:
            logger.info("부모-자식 관계 설정 시작")
            
            # 지역 코드 → region_id 매핑 테이블 생성
            code_to_id = {}
            for mapping in unified_mappings:
                if mapping.get('region_code') and mapping.get('region_id'):
                    code_to_id[mapping['region_code']] = mapping['region_id']
            
            # 각 지역의 부모 지역 찾기 및 업데이트
            for mapping in unified_mappings:
                parent_code = self._find_parent_region_code(mapping.get('region_code'))
                
                if parent_code and parent_code in code_to_id:
                    parent_id = code_to_id[parent_code]
                    
                    query = """
                    UPDATE unified_regions 
                    SET parent_region_id = %s 
                    WHERE region_id = %s
                    """
                    
                    self.db_manager.execute_update(query, (parent_id, mapping['region_id']))
            
            logger.info("부모-자식 관계 설정 완료")
            
        except Exception as e:
            logger.error(f"부모-자식 관계 설정 실패: {e}")
    
    def _find_parent_region_code(self, region_code: str) -> Optional[str]:
        """지역 코드의 부모 지역 코드 찾기"""
        if not region_code or len(region_code) < 2:
            return None
        
        # 행정구역 코드 기반 부모 찾기
        if region_code.isdigit():
            if len(region_code) == 5:  # 시군구 → 시도
                return region_code[:2]
            elif len(region_code) > 5:  # 읍면동 → 시군구
                return region_code[:5]
        
        # 예보구역 코드 기반 부모 찾기
        elif len(region_code) == 8:
            if region_code.endswith("000"):  # 시군구 → 시도
                return region_code[:2] + "000000"
            else:  # 읍면동 → 시군구
                return region_code[:5] + "000"
        
        return None


def main():
    """메인 실행 함수"""
    parser = argparse.ArgumentParser(description="지역정보 통합")
    parser.add_argument("--mode", choices=["full", "forecast_only", "analysis", "test"], 
                       default="full", help="실행 모드")
    parser.add_argument("--backup", action="store_true", help="기존 데이터 백업")
    parser.add_argument("--verbose", "-v", action="store_true", help="상세 로그 출력")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        integrator = RegionDataIntegrator()
        
        if args.mode == "analysis":
            logger.info("데이터 분석 모드")
            analysis = integrator.analyze_current_data()
            print(json.dumps(analysis, indent=2, ensure_ascii=False, default=str))
            
        elif args.mode == "test":
            logger.info("테스트 모드: 소량 데이터 통합")
            # 테스트용 로직 구현
            analysis = integrator.analyze_current_data()
            logger.info(f"테스트 분석 결과: {analysis}")
            
        else:
            logger.info(f"{args.mode} 모드로 지역정보 통합 시작")
            
            # 1. 현재 데이터 분석
            analysis = integrator.analyze_current_data()
            logger.info(f"현재 데이터 분석: {analysis}")
            
            # 2. 통합 매핑 생성
            unified_mappings = integrator.create_unified_region_mapping()
            
            if unified_mappings:
                # 3. 통합 데이터 저장
                saved_count = integrator.save_unified_regions(unified_mappings)
                logger.info(f"지역정보 통합 완료: {saved_count}개 저장")
            else:
                logger.warning("통합할 지역 데이터가 없습니다")
                
    except Exception as e:
        logger.error(f"지역정보 통합 작업 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()