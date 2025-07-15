"""
하이브리드 데이터베이스 매니저

기존 Raw SQL 기반 DatabaseManager와 새로운 SQLAlchemy ORM을 통합하여
점진적 마이그레이션을 지원하는 하이브리드 데이터베이스 접근 계층입니다.
"""

import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from contextlib import contextmanager

from app.core.database_manager_extension import get_extended_database_manager
from app.core.orm_integration import (
    get_orm_manager, get_tourism_manager, get_query_builder,
    ORMDatabaseManager, TourismDataManager, HybridQueryBuilder,
    is_orm_available
)

logger = logging.getLogger(__name__)


class HybridDatabaseManager:
    """
    하이브리드 데이터베이스 매니저
    
    기존 Raw SQL 시스템과 SQLAlchemy ORM을 통합하여 제공합니다.
    - 성능이 중요한 배치 작업: Raw SQL 사용
    - 복잡한 관계형 쿼리: ORM 사용
    - 타입 안전성이 필요한 CRUD: ORM 사용
    """
    
    def __init__(self, prefer_orm: bool = True, fallback_to_raw: bool = True):
        """
        하이브리드 데이터베이스 매니저 초기화
        
        Args:
            prefer_orm: ORM 우선 사용 여부
            fallback_to_raw: ORM 실패 시 Raw SQL로 폴백 여부
        """
        self.prefer_orm = prefer_orm and is_orm_available()
        self.fallback_to_raw = fallback_to_raw
        
        # 기존 Raw SQL 매니저 (확장 기능 포함)
        self.raw_manager = get_extended_database_manager()
        
        # ORM 관련 매니저들
        if is_orm_available():
            self.orm_manager: ORMDatabaseManager = get_orm_manager()
            self.tourism_manager: TourismDataManager = get_tourism_manager()
            self.query_builder: HybridQueryBuilder = get_query_builder()
        else:
            self.orm_manager = None
            self.tourism_manager = None
            self.query_builder = None
            logger.warning("ORM 기능을 사용할 수 없습니다. Raw SQL만 사용됩니다.")
        
        self.operation_stats = {
            "total_operations": 0,
            "orm_operations": 0,
            "raw_sql_operations": 0,
            "fallback_operations": 0,
            "failed_operations": 0
        }
        
        logger.info(f"하이브리드 데이터베이스 매니저 초기화 (ORM 우선: {self.prefer_orm})")
    
    def _record_operation(self, method: str, success: bool = True):
        """작업 통계 기록"""
        self.operation_stats["total_operations"] += 1
        if method == "orm":
            self.operation_stats["orm_operations"] += 1
        elif method == "raw":
            self.operation_stats["raw_sql_operations"] += 1
        elif method == "fallback":
            self.operation_stats["fallback_operations"] += 1
        
        if not success:
            self.operation_stats["failed_operations"] += 1
    
    # =============================================================================
    # 관광지 데이터 관리 (ORM 우선)
    # =============================================================================
    
    def upsert_tourist_attraction(self, data: Dict[str, Any]) -> bool:
        """
        관광지 데이터 UPSERT
        
        ORM을 우선 사용하고, 실패 시 Raw SQL로 폴백
        """
        if self.prefer_orm and self.tourism_manager:
            try:
                result = self.tourism_manager.upsert_tourist_attraction(data)
                self._record_operation("orm", result)
                return result
            except Exception as e:
                logger.warning(f"ORM 관광지 UPSERT 실패, Raw SQL로 폴백: {e}")
                if self.fallback_to_raw:
                    try:
                        result = self.raw_manager.upsert_tourist_attraction(data)
                        self._record_operation("fallback", result)
                        return result
                    except Exception as e2:
                        logger.error(f"Raw SQL 관광지 UPSERT도 실패: {e2}")
                        self._record_operation("fallback", False)
                        return False
        
        # Raw SQL 직접 사용
        try:
            result = self.raw_manager.upsert_tourist_attraction(data)
            self._record_operation("raw", result)
            return result
        except Exception as e:
            logger.error(f"관광지 UPSERT 실패: {e}")
            self._record_operation("raw", False)
            return False
    
    def get_tourist_attractions_by_region(self, region_code: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        지역별 관광지 조회
        
        ORM 사용으로 타입 안전성 확보
        """
        if self.prefer_orm and self.tourism_manager:
            try:
                orm_results = self.tourism_manager.get_tourist_attractions_by_region(region_code, limit)
                # ORM 객체를 딕셔너리로 변환
                results = []
                for attraction in orm_results:
                    if hasattr(attraction, '__dict__'):
                        result_dict = {}
                        for key, value in attraction.__dict__.items():
                            if not key.startswith('_'):
                                if isinstance(value, datetime):
                                    result_dict[key] = value.isoformat()
                                else:
                                    result_dict[key] = value
                        results.append(result_dict)
                
                self._record_operation("orm", True)
                return results
            except Exception as e:
                logger.warning(f"ORM 관광지 조회 실패, Raw SQL로 폴백: {e}")
        
        # Raw SQL 폴백
        try:
            query = """
            SELECT * FROM tourist_attractions 
            WHERE region_code = %s 
            ORDER BY created_at DESC 
            LIMIT %s
            """
            results = self.raw_manager.fetch_all(query, (region_code, limit))
            self._record_operation("fallback" if self.prefer_orm else "raw", True)
            return results
        except Exception as e:
            logger.error(f"관광지 조회 실패: {e}")
            self._record_operation("fallback" if self.prefer_orm else "raw", False)
            return []
    
    def search_tourist_attractions(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        관광지 동적 검색
        
        복잡한 검색 조건은 하이브리드 쿼리 빌더 사용
        """
        if self.query_builder:
            try:
                results = self.query_builder.execute_tourism_search(filters)
                self._record_operation("orm", True)
                return results
            except Exception as e:
                logger.warning(f"하이브리드 검색 실패: {e}")
        
        # 간단한 Raw SQL 폴백
        try:
            base_query = "SELECT * FROM tourist_attractions WHERE 1=1"
            params = []
            
            if filters.get('region_code'):
                base_query += " AND region_code = %s"
                params.append(filters['region_code'])
            
            if filters.get('keyword'):
                base_query += " AND (attraction_name ILIKE %s OR description ILIKE %s)"
                keyword = f"%{filters['keyword']}%"
                params.extend([keyword, keyword])
            
            base_query += " ORDER BY created_at DESC"
            
            if filters.get('limit'):
                base_query += f" LIMIT {filters['limit']}"
            
            results = self.raw_manager.fetch_all(base_query, params)
            self._record_operation("fallback" if self.query_builder else "raw", True)
            return results
        except Exception as e:
            logger.error(f"관광지 검색 실패: {e}")
            self._record_operation("fallback" if self.query_builder else "raw", False)
            return []
    
    # =============================================================================
    # 배치 작업용 메서드들 (Raw SQL 우선)
    # =============================================================================
    
    def batch_insert_tourist_attractions(self, attractions_data: List[Dict[str, Any]]) -> int:
        """
        관광지 데이터 배치 삽입
        
        성능이 중요한 배치 작업은 Raw SQL 사용
        """
        try:
            # 기존 Raw SQL 배치 삽입 로직 사용
            success_count = 0
            for data in attractions_data:
                if self.raw_manager.upsert_tourist_attraction(data):
                    success_count += 1
            
            self._record_operation("raw", True)
            logger.info(f"배치 삽입 완료: {success_count}/{len(attractions_data)}")
            return success_count
        except Exception as e:
            logger.error(f"배치 삽입 실패: {e}")
            self._record_operation("raw", False)
            return 0
    
    def insert_raw_api_data(self, raw_data: Dict[str, Any]) -> Optional[str]:
        """
        원본 API 데이터 삽입
        
        대용량 JSONB 처리는 Raw SQL이 더 효율적
        """
        try:
            result = self.raw_manager.insert_raw_data(raw_data)
            self._record_operation("raw", result is not None)
            return result
        except Exception as e:
            logger.error(f"원본 데이터 삽입 실패: {e}")
            self._record_operation("raw", False)
            return None
    
    # =============================================================================
    # 분석 및 통계 (하이브리드)
    # =============================================================================
    
    def get_tourism_statistics(self) -> Dict[str, Any]:
        """
        관광지 통계 조회
        
        복잡한 집계 쿼리는 Raw SQL 사용
        """
        if self.tourism_manager:
            try:
                stats = self.tourism_manager.get_attractions_statistics()
                self._record_operation("orm", True)
                return {"attractions_by_region": stats}
            except Exception as e:
                logger.warning(f"ORM 통계 조회 실패: {e}")
        
        # Raw SQL 폴백
        try:
            query = """
            SELECT 
                'total_attractions' as metric,
                COUNT(*) as value
            FROM tourist_attractions
            
            UNION ALL
            
            SELECT 
                'regions_count' as metric,
                COUNT(DISTINCT region_code) as value
            FROM tourist_attractions
            
            UNION ALL
            
            SELECT 
                'recent_additions' as metric,
                COUNT(*) as value
            FROM tourist_attractions
            WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
            """
            
            raw_stats = self.raw_manager.fetch_all(query)
            stats = {row['metric']: row['value'] for row in raw_stats}
            
            self._record_operation("fallback" if self.tourism_manager else "raw", True)
            return stats
        except Exception as e:
            logger.error(f"통계 조회 실패: {e}")
            self._record_operation("fallback" if self.tourism_manager else "raw", False)
            return {}
    
    def get_data_quality_metrics(self) -> Dict[str, Any]:
        """
        데이터 품질 메트릭 조회
        
        기존 Raw SQL 로직 재사용
        """
        try:
            # 기존 데이터 품질 체크 로직 활용
            metrics = {}
            
            # 관광지 데이터 품질
            query = """
            SELECT 
                COUNT(*) as total_count,
                COUNT(CASE WHEN attraction_name IS NOT NULL AND attraction_name != '' THEN 1 END) as with_name,
                COUNT(CASE WHEN description IS NOT NULL AND description != '' THEN 1 END) as with_description,
                COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as with_coordinates
            FROM tourist_attractions
            """
            
            result = self.raw_manager.fetch_all(query)
            if result:
                row = result[0]
                total = row['total_count']
                if total > 0:
                    metrics['tourist_attractions'] = {
                        'total_count': total,
                        'name_completeness': (row['with_name'] / total) * 100,
                        'description_completeness': (row['with_description'] / total) * 100,
                        'coordinates_completeness': (row['with_coordinates'] / total) * 100
                    }
            
            self._record_operation("raw", True)
            return metrics
        except Exception as e:
            logger.error(f"데이터 품질 메트릭 조회 실패: {e}")
            self._record_operation("raw", False)
            return {}
    
    # =============================================================================
    # 직접 접근 메서드들
    # =============================================================================
    
    def execute_raw_sql(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        """Raw SQL 직접 실행"""
        try:
            if self.orm_manager:
                # ORM 매니저의 Raw SQL 실행 사용 (타입 변환 등 이점)
                param_dict = {}
                if params:
                    for i, param in enumerate(params):
                        param_dict[f'param_{i}'] = param
                    # %s를 %(param_N)s로 변경
                    modified_query = query
                    for i in range(len(params)):
                        modified_query = modified_query.replace('%s', f'%(param_{i})s', 1)
                    
                    result = self.orm_manager.execute_raw_sql(modified_query, param_dict)
                else:
                    result = self.orm_manager.execute_raw_sql(query)
                
                self._record_operation("orm", True)
                return result
            else:
                result = self.raw_manager.fetch_all(query, params)
                self._record_operation("raw", True)
                return result
        except Exception as e:
            logger.error(f"Raw SQL 실행 실패: {e}")
            self._record_operation("orm" if self.orm_manager else "raw", False)
            return []
    
    @contextmanager
    def get_raw_session(self):
        """기존 Raw SQL 세션 접근"""
        with self.raw_manager.get_connection() as conn:
            yield conn
    
    @contextmanager
    def get_orm_session(self):
        """ORM 세션 접근"""
        if not self.orm_manager:
            raise RuntimeError("ORM이 사용 불가능합니다")
        
        with self.orm_manager.get_session() as session:
            yield session
    
    # =============================================================================
    # 통계 및 상태 조회
    # =============================================================================
    
    def get_operation_statistics(self) -> Dict[str, Any]:
        """하이브리드 매니저 작업 통계"""
        total = self.operation_stats["total_operations"]
        if total == 0:
            return self.operation_stats
        
        return {
            **self.operation_stats,
            "orm_percentage": (self.operation_stats["orm_operations"] / total) * 100,
            "raw_sql_percentage": (self.operation_stats["raw_sql_operations"] / total) * 100,
            "fallback_percentage": (self.operation_stats["fallback_operations"] / total) * 100,
            "success_rate": ((total - self.operation_stats["failed_operations"]) / total) * 100
        }
    
    def get_system_info(self) -> Dict[str, Any]:
        """시스템 정보 반환"""
        return {
            "orm_available": is_orm_available(),
            "prefer_orm": self.prefer_orm,
            "fallback_enabled": self.fallback_to_raw,
            "raw_manager_type": type(self.raw_manager).__name__,
            "orm_manager_available": self.orm_manager is not None,
            "operation_statistics": self.get_operation_statistics()
        }


# 싱글톤 인스턴스
_hybrid_manager: Optional[HybridDatabaseManager] = None


def get_hybrid_database_manager(prefer_orm: bool = True, fallback_to_raw: bool = True) -> HybridDatabaseManager:
    """하이브리드 데이터베이스 매니저 싱글톤 인스턴스 반환"""
    global _hybrid_manager
    if _hybrid_manager is None:
        _hybrid_manager = HybridDatabaseManager(prefer_orm, fallback_to_raw)
    return _hybrid_manager


def reset_hybrid_database_manager():
    """하이브리드 데이터베이스 매니저 재설정 (테스트용)"""
    global _hybrid_manager
    _hybrid_manager = None