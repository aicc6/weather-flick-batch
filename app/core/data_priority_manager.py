"""
데이터 수집 우선순위 관리자

현재 데이터베이스의 수집 현황을 분석하여 
수집된 데이터가 적은 항목을 우선순위로 정렬하는 시스템입니다.
"""

import logging
from typing import Dict, List, Tuple
from datetime import datetime

from app.core.database_manager_extension import get_extended_database_manager


class DataPriorityManager:
    """데이터 수집 우선순위 관리자"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.db_manager = get_extended_database_manager()
        
        # 컨텐츠 타입별 테이블 매핑
        self.content_type_tables = {
            "12": "tourist_attractions",   # 관광지
            "14": "cultural_facilities",   # 문화시설  
            "15": "festivals_events",      # 축제공연행사
            "25": "travel_courses",        # 여행코스
            "28": "leisure_sports",        # 레포츠
            "32": "accommodations",        # 숙박
            "38": "shopping",              # 쇼핑
            "39": "restaurants",           # 음식점
        }
        
        # 컨텐츠 타입별 한국어 이름
        self.content_type_names = {
            "12": "관광지",
            "14": "문화시설",
            "15": "축제공연행사", 
            "25": "여행코스",
            "28": "레포츠",
            "32": "숙박",
            "38": "쇼핑",
            "39": "음식점",
        }

    def get_current_data_counts(self) -> Dict[str, int]:
        """현재 각 컨텐츠 타입별 데이터 수 조회"""
        data_counts = {}
        
        try:
            # 직접 PostgreSQL 연결 사용
            import psycopg2
            import psycopg2.extras
            import os
            
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT', 5432)),
                database=os.getenv('DB_NAME'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD')
            )
            
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                for content_type, table_name in self.content_type_tables.items():
                    try:
                        # 테이블 존재 확인
                        cursor.execute("""
                            SELECT EXISTS (
                                SELECT FROM information_schema.tables 
                                WHERE table_name = %s
                            )
                        """, (table_name,))
                        
                        table_exists = cursor.fetchone()['exists']
                        
                        if table_exists:
                            # 레코드 수 조회
                            cursor.execute(f"SELECT COUNT(*) as count FROM {table_name}")
                            count = cursor.fetchone()['count']
                            data_counts[content_type] = count
                        else:
                            data_counts[content_type] = 0
                            self.logger.warning(f"테이블 {table_name}이 존재하지 않습니다.")
                            
                    except Exception as e:
                        self.logger.error(f"컨텐츠 타입 {content_type} ({table_name}) 조회 실패: {e}")
                        data_counts[content_type] = 0
                        
            conn.close()
                        
        except Exception as e:
            self.logger.error(f"데이터 수 조회 중 오류 발생: {e}")
            # 기본값으로 모든 컨텐츠 타입을 0으로 설정
            for content_type in self.content_type_tables.keys():
                data_counts[content_type] = 0
        
        return data_counts

    def get_priority_sorted_content_types(self, 
                                        content_types: List[str] = None,
                                        reverse: bool = False) -> List[Tuple[str, int, str]]:
        """
        우선순위에 따라 정렬된 컨텐츠 타입 목록 반환
        
        Args:
            content_types: 대상 컨텐츠 타입 목록 (None이면 전체)
            reverse: True면 데이터가 많은 순으로 정렬 (기본: False - 적은 순)
            
        Returns:
            List of (content_type, count, name) tuples
        """
        if content_types is None:
            content_types = list(self.content_type_tables.keys())
        
        # 현재 데이터 수 조회
        data_counts = self.get_current_data_counts()
        
        # 요청된 컨텐츠 타입만 필터링하고 정렬
        priority_list = []
        for content_type in content_types:
            if content_type in data_counts:
                count = data_counts[content_type]
                name = self.content_type_names.get(content_type, f"타입{content_type}")
                priority_list.append((content_type, count, name))
        
        # 데이터 수에 따라 정렬 (기본: 적은 순)
        priority_list.sort(key=lambda x: x[1], reverse=reverse)
        
        return priority_list

    def get_area_priority_by_content_type(self, content_type: str) -> List[Tuple[str, int]]:
        """
        특정 컨텐츠 타입에서 지역별 데이터 수 조회 및 우선순위 정렬
        
        Args:
            content_type: 컨텐츠 타입 코드
            
        Returns:
            List of (area_code, count) tuples sorted by count (ascending)
        """
        table_name = self.content_type_tables.get(content_type)
        if not table_name:
            self.logger.error(f"알 수 없는 컨텐츠 타입: {content_type}")
            return []
        
        area_counts = []
        
        try:
            with self.db_manager.get_cursor() as cursor:
                # 테이블 존재 확인
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = %s
                    )
                """, (table_name,))
                
                if not cursor.fetchone()[0]:
                    self.logger.warning(f"테이블 {table_name}이 존재하지 않습니다.")
                    return []
                
                # areacode 컬럼이 있는지 확인
                cursor.execute("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s AND column_name IN ('areacode', 'area_code')
                """, (table_name,))
                
                area_column = cursor.fetchone()
                if not area_column:
                    self.logger.warning(f"테이블 {table_name}에 지역 코드 컬럼이 없습니다.")
                    return []
                
                area_col_name = area_column[0]
                
                # 지역별 데이터 수 조회
                cursor.execute(f"""
                    SELECT {area_col_name}, COUNT(*) 
                    FROM {table_name} 
                    WHERE {area_col_name} IS NOT NULL
                    GROUP BY {area_col_name}
                    ORDER BY COUNT(*) ASC
                """)
                
                area_counts = cursor.fetchall()
                
        except Exception as e:
            self.logger.error(f"지역별 데이터 수 조회 실패 ({content_type}): {e}")
        
        return area_counts

    def get_recommended_collection_order(self, 
                                       content_types: List[str] = None,
                                       max_per_type: int = 5) -> Dict:
        """
        권장 수집 순서 제안
        
        Args:
            content_types: 대상 컨텐츠 타입 목록
            max_per_type: 컨텐츠 타입당 최대 추천 지역 수
            
        Returns:
            추천 수집 계획
        """
        if content_types is None:
            content_types = list(self.content_type_tables.keys())
        
        # 컨텐츠 타입별 우선순위 조회
        priority_content_types = self.get_priority_sorted_content_types(content_types)
        
        collection_plan = {
            "analysis_time": datetime.now().isoformat(),
            "total_content_types": len(priority_content_types),
            "priority_order": [],
            "detailed_plan": {}
        }
        
        for rank, (content_type, current_count, name) in enumerate(priority_content_types, 1):
            # 해당 컨텐츠 타입의 지역별 우선순위 조회
            area_priorities = self.get_area_priority_by_content_type(content_type)
            
            # 상위 N개 지역만 선택
            recommended_areas = area_priorities[:max_per_type]
            
            priority_info = {
                "rank": rank,
                "content_type": content_type,
                "name": name,
                "current_count": current_count,
                "priority_reason": "데이터 부족" if current_count < 1000 else "상대적 부족",
                "recommended_areas": [
                    {
                        "area_code": area_code,
                        "current_count": count,
                        "area_name": self._get_area_name(area_code)
                    } for area_code, count in recommended_areas
                ],
                "total_recommended_areas": len(recommended_areas)
            }
            
            collection_plan["priority_order"].append(priority_info)
            collection_plan["detailed_plan"][content_type] = priority_info
        
        return collection_plan

    def _get_area_name(self, area_code: str) -> str:
        """지역 코드에서 지역명 반환"""
        area_names = {
            "1": "서울특별시", "2": "인천광역시", "3": "대전광역시", "4": "대구광역시",
            "5": "광주광역시", "6": "부산광역시", "7": "울산광역시", "8": "세종특별자치시",
            "31": "경기도", "32": "강원특별자치도", "33": "충청북도", "34": "충청남도", 
            "35": "경상북도", "36": "경상남도", "37": "전북특별자치도", "38": "전라남도", "39": "제주도"
        }
        return area_names.get(str(area_code), f"지역{area_code}")

    def print_priority_analysis(self, content_types: List[str] = None):
        """우선순위 분석 결과를 콘솔에 출력"""
        print("📊 데이터 수집 우선순위 분석")
        print("=" * 60)
        print(f"분석 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 현재 데이터 현황
        priority_list = self.get_priority_sorted_content_types(content_types)
        
        print(f"\n🎯 컨텐츠 타입별 우선순위 (데이터 부족 순):")
        for rank, (content_type, count, name) in enumerate(priority_list, 1):
            urgency = "🔥 긴급" if count == 0 else "⚠️ 부족" if count < 1000 else "✅ 충분"
            print(f"  {rank}. {name} (타입 {content_type}): {count:,}개 - {urgency}")
        
        # 상위 3개 컨텐츠 타입의 상세 분석
        print(f"\n🔍 상위 3개 우선순위 상세 분석:")
        for content_type, count, name in priority_list[:3]:
            print(f"\n📋 {name} (타입 {content_type}) - 현재 {count:,}개:")
            
            area_priorities = self.get_area_priority_by_content_type(content_type)
            if area_priorities:
                print(f"  지역별 현황 (상위 5개):")
                for area_code, area_count in area_priorities[:5]:
                    area_name = self._get_area_name(area_code)
                    print(f"    - {area_name}: {area_count:,}개")
            else:
                print(f"  ⚠️ 지역별 데이터 없음 또는 조회 실패")


# 전역 인스턴스
_priority_manager = None


def get_priority_manager() -> DataPriorityManager:
    """데이터 우선순위 매니저 싱글톤 인스턴스 반환"""
    global _priority_manager
    if _priority_manager is None:
        _priority_manager = DataPriorityManager()
    return _priority_manager