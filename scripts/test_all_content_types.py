#!/usr/bin/env python3
"""
모든 컨텐츠 타입에 대한 상세 정보 처리 테스트
작성일: 2025-07-06
목적: 8개 컨텐츠 타입 모두에 대해 상세 API 호출 및 데이터베이스 저장 검증
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent.parent))

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from app.collectors.unified_kto_client import UnifiedKTOClient
from app.core.database_manager_extension import get_extended_database_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AllContentTypeTester:
    """모든 컨텐츠 타입 테스트 클래스"""
    
    def __init__(self):
        self.kto_client = UnifiedKTOClient()
        self.db_manager = get_extended_database_manager()
        
        # 컨텐츠 타입별 테이블 매핑
        self.content_type_tables = {
            "12": "tourist_attractions",  # 관광지
            "14": "cultural_facilities",  # 문화시설
            "15": "festivals_events",     # 축제공연행사
            "25": "travel_courses",       # 여행코스
            "28": "leisure_sports",       # 레포츠
            "32": "accommodations",       # 숙박
            "38": "shopping",             # 쇼핑
            "39": "restaurants"           # 음식점
        }
    
    async def get_sample_content_for_type(self, content_type: str, table_name: str, limit: int = 2):
        """각 컨텐츠 타입별 샘플 데이터 조회"""
        
        try:
            query = f"""
            SELECT content_id 
            FROM {table_name} 
            WHERE content_id IS NOT NULL
            ORDER BY created_at DESC
            LIMIT {limit}
            """
            
            results = self.db_manager.fetch_all(query)
            if results:
                return [{'content_id': row['content_id'], 'content_type': content_type} for row in results]
            else:
                logger.warning(f"⚠️ {table_name}에서 샘플 데이터를 찾을 수 없습니다.")
                return []
                
        except Exception as e:
            logger.error(f"❌ {table_name} 샘플 데이터 조회 실패: {e}")
            return []
    
    async def test_content_type(self, content_type: str, table_name: str):
        """단일 컨텐츠 타입 테스트"""
        
        logger.info(f"🔍 {table_name} (타입 {content_type}) 테스트 시작")
        
        # 샘플 데이터 조회
        sample_contents = await self.get_sample_content_for_type(content_type, table_name, 2)
        
        if not sample_contents:
            return {
                'content_type': content_type,
                'table_name': table_name,
                'total_tests': 0,
                'success_count': 0,
                'details': [],
                'error': '샘플 데이터 없음'
            }
        
        results = {
            'content_type': content_type,
            'table_name': table_name,
            'total_tests': len(sample_contents),
            'success_count': 0,
            'details': []
        }
        
        for content_data in sample_contents:
            content_id = content_data['content_id']
            logger.info(f"  📋 컨텐츠 {content_id} 테스트 중...")
            
            content_result = {
                'content_id': content_id,
                'detailCommon2': False,
                'detailIntro2': False,
                'detailInfo2': False,
                'detailImage2': False
            }
            
            # detailCommon2 테스트
            try:
                result = await self.kto_client.collect_detail_common(content_id, content_type)
                if result:
                    content_result['detailCommon2'] = True
                    logger.info(f"    ✅ detailCommon2 성공")
                else:
                    logger.info(f"    ⚠️ detailCommon2 데이터 없음")
            except Exception as e:
                logger.warning(f"    ❌ detailCommon2 실패: {e}")
            
            await asyncio.sleep(0.5)
            
            # detailIntro2 테스트
            try:
                result = await self.kto_client.collect_detail_intro(content_id, content_type)
                if result:
                    content_result['detailIntro2'] = True
                    logger.info(f"    ✅ detailIntro2 성공")
                else:
                    logger.info(f"    ⚠️ detailIntro2 데이터 없음")
            except Exception as e:
                logger.warning(f"    ❌ detailIntro2 실패: {e}")
            
            await asyncio.sleep(0.5)
            
            # detailInfo2 테스트
            try:
                result = await self.kto_client.collect_detail_info(content_id, content_type)
                if result:
                    content_result['detailInfo2'] = True
                    logger.info(f"    ✅ detailInfo2 성공: {len(result)}건")
                else:
                    logger.info(f"    ⚠️ detailInfo2 데이터 없음")
            except Exception as e:
                logger.warning(f"    ❌ detailInfo2 실패: {e}")
            
            await asyncio.sleep(0.5)
            
            # detailImage2 테스트
            try:
                result = await self.kto_client.collect_detail_images(content_id)
                if result:
                    content_result['detailImage2'] = True
                    logger.info(f"    ✅ detailImage2 성공: {len(result)}건")
                else:
                    logger.info(f"    ⚠️ detailImage2 데이터 없음")
            except Exception as e:
                logger.warning(f"    ❌ detailImage2 실패: {e}")
            
            # 성공 여부 계산
            success_apis = sum([content_result['detailCommon2'], content_result['detailIntro2'], 
                               content_result['detailInfo2'], content_result['detailImage2']])
            
            if success_apis > 0:
                results['success_count'] += 1
                logger.info(f"    📊 컨텐츠 {content_id}: {success_apis}/4 API 성공")
            else:
                logger.warning(f"    📊 컨텐츠 {content_id}: 모든 API 실패")
            
            results['details'].append(content_result)
            
            # API 호출 간격
            await asyncio.sleep(1.0)
        
        success_rate = (results['success_count'] / results['total_tests'] * 100) if results['total_tests'] > 0 else 0
        logger.info(f"✅ {table_name} 테스트 완료: {results['success_count']}/{results['total_tests']} 성공 ({success_rate:.1f}%)")
        
        return results
    
    async def test_all_content_types(self):
        """모든 컨텐츠 타입 테스트"""
        
        logger.info("=== 모든 컨텐츠 타입 상세 정보 테스트 시작 ===")
        
        all_results = {}
        total_success = 0
        total_tests = 0
        
        for content_type, table_name in self.content_type_tables.items():
            result = await self.test_content_type(content_type, table_name)
            all_results[content_type] = result
            total_success += result['success_count']
            total_tests += result['total_tests']
            
            # 컨텐츠 타입 간 대기
            await asyncio.sleep(2.0)
        
        # 전체 결과 요약
        logger.info("=== 전체 테스트 결과 요약 ===")
        
        for content_type, result in all_results.items():
            table_name = result['table_name']
            success_count = result['success_count']
            total_count = result['total_tests']
            
            if 'error' in result:
                logger.info(f"{table_name} (타입 {content_type}): {result['error']}")
            else:
                success_rate = (success_count / total_count * 100) if total_count > 0 else 0
                logger.info(f"{table_name} (타입 {content_type}): {success_count}/{total_count} 성공 ({success_rate:.1f}%)")
        
        overall_success_rate = (total_success / total_tests * 100) if total_tests > 0 else 0
        logger.info(f"전체 성공률: {total_success}/{total_tests} ({overall_success_rate:.1f}%)")
        
        return all_results


async def main():
    """메인 실행 함수"""
    
    print("=== Weather Flick 모든 컨텐츠 타입 테스트 ===")
    print()
    
    tester = AllContentTypeTester()
    results = await tester.test_all_content_types()
    
    print()
    print("모든 컨텐츠 타입 테스트가 완료되었습니다.")


if __name__ == "__main__":
    asyncio.run(main())