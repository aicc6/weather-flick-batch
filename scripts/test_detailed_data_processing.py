#!/usr/bin/env python3
"""
상세 정보 처리 로직 테스트 스크립트
작성일: 2025-07-06
목적: 새로 추가된 상세 정보 API 응답 처리 및 변환 로직 테스트
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
from app.processors.data_transformation_pipeline import get_transformation_pipeline
from app.core.database_manager_extension import get_extended_database_manager

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DetailedDataProcessingTester:
    """상세 정보 처리 로직 테스트 클래스"""
    
    def __init__(self):
        self.kto_client = UnifiedKTOClient()
        self.transformation_pipeline = get_transformation_pipeline()
        self.db_manager = get_extended_database_manager()
    
    async def test_detailed_info_processing(self):
        """상세 정보 처리 로직 전체 테스트"""
        
        logger.info("=== 상세 정보 처리 로직 테스트 시작 ===")
        
        # 1. 테스트용 content_id 가져오기
        test_content_ids = await self.get_test_content_ids()
        
        if not test_content_ids:
            logger.error("❌ 테스트할 content_id를 찾을 수 없습니다.")
            return False
        
        logger.info(f"📋 테스트 대상: {len(test_content_ids)}개 컨텐츠")
        
        # 2. 각 상세 정보 API 테스트
        test_results = {
            'detailCommon2': {'success': 0, 'failed': 0},
            'detailIntro2': {'success': 0, 'failed': 0},
            'detailInfo2': {'success': 0, 'failed': 0},
            'detailImage2': {'success': 0, 'failed': 0}
        }
        
        for content_data in test_content_ids[:3]:  # 처음 3개만 테스트
            content_id = content_data['content_id']
            content_type = content_data.get('content_type', '12')
            
            logger.info(f"🔍 컨텐츠 {content_id} (타입: {content_type}) 테스트 중...")
            
            # detailCommon2 테스트
            try:
                result = await self.kto_client.collect_detail_common(content_id, content_type)
                if result:
                    test_results['detailCommon2']['success'] += 1
                    logger.info(f"  ✅ detailCommon2 성공")
                else:
                    test_results['detailCommon2']['failed'] += 1
                    logger.warning(f"  ⚠️ detailCommon2 데이터 없음")
            except Exception as e:
                test_results['detailCommon2']['failed'] += 1
                logger.error(f"  ❌ detailCommon2 실패: {e}")
            
            # detailIntro2 테스트
            try:
                result = await self.kto_client.collect_detail_intro(content_id, content_type)
                if result:
                    test_results['detailIntro2']['success'] += 1
                    logger.info(f"  ✅ detailIntro2 성공")
                else:
                    test_results['detailIntro2']['failed'] += 1
                    logger.warning(f"  ⚠️ detailIntro2 데이터 없음")
            except Exception as e:
                test_results['detailIntro2']['failed'] += 1
                logger.error(f"  ❌ detailIntro2 실패: {e}")
            
            # detailInfo2 테스트
            try:
                result = await self.kto_client.collect_detail_info(content_id, content_type)
                if result:
                    test_results['detailInfo2']['success'] += 1
                    logger.info(f"  ✅ detailInfo2 성공: {len(result)}건")
                else:
                    test_results['detailInfo2']['failed'] += 1
                    logger.warning(f"  ⚠️ detailInfo2 데이터 없음")
            except Exception as e:
                test_results['detailInfo2']['failed'] += 1
                logger.error(f"  ❌ detailInfo2 실패: {e}")
            
            # detailImage2 테스트
            try:
                result = await self.kto_client.collect_detail_images(content_id)
                if result:
                    test_results['detailImage2']['success'] += 1
                    logger.info(f"  ✅ detailImage2 성공: {len(result)}건")
                else:
                    test_results['detailImage2']['failed'] += 1
                    logger.warning(f"  ⚠️ detailImage2 데이터 없음")
            except Exception as e:
                test_results['detailImage2']['failed'] += 1
                logger.error(f"  ❌ detailImage2 실패: {e}")
            
            # API 호출 간격
            await asyncio.sleep(2.0)
        
        # 3. 결과 요약
        logger.info("=== 테스트 결과 요약 ===")
        
        total_success = 0
        total_tests = 0
        
        for api_name, results in test_results.items():
            success = results['success']
            failed = results['failed']
            total = success + failed
            
            total_success += success
            total_tests += total
            
            success_rate = (success / total * 100) if total > 0 else 0
            logger.info(f"{api_name}: {success}/{total} 성공 ({success_rate:.1f}%)")
        
        overall_success_rate = (total_success / total_tests * 100) if total_tests > 0 else 0
        logger.info(f"전체 성공률: {total_success}/{total_tests} ({overall_success_rate:.1f}%)")
        
        return overall_success_rate > 50  # 50% 이상 성공하면 통과
    
    async def test_database_updates(self):
        """데이터베이스 업데이트 확인 테스트"""
        
        logger.info("=== 데이터베이스 업데이트 확인 테스트 ===")
        
        # 1. 최근 업데이트된 레코드 확인
        tables_to_check = [
            'tourist_attractions', 'accommodations', 'festivals_events', 
            'cultural_facilities', 'travel_courses', 'leisure_sports', 
            'shopping', 'restaurants'
        ]
        
        update_results = {}
        
        for table in tables_to_check:
            try:
                # 최근 10분 이내 업데이트된 레코드 수 확인
                query = f"""
                SELECT COUNT(*) as count 
                FROM {table} 
                WHERE updated_at >= NOW() - INTERVAL '10 minutes'
                """
                result = self.db_manager.fetch_one(query)
                
                recent_updates = result.get('count', 0) if result else 0
                update_results[table] = recent_updates
                
                if recent_updates > 0:
                    logger.info(f"✅ {table}: {recent_updates}건 최근 업데이트")
                else:
                    logger.info(f"⚪ {table}: 최근 업데이트 없음")
                    
            except Exception as e:
                logger.error(f"❌ {table} 테이블 확인 실패: {e}")
                update_results[table] = -1
        
        # 2. 새 테이블 데이터 확인
        try:
            # content_images 테이블 확인
            query = "SELECT COUNT(*) as count FROM content_images WHERE created_at >= NOW() - INTERVAL '10 minutes'"
            result = self.db_manager.fetch_one(query)
            image_count = result.get('count', 0) if result else 0
            
            logger.info(f"📸 content_images: {image_count}건 새 이미지 데이터")
            
            # content_detail_info 테이블 확인
            query = "SELECT COUNT(*) as count FROM content_detail_info WHERE created_at >= NOW() - INTERVAL '10 minutes'"
            result = self.db_manager.fetch_one(query)
            detail_count = result.get('count', 0) if result else 0
            
            logger.info(f"📝 content_detail_info: {detail_count}건 새 상세 정보 데이터")
            
        except Exception as e:
            logger.error(f"❌ 새 테이블 확인 실패: {e}")
        
        # 3. JSONB 필드 업데이트 확인
        try:
            query = """
            SELECT COUNT(*) as count 
            FROM tourist_attractions 
            WHERE detail_intro_info IS NOT NULL 
            AND detail_intro_info != '{}'::jsonb
            """
            result = self.db_manager.fetch_one(query)
            intro_count = result.get('count', 0) if result else 0
            
            logger.info(f"🗂️ detail_intro_info가 있는 관광지: {intro_count}건")
            
        except Exception as e:
            logger.error(f"❌ JSONB 필드 확인 실패: {e}")
        
        total_updates = sum(count for count in update_results.values() if count > 0)
        logger.info(f"총 업데이트 레코드 수: {total_updates}건")
        
        return total_updates > 0
    
    async def get_test_content_ids(self):
        """테스트용 content_id 목록 가져오기"""
        
        try:
            # 각 컨텐츠 타입별로 1개씩 가져오기
            content_ids = []
            
            # 관광지에서 3개
            query = "SELECT content_id, '12' as content_type FROM tourist_attractions WHERE content_id IS NOT NULL LIMIT 3"
            results = self.db_manager.fetch_all(query)
            
            for result in results:
                content_ids.append({
                    'content_id': result['content_id'],
                    'content_type': result['content_type']
                })
            
            return content_ids
            
        except Exception as e:
            logger.error(f"테스트 content_id 조회 실패: {e}")
            return []
    
    async def run_all_tests(self):
        """모든 테스트 실행"""
        
        logger.info("=== 상세 정보 처리 로직 종합 테스트 시작 ===")
        
        test_results = {}
        
        # 1. 상세 정보 처리 테스트
        test_results['detailed_processing'] = await self.test_detailed_info_processing()
        
        # 2. 데이터베이스 업데이트 확인
        test_results['database_updates'] = await self.test_database_updates()
        
        # 결과 요약
        logger.info("=== 최종 결과 요약 ===")
        for test_name, result in test_results.items():
            status = "✅ 성공" if result else "❌ 실패"
            logger.info(f"{test_name}: {status}")
        
        all_passed = all(test_results.values())
        logger.info(f"전체 테스트 결과: {'✅ 모든 테스트 통과' if all_passed else '❌ 일부 테스트 실패'}")
        
        return test_results


async def main():
    """메인 실행 함수"""
    
    print("=== Weather Flick 상세 정보 처리 로직 테스트 ===")
    print()
    
    tester = DetailedDataProcessingTester()
    results = await tester.run_all_tests()
    
    print()
    print("테스트가 완료되었습니다.")
    
    # 실패한 테스트가 있으면 종료 코드 1 반환
    if not all(results.values()):
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())