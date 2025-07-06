#!/usr/bin/env python3
"""
스키마 확장 기능 테스트 스크립트
작성일: 2025-07-06
목적: 새로 추가된 필드와 기능들의 동작 확인
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent.parent))

# 환경 변수 로드
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("python-dotenv가 설치되지 않았습니다. 환경 변수에서 직접 값을 가져옵니다.")

from app.collectors.unified_kto_client import get_unified_kto_client
from app.core.database_manager_extension import get_extended_database_manager
from app.processors.data_transformation_pipeline import get_transformation_pipeline

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchemaEnhancementTester:
    """스키마 확장 기능 테스트 클래스"""
    
    def __init__(self):
        self.kto_client = get_unified_kto_client()
        self.db_manager = get_extended_database_manager()
        self.transformation_pipeline = get_transformation_pipeline()
    
    async def test_database_schema(self):
        """데이터베이스 스키마 확인 테스트"""
        logger.info("=== 데이터베이스 스키마 확인 테스트 ===")
        
        try:
            # 1. 새로운 테이블 존재 확인
            tables_to_check = ['content_images', 'content_detail_info']
            
            for table in tables_to_check:
                query = """
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = 'public' AND table_name = %s
                """
                result = self.db_manager.fetch_one(query, (table,))
                if result and result.get('count', 0) > 0:
                    logger.info(f"✅ 테이블 {table} 존재 확인")
                else:
                    logger.error(f"❌ 테이블 {table}이 존재하지 않음")
            
            # 2. 기존 테이블의 새 필드 확인
            test_table = 'tourist_attractions'
            new_fields = ['homepage', 'booktour', 'createdtime', 'modifiedtime', 
                         'telname', 'faxno', 'zipcode', 'mlevel', 
                         'detail_intro_info', 'detail_additional_info']
            
            for field in new_fields:
                query = """
                    SELECT COUNT(*) FROM information_schema.columns 
                    WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
                """
                result = self.db_manager.fetch_one(query, (test_table, field))
                if result and result.get('count', 0) > 0:
                    logger.info(f"✅ 필드 {test_table}.{field} 존재 확인")
                else:
                    logger.error(f"❌ 필드 {test_table}.{field}이 존재하지 않음")
            
            return True
            
        except Exception as e:
            logger.error(f"스키마 확인 테스트 실패: {e}")
            return False
    
    async def test_data_collection(self):
        """데이터 수집 기능 테스트 (소규모)"""
        logger.info("=== 데이터 수집 기능 테스트 ===")
        
        try:
            # 소규모 데이터 수집 (서울 관광지 5개만)
            result = await self.kto_client.collect_all_data(
                content_types=['12'],  # 관광지만
                area_codes=['1'],      # 서울만
                store_raw=True,
                auto_transform=True,
                include_new_apis=False,  # 기본 수집만 먼저 테스트
                include_hierarchical_regions=False,
                use_priority_sorting=False
            )
            
            logger.info(f"수집 결과: {result}")
            
            if result.get('total_processed_records', 0) > 0:
                logger.info("✅ 기본 데이터 수집 성공")
                return True
            else:
                logger.warning("⚠️ 수집된 데이터가 없음")
                return False
                
        except Exception as e:
            logger.error(f"데이터 수집 테스트 실패: {e}")
            return False
    
    async def test_detailed_info_collection(self):
        """상세 정보 수집 기능 테스트"""
        logger.info("=== 상세 정보 수집 기능 테스트 ===")
        
        try:
            # 기존 content_id 몇 개 가져오기
            query = "SELECT content_id FROM tourist_attractions WHERE content_id IS NOT NULL LIMIT 3"
            content_ids_result = self.db_manager.fetch_all(query)
            
            if not content_ids_result:
                logger.warning("⚠️ 테스트할 기존 content_id가 없음. 먼저 기본 데이터를 수집하세요.")
                return False
            
            content_ids = [row.get('content_id') for row in content_ids_result]
            logger.info(f"테스트할 content_id들: {content_ids}")
            
            # 상세 정보 수집 테스트
            for content_id in content_ids:
                try:
                    # detailCommon2 테스트
                    detail_common = await self.kto_client.collect_detail_common(content_id, "12")
                    if detail_common:
                        logger.info(f"✅ {content_id} detailCommon2 수집 성공")
                    
                    # detailIntro2 테스트
                    detail_intro = await self.kto_client.collect_detail_intro(content_id, "12")
                    if detail_intro:
                        logger.info(f"✅ {content_id} detailIntro2 수집 성공")
                    
                    # detailImage2 테스트
                    detail_images = await self.kto_client.collect_detail_images(content_id)
                    if detail_images:
                        logger.info(f"✅ {content_id} detailImage2 수집 성공: {len(detail_images)}개 이미지")
                    
                    # API 호출 간격
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"❌ {content_id} 상세 정보 수집 실패: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"상세 정보 수집 테스트 실패: {e}")
            return False
    
    async def test_new_table_operations(self):
        """새로운 테이블 CRUD 테스트"""
        logger.info("=== 새로운 테이블 CRUD 테스트 ===")
        
        try:
            # content_images 테이블 테스트
            test_image_data = {
                "content_id": "test_content_123",
                "content_type_id": "12",
                "img_name": "test_image.jpg",
                "origin_img_url": "https://example.com/test.jpg",
                "small_image_url": "https://example.com/test_thumb.jpg",
                "serial_num": 1,
                "img_size": "1024x768",
                "img_width": 1024,
                "img_height": 768
            }
            
            if self.db_manager.upsert_content_images(test_image_data):
                logger.info("✅ content_images 테이블 삽입 성공")
            else:
                logger.error("❌ content_images 테이블 삽입 실패")
            
            # content_detail_info 테이블 테스트
            test_detail_data = {
                "content_id": "test_content_123",
                "content_type_id": "12",
                "info_name": "테스트 정보",
                "info_text": "테스트 상세 내용입니다.",
                "serial_num": 1
            }
            
            if self.db_manager.upsert_content_detail_info(test_detail_data):
                logger.info("✅ content_detail_info 테이블 삽입 성공")
            else:
                logger.error("❌ content_detail_info 테이블 삽입 실패")
            
            return True
            
        except Exception as e:
            logger.error(f"새로운 테이블 CRUD 테스트 실패: {e}")
            return False
    
    async def test_data_transformation(self):
        """데이터 변환 파이프라인 테스트"""
        logger.info("=== 데이터 변환 파이프라인 테스트 ===")
        
        try:
            # 테스트용 KTO API 응답 데이터 시뮬레이션
            mock_kto_response = {
                "response": {
                    "body": {
                        "items": {
                            "item": [
                                {
                                    "contentid": "test_123",
                                    "title": "테스트 관광지",
                                    "addr1": "서울시 중구 테스트동",
                                    "mapx": "126.9784",
                                    "mapy": "37.5666",
                                    "booktour": "Y",
                                    "createdtime": "20240101120000",
                                    "modifiedtime": "20250101120000",
                                    "telname": "문의처",
                                    "faxno": "02-123-4567",
                                    "zipcode": "04567",
                                    "mlevel": "5",
                                    "homepage": "https://test.com",
                                    "overview": "테스트 관광지 설명입니다."
                                }
                            ]
                        }
                    }
                }
            }
            
            # 변환 테스트
            result = self.transformation_pipeline.transform_data(
                api_provider="KTO",
                endpoint="areaBasedList2",
                raw_response=mock_kto_response
            )
            
            if result.success and result.processed_data:
                logger.info("✅ 데이터 변환 성공")
                processed_item = result.processed_data[0]
                
                # 새 필드들이 올바르게 처리되었는지 확인
                new_fields_check = [
                    'booktour', 'createdtime', 'modifiedtime', 
                    'telname', 'faxno', 'zipcode', 'mlevel'
                ]
                
                for field in new_fields_check:
                    if field in processed_item:
                        logger.info(f"✅ 필드 {field} 변환 확인: {processed_item[field]}")
                    else:
                        logger.warning(f"⚠️ 필드 {field}이 변환되지 않음")
                
                return True
            else:
                logger.error("❌ 데이터 변환 실패")
                return False
                
        except Exception as e:
            logger.error(f"데이터 변환 테스트 실패: {e}")
            return False
    
    async def run_all_tests(self):
        """모든 테스트 실행"""
        logger.info("=== 스키마 확장 기능 종합 테스트 시작 ===")
        
        test_results = {}
        
        # 1. 스키마 확인
        test_results['schema'] = await self.test_database_schema()
        
        # 2. 데이터 변환 테스트 (API 호출 없이)
        test_results['transformation'] = await self.test_data_transformation()
        
        # 3. 새로운 테이블 CRUD 테스트
        test_results['new_tables'] = await self.test_new_table_operations()
        
        # 4. 기본 데이터 수집 테스트 (선택적)
        if os.getenv('KTO_API_KEY'):
            test_results['data_collection'] = await self.test_data_collection()
            test_results['detailed_collection'] = await self.test_detailed_info_collection()
        else:
            logger.warning("⚠️ KTO_API_KEY가 없어 데이터 수집 테스트를 건너뜁니다.")
            test_results['data_collection'] = None
            test_results['detailed_collection'] = None
        
        # 결과 요약
        logger.info("=== 테스트 결과 요약 ===")
        for test_name, result in test_results.items():
            if result is True:
                logger.info(f"✅ {test_name}: 성공")
            elif result is False:
                logger.info(f"❌ {test_name}: 실패")
            else:
                logger.info(f"⚠️ {test_name}: 건너뜀")
        
        # 전체 성공 여부
        passed_tests = sum(1 for r in test_results.values() if r is True)
        total_tests = sum(1 for r in test_results.values() if r is not None)
        
        if total_tests > 0:
            success_rate = passed_tests / total_tests * 100
            logger.info(f"전체 테스트 성공률: {success_rate:.1f}% ({passed_tests}/{total_tests})")
        
        return test_results

async def main():
    """메인 실행 함수"""
    print("=== Weather Flick Batch 스키마 확장 테스트 ===")
    print()
    
    tester = SchemaEnhancementTester()
    results = await tester.run_all_tests()
    
    print()
    print("테스트가 완료되었습니다.")
    
    # 실패한 테스트가 있으면 종료 코드 1 반환
    if any(result is False for result in results.values()):
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())