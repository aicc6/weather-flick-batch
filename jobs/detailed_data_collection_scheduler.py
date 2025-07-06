#!/usr/bin/env python3
"""
상세 정보 수집 스케줄러
작성일: 2025-07-06
목적: 기존 컨텐츠에 대한 상세 정보(detailCommon2, detailIntro2, detailInfo2, detailImage2) 수집
"""

import os
import sys
import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
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


@dataclass
class DetailedCollectionConfig:
    """상세 정보 수집 설정"""
    
    # 수집 대상 컨텐츠 타입
    content_types: List[str]
    
    # 배치 사이즈 (API 호출 간격 조정을 위해)
    batch_size: int = 50
    
    # API 호출 간격 (초)
    api_delay: float = 0.5
    
    # 최대 재시도 횟수
    max_retries: int = 3
    
    # 수집할 상세 정보 타입들
    detail_apis: List[str] = None
    
    # 기존 데이터를 다시 수집할지 여부
    force_refresh: bool = False
    
    def __post_init__(self):
        if self.detail_apis is None:
            self.detail_apis = ['detailCommon2', 'detailIntro2', 'detailInfo2', 'detailImage2']


class DetailedDataCollectionScheduler:
    """상세 정보 수집 스케줄러"""
    
    def __init__(self, config: DetailedCollectionConfig):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.kto_client = UnifiedKTOClient()
        self.db_manager = get_extended_database_manager()
        
        # 컨텐츠 타입별 테이블 매핑
        self.content_type_tables = {
            "12": "tourist_attractions",
            "14": "cultural_facilities", 
            "15": "festivals_events",
            "25": "travel_courses",
            "28": "leisure_sports",
            "32": "accommodations",
            "38": "shopping",
            "39": "restaurants"
        }
    
    async def collect_detailed_data_for_all_content_types(
        self, 
        limit_per_content_type: Optional[int] = None
    ) -> Dict:
        """모든 컨텐츠 타입에 대해 상세 정보 수집"""
        
        self.logger.info("=== 상세 정보 수집 스케줄러 시작 ===")
        
        total_results = {
            'started_at': datetime.now().isoformat(),
            'content_types': {},
            'total_processed': 0,
            'total_errors': 0,
            'errors': []
        }
        
        for content_type in self.config.content_types:
            if content_type not in self.content_type_tables:
                self.logger.warning(f"⚠️ 지원하지 않는 컨텐츠 타입: {content_type}")
                continue
                
            table_name = self.content_type_tables[content_type]
            self.logger.info(f"📊 {table_name} (타입 {content_type}) 상세 정보 수집 시작")
            
            try:
                result = await self.collect_detailed_data_for_content_type(
                    content_type, 
                    table_name, 
                    limit_per_content_type
                )
                
                total_results['content_types'][content_type] = result
                total_results['total_processed'] += result.get('processed_count', 0)
                total_results['total_errors'] += result.get('error_count', 0)
                
                self.logger.info(
                    f"✅ {table_name} 상세 정보 수집 완료: "
                    f"처리 {result.get('processed_count', 0)}건, "
                    f"오류 {result.get('error_count', 0)}건"
                )
                
            except Exception as e:
                error_msg = f"{table_name} 상세 정보 수집 실패: {e}"
                self.logger.error(f"❌ {error_msg}")
                total_results['errors'].append(error_msg)
                total_results['total_errors'] += 1
        
        total_results['completed_at'] = datetime.now().isoformat()
        
        self.logger.info("=== 상세 정보 수집 스케줄러 완료 ===")
        self.logger.info(
            f"전체 결과: 처리 {total_results['total_processed']}건, "
            f"오류 {total_results['total_errors']}건"
        )
        
        return total_results
    
    async def collect_detailed_data_for_content_type(
        self, 
        content_type: str, 
        table_name: str,
        limit: Optional[int] = None
    ) -> Dict:
        """특정 컨텐츠 타입에 대한 상세 정보 수집"""
        
        # 1. 상세 정보가 필요한 컨텐츠 목록 조회
        content_items = await self.get_content_items_needing_details(
            table_name, content_type, limit
        )
        
        if not content_items:
            self.logger.info(f"📭 {table_name}: 상세 정보 수집 대상이 없습니다")
            return {
                'content_type': content_type,
                'table_name': table_name,
                'candidates_count': 0,
                'processed_count': 0,
                'error_count': 0,
                'skipped_count': 0
            }
        
        self.logger.info(f"📋 {table_name}: {len(content_items)}개 항목에 대한 상세 정보 수집 시작")
        
        # 2. 배치별로 상세 정보 수집
        processed_count = 0
        error_count = 0
        skipped_count = 0
        
        for i in range(0, len(content_items), self.config.batch_size):
            batch = content_items[i:i + self.config.batch_size]
            batch_num = (i // self.config.batch_size) + 1
            total_batches = (len(content_items) + self.config.batch_size - 1) // self.config.batch_size
            
            self.logger.info(f"🔄 배치 {batch_num}/{total_batches} 처리 중... ({len(batch)}개 항목)")
            
            batch_results = await self.process_batch(batch, content_type)
            
            processed_count += batch_results['processed']
            error_count += batch_results['errors']
            skipped_count += batch_results['skipped']
            
            # 배치 간 지연
            if i + self.config.batch_size < len(content_items):
                await asyncio.sleep(self.config.api_delay * 2)  # 배치 간 더 긴 지연
        
        return {
            'content_type': content_type,
            'table_name': table_name,
            'candidates_count': len(content_items),
            'processed_count': processed_count,
            'error_count': error_count,
            'skipped_count': skipped_count
        }
    
    async def get_content_items_needing_details(
        self, 
        table_name: str, 
        content_type: str,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """상세 정보가 필요한 컨텐츠 항목들 조회"""
        
        try:
            # 기본 조건: detail_intro_info가 NULL이거나 빈 경우
            where_conditions = [
                "(detail_intro_info IS NULL OR detail_intro_info = '{}'::jsonb)",
                "content_id IS NOT NULL"
            ]
            
            # force_refresh가 True면 모든 항목을 대상으로 함
            if not self.config.force_refresh:
                where_conditions.append("(detail_intro_info IS NULL OR detail_intro_info = '{}'::jsonb)")
            
            where_clause = " AND ".join(where_conditions)
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query = f"""
            SELECT content_id 
            FROM {table_name} 
            WHERE {where_clause}
            ORDER BY created_at DESC
            {limit_clause}
            """
            
            results = self.db_manager.fetch_all(query)
            
            if results:
                return [{'content_id': row['content_id']} for row in results]
            else:
                return []
                
        except Exception as e:
            self.logger.error(f"❌ {table_name} 컨텐츠 조회 실패: {e}")
            return []
    
    async def process_batch(self, batch: List[Dict], content_type: str) -> Dict:
        """배치 단위로 상세 정보 수집 처리"""
        
        processed = 0
        errors = 0
        skipped = 0
        
        for item in batch:
            content_id = item['content_id']
            
            try:
                # 각 API별로 상세 정보 수집
                success_count = 0
                
                for api_name in self.config.detail_apis:
                    success = await self.collect_single_detail_api(
                        content_id, content_type, api_name
                    )
                    if success:
                        success_count += 1
                    
                    # API 호출 간 지연
                    await asyncio.sleep(self.config.api_delay)
                
                if success_count > 0:
                    processed += 1
                    self.logger.debug(f"✅ {content_id}: {success_count}/{len(self.config.detail_apis)} API 성공")
                else:
                    skipped += 1
                    self.logger.debug(f"⚠️ {content_id}: 모든 API 호출 실패")
                
            except Exception as e:
                errors += 1
                self.logger.warning(f"❌ {content_id} 처리 실패: {e}")
        
        return {
            'processed': processed,
            'errors': errors,
            'skipped': skipped
        }
    
    async def collect_single_detail_api(
        self, 
        content_id: str, 
        content_type: str, 
        api_name: str
    ) -> bool:
        """단일 상세 정보 API 호출"""
        
        try:
            if api_name == 'detailCommon2':
                result = await self.kto_client.collect_detail_common(content_id, content_type)
            elif api_name == 'detailIntro2':
                result = await self.kto_client.collect_detail_intro(content_id, content_type)
            elif api_name == 'detailInfo2':
                result = await self.kto_client.collect_detail_info(content_id, content_type)
            elif api_name == 'detailImage2':
                result = await self.kto_client.collect_detail_images(content_id)
            else:
                self.logger.warning(f"⚠️ 지원하지 않는 API: {api_name}")
                return False
            
            return result is not None
            
        except Exception as e:
            self.logger.debug(f"❌ {content_id} {api_name} 호출 실패: {e}")
            return False


# 실행 스크립트
async def main():
    """메인 실행 함수"""
    
    print("=== Weather Flick 상세 정보 수집 스케줄러 ===")
    print()
    
    # 로깅 설정
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # 설정 생성
    config = DetailedCollectionConfig(
        content_types=["12", "14", "15", "25", "28", "32", "38", "39"],  # 모든 컨텐츠 타입
        batch_size=20,  # 배치 크기를 작게 설정
        api_delay=1.0,  # API 호출 간격을 늘림
        max_retries=3,
        force_refresh=False  # 기존 데이터가 있으면 건너뜀
    )
    
    # 스케줄러 실행
    scheduler = DetailedDataCollectionScheduler(config)
    
    # 테스트를 위해 각 컨텐츠 타입당 최대 5개만 처리
    results = await scheduler.collect_detailed_data_for_all_content_types(
        limit_per_content_type=5
    )
    
    print()
    print("=== 수집 완료 ===")
    print(f"전체 처리: {results['total_processed']}건")
    print(f"전체 오류: {results['total_errors']}건")
    
    if results['errors']:
        print()
        print("=== 오류 목록 ===")
        for error in results['errors']:
            print(f"- {error}")


if __name__ == "__main__":
    asyncio.run(main())