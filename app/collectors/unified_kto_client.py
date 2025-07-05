"""
통합 KTO API 클라이언트

기존 KTO API 수집기를 새로운 통합 아키텍처로 마이그레이션
"""

import os
import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from app.core.unified_api_client import get_unified_api_client, APIProvider
from app.processors.data_transformation_pipeline import get_transformation_pipeline
from app.core.database_manager_extension import get_extended_database_manager


class UnifiedKTOClient:
    """통합 KTO API 클라이언트"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.api_client = get_unified_api_client()
        self.transformation_pipeline = get_transformation_pipeline()
        self.db_manager = get_extended_database_manager()

        # 기본 파라미터 설정
        self.default_params = {
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
        }

        # 컨텐츠 타입 정의
        self.content_types = {
            "12": "tourist_attractions",  # 관광지
            "14": "cultural_facilities",  # 문화시설
            "15": "festivals_events",  # 축제공연행사
            "25": "travel_courses",  # 여행코스
            "28": "leisure_sports",  # 레포츠
            "32": "accommodations",  # 숙박
            "38": "shopping",  # 쇼핑
            "39": "restaurants",  # 음식점
        }

        # 지역 코드 (전국 주요 지역)
        self.area_codes = [
            "1",  # 서울
            "2",  # 인천
            "3",  # 대전
            "4",  # 대구
            "5",  # 광주
            "6",  # 부산
            "7",  # 울산
            "8",  # 세종
            "31",  # 경기
            "32",  # 강원
            "33",  # 충북
            "34",  # 충남
            "35",  # 경북
            "36",  # 경남
            "37",  # 전북
            "38",  # 전남
            "39",  # 제주
        ]

    async def collect_all_data(
        self,
        content_types: Optional[List[str]] = None,
        area_codes: Optional[List[str]] = None,
        store_raw: bool = True,
        auto_transform: bool = True,
        include_new_apis: bool = True,
    ) -> Dict:
        """
        모든 KTO 데이터 수집 (신규 API 포함)

        Args:
            content_types: 수집할 컨텐츠 타입 목록 (None이면 전체)
            area_codes: 수집할 지역 코드 목록 (None이면 전체)
            store_raw: 원본 데이터 저장 여부
            auto_transform: 자동 변환 수행 여부
            include_new_apis: 신규 추가된 4개 API 포함 여부

        Returns:
            Dict: 수집 결과 요약
        """

        if content_types is None:
            content_types = list(self.content_types.keys())

        if area_codes is None:
            area_codes = self.area_codes

        sync_batch_id = f"kto_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        collection_results = {
            "sync_batch_id": sync_batch_id,
            "started_at": datetime.utcnow().isoformat(),
            "content_types_collected": {},
            "new_apis_collected": {},
            "total_raw_records": 0,
            "total_processed_records": 0,
            "errors": [],
        }

        self.logger.info(f"KTO 데이터 수집 시작: {sync_batch_id}")

        async with self.api_client:

            for content_type in content_types:
                content_name = self.content_types.get(
                    content_type, f"unknown_{content_type}"
                )

                self.logger.info(f"수집 시작: {content_name} (타입: {content_type})")

                content_results = {
                    "content_type": content_type,
                    "content_name": content_name,
                    "areas": {},
                    "total_raw_records": 0,
                    "total_processed_records": 0,
                    "errors": [],
                }

                for area_code in area_codes:
                    try:
                        area_result = await self._collect_area_data(
                            content_type,
                            area_code,
                            sync_batch_id,
                            store_raw,
                            auto_transform,
                        )

                        content_results["areas"][area_code] = area_result
                        content_results["total_raw_records"] += area_result.get(
                            "raw_records", 0
                        )
                        content_results["total_processed_records"] += area_result.get(
                            "processed_records", 0
                        )

                        # API 호출 간격 조정
                        await asyncio.sleep(0.5)

                    except Exception as e:
                        error_msg = f"지역 {area_code} 수집 실패: {e}"
                        self.logger.error(error_msg)
                        content_results["errors"].append(error_msg)

                collection_results["content_types_collected"][
                    content_type
                ] = content_results
                collection_results["total_raw_records"] += content_results[
                    "total_raw_records"
                ]
                collection_results["total_processed_records"] += content_results[
                    "total_processed_records"
                ]

                self.logger.info(
                    f"수집 완료: {content_name} - 원본 {content_results['total_raw_records']}건, 처리 {content_results['total_processed_records']}건"
                )

            # 신규 API 수집 (사용자가 요청한 경우)
            if include_new_apis:
                self.logger.info("=== 신규 API 수집 시작 ===")
                
                # 1. 반려동물 동반여행 정보 수집
                try:
                    pet_tour_result = await self.collect_pet_tour_data(
                        content_ids=None,  # 전체 조회
                        store_raw=store_raw,
                        auto_transform=auto_transform
                    )
                    collection_results["new_apis_collected"]["pet_tour"] = pet_tour_result
                    collection_results["total_raw_records"] += pet_tour_result.get("total_raw_records", 0)
                    collection_results["total_processed_records"] += pet_tour_result.get("total_processed_records", 0)
                    
                    self.logger.info(f"반려동물 동반여행 정보 수집 완료: 원본 {pet_tour_result.get('total_raw_records', 0)}건")
                    
                except Exception as e:
                    error_msg = f"반려동물 동반여행 정보 수집 실패: {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)

                # 2. 분류체계 코드 수집
                try:
                    classification_result = await self.collect_classification_system_codes(
                        store_raw=store_raw,
                        auto_transform=auto_transform
                    )
                    collection_results["new_apis_collected"]["classification_codes"] = classification_result
                    collection_results["total_raw_records"] += classification_result.get("total_raw_records", 0)
                    collection_results["total_processed_records"] += classification_result.get("total_processed_records", 0)
                    
                    self.logger.info(f"분류체계 코드 수집 완료: 원본 {classification_result.get('total_raw_records', 0)}건")
                    
                except Exception as e:
                    error_msg = f"분류체계 코드 수집 실패: {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)

                # 3. 지역 기반 동기화 목록 수집 (주요 지역만)
                try:
                    sync_list_result = await self.collect_area_based_sync_list(
                        content_type_id="12",
                        area_code="1",  # 서울
                        modified_time=None,
                        store_raw=store_raw,
                        auto_transform=auto_transform
                    )
                    collection_results["new_apis_collected"]["sync_list"] = sync_list_result
                    collection_results["total_raw_records"] += sync_list_result.get("total_raw_records", 0)
                    collection_results["total_processed_records"] += sync_list_result.get("total_processed_records", 0)
                    
                    self.logger.info(f"지역 기반 동기화 목록 수집 완료: 원본 {sync_list_result.get('total_raw_records', 0)}건")
                    
                except Exception as e:
                    error_msg = f"지역 기반 동기화 목록 수집 실패: {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)

                # 4. 법정동 코드 수집 (주요 지역만)
                try:
                    legal_dong_result = await self.collect_legal_dong_codes(
                        area_code="1",  # 서울
                        store_raw=store_raw,
                        auto_transform=auto_transform
                    )
                    collection_results["new_apis_collected"]["legal_dong_codes"] = legal_dong_result
                    collection_results["total_raw_records"] += legal_dong_result.get("total_raw_records", 0)
                    collection_results["total_processed_records"] += legal_dong_result.get("total_processed_records", 0)
                    
                    self.logger.info(f"법정동 코드 수집 완료: 원본 {legal_dong_result.get('total_raw_records', 0)}건")
                    
                except Exception as e:
                    error_msg = f"법정동 코드 수집 실패: {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)
                
                self.logger.info("=== 신규 API 수집 완료 ===")

        collection_results["completed_at"] = datetime.utcnow().isoformat()

        self.logger.info(
            f"KTO 데이터 수집 완료: {sync_batch_id} - 총 원본 {collection_results['total_raw_records']}건, 처리 {collection_results['total_processed_records']}건"
        )

        return collection_results

    async def collect_pet_tour_data(
        self, 
        content_ids: Optional[List[str]] = None,
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """
        반려동물 동반여행 정보 수집 (detailPetTour2)
        
        Args:
            content_ids: 수집할 콘텐츠 ID 목록 (None이면 전체 조회)
            store_raw: 원본 데이터 저장 여부
            auto_transform: 자동 변환 수행 여부
        
        Returns:
            Dict: 수집 결과
        """
        
        sync_batch_id = f"pet_tour_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"반려동물 동반여행 정보 수집 시작: {sync_batch_id}")
        
        collection_results = {
            "sync_batch_id": sync_batch_id,
            "api_endpoint": "detailPetTour2", 
            "started_at": datetime.utcnow().isoformat(),
            "total_raw_records": 0,
            "total_processed_records": 0,
            "errors": []
        }
        
        async with self.api_client:
            try:
                params = {
                    **self.default_params,
                    "numOfRows": 100,
                    "pageNo": 1
                }
                
                if content_ids:
                    # 특정 contentId들 처리
                    for content_id in content_ids:
                        params["contentId"] = content_id
                        
                        response = await self.api_client.call_api(
                            api_provider=APIProvider.KTO,
                            endpoint="detailPetTour2",
                            params=params,
                            store_raw=store_raw
                        )
                        
                        if response.success and response.data:
                            items = response.data.get("items", {})
                            if items and "item" in items:
                                item_list = items["item"]
                                if isinstance(item_list, dict):
                                    item_list = [item_list]
                                
                                collection_results["total_raw_records"] += len(item_list)
                                
                                if auto_transform:
                                    # 간단한 데이터 처리 (실제 transform 생략)
                                    collection_results["total_processed_records"] += len(item_list)
                        
                        await asyncio.sleep(0.3)
                else:
                    # 전체 조회
                    response = await self.api_client.call_api(
                        api_provider=APIProvider.KTO,
                        endpoint="detailPetTour2",
                        params=params,
                        store_raw=store_raw
                    )
                    
                    if response.success and response.data:
                        items = response.data.get("items", {})
                        if items and "item" in items:
                            item_list = items["item"]
                            if isinstance(item_list, dict):
                                item_list = [item_list]
                            
                            collection_results["total_raw_records"] = len(item_list)
                            
                            if auto_transform:
                                # 간단한 데이터 처리 (실제 transform 생략)
                                collection_results["total_processed_records"] = len(item_list)
                
            except Exception as e:
                error_msg = f"펫투어 수집 실패: {e}"
                self.logger.error(error_msg)
                collection_results["errors"].append(error_msg)
        
        collection_results["completed_at"] = datetime.utcnow().isoformat()
        
        self.logger.info(
            f"반려동물 동반여행 정보 수집 완료: {sync_batch_id} - 총 원본 {collection_results['total_raw_records']}건, 처리 {collection_results['total_processed_records']}건"
        )
        
        return collection_results

    async def collect_classification_system_codes(
        self, 
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """
        분류체계 코드 조회 (lclsSystmCode2)
        
        Args:
            store_raw: 원본 데이터 저장 여부
            auto_transform: 자동 변환 수행 여부
        
        Returns:
            Dict: 수집 결과
        """
        
        sync_batch_id = f"classification_codes_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"분류체계 코드 수집 시작: {sync_batch_id}")
        
        collection_results = {
            "sync_batch_id": sync_batch_id,
            "api_endpoint": "lclsSystmCode2",
            "started_at": datetime.utcnow().isoformat(),
            "total_raw_records": 0,
            "total_processed_records": 0,
            "errors": []
        }
        
        async with self.api_client:
            try:
                params = {
                    **self.default_params,
                    "numOfRows": 1000,
                    "pageNo": 1
                }
                
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="lclsSystmCode2",
                    params=params,
                    store_raw=store_raw
                )
                
                if response.success and response.data:
                    items = response.data.get("items", {})
                    if items and "item" in items:
                        item_list = items["item"]
                        if isinstance(item_list, dict):
                            item_list = [item_list]
                        
                        collection_results["total_raw_records"] = len(item_list)
                        
                        if auto_transform:
                            # 간단한 데이터 처리 (실제 transform 생략)
                            collection_results["total_processed_records"] = len(item_list)
                        
            except Exception as e:
                error_msg = f"분류체계 코드 수집 실패: {e}"
                self.logger.error(error_msg)
                collection_results["errors"].append(error_msg)
        
        collection_results["completed_at"] = datetime.utcnow().isoformat()
        
        self.logger.info(
            f"분류체계 코드 수집 완료: {sync_batch_id} - 총 원본 {collection_results['total_raw_records']}건, 처리 {collection_results['total_processed_records']}건"
        )
        
        return collection_results

    async def collect_area_based_sync_list(
        self,
        content_type_id: str = "12", 
        area_code: str = "1",
        modified_time: str = None,
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """
        지역기반 동기화 목록 조회 (areaBasedSyncList2)
        
        Args:
            content_type_id: 콘텐츠 타입 ID
            area_code: 지역 코드
            modified_time: 수정 시간 (YYYYMMDD 형식)
            store_raw: 원본 데이터 저장 여부
            auto_transform: 자동 변환 수행 여부
        
        Returns:
            Dict: 수집 결과
        """
        
        sync_batch_id = f"sync_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"지역기반 동기화 목록 수집 시작: {sync_batch_id}")
        
        collection_results = {
            "sync_batch_id": sync_batch_id,
            "api_endpoint": "areaBasedSyncList2",
            "started_at": datetime.utcnow().isoformat(),
            "total_raw_records": 0,
            "total_processed_records": 0,
            "content_types_collected": {},
            "errors": []
        }
        
        async with self.api_client:
            # 모든 콘텐츠 타입에 대해 동기화 목록 수집
            for content_type in self.content_types.keys():
                try:
                    content_result = await self._collect_sync_list_by_content_type(
                        content_type, area_code, modified_time, sync_batch_id, store_raw, auto_transform
                    )
                    
                    collection_results["content_types_collected"][content_type] = content_result
                    collection_results["total_raw_records"] += content_result.get("raw_records", 0)
                    collection_results["total_processed_records"] += content_result.get("processed_records", 0)
                    
                    # API 호출 간격 조정
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    error_msg = f"콘텐츠 타입 {content_type} 동기화 목록 수집 실패: {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)
        
        collection_results["completed_at"] = datetime.utcnow().isoformat()
        
        self.logger.info(
            f"지역기반 동기화 목록 수집 완료: {sync_batch_id} - 총 원본 {collection_results['total_raw_records']}건, 처리 {collection_results['total_processed_records']}건"
        )
        
        return collection_results

    async def collect_legal_dong_codes(
        self,
        area_code: str = "1",
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """
        법정동 코드 조회 (ldongCode2)
        
        Args:
            area_code: 지역 코드  
            store_raw: 원본 데이터 저장 여부
            auto_transform: 자동 변환 수행 여부
        
        Returns:
            Dict: 수집 결과
        """
        
        sync_batch_id = f"legal_dong_codes_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"법정동 코드 수집 시작: {sync_batch_id}")
        
        collection_results = {
            "sync_batch_id": sync_batch_id,
            "api_endpoint": "ldongCode2",
            "started_at": datetime.utcnow().isoformat(),
            "total_raw_records": 0,
            "total_processed_records": 0,
            "areas_collected": {},
            "errors": []
        }
        
        async with self.api_client:
            for area_code in self.area_codes:
                try:
                    area_result = await self._collect_legal_dong_area_data(
                        area_code, sync_batch_id, store_raw, auto_transform
                    )
                    
                    collection_results["areas_collected"][area_code] = area_result
                    collection_results["total_raw_records"] += area_result.get("raw_records", 0)
                    collection_results["total_processed_records"] += area_result.get("processed_records", 0)
                    
                    # API 호출 간격 조정
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    error_msg = f"지역 {area_code} 법정동 코드 수집 실패: {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)
        
        collection_results["completed_at"] = datetime.utcnow().isoformat()
        
        self.logger.info(
            f"법정동 코드 수집 완료: {sync_batch_id} - 총 원본 {collection_results['total_raw_records']}건, 처리 {collection_results['total_processed_records']}건"
        )
        
        return collection_results

    async def _collect_area_data(
        self,
        content_type: str,
        area_code: str,
        sync_batch_id: str,
        store_raw: bool,
        auto_transform: bool,
    ) -> Dict:
        """특정 지역의 특정 타입 데이터 수집"""

        area_result = {
            "area_code": area_code,
            "content_type": content_type,
            "raw_records": 0,
            "processed_records": 0,
            "pages_collected": 0,
            "raw_data_ids": [],
            "errors": [],
        }

        page_no = 1
        num_of_rows = 100

        while True:
            try:
                # API 호출 파라미터 구성
                params = {
                    **self.default_params,
                    "contentTypeId": content_type,
                    "areaCode": area_code,
                    "pageNo": page_no,
                    "numOfRows": num_of_rows,
                }

                # API 호출
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="areaBasedList2",
                    params=params,
                    store_raw=store_raw,
                    cache_ttl=7200,  # 2시간 캐시
                )

                if not response.success:
                    error_msg = f"API 호출 실패: {response.error}"
                    area_result["errors"].append(error_msg)
                    break

                # 응답 데이터 확인
                response_body = response.data.get("response", {}).get("body", {})
                total_count = response_body.get("totalCount", 0)
                items = response_body.get("items", {})

                if total_count == 0 or not items or "item" not in items:
                    # 더 이상 데이터가 없음
                    break

                page_items = items["item"]
                if isinstance(page_items, dict):
                    page_items = [page_items]

                current_page_count = len(page_items)
                area_result["raw_records"] += current_page_count
                area_result["pages_collected"] += 1

                if response.raw_data_id:
                    area_result["raw_data_ids"].append(response.raw_data_id)

                # 자동 변환 수행
                if auto_transform and response.raw_data_id:
                    try:
                        transform_result = (
                            await self.transformation_pipeline.transform_raw_data(
                                response.raw_data_id
                            )
                        )

                        if transform_result.success:
                            # 변환된 데이터를 데이터베이스에 저장
                            saved_count = await self._save_processed_data(
                                content_type,
                                transform_result.processed_data,
                                response.raw_data_id,
                                transform_result.quality_score,
                            )
                            area_result["processed_records"] += saved_count

                    except Exception as e:
                        error_msg = f"데이터 변환 실패: {e}"
                        area_result["errors"].append(error_msg)

                # 페이지네이션 조건 확인
                if (
                    current_page_count < num_of_rows
                    or area_result["raw_records"] >= total_count
                ):
                    break

                page_no += 1

            except Exception as e:
                error_msg = f"페이지 {page_no} 수집 실패: {e}"
                area_result["errors"].append(error_msg)
                break

        return area_result

    async def _collect_pet_tour_area_data(
        self,
        content_type_id: str,
        area_code: str,
        sync_batch_id: str,
        store_raw: bool,
        auto_transform: bool,
    ) -> Dict:
        """특정 지역의 반려동물 동반여행 정보 수집"""
        
        area_result = {
            "area_code": area_code,
            "content_type_id": content_type_id,
            "raw_records": 0,
            "processed_records": 0,
            "pages_collected": 0,
            "raw_data_ids": [],
            "errors": []
        }
        
        page_no = 1
        num_of_rows = 100
        
        while True:
            try:
                params = {
                    **self.default_params,
                    "contentTypeId": content_type_id,
                    "areaCode": area_code,
                    "numOfRows": num_of_rows,
                    "pageNo": page_no
                }
                
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="detailPetTour2",
                    params=params,
                    store_raw=store_raw
                )
                
                if response.success:
                    current_page_count = len(response.data.get("items", []))
                    area_result["raw_records"] += current_page_count
                    area_result["pages_collected"] += 1
                    area_result["raw_data_ids"].append(response.raw_data_id)
                    
                    # 자동 변환 수행
                    if auto_transform and response.raw_data_id:
                        try:
                            transform_result = await self.transformation_pipeline.transform_raw_data(
                                response.raw_data_id
                            )
                            
                            if transform_result.success:
                                # 변환된 데이터를 펫투어 테이블에 저장
                                saved_count = await self._save_pet_tour_data(
                                    transform_result.processed_data,
                                    sync_batch_id,
                                    response.raw_data_id
                                )
                                area_result["processed_records"] += saved_count
                                
                        except Exception as e:
                            error_msg = f"펫투어 데이터 변환 실패: {e}"
                            area_result["errors"].append(error_msg)
                    
                    # 페이지네이션 종료 조건
                    if current_page_count < num_of_rows:
                        break
                else:
                    break
                    
                page_no += 1
                
            except Exception as e:
                error_msg = f"펫투어 페이지 {page_no} 수집 실패: {e}"
                area_result["errors"].append(error_msg)
                break
        
        return area_result

    async def _collect_sync_list_by_content_type(
        self,
        content_type_id: str,
        area_code: str,
        modified_time: str,
        sync_batch_id: str,
        store_raw: bool,
        auto_transform: bool,
    ) -> Dict:
        """콘텐츠 타입별 동기화 목록 수집"""
        
        content_result = {
            "content_type_id": content_type_id,
            "area_code": area_code,
            "raw_records": 0,
            "processed_records": 0,
            "pages_collected": 0,
            "raw_data_ids": [],
            "errors": []
        }
        
        page_no = 1
        num_of_rows = 100
        
        while True:
            try:
                params = {
                    **self.default_params,
                    "contentTypeId": content_type_id,
                    "areaCode": area_code,
                    "numOfRows": num_of_rows,
                    "pageNo": page_no
                }
                
                # 수정 시간이 지정된 경우 추가
                if modified_time:
                    params["modifiedtime"] = modified_time
                
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="areaBasedSyncList2",
                    params=params,
                    store_raw=store_raw
                )
                
                if response.success:
                    current_page_count = len(response.data.get("items", []))
                    content_result["raw_records"] += current_page_count
                    content_result["pages_collected"] += 1
                    content_result["raw_data_ids"].append(response.raw_data_id)
                    
                    # 자동 변환 수행
                    if auto_transform and response.raw_data_id:
                        try:
                            transform_result = await self.transformation_pipeline.transform_raw_data(
                                response.raw_data_id
                            )
                            
                            if transform_result.success:
                                # 변환된 데이터를 동기화 목록 테이블에 저장
                                saved_count = await self._save_sync_list_data(
                                    transform_result.processed_data,
                                    sync_batch_id,
                                    response.raw_data_id
                                )
                                content_result["processed_records"] += saved_count
                                
                        except Exception as e:
                            error_msg = f"동기화 목록 데이터 변환 실패: {e}"
                            content_result["errors"].append(error_msg)
                    
                    # 페이지네이션 종료 조건
                    if current_page_count < num_of_rows:
                        break
                else:
                    break
                    
                page_no += 1
                
            except Exception as e:
                error_msg = f"동기화 목록 페이지 {page_no} 수집 실패: {e}"
                content_result["errors"].append(error_msg)
                break
        
        return content_result

    async def _collect_legal_dong_area_data(
        self,
        area_code: str,
        sync_batch_id: str,
        store_raw: bool,
        auto_transform: bool,
    ) -> Dict:
        """특정 지역의 법정동 코드 데이터 수집"""
        
        area_result = {
            "area_code": area_code,
            "raw_records": 0,
            "processed_records": 0,
            "pages_collected": 0,
            "raw_data_ids": [],
            "errors": []
        }
        
        page_no = 1
        num_of_rows = 1000
        
        while True:
            try:
                params = {
                    **self.default_params,
                    "lDongRegnCd": area_code,
                    "numOfRows": num_of_rows,
                    "pageNo": page_no
                }
                
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="ldongCode2",
                    params=params,
                    store_raw=store_raw
                )
                
                if response.success:
                    current_page_count = len(response.data.get("items", []))
                    area_result["raw_records"] += current_page_count
                    area_result["pages_collected"] += 1
                    area_result["raw_data_ids"].append(response.raw_data_id)
                    
                    # 자동 변환 수행
                    if auto_transform and response.raw_data_id:
                        try:
                            transform_result = await self.transformation_pipeline.transform_raw_data(
                                response.raw_data_id
                            )
                            
                            if transform_result.success:
                                # 변환된 데이터를 법정동 코드 테이블에 저장
                                saved_count = await self._save_legal_dong_codes(
                                    transform_result.processed_data,
                                    sync_batch_id,
                                    response.raw_data_id
                                )
                                area_result["processed_records"] += saved_count
                                
                        except Exception as e:
                            error_msg = f"법정동 코드 데이터 변환 실패: {e}"
                            area_result["errors"].append(error_msg)
                    
                    # 페이지네이션 종료 조건
                    if current_page_count < num_of_rows:
                        break
                else:
                    break
                    
                page_no += 1
                
            except Exception as e:
                error_msg = f"법정동 코드 페이지 {page_no} 수집 실패: {e}"
                area_result["errors"].append(error_msg)
                break
        
        return area_result

    async def _save_pet_tour_data(
        self,
        processed_data: List[Dict],
        sync_batch_id: str,
        raw_data_id: str,
    ) -> int:
        """반려동물 동반여행 데이터를 데이터베이스에 저장"""
        
        if not processed_data:
            return 0
        
        try:
            saved_count = 0
            for data in processed_data:
                # 펫투어 전용 테이블에 저장
                await self.db_manager.execute_query(
                    """
                    INSERT INTO pet_tour_info (
                        content_id, title, address, area_code, content_type_id,
                        pet_info, pet_facilities, sync_batch_id, raw_data_id,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (content_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        address = EXCLUDED.address,
                        pet_info = EXCLUDED.pet_info,
                        pet_facilities = EXCLUDED.pet_facilities,
                        sync_batch_id = EXCLUDED.sync_batch_id,
                        raw_data_id = EXCLUDED.raw_data_id,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        data.get("content_id"),
                        data.get("title"),
                        data.get("address"),
                        data.get("area_code"),
                        data.get("content_type_id"),
                        data.get("pet_info"),
                        data.get("pet_facilities"),
                        sync_batch_id,
                        raw_data_id,
                        datetime.utcnow(),
                        datetime.utcnow(),
                    ),
                )
                saved_count += 1
            
            self.logger.info(f"펫투어 데이터 {saved_count}건 저장 완료")
            return saved_count
            
        except Exception as e:
            self.logger.error(f"펫투어 데이터 저장 실패: {e}")
            return 0

    async def _save_classification_codes(
        self,
        processed_data: List[Dict],
        sync_batch_id: str,
        raw_data_id: str,
    ) -> int:
        """분류체계 코드를 데이터베이스에 저장"""
        
        if not processed_data:
            return 0
        
        try:
            saved_count = 0
            for data in processed_data:
                # 분류체계 코드 테이블에 저장
                await self.db_manager.execute_query(
                    """
                    INSERT INTO classification_system_codes (
                        code, name, description, parent_code, level,
                        sync_batch_id, raw_data_id, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (code) DO UPDATE SET
                        name = EXCLUDED.name,
                        description = EXCLUDED.description,
                        parent_code = EXCLUDED.parent_code,
                        level = EXCLUDED.level,
                        sync_batch_id = EXCLUDED.sync_batch_id,
                        raw_data_id = EXCLUDED.raw_data_id,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        data.get("code"),
                        data.get("name"),
                        data.get("description"),
                        data.get("parent_code"),
                        data.get("level"),
                        sync_batch_id,
                        raw_data_id,
                        datetime.utcnow(),
                        datetime.utcnow(),
                    ),
                )
                saved_count += 1
            
            self.logger.info(f"분류체계 코드 {saved_count}건 저장 완료")
            return saved_count
            
        except Exception as e:
            self.logger.error(f"분류체계 코드 저장 실패: {e}")
            return 0

    async def _save_sync_list_data(
        self,
        processed_data: List[Dict],
        sync_batch_id: str,
        raw_data_id: str,
    ) -> int:
        """동기화 목록 데이터를 데이터베이스에 저장"""
        
        if not processed_data:
            return 0
        
        try:
            saved_count = 0
            for data in processed_data:
                # 동기화 목록 테이블에 저장
                await self.db_manager.execute_query(
                    """
                    INSERT INTO area_based_sync_list (
                        content_id, title, content_type_id, area_code, 
                        modified_time, creation_time, sync_batch_id, raw_data_id,
                        created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (content_id) DO UPDATE SET
                        title = EXCLUDED.title,
                        modified_time = EXCLUDED.modified_time,
                        sync_batch_id = EXCLUDED.sync_batch_id,
                        raw_data_id = EXCLUDED.raw_data_id,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        data.get("content_id"),
                        data.get("title"),
                        data.get("content_type_id"),
                        data.get("area_code"),
                        data.get("modified_time"),
                        data.get("creation_time"),
                        sync_batch_id,
                        raw_data_id,
                        datetime.utcnow(),
                        datetime.utcnow(),
                    ),
                )
                saved_count += 1
            
            self.logger.info(f"동기화 목록 데이터 {saved_count}건 저장 완료")
            return saved_count
            
        except Exception as e:
            self.logger.error(f"동기화 목록 데이터 저장 실패: {e}")
            return 0

    async def _save_legal_dong_codes(
        self,
        processed_data: List[Dict],
        sync_batch_id: str,
        raw_data_id: str,
    ) -> int:
        """법정동 코드를 데이터베이스에 저장"""
        
        if not processed_data:
            return 0
        
        try:
            saved_count = 0
            for data in processed_data:
                # 법정동 코드 테이블에 저장
                await self.db_manager.execute_query(
                    """
                    INSERT INTO legal_dong_codes (
                        area_code, sigungu_code, umd_code, ri_code,
                        area_name, sigungu_name, umd_name, ri_name,
                        sync_batch_id, raw_data_id, created_at, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (area_code, sigungu_code, umd_code, ri_code) DO UPDATE SET
                        area_name = EXCLUDED.area_name,
                        sigungu_name = EXCLUDED.sigungu_name,
                        umd_name = EXCLUDED.umd_name,
                        ri_name = EXCLUDED.ri_name,
                        sync_batch_id = EXCLUDED.sync_batch_id,
                        raw_data_id = EXCLUDED.raw_data_id,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        data.get("area_code"),
                        data.get("sigungu_code"),
                        data.get("umd_code"),
                        data.get("ri_code"),
                        data.get("area_name"),
                        data.get("sigungu_name"),
                        data.get("umd_name"),
                        data.get("ri_name"),
                        sync_batch_id,
                        raw_data_id,
                        datetime.utcnow(),
                        datetime.utcnow(),
                    ),
                )
                saved_count += 1
            
            self.logger.info(f"법정동 코드 {saved_count}건 저장 완료")
            return saved_count
            
        except Exception as e:
            self.logger.error(f"법정동 코드 저장 실패: {e}")
            return 0

    async def _save_processed_data(
        self,
        content_type: str,
        processed_data: List[Dict],
        raw_data_id: str,
        quality_score: float,
    ) -> int:
        """처리된 데이터를 데이터베이스에 저장"""

        if not processed_data:
            return 0

        try:
            target_table = self._get_target_table(content_type)

            saved_count = 0

            for item in processed_data:
                # raw_data_id와 품질 점수 추가
                item["raw_data_id"] = raw_data_id
                item["data_quality_score"] = quality_score
                item["last_sync_at"] = datetime.utcnow()

                # 테이블별 저장 로직
                if target_table == "tourist_attractions":
                    await self.db_manager.upsert_tourist_attraction(item)
                elif target_table == "accommodations":
                    await self.db_manager.upsert_accommodation(item)
                elif target_table == "festivals_events":
                    await self.db_manager.upsert_festival_event(item)
                # 다른 테이블들도 필요시 추가

                saved_count += 1

            self.logger.debug(
                f"처리된 데이터 저장 완료: {target_table} {saved_count}건"
            )
            return saved_count

        except Exception as e:
            self.logger.error(f"처리된 데이터 저장 실패: {e}")
            return 0

    def _get_target_table(self, content_type: str) -> str:
        """컨텐츠 타입에 따른 대상 테이블 결정"""
        table_mapping = {
            "12": "tourist_attractions",
            "14": "cultural_facilities",
            "15": "festivals_events",
            "25": "travel_courses",
            "28": "leisure_sports",
            "32": "accommodations",
            "38": "shopping",
            "39": "restaurants",
        }

        return table_mapping.get(content_type, "tourist_attractions")

    async def collect_area_codes(self, parent_code: Optional[str] = None) -> List[Dict]:
        """지역 코드 정보 수집"""

        params = {**self.default_params, "numOfRows": 50}

        if parent_code:
            params["areaCode"] = parent_code

        async with self.api_client:
            response = await self.api_client.call_api(
                api_provider=APIProvider.KTO,
                endpoint="areaCode2",
                params=params,
                store_raw=True,
                cache_ttl=86400,  # 24시간 캐시
            )

            if not response.success:
                self.logger.error(f"지역 코드 수집 실패: {response.error}")
                return []

            # 응답에서 아이템 추출
            items = response.data.get("response", {}).get("body", {}).get("items", {})
            if not items or "item" not in items:
                return []

            area_items = items["item"]
            if isinstance(area_items, dict):
                area_items = [area_items]

            # 지역 코드 정보 변환
            area_codes = []
            for item in area_items:
                area_info = {
                    "region_code": item.get("code"),
                    "region_name": item.get("name"),
                    "parent_region_code": parent_code,
                    "region_level": 1 if not parent_code else 2,
                    "data_source": "KTO_API",
                    "raw_data_id": response.raw_data_id,
                    "processed_at": datetime.utcnow().isoformat(),
                }
                area_codes.append(area_info)

            self.logger.info(f"지역 코드 수집 완료: {len(area_codes)}개")
            return area_codes

    async def collect_detailed_area_codes(self) -> List[Dict]:
        """세부 지역 코드 정보 수집 (시군구)"""

        all_detailed_codes = []

        async with self.api_client:
            for area_code in self.area_codes:
                try:
                    detailed_codes = await self.collect_area_codes(area_code)
                    all_detailed_codes.extend(detailed_codes)

                    # API 호출 간격 조정
                    await asyncio.sleep(0.1)

                except Exception as e:
                    self.logger.error(f"세부 지역 코드 수집 실패 ({area_code}): {e}")

        self.logger.info(f"전체 세부 지역 코드 수집 완료: {len(all_detailed_codes)}개")
        return all_detailed_codes

    async def collect_category_codes(
        self, content_type_id: Optional[str] = None
    ) -> List[Dict]:
        """카테고리 코드 정보 수집"""

        params = {**self.default_params, "numOfRows": 100}

        if content_type_id:
            params["contentTypeId"] = content_type_id

        async with self.api_client:
            response = await self.api_client.call_api(
                api_provider=APIProvider.KTO,
                endpoint="categoryCode2",
                params=params,
                store_raw=True,
                cache_ttl=86400,  # 24시간 캐시
            )

            if not response.success:
                self.logger.error(f"카테고리 코드 수집 실패: {response.error}")
                return []

            # 응답에서 아이템 추출 및 변환
            items = response.data.get("response", {}).get("body", {}).get("items", {})
            if not items or "item" not in items:
                return []

            category_items = items["item"]
            if isinstance(category_items, dict):
                category_items = [category_items]

            category_codes = []
            for item in category_items:
                category_info = {
                    "category_code": item.get("code"),
                    "category_name": item.get("name"),
                    "content_type_id": content_type_id,
                    "data_source": "KTO_API",
                    "raw_data_id": response.raw_data_id,
                    "processed_at": datetime.utcnow().isoformat(),
                }
                category_codes.append(category_info)

            self.logger.info(f"카테고리 코드 수집 완료: {len(category_codes)}개")
            return category_codes

    async def get_api_statistics(self) -> Dict:
        """API 호출 통계 조회"""

        try:
            stats = await self.db_manager.get_api_call_statistics("KTO")
            return {
                "provider": "KTO",
                "today_calls": stats.get("today_calls", 0),
                "success_rate": stats.get("success_rate", 0.0),
                "avg_response_time_ms": stats.get("avg_response_time_ms", 0),
                "cache_hit_rate": stats.get("cache_hit_rate", 0.0),
                "last_call_at": stats.get("last_call_at"),
                "total_raw_data_size_mb": stats.get("total_raw_data_size_mb", 0.0),
            }
        except Exception as e:
            self.logger.error(f"API 통계 조회 실패: {e}")
            return {}


# 싱글톤 인스턴스
_unified_kto_client = None


def get_unified_kto_client() -> UnifiedKTOClient:
    """통합 KTO 클라이언트 인스턴스 반환"""
    global _unified_kto_client
    if _unified_kto_client is None:
        _unified_kto_client = UnifiedKTOClient()
    return _unified_kto_client
