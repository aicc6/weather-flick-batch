"""
통합 한국관광공사 API 클라이언트 확장 버전

누락된 엔드포인트들을 포함한 완전한 API 수집 기능을 제공합니다.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from app.collectors.unified_kto_client import UnifiedKTOClient
from app.core.multi_api_key_manager import APIProvider


class ExtendedUnifiedKTOClient(UnifiedKTOClient):
    """확장된 통합 KTO API 클라이언트"""
    
    def __init__(self):
        super().__init__()
        self.logger.info("확장된 통합 KTO 클라이언트 초기화")
    
    # ===== 무장애 여행 정보 =====
    async def collect_detail_with_tour(
        self,
        content_ids: List[str],
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """무장애 여행정보 수집"""
        sync_batch_id = f"with_tour_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"무장애 여행정보 수집 시작: {sync_batch_id}")
        
        collection_results = {
            "sync_batch_id": sync_batch_id,
            "api_endpoint": "detailWithTour2",
            "started_at": datetime.utcnow().isoformat(),
            "total_raw_records": 0,
            "total_processed_records": 0,
            "errors": []
        }
        
        async with self.api_client:
            for content_id in content_ids:
                try:
                    params = {
                        **self.default_params,
                        "contentId": content_id
                    }
                    
                    response = await self.api_client.call_api(
                        api_provider=APIProvider.KTO,
                        endpoint="detailWithTour2",
                        params=params,
                        store_raw=store_raw
                    )
                    
                    if response.success and response.data:
                        items = response.data.get("items", {})
                        if items and "item" in items:
                            collection_results["total_raw_records"] += 1
                            
                            if auto_transform and response.raw_data_id:
                                # 데이터 변환 및 저장
                                processed_count = await self._transform_and_save_with_tour_data(
                                    response.raw_data_id, items["item"]
                                )
                                collection_results["total_processed_records"] += processed_count
                    
                    await asyncio.sleep(0.3)
                    
                except Exception as e:
                    error_msg = f"무장애 여행정보 수집 실패 ({content_id}): {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)
        
        collection_results["completed_at"] = datetime.utcnow().isoformat()
        return collection_results
    
    # ===== 반려동물 동반 여행 목록 =====
    async def collect_pet_tour_list(
        self,
        area_codes: List[str] = None,
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """반려동물 동반 가능 여행정보 목록 수집"""
        if not area_codes:
            area_codes = ["1", "39"]  # 서울, 제주
        
        sync_batch_id = f"pet_tour_list_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"반려동물 동반 여행 목록 수집 시작: {sync_batch_id}")
        
        collection_results = {
            "sync_batch_id": sync_batch_id,
            "api_endpoint": "petTour2",
            "started_at": datetime.utcnow().isoformat(),
            "total_raw_records": 0,
            "total_processed_records": 0,
            "areas_collected": {},
            "errors": []
        }
        
        async with self.api_client:
            for area_code in area_codes:
                area_result = await self._collect_area_pet_tours(
                    area_code, sync_batch_id, store_raw, auto_transform
                )
                collection_results["areas_collected"][area_code] = area_result
                collection_results["total_raw_records"] += area_result.get("raw_records", 0)
                collection_results["total_processed_records"] += area_result.get("processed_records", 0)
                
                await asyncio.sleep(0.5)
        
        collection_results["completed_at"] = datetime.utcnow().isoformat()
        return collection_results
    
    async def _collect_area_pet_tours(
        self,
        area_code: str,
        sync_batch_id: str,
        store_raw: bool,
        auto_transform: bool
    ) -> Dict:
        """특정 지역의 반려동물 동반 여행정보 수집"""
        area_result = {
            "area_code": area_code,
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
                params = {
                    **self.default_params,
                    "areaCode": area_code,
                    "pageNo": page_no,
                    "numOfRows": num_of_rows,
                }
                
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="petTour2",
                    params=params,
                    store_raw=store_raw,
                )
                
                if not response.success:
                    break
                
                response_body = response.data
                total_count = response_body.get("totalCount", 0)
                items = response_body.get("items", {})
                
                if total_count == 0 or not items or "item" not in items:
                    break
                
                page_items = items["item"]
                if isinstance(page_items, dict):
                    page_items = [page_items]
                
                current_page_count = len(page_items)
                area_result["raw_records"] += current_page_count
                area_result["pages_collected"] += 1
                
                if response.raw_data_id:
                    area_result["raw_data_ids"].append(response.raw_data_id)
                
                # 페이지네이션 종료 조건
                if current_page_count < num_of_rows or area_result["raw_records"] >= total_count:
                    break
                
                page_no += 1
                await asyncio.sleep(0.3)
                
            except Exception as e:
                error_msg = f"반려동물 동반 여행 수집 실패 (지역 {area_code}, 페이지 {page_no}): {e}"
                area_result["errors"].append(error_msg)
                self.logger.error(error_msg)
                break
        
        return area_result
    
    # ===== 친환경 여행 정보 =====
    async def collect_green_tour_list(
        self,
        area_codes: List[str] = None,
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """친환경 여행정보 수집"""
        if not area_codes:
            area_codes = ["1", "39"]
        
        sync_batch_id = f"green_tour_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return await self._collect_theme_tour_data(
            endpoint="greenTour2",
            theme_name="친환경 여행",
            area_codes=area_codes,
            sync_batch_id=sync_batch_id,
            store_raw=store_raw,
            auto_transform=auto_transform
        )
    
    # ===== 웰니스 관광 정보 =====
    async def collect_healing_tour_list(
        self,
        area_codes: List[str] = None,
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """웰니스 관광정보 수집"""
        if not area_codes:
            area_codes = ["1", "39"]
        
        sync_batch_id = f"healing_tour_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        return await self._collect_theme_tour_data(
            endpoint="healingTour2",
            theme_name="웰니스 관광",
            area_codes=area_codes,
            sync_batch_id=sync_batch_id,
            store_raw=store_raw,
            auto_transform=auto_transform
        )
    
    # ===== 공통 테마 수집 메서드 =====
    async def _collect_theme_tour_data(
        self,
        endpoint: str,
        theme_name: str,
        area_codes: List[str],
        sync_batch_id: str,
        store_raw: bool,
        auto_transform: bool
    ) -> Dict:
        """테마별 여행정보 수집 공통 메서드"""
        self.logger.info(f"{theme_name} 정보 수집 시작: {sync_batch_id}")
        
        collection_results = {
            "sync_batch_id": sync_batch_id,
            "api_endpoint": endpoint,
            "theme": theme_name,
            "started_at": datetime.utcnow().isoformat(),
            "total_raw_records": 0,
            "total_processed_records": 0,
            "areas_collected": {},
            "errors": []
        }
        
        async with self.api_client:
            for area_code in area_codes:
                try:
                    area_result = await self._collect_area_theme_data(
                        endpoint=endpoint,
                        area_code=area_code,
                        sync_batch_id=sync_batch_id,
                        store_raw=store_raw,
                        auto_transform=auto_transform
                    )
                    
                    collection_results["areas_collected"][area_code] = area_result
                    collection_results["total_raw_records"] += area_result.get("raw_records", 0)
                    collection_results["total_processed_records"] += area_result.get("processed_records", 0)
                    
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    error_msg = f"{theme_name} 수집 실패 (지역 {area_code}): {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)
        
        collection_results["completed_at"] = datetime.utcnow().isoformat()
        return collection_results
    
    async def _collect_area_theme_data(
        self,
        endpoint: str,
        area_code: str,
        sync_batch_id: str,
        store_raw: bool,
        auto_transform: bool
    ) -> Dict:
        """특정 지역의 테마 데이터 수집"""
        area_result = {
            "area_code": area_code,
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
                params = {
                    **self.default_params,
                    "areaCode": area_code,
                    "pageNo": page_no,
                    "numOfRows": num_of_rows,
                    "arrange": "A",
                }
                
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint=endpoint,
                    params=params,
                    store_raw=store_raw,
                )
                
                if not response.success:
                    break
                
                response_body = response.data
                total_count = response_body.get("totalCount", 0)
                items = response_body.get("items", {})
                
                if total_count == 0 or not items or "item" not in items:
                    break
                
                page_items = items["item"]
                if isinstance(page_items, dict):
                    page_items = [page_items]
                
                current_page_count = len(page_items)
                area_result["raw_records"] += current_page_count
                area_result["pages_collected"] += 1
                
                if response.raw_data_id:
                    area_result["raw_data_ids"].append(response.raw_data_id)
                
                if current_page_count < num_of_rows or area_result["raw_records"] >= total_count:
                    break
                
                page_no += 1
                await asyncio.sleep(0.3)
                
            except Exception as e:
                error_msg = f"테마 데이터 수집 실패: {e}"
                area_result["errors"].append(error_msg)
                self.logger.error(error_msg)
                break
        
        return area_result
    
    # ===== 교통시설 정보 =====
    async def collect_transport_facilities(
        self,
        area_codes: List[str] = None,
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """교통시설 정보 수집 (콘텐츠타입 40)"""
        if not area_codes:
            area_codes = ["1", "39"]
        
        sync_batch_id = f"transport_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"교통시설 정보 수집 시작: {sync_batch_id}")
        
        # 기존 comprehensive_tourism_collection 메서드 활용
        return await self.comprehensive_tourism_collection(
            content_types=["40"],  # 교통시설
            area_codes=area_codes,
            store_raw=store_raw,
            auto_transform=auto_transform,
            include_new_apis=False
        )
    
    # ===== 종합 수집 메서드 (확장) =====
    async def collect_all_extended_data(
        self,
        area_codes: List[str] = None,
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """모든 확장 데이터 종합 수집"""
        if not area_codes:
            area_codes = ["1", "39"]  # 서울, 제주
        
        sync_batch_id = f"extended_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.logger.info(f"확장 데이터 전체 수집 시작: {sync_batch_id}")
        
        all_results = {
            "sync_batch_id": sync_batch_id,
            "started_at": datetime.utcnow().isoformat(),
            "basic_data": {},
            "extended_data": {},
            "total_raw_records": 0,
            "total_processed_records": 0,
            "errors": []
        }
        
        try:
            # 1. 기본 데이터 수집 (기존 콘텐츠 타입 + 교통시설)
            self.logger.info("1. 기본 데이터 수집")
            basic_result = await self.comprehensive_tourism_collection(
                content_types=["12", "14", "15", "25", "28", "32", "38", "39", "40"],
                area_codes=area_codes,
                store_raw=store_raw,
                auto_transform=auto_transform,
                include_new_apis=True
            )
            all_results["basic_data"] = basic_result
            all_results["total_raw_records"] += basic_result.get("total_raw_records", 0)
            all_results["total_processed_records"] += basic_result.get("total_processed_records", 0)
            
            # 2. 반려동물 동반 여행 목록
            self.logger.info("2. 반려동물 동반 여행 목록 수집")
            pet_result = await self.collect_pet_tour_list(
                area_codes=area_codes,
                store_raw=store_raw,
                auto_transform=auto_transform
            )
            all_results["extended_data"]["pet_tour_list"] = pet_result
            all_results["total_raw_records"] += pet_result.get("total_raw_records", 0)
            all_results["total_processed_records"] += pet_result.get("total_processed_records", 0)
            
            # 3. 친환경 여행
            self.logger.info("3. 친환경 여행정보 수집")
            green_result = await self.collect_green_tour_list(
                area_codes=area_codes,
                store_raw=store_raw,
                auto_transform=auto_transform
            )
            all_results["extended_data"]["green_tour"] = green_result
            all_results["total_raw_records"] += green_result.get("total_raw_records", 0)
            all_results["total_processed_records"] += green_result.get("total_processed_records", 0)
            
            # 4. 웰니스 관광
            self.logger.info("4. 웰니스 관광정보 수집")
            healing_result = await self.collect_healing_tour_list(
                area_codes=area_codes,
                store_raw=store_raw,
                auto_transform=auto_transform
            )
            all_results["extended_data"]["healing_tour"] = healing_result
            all_results["total_raw_records"] += healing_result.get("total_raw_records", 0)
            all_results["total_processed_records"] += healing_result.get("total_processed_records", 0)
            
        except Exception as e:
            error_msg = f"확장 데이터 수집 중 오류: {e}"
            self.logger.error(error_msg)
            all_results["errors"].append(error_msg)
        
        all_results["completed_at"] = datetime.utcnow().isoformat()
        
        self.logger.info(
            f"확장 데이터 수집 완료: 총 원본 {all_results['total_raw_records']}건, "
            f"처리 {all_results['total_processed_records']}건"
        )
        
        return all_results
    
    async def _transform_and_save_with_tour_data(self, raw_data_id: str, item_data: Dict) -> int:
        """무장애 여행 데이터 변환 및 저장"""
        try:
            # 실제 구현 시 데이터 변환 로직 추가
            self.logger.info(f"무장애 여행 데이터 변환: {item_data.get('title', 'N/A')}")
            return 1
        except Exception as e:
            self.logger.error(f"무장애 여행 데이터 변환 실패: {e}")
            return 0


if __name__ == "__main__":
    import logging
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    async def test_extended_client():
        client = ExtendedUnifiedKTOClient()
        
        print("=== 확장 KTO 클라이언트 테스트 ===")
        
        # 반려동물 동반 여행 테스트
        print("\n1. 반려동물 동반 여행 목록 테스트")
        pet_result = await client.collect_pet_tour_list(
            area_codes=["1"],  # 서울만
            store_raw=False,
            auto_transform=False
        )
        print(f"   수집된 반려동물 동반 여행지: {pet_result['total_raw_records']}개")
        
        # 교통시설 테스트
        print("\n2. 교통시설 정보 테스트")
        transport_result = await client.collect_transport_facilities(
            area_codes=["1"],
            store_raw=False,
            auto_transform=False
        )
        print(f"   수집된 교통시설: {transport_result['total_raw_records']}개")
        
        print("\n=== 테스트 완료 ===")
    
    # 테스트 실행
    asyncio.run(test_extended_client())