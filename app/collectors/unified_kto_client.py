"""
통합 KTO API 클라이언트

기존 KTO API 수집기를 새로운 통합 아키텍처로 마이그레이션
"""

import logging
import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from app.core.unified_api_client import get_unified_api_client, APIProvider
from app.processors.data_transformation_pipeline import get_transformation_pipeline
from app.core.database_manager_extension import get_extended_database_manager
from app.core.multi_api_key_manager import get_api_key_manager
from app.core.concurrent_api_manager import (
    get_concurrent_api_manager,
    APICallTask,
    APICallPriority,
    ConcurrencyConfig
)
from app.core.selective_storage_manager import get_storage_manager
from app.archiving.archival_engine import get_archival_engine
from app.archiving.backup_manager import get_backup_manager


class UnifiedKTOClient:
    """통합 KTO API 클라이언트"""

    def __init__(self, enable_parallel: bool = True, concurrency_config: ConcurrencyConfig = None):
        self.logger = logging.getLogger(__name__)
        self.api_client = get_unified_api_client()
        self.transformation_pipeline = get_transformation_pipeline()
        self.db_manager = get_extended_database_manager()
        self.key_manager = get_api_key_manager()
        self.storage_manager = get_storage_manager()

        # 아카이빙 시스템
        self.archival_engine = get_archival_engine()
        self.backup_manager = get_backup_manager()

        # 병렬 처리 설정
        self.enable_parallel = enable_parallel
        if enable_parallel:
            self.concurrent_manager = get_concurrent_api_manager(concurrency_config)
        else:
            self.concurrent_manager = None

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
        include_hierarchical_regions: bool = False,
        use_priority_sorting: bool = False,
    ) -> Dict:
        """
        모든 KTO 데이터 수집 (신규 API 포함)

        Args:
            content_types: 수집할 컨텐츠 타입 목록 (None이면 전체)
            area_codes: 수집할 지역 코드 목록 (None이면 전체)
            store_raw: 원본 데이터 저장 여부
            auto_transform: 자동 변환 수행 여부
            include_new_apis: 신규 추가된 4개 API 포함 여부
            include_hierarchical_regions: 계층적 지역코드 수집 포함 여부
            use_priority_sorting: 데이터 부족 순으로 우선순위 정렬 여부

        Returns:
            Dict: 수집 결과 요약
        """

        if content_types is None:
            content_types = list(self.content_types.keys())

        # 우선순위 정렬 사용 시 데이터 부족 순으로 정렬
        if use_priority_sorting:
            from app.core.data_priority_manager import get_priority_manager
            priority_manager = get_priority_manager()

            # 컨텐츠 타입을 데이터 부족 순으로 정렬
            priority_list = priority_manager.get_priority_sorted_content_types(content_types)
            content_types = [item[0] for item in priority_list]  # 우선순위 순서로 재정렬

            self.logger.info(f"🎯 우선순위 정렬 활성화: {len(content_types)}개 컨텐츠 타입")
            for rank, (content_type, count, name) in enumerate(priority_list, 1):
                urgency = "🔥" if count == 0 else "⚠️" if count < 1000 else "✅"
                self.logger.info(f"  {rank}. {name} (타입 {content_type}): {count:,}개 {urgency}")

        if area_codes is None:
            area_codes = self.area_codes

        # API 제한 상태 확인 - 모든 KTO API 키가 제한되었는지 확인
        # 모든 키가 제한된 경우 작업을 건너뛰고 다음 배치 실행 시간까지 대기
        if self.key_manager.are_all_keys_rate_limited(APIProvider.KTO):
            next_reset_time = self.key_manager.get_next_reset_time(APIProvider.KTO)
            rate_limit_status = self.key_manager.get_rate_limit_status(APIProvider.KTO)

            error_msg = (
                f"모든 KTO API 키가 제한되어 있습니다. "
                f"활성 키: {rate_limit_status['active_keys']}/{rate_limit_status['total_keys']}, "
                f"제한된 키: {rate_limit_status['limited_keys']}개"
            )

            if next_reset_time:
                time_until_reset = next_reset_time - datetime.now()
                hours = int(time_until_reset.total_seconds() // 3600)
                minutes = int((time_until_reset.total_seconds() % 3600) // 60)
                error_msg += f" 다음 재시도 가능 시간: {next_reset_time.strftime('%Y-%m-%d %H:%M:%S')} (약 {hours}시간 {minutes}분 후)"

            self.logger.warning(error_msg)

            return {
                "sync_batch_id": f"kto_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                "started_at": datetime.utcnow().isoformat(),
                "completed_at": datetime.utcnow().isoformat(),
                "status": "skipped",
                "reason": "all_api_keys_rate_limited",
                "next_retry_time": next_reset_time.isoformat() if next_reset_time else None,
                "rate_limit_status": rate_limit_status,
                "content_types_collected": {},
                "new_apis_collected": {},
                "total_raw_records": 0,
                "total_processed_records": 0,
                "errors": [error_msg],
            }

        sync_batch_id = f"kto_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        collection_results = {
            "sync_batch_id": sync_batch_id,
            "started_at": datetime.utcnow().isoformat(),
            "status": "running",
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

            # 상세 정보 수집 (detailCommon2, detailIntro2, detailInfo2, detailImage2)
            if include_new_apis:
                self.logger.info("=== 상세 정보 수집 시작 ===")

                try:
                    detail_collection_result = await self.collect_detailed_information(
                        content_types=content_types or list(self.content_types.keys()),
                        max_content_ids=50,  # 테스트용으로 제한
                        store_raw=store_raw,
                        auto_transform=auto_transform
                    )
                    collection_results["new_apis_collected"]["detailed_info"] = detail_collection_result
                    collection_results["total_raw_records"] += detail_collection_result.get("total_raw_records", 0)
                    collection_results["total_processed_records"] += detail_collection_result.get("total_processed_records", 0)

                    self.logger.info(f"상세 정보 수집 완료: 원본 {detail_collection_result.get('total_raw_records', 0)}건")

                except Exception as e:
                    error_msg = f"상세 정보 수집 실패: {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)

                self.logger.info("=== 신규 API 수집 완료 ===")

            # 5. 계층적 지역코드 수집 (옵션)
            if include_hierarchical_regions:
                self.logger.info("=== 계층적 지역코드 수집 시작 ===")
                try:
                    hierarchical_result = await self.collect_hierarchical_area_codes(
                        force_update=False,
                        store_raw=store_raw
                    )
                    collection_results["hierarchical_regions_collected"] = hierarchical_result

                    # 수집 통계에 추가
                    provinces_count = hierarchical_result.get("total_provinces", 0)
                    districts_count = hierarchical_result.get("total_districts", 0)
                    collection_results["total_raw_records"] += provinces_count + districts_count

                    self.logger.info(f"계층적 지역코드 수집 완료: 시도 {provinces_count}개, 시군구 {districts_count}개")

                except Exception as e:
                    error_msg = f"계층적 지역코드 수집 실패: {e}"
                    self.logger.error(error_msg)
                    collection_results["errors"].append(error_msg)

                self.logger.info("=== 계층적 지역코드 수집 완료 ===")

        collection_results["completed_at"] = datetime.utcnow().isoformat()
        collection_results["status"] = "completed"

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

                                if auto_transform and response.raw_data_id:
                                    # 실제 데이터 변환 및 저장
                                    processed_count = await self._transform_and_save_pet_tour_data(
                                        response.raw_data_id, item_list
                                    )
                                    collection_results["total_processed_records"] += processed_count

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

                            if auto_transform and response.raw_data_id:
                                # 실제 데이터 변환 및 저장
                                processed_count = await self._transform_and_save_pet_tour_data(
                                    response.raw_data_id, item_list
                                )
                                collection_results["total_processed_records"] = processed_count

            except Exception as e:
                error_msg = f"펫투어 수집 실패: {e}"
                self.logger.error(error_msg)
                collection_results["errors"].append(error_msg)

        collection_results["completed_at"] = datetime.utcnow().isoformat()

        self.logger.info(
            f"반려동물 동반여행 정보 수집 완료: {sync_batch_id} - 총 원본 {collection_results['total_raw_records']}건, 처리 {collection_results['total_processed_records']}건"
        )

        return collection_results

    async def _transform_and_save_pet_tour_data(self, raw_data_id: str, item_list: List[Dict]) -> int:
        """반려동물 동반여행 데이터 변환 및 저장"""
        try:
            # 데이터 변환
            transformation_result = await self.transformation_pipeline.transform_raw_data(raw_data_id)

            if not transformation_result.success or not transformation_result.processed_data:
                self.logger.warning(f"반려동물 동반여행 데이터 변환 실패: {raw_data_id}")
                return 0

            # 데이터베이스 저장
            saved_count = 0
            for processed_item in transformation_result.processed_data:
                # 필수 메타데이터 추가
                processed_item["raw_data_id"] = raw_data_id
                processed_item["data_quality_score"] = transformation_result.quality_score or 0.0
                processed_item["processing_status"] = "processed"
                processed_item["last_sync_at"] = datetime.utcnow()

                # pet_tour_info 테이블에 저장
                if self.db_manager.upsert_pet_tour_info(processed_item):
                    saved_count += 1
                    self.logger.debug(f"반려동물 동반여행 정보 저장 성공: {processed_item.get('title')}")
                else:
                    self.logger.warning(f"반려동물 동반여행 정보 저장 실패: {processed_item.get('title')}")

            self.logger.info(f"반려동물 동반여행 데이터 처리 완료: {saved_count}/{len(transformation_result.processed_data)}건 저장")
            return saved_count

        except Exception as e:
            self.logger.error(f"반려동물 동반여행 데이터 변환/저장 실패: {e}")
            return 0

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

    async def collect_hierarchical_area_codes(
        self,
        force_update: bool = False,
        store_raw: bool = True
    ) -> Dict:
        """
        계층적 지역코드 완전 수집 (시도 + 시군구)

        Args:
            force_update: 기존 데이터 무시하고 강제 업데이트
            store_raw: 원본 데이터 저장 여부

        Returns:
            Dict: 수집 결과
        """
        sync_batch_id = f"area_codes_hierarchical_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        collection_results = {
            "sync_batch_id": sync_batch_id,
            "started_at": datetime.utcnow().isoformat(),
            "provinces_collected": {},  # 시도 데이터
            "districts_collected": {},  # 시군구 데이터
            "total_provinces": 0,
            "total_districts": 0,
            "errors": []
        }

        self.logger.info(f"계층적 지역코드 수집 시작: {sync_batch_id}")

        async with self.api_client:
            try:
                # 1단계: 전체 시도 코드 수집
                self.logger.info("1단계: 시도 코드 수집")
                provinces_result = await self._collect_province_codes(sync_batch_id, store_raw)
                collection_results["provinces_collected"] = provinces_result
                collection_results["total_provinces"] = provinces_result.get("total_records", 0)

                # 2단계: 각 시도별 시군구 코드 수집
                self.logger.info("2단계: 시군구 코드 수집")
                province_codes = provinces_result.get("province_codes", [])

                for province_code in province_codes:
                    try:
                        district_result = await self._collect_district_codes(
                            province_code, sync_batch_id, store_raw
                        )
                        collection_results["districts_collected"][province_code] = district_result
                        collection_results["total_districts"] += district_result.get("total_records", 0)

                        # API 호출 간격 조정
                        await asyncio.sleep(0.5)

                    except Exception as e:
                        error_msg = f"지역 {province_code} 시군구 수집 실패: {e}"
                        self.logger.error(error_msg)
                        collection_results["errors"].append(error_msg)

                # 3단계: 데이터베이스 저장 및 업데이트
                await self._save_hierarchical_region_data(collection_results)

            except Exception as e:
                error_msg = f"계층적 지역코드 수집 실패: {e}"
                self.logger.error(error_msg)
                collection_results["errors"].append(error_msg)

        collection_results["completed_at"] = datetime.utcnow().isoformat()

        self.logger.info(
            f"계층적 지역코드 수집 완료: 시도 {collection_results['total_provinces']}개, "
            f"시군구 {collection_results['total_districts']}개"
        )

        return collection_results

    async def _collect_province_codes(self, sync_batch_id: str, store_raw: bool) -> Dict:
        """시도 코드 수집"""
        result = {
            "total_records": 0,
            "province_codes": [],
            "raw_data_ids": [],
            "errors": []
        }

        try:
            params = {
                **self.default_params,
                "numOfRows": 20  # 시도는 17개이므로 충분
            }

            response = await self.api_client.call_api(
                api_provider=APIProvider.KTO,
                endpoint="areaCode2",
                params=params,
                store_raw=store_raw
            )

            if response.success and response.data:
                items = response.data.get("items", {}).get("item", [])
                if not isinstance(items, list):
                    items = [items]

                result["total_records"] = len(items)
                result["province_codes"] = [item.get("code") for item in items]

                if response.raw_data_id:
                    result["raw_data_ids"].append(response.raw_data_id)

                self.logger.info(f"시도 코드 {len(items)}개 수집 완료")

        except Exception as e:
            error_msg = f"시도 코드 수집 실패: {e}"
            result["errors"].append(error_msg)
            self.logger.error(error_msg)

        return result

    async def _collect_district_codes(self, province_code: str, sync_batch_id: str, store_raw: bool) -> Dict:
        """특정 시도의 시군구 코드 수집"""
        result = {
            "province_code": province_code,
            "total_records": 0,
            "district_codes": [],
            "raw_data_ids": [],
            "errors": []
        }

        try:
            params = {
                **self.default_params,
                "areaCode": province_code,
                "numOfRows": 50  # 경기도가 31개로 가장 많음
            }

            response = await self.api_client.call_api(
                api_provider=APIProvider.KTO,
                endpoint="areaCode2",
                params=params,
                store_raw=store_raw
            )

            if response.success and response.data:
                items = response.data.get("items", {}).get("item", [])
                if not isinstance(items, list):
                    items = [items]

                result["total_records"] = len(items)
                result["district_codes"] = [
                    {"code": item.get("code"), "name": item.get("name")}
                    for item in items
                ]

                if response.raw_data_id:
                    result["raw_data_ids"].append(response.raw_data_id)

                self.logger.info(f"지역 {province_code} 시군구 {len(items)}개 수집 완료")

        except Exception as e:
            error_msg = f"지역 {province_code} 시군구 수집 실패: {e}"
            result["errors"].append(error_msg)
            self.logger.error(error_msg)

        return result

    async def _save_hierarchical_region_data(self, collection_results: Dict):
        """수집된 계층적 지역 데이터를 데이터베이스에 저장"""
        try:
            provinces_data = collection_results.get("provinces_collected", {})
            districts_data = collection_results.get("districts_collected", {})

            saved_count = 0

            # 1. 시도 데이터 저장 (regions 테이블에 직접 저장)
            if provinces_data.get("province_codes"):
                for province_code in provinces_data["province_codes"]:
                    try:
                        # 시도명 매핑
                        province_names = {
                            "1": "서울특별시", "2": "인천광역시", "3": "대전광역시",
                            "4": "대구광역시", "5": "광주광역시", "6": "부산광역시",
                            "7": "울산광역시", "8": "세종특별자치시", "31": "경기도",
                            "32": "강원특별자치도", "33": "충청북도", "34": "충청남도",
                            "35": "경상북도", "36": "경상남도", "37": "전북특별자치도",
                            "38": "전라남도", "39": "제주도"
                        }

                        province_name = province_names.get(province_code, f"지역{province_code}")

                        # regions 테이블에 UPSERT
                        upsert_query = """
                            INSERT INTO regions (region_code, region_name, parent_region_code, region_level, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, NOW(), NOW())
                            ON CONFLICT (region_code)
                            DO UPDATE SET
                                region_name = EXCLUDED.region_name,
                                parent_region_code = EXCLUDED.parent_region_code,
                                region_level = EXCLUDED.region_level,
                                updated_at = NOW()
                        """

                        self.db_manager.execute_update(
                            upsert_query,
                            (province_code, province_name, None, 1)
                        )
                        saved_count += 1

                    except Exception as e:
                        self.logger.error(f"시도 {province_code} 저장 실패: {e}")

            # 2. 시군구 데이터 저장
            for province_code, district_data in districts_data.items():
                district_codes = district_data.get("district_codes", [])

                for district in district_codes:
                    try:
                        district_code = district.get("code")
                        district_name = district.get("name")

                        if district_code and district_name:
                            # 시군구 코드는 시도코드_시군구코드 형태로 저장
                            full_district_code = f"{province_code}_{district_code}"

                            # regions 테이블에 UPSERT
                            upsert_query = """
                                INSERT INTO regions (region_code, region_name, parent_region_code, region_level, created_at, updated_at)
                                VALUES (%s, %s, %s, %s, NOW(), NOW())
                                ON CONFLICT (region_code)
                                DO UPDATE SET
                                    region_name = EXCLUDED.region_name,
                                    parent_region_code = EXCLUDED.parent_region_code,
                                    region_level = EXCLUDED.region_level,
                                    updated_at = NOW()
                            """

                            self.db_manager.execute_update(
                                upsert_query,
                                (full_district_code, district_name, province_code, 2)
                            )
                            saved_count += 1

                    except Exception as e:
                        self.logger.error(f"시군구 {province_code}_{district.get('code', 'N/A')} 저장 실패: {e}")

            self.logger.info(f"계층적 지역 데이터 저장 완료: 총 {saved_count}개 저장")

        except Exception as e:
            self.logger.error(f"계층적 지역 데이터 저장 실패: {e}")

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
                response_body = response.data  # UnifiedAPIClient가 이미 body만 반환
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
                self.db_manager.execute_update(
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
                self.db_manager.execute_update(
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
                self.db_manager.execute_update(
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
                self.db_manager.execute_update(
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
                self.logger.debug(f"원본 raw_data_id: {raw_data_id} (타입: {type(raw_data_id)})")
                # raw_data_id가 유효한 UUID 형식인지 확인하고, 아니면 None으로 설정
                try:
                    import uuid
                    uuid.UUID(str(raw_data_id))  # UUID 유효성 검사
                    item["raw_data_id"] = raw_data_id
                except (ValueError, TypeError, AttributeError):
                    self.logger.warning(f"유효하지 않은 raw_data_id: {raw_data_id}, NULL로 설정")
                    item["raw_data_id"] = None
                item["data_quality_score"] = quality_score
                item["last_sync_at"] = datetime.utcnow()

                # 테이블별 저장 로직
                if target_table == "tourist_attractions":
                    self.db_manager.upsert_tourist_attraction(item)
                elif target_table == "accommodations":
                    self.db_manager.upsert_accommodation(item)
                elif target_table == "festivals_events":
                    self.db_manager.upsert_festival_event(item)
                elif target_table == "restaurants":
                    self.db_manager.upsert_restaurant(item)
                elif target_table == "leisure_sports":
                    # facility_name이 없거나 빈 값이면 기본값 대입
                    if not item.get("facility_name"):
                        item["facility_name"] = "미상"
                    self.db_manager.upsert_leisure_sport(item)
                elif target_table == "cultural_facilities":
                    self.db_manager.upsert_cultural_facility(item)
                else:
                    self.logger.warning(f"지원하지 않는 테이블: {target_table}, 관광지로 저장")
                    self.db_manager.upsert_tourist_attraction(item)

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

    async def collect_detailed_information(
        self,
        content_types: List[str],
        max_content_ids: int = 100,
        store_raw: bool = True,
        auto_transform: bool = True
    ) -> Dict:
        """상세 정보 수집 (detailCommon2, detailIntro2, detailInfo2, detailImage2)"""

        result = {
            "total_raw_records": 0,
            "total_processed_records": 0,
            "content_types_processed": {},
            "errors": []
        }

        for content_type_id in content_types:
            content_name = self.content_types.get(content_type_id, f"unknown_{content_type_id}")
            self.logger.info(f"=== {content_name} 상세 정보 수집 시작 ===")

            try:
                # 기존 content_id들 가져오기
                content_ids = await self._get_existing_content_ids(content_type_id, max_content_ids)

                if not content_ids:
                    self.logger.warning(f"{content_name}: 기존 콘텐츠 ID를 찾을 수 없음")
                    continue

                content_result = {
                    "content_ids_processed": len(content_ids),
                    "detail_common": 0,
                    "detail_intro": 0,
                    "detail_info": 0,
                    "detail_images": 0,
                    "errors": []
                }

                # 각 content_id에 대해 상세 정보 수집
                for i, content_id in enumerate(content_ids):
                    if i % 10 == 0:
                        self.logger.info(f"{content_name}: {i+1}/{len(content_ids)} 처리 중...")

                    try:
                        # 1. detailCommon2 - 기본 상세 정보
                        detail_common = await self.collect_detail_common(content_id, content_type_id, store_raw)
                        if detail_common:
                            content_result["detail_common"] += 1
                            result["total_raw_records"] += 1

                        # 2. detailIntro2 - 소개 정보
                        detail_intro = await self.collect_detail_intro(content_id, content_type_id, store_raw)
                        if detail_intro:
                            content_result["detail_intro"] += 1
                            result["total_raw_records"] += 1

                        # 3. detailInfo2 - 추가 상세 정보
                        detail_info = await self.collect_detail_info(content_id, content_type_id, store_raw)
                        if detail_info:
                            content_result["detail_info"] += 1
                            result["total_raw_records"] += 1

                        # 4. detailImage2 - 이미지 정보
                        detail_images = await self.collect_detail_images(content_id, store_raw)
                        if detail_images:
                            content_result["detail_images"] += 1
                            result["total_raw_records"] += 1

                        # API 호출 간격 조정
                        await asyncio.sleep(0.2)

                    except Exception as e:
                        error_msg = f"{content_name} {content_id} 상세정보 수집 실패: {e}"
                        content_result["errors"].append(error_msg)
                        self.logger.error(error_msg)

                result["content_types_processed"][content_name] = content_result
                self.logger.info(f"{content_name} 상세 정보 수집 완료: {content_result}")

            except Exception as e:
                error_msg = f"{content_name} 상세 정보 수집 실패: {e}"
                result["errors"].append(error_msg)
                self.logger.error(error_msg)

        return result

    async def _get_existing_content_ids(self, content_type_id: str, limit: int) -> List[str]:
        """기존 데이터베이스에서 content_id 조회"""
        table_name = self.content_types.get(content_type_id)
        if not table_name:
            return []

        try:
            # 데이터베이스에서 기존 content_id들 조회
            query = f"SELECT content_id FROM {table_name} WHERE content_id IS NOT NULL LIMIT %s"
            result = self.db_manager.execute_query(query, (limit,))
            return [row[0] for row in result if row[0]]
        except Exception as e:
            self.logger.error(f"기존 content_id 조회 실패: {e}")
            return []

    async def collect_detail_common(self, content_id: str, content_type_id: str, store_raw: bool = True) -> Optional[Dict]:
        """detailCommon2 API 호출 - 기본 상세 정보"""
        try:
            params = {
                **self.default_params,
                "contentId": content_id
            }

            async with self.api_client:
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="detailCommon2",
                    params=params,
                    store_raw=store_raw
                )

            if response.success and response.data:
                # 새로운 변환 파이프라인을 사용하여 데이터베이스에 저장
                process_result = await self.transformation_pipeline.process_detailed_api_response(
                    api_name="detailCommon2",
                    content_id=content_id,
                    content_type_id=content_type_id,
                    raw_response=response.data,
                    raw_data_id=response.raw_data_id
                )

                if process_result.get('success'):
                    self.logger.debug(f"✅ detailCommon2 정보 처리 성공: {content_id}")
                else:
                    self.logger.warning(f"⚠️ detailCommon2 정보 처리 실패: {content_id} - {process_result.get('error')}")

                # 원본 데이터 반환 (하위 호환성)
                items = response.data.get("items", {}).get("item", [])
                if items and not isinstance(items, list):
                    items = [items]
                return items[0] if items else None

        except Exception as e:
            self.logger.error(f"detailCommon2 호출 실패 (content_id: {content_id}): {e}")

        return None

    async def collect_detail_intro(self, content_id: str, content_type_id: str, store_raw: bool = True) -> Optional[Dict]:
        """detailIntro2 API 호출 - 소개 정보"""
        try:
            params = {
                **self.default_params,
                "contentId": content_id,
                "contentTypeId": content_type_id
            }

            async with self.api_client:
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="detailIntro2",
                    params=params,
                    store_raw=store_raw
                )

            if response.success and response.data:
                # 데이터 타입 확인
                if not isinstance(response.data, dict):
                    self.logger.warning(f"⚠️ detailIntro2 응답이 dict가 아님: {type(response.data)} - {response.data}")
                    return None

                # 새로운 변환 파이프라인을 사용하여 데이터베이스에 저장
                process_result = await self.transformation_pipeline.process_detailed_api_response(
                    api_name="detailIntro2",
                    content_id=content_id,
                    content_type_id=content_type_id,
                    raw_response=response.data,
                    raw_data_id=response.raw_data_id
                )

                if process_result.get('success'):
                    self.logger.debug(f"✅ detailIntro2 정보 처리 성공: {content_id}")
                else:
                    self.logger.warning(f"⚠️ detailIntro2 정보 처리 실패: {content_id} - {process_result.get('error')}")

                # 원본 데이터 반환 (하위 호환성)
                try:
                    items_data = response.data.get("items", {})
                    # items가 빈 문자열인 경우 처리
                    if items_data == "" or not items_data:
                        self.logger.debug(f"detailIntro2 데이터 없음: {response.data}")
                        return None

                    items = items_data.get("item", [])
                    if items and not isinstance(items, list):
                        items = [items]
                    return items[0] if items else None
                except (AttributeError, TypeError) as e:
                    self.logger.warning(f"⚠️ detailIntro2 데이터 접근 실패: {e} - {response.data}")
                    return None

        except Exception as e:
            self.logger.error(f"detailIntro2 호출 실패 (content_id: {content_id}): {e}")

        return None

    async def collect_detail_info(self, content_id: str, content_type_id: str, store_raw: bool = True) -> Optional[List[Dict]]:
        """detailInfo2 API 호출 - 추가 상세 정보"""
        try:
            params = {
                **self.default_params,
                "contentId": content_id,
                "contentTypeId": content_type_id
            }

            async with self.api_client:
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="detailInfo2",
                    params=params,
                    store_raw=store_raw
                )

            if response.success and response.data:
                # 데이터 타입 확인
                if not isinstance(response.data, dict):
                    self.logger.warning(f"⚠️ detailInfo2 응답이 dict가 아님: {type(response.data)} - {response.data}")
                    return None

                # 새로운 변환 파이프라인을 사용하여 데이터베이스에 저장
                process_result = await self.transformation_pipeline.process_detailed_api_response(
                    api_name="detailInfo2",
                    content_id=content_id,
                    content_type_id=content_type_id,
                    raw_response=response.data,
                    raw_data_id=response.raw_data_id
                )

                if process_result.get('success'):
                    self.logger.debug(f"✅ detailInfo2 정보 처리 성공: {content_id}")
                else:
                    self.logger.warning(f"⚠️ detailInfo2 정보 처리 실패: {content_id} - {process_result.get('error')}")

                # 원본 데이터 반환 (하위 호환성)
                try:
                    items_data = response.data.get("items", {})
                    # items가 빈 문자열인 경우 처리
                    if items_data == "" or not items_data:
                        self.logger.debug(f"detailInfo2 데이터 없음: {response.data}")
                        return None

                    items = items_data.get("item", [])
                    if items and not isinstance(items, list):
                        items = [items]
                    return items
                except (AttributeError, TypeError) as e:
                    self.logger.warning(f"⚠️ detailInfo2 데이터 접근 실패: {e} - {response.data}")
                    return None

        except Exception as e:
            self.logger.error(f"detailInfo2 호출 실패 (content_id: {content_id}): {e}")

        return None

    async def collect_detail_images(self, content_id: str, store_raw: bool = True) -> Optional[List[Dict]]:
        """detailImage2 API 호출 - 이미지 정보"""
        try:
            params = {
                **self.default_params,
                "contentId": content_id,
                "imageYN": "Y"
            }

            async with self.api_client:
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint="detailImage2",
                    params=params,
                    store_raw=store_raw
                )

            if response.success and response.data:
                # 데이터 타입 확인
                if not isinstance(response.data, dict):
                    self.logger.warning(f"⚠️ detailImage2 응답이 dict가 아님: {type(response.data)} - {response.data}")
                    return None

                # 새로운 변환 파이프라인을 사용하여 데이터베이스에 저장
                # detailImage2는 content_type_id가 없으므로 기본값 사용
                process_result = await self.transformation_pipeline.process_detailed_api_response(
                    api_name="detailImage2",
                    content_id=content_id,
                    content_type_id="12",  # 기본값 (관광지)
                    raw_response=response.data,
                    raw_data_id=response.raw_data_id
                )

                if process_result.get('success'):
                    self.logger.debug(f"✅ detailImage2 정보 처리 성공: {content_id}")
                else:
                    self.logger.warning(f"⚠️ detailImage2 정보 처리 실패: {content_id} - {process_result.get('error')}")

                # 원본 데이터 반환 (하위 호환성)
                try:
                    items_data = response.data.get("items", {})
                    # items가 빈 문자열인 경우 처리
                    if items_data == "" or not items_data:
                        self.logger.debug(f"detailImage2 데이터 없음: {response.data}")
                        return None

                    items = items_data.get("item", [])
                    if items and not isinstance(items, list):
                        items = [items]
                    return items
                except (AttributeError, TypeError) as e:
                    self.logger.warning(f"⚠️ detailImage2 데이터 접근 실패: {e} - {response.data}")
                    return None

        except Exception as e:
            self.logger.error(f"detailImage2 호출 실패 (content_id: {content_id}): {e}")

        return None

    async def collect_detailed_info_parallel(
        self,
        content_ids: List[str],
        content_type_id: str,
        store_raw: bool = True,
        batch_size: int = 50
    ) -> Dict:
        """병렬 상세 정보 수집"""

        if not self.enable_parallel or not self.concurrent_manager:
            self.logger.warning("병렬 처리가 비활성화됨. 순차 처리로 대체")
            return await self._collect_detailed_info_sequential(content_ids, content_type_id, store_raw)

        self.logger.info(f"병렬 상세 정보 수집 시작: {len(content_ids)}개 컨텐츠")

        # 결과 초기화
        result = {
            "started_at": datetime.utcnow().isoformat(),
            "content_type_id": content_type_id,
            "total_content_ids": len(content_ids),
            "detail_common": 0,
            "detail_intro": 0,
            "detail_info": 0,
            "detail_images": 0,
            "successful_content_ids": [],
            "failed_content_ids": [],
            "errors": []
        }

        # 배치 단위로 처리
        for batch_start in range(0, len(content_ids), batch_size):
            batch_end = min(batch_start + batch_size, len(content_ids))
            batch_content_ids = content_ids[batch_start:batch_end]

            self.logger.info(f"배치 {batch_start//batch_size + 1} 처리 중: {len(batch_content_ids)}개 컨텐츠")

            # 배치별 API 작업 생성
            batch_tasks = []

            for content_id in batch_content_ids:
                # detailCommon2 작업
                batch_tasks.append(APICallTask(
                    task_id=f"detail_common_{content_id}",
                    api_provider=APIProvider.KTO,
                    endpoint="detailCommon2",
                    params={
                        **self.default_params,
                        "contentId": content_id,
                        "contentTypeId": content_type_id
                    },
                    callback=self._create_api_callback("detailCommon2", content_id, content_type_id, store_raw),
                    priority=APICallPriority.HIGH
                ))

                # detailIntro2 작업
                batch_tasks.append(APICallTask(
                    task_id=f"detail_intro_{content_id}",
                    api_provider=APIProvider.KTO,
                    endpoint="detailIntro2",
                    params={
                        **self.default_params,
                        "contentId": content_id,
                        "contentTypeId": content_type_id
                    },
                    callback=self._create_api_callback("detailIntro2", content_id, content_type_id, store_raw),
                    priority=APICallPriority.MEDIUM
                ))

                # detailInfo2 작업
                batch_tasks.append(APICallTask(
                    task_id=f"detail_info_{content_id}",
                    api_provider=APIProvider.KTO,
                    endpoint="detailInfo2",
                    params={
                        **self.default_params,
                        "contentId": content_id,
                        "contentTypeId": content_type_id
                    },
                    callback=self._create_api_callback("detailInfo2", content_id, content_type_id, store_raw),
                    priority=APICallPriority.MEDIUM
                ))

                # detailImage2 작업
                batch_tasks.append(APICallTask(
                    task_id=f"detail_images_{content_id}",
                    api_provider=APIProvider.KTO,
                    endpoint="detailImage2",
                    params={
                        **self.default_params,
                        "contentId": content_id,
                        "imageYN": "Y"
                    },
                    callback=self._create_api_callback("detailImage2", content_id, "12", store_raw),
                    priority=APICallPriority.LOW
                ))

            # 배치 병렬 실행
            batch_results = await self.concurrent_manager.execute_batch(batch_tasks)

            # 결과 집계
            content_success_count = {}

            for batch_result in batch_results:
                if batch_result['success']:
                    # 작업 ID에서 API 타입과 content_id 추출
                    task_id = batch_result['task_id']

                    # API 타입 매핑
                    if task_id.startswith('detail_common_'):
                        api_type = 'detail_common'
                        content_id = task_id.replace('detail_common_', '')
                    elif task_id.startswith('detail_intro_'):
                        api_type = 'detail_intro'
                        content_id = task_id.replace('detail_intro_', '')
                    elif task_id.startswith('detail_info_'):
                        api_type = 'detail_info'
                        content_id = task_id.replace('detail_info_', '')
                    elif task_id.startswith('detail_images_'):
                        api_type = 'detail_images'
                        content_id = task_id.replace('detail_images_', '')
                    else:
                        self.logger.warning(f"알 수 없는 작업 ID 형식: {task_id}")
                        continue

                    result[api_type] += 1

                    # 컨텐츠별 성공 카운트
                    if content_id not in content_success_count:
                        content_success_count[content_id] = 0
                    content_success_count[content_id] += 1

                else:
                    # 실패한 작업 처리
                    task_id = batch_result['task_id']

                    # API 타입 매핑
                    if task_id.startswith('detail_common_'):
                        api_type = 'detail_common'
                        content_id = task_id.replace('detail_common_', '')
                    elif task_id.startswith('detail_intro_'):
                        api_type = 'detail_intro'
                        content_id = task_id.replace('detail_intro_', '')
                    elif task_id.startswith('detail_info_'):
                        api_type = 'detail_info'
                        content_id = task_id.replace('detail_info_', '')
                    elif task_id.startswith('detail_images_'):
                        api_type = 'detail_images'
                        content_id = task_id.replace('detail_images_', '')
                    else:
                        api_type = 'unknown'
                        content_id = task_id

                    error_msg = f"{content_id} {api_type}: {batch_result['error']}"
                    result["errors"].append(error_msg)

            # 성공/실패한 컨텐츠 ID 분류
            for content_id in batch_content_ids:
                success_count = content_success_count.get(content_id, 0)
                if success_count > 0:
                    result["successful_content_ids"].append({
                        "content_id": content_id,
                        "successful_apis": success_count,
                        "total_apis": 4
                    })
                else:
                    result["failed_content_ids"].append(content_id)

            # 배치 간 대기 (API 과부하 방지)
            if batch_end < len(content_ids):
                await asyncio.sleep(1.0)

        result["completed_at"] = datetime.utcnow().isoformat()

        # 통계 요약
        successful_content_count = len(result["successful_content_ids"])
        success_rate = (successful_content_count / len(content_ids) * 100) if content_ids else 0

        self.logger.info(
            f"병렬 상세 정보 수집 완료: {successful_content_count}/{len(content_ids)} 컨텐츠 성공 "
            f"({success_rate:.1f}%)"
        )

        return result

    def _create_api_callback(self, api_name: str, content_id: str, content_type_id: str, store_raw: bool):
        """API 콜백 함수 생성"""

        async def callback(endpoint: str, params: Dict) -> Optional[Dict]:
            async with self.api_client:
                response = await self.api_client.call_api(
                    api_provider=APIProvider.KTO,
                    endpoint=endpoint,
                    params=params,
                    store_raw=store_raw
                )

            if response.success and response.data:
                # 데이터 변환 파이프라인 처리
                process_result = await self.transformation_pipeline.process_detailed_api_response(
                    api_name=api_name,
                    content_id=content_id,
                    content_type_id=content_type_id,
                    raw_response=response.data,
                    raw_data_id=response.raw_data_id
                )

                if process_result.get('success'):
                    self.logger.debug(f"✅ {api_name} 정보 처리 성공: {content_id}")
                    return response.data
                else:
                    self.logger.warning(f"⚠️ {api_name} 정보 처리 실패: {content_id} - {process_result.get('error')}")
                    return None

            return None

        return callback

    async def _collect_detailed_info_sequential(
        self,
        content_ids: List[str],
        content_type_id: str,
        store_raw: bool = True
    ) -> Dict:
        """순차 상세 정보 수집 (기존 방식)"""

        self.logger.info(f"순차 상세 정보 수집 시작: {len(content_ids)}개 컨텐츠")

        result = {
            "started_at": datetime.utcnow().isoformat(),
            "content_type_id": content_type_id,
            "total_content_ids": len(content_ids),
            "detail_common": 0,
            "detail_intro": 0,
            "detail_info": 0,
            "detail_images": 0,
            "successful_content_ids": [],
            "failed_content_ids": [],
            "errors": []
        }

        for i, content_id in enumerate(content_ids):
            if i % 10 == 0:
                self.logger.info(f"순차 처리: {i+1}/{len(content_ids)} 진행 중...")

            success_count = 0

            try:
                # detailCommon2
                detail_common = await self.collect_detail_common(content_id, content_type_id, store_raw)
                if detail_common:
                    result["detail_common"] += 1
                    success_count += 1

                # detailIntro2
                detail_intro = await self.collect_detail_intro(content_id, content_type_id, store_raw)
                if detail_intro:
                    result["detail_intro"] += 1
                    success_count += 1

                # detailInfo2
                detail_info = await self.collect_detail_info(content_id, content_type_id, store_raw)
                if detail_info:
                    result["detail_info"] += 1
                    success_count += 1

                # detailImage2
                detail_images = await self.collect_detail_images(content_id, store_raw)
                if detail_images:
                    result["detail_images"] += 1
                    success_count += 1

                if success_count > 0:
                    result["successful_content_ids"].append({
                        "content_id": content_id,
                        "successful_apis": success_count,
                        "total_apis": 4
                    })
                else:
                    result["failed_content_ids"].append(content_id)

                # API 호출 간격
                await asyncio.sleep(0.2)

            except Exception as e:
                error_msg = f"{content_id} 상세정보 수집 실패: {e}"
                result["errors"].append(error_msg)
                result["failed_content_ids"].append(content_id)
                self.logger.error(error_msg)

        result["completed_at"] = datetime.utcnow().isoformat()

        return result

    async def get_api_statistics(self) -> Dict:
        """API 호출 통계 조회"""

        try:
            stats = self.db_manager.get_api_call_statistics("KTO")
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
