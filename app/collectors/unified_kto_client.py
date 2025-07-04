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
            "arrange": "A",
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
    ) -> Dict:
        """
        모든 KTO 데이터 수집

        Args:
            content_types: 수집할 컨텐츠 타입 목록 (None이면 전체)
            area_codes: 수집할 지역 코드 목록 (None이면 전체)
            store_raw: 원본 데이터 저장 여부
            auto_transform: 자동 변환 수행 여부

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

        collection_results["completed_at"] = datetime.utcnow().isoformat()

        self.logger.info(
            f"KTO 데이터 수집 완료: {sync_batch_id} - 총 원본 {collection_results['total_raw_records']}건, 처리 {collection_results['total_processed_records']}건"
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
                    "sync_batch_id": sync_batch_id,
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
