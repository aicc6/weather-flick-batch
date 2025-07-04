"""
한국관광공사 API 클라이언트 베이스 클래스

API 요청 한도 처리, 오류 응답 파싱, 재시도 로직 등의 공통 기능을 제공합니다.
"""

import time
import json
import logging
import xml.etree.ElementTree as ET
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any
from enum import Enum

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class APIErrorType(Enum):
    """API 오류 유형"""

    RATE_LIMIT_EXCEEDED = "LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS_ERROR"
    INVALID_PARAMETER = "INVALID_REQUEST_PARAMETER_ERROR"
    UNREGISTERED_KEY = "SERVICE_KEY_IS_NOT_REGISTERED_ERROR"
    ACCESS_DENIED = "SERVICE_ACCESS_DENIED_ERROR"
    TEMPORARILY_DISABLED = "TEMPORARILY_DISABLE_THE_SERVICEKEY_ERROR"


class KTOAPIClient(ABC):
    """한국관광공사 API 클라이언트 베이스 클래스"""

    def __init__(self, api_key: str, base_url: str = None):
        self.api_key = api_key
        self.base_url = base_url or "http://apis.data.go.kr/B551011/KorService2"
        self.logger = logging.getLogger(self.__class__.__name__)

        # API 호출 제한 설정
        self.call_delay = 1.0  # 1초 간격 (보수적 설정)
        self.last_call_time = 0
        self.max_daily_requests = 1000  # 안전한 일일 요청 한도
        self.daily_request_count = 0
        self.rate_limit_count = 0

        # HTTP 세션 설정
        self._setup_session()

    def _setup_session(self):
        """HTTP 세션 초기화"""
        self.session = requests.Session()

        # SSL 설정
        self.session.verify = True
        self.session.headers.update(
            {"User-Agent": "Mozilla/5.0 (WeatherFlick/1.0) AppleWebKit/537.36"}
        )

        # 재시도 전략 설정
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            backoff_factor=1,
            respect_retry_after_header=True,
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _wait_for_rate_limit(self) -> bool:
        """API 호출 제한 대기"""
        # 일일 요청 한도 확인
        if self.daily_request_count >= self.max_daily_requests:
            self.logger.warning(f"⚠️ 일일 요청 한도 ({self.max_daily_requests}건) 도달")
            return False

        # 요청 간격 제어
        current_time = time.time()
        elapsed = current_time - self.last_call_time

        if elapsed < self.call_delay:
            time.sleep(self.call_delay - elapsed)

        self.last_call_time = time.time()
        self.daily_request_count += 1

        # 진행 상황 로깅 (100건마다)
        if self.daily_request_count % 100 == 0:
            self.logger.info(
                f"📊 API 요청 진행: {self.daily_request_count}/{self.max_daily_requests}"
            )

        return True

    def _handle_xml_error_response(self, xml_response: str) -> Optional[Dict]:
        """XML 오류 응답 처리"""
        try:
            root = ET.fromstring(xml_response)

            # 오류 정보 추출
            err_msg = root.find(".//errMsg")
            return_auth_msg = root.find(".//returnAuthMsg")
            return_reason_code = root.find(".//returnReasonCode")

            error_message = err_msg.text if err_msg is not None else "Unknown error"
            auth_message = return_auth_msg.text if return_auth_msg is not None else ""
            reason_code = (
                return_reason_code.text if return_reason_code is not None else ""
            )

            # API 오류 유형별 처리
            if APIErrorType.RATE_LIMIT_EXCEEDED.value in auth_message:
                self.logger.warning("⚠️ API 요청 한도 초과 - 대기 후 재시도")
                self._handle_rate_limit_exceeded()
                return None

            elif APIErrorType.INVALID_PARAMETER.value in auth_message:
                self.logger.error("❌ 잘못된 요청 파라미터")
                return None

            elif APIErrorType.UNREGISTERED_KEY.value in auth_message:
                self.logger.error("❌ 등록되지 않은 서비스 키")
                self._log_api_key_help()
                return None

            elif APIErrorType.ACCESS_DENIED.value in auth_message:
                self.logger.error("❌ API 접근 거부")
                self._log_api_key_help()
                return None

            elif APIErrorType.TEMPORARILY_DISABLED.value in auth_message:
                self.logger.error("❌ 서비스 키 일시 비활성화")
                return None

            else:
                self.logger.error(
                    f"❌ API 오류 - {error_message}: {auth_message} (코드: {reason_code})"
                )
                return None

        except ET.ParseError as e:
            self.logger.error(f"XML 파싱 오류: {e}")
            return None
        except Exception as e:
            self.logger.error(f"XML 오류 응답 처리 실패: {e}")
            return None

    def _handle_rate_limit_exceeded(self):
        """API 요청 한도 초과 시 처리"""
        # 점진적 백오프 적용
        base_delay = 60  # 기본 1분 대기
        self.rate_limit_count += 1

        if self.rate_limit_count <= 3:
            delay = base_delay
        else:
            # 3번 이상 초과 시 더 긴 대기 (최대 5분)
            delay = min(300, base_delay + (self.rate_limit_count - 3) * 60)

        self.logger.info(f"🕐 API 한도 초과로 {delay}초 대기합니다...")
        time.sleep(delay)

    def _log_api_key_help(self):
        """API 키 관련 도움말 로깅"""
        self.logger.info("해결 방법:")
        self.logger.info(
            "1. https://data.go.kr 에서 '한국관광공사_국문 관광정보 서비스_GW' 검색"
        )
        self.logger.info("2. 활용신청 후 승인된 인증키를 KTO_API_KEY 환경변수에 설정")
        self.logger.info("3. 신청한 서비스가 승인될 때까지 대기 (보통 1-2일 소요)")

    def _is_api_key_valid(self) -> bool:
        """API 키 유효성 검사"""
        if (
            not self.api_key
            or self.api_key.strip() == ""
            or "your_kto_api_key_here" in self.api_key
        ):
            self.logger.warning("한국관광공사 API 키가 설정되지 않았습니다.")
            self._log_api_key_help()
            return False
        return True

    def make_request(
        self,
        endpoint: str,
        params: Dict[str, Any],
        retry_count: int = 0,
        max_retries: int = 3,
    ) -> Optional[Dict]:
        """API 요청 수행 (재시도 로직 포함)"""

        if not self._is_api_key_valid():
            return None

        if not self._wait_for_rate_limit():
            self.logger.warning("일일 요청 한도 초과로 API 요청을 중단합니다.")
            return None

        # 기본 파라미터 설정
        default_params = {
            "serviceKey": self.api_key,
            "MobileOS": "ETC",
            "MobileApp": "WeatherFlick",
            "_type": "json",
            "numOfRows": params.get("numOfRows", 10),
            "pageNo": params.get("pageNo", 1),
        }
        params.update(default_params)

        url = f"{self.base_url}/{endpoint}"

        try:
            self.logger.debug(f"API 요청: {endpoint}")
            response = self.session.get(url, params=params, timeout=30)

            if response.status_code != 200:
                self.logger.error(f"HTTP 오류 {response.status_code}")
                return None

            response_text = response.text.strip()
            if not response_text:
                self.logger.warning(f"빈 응답: {endpoint}")
                return None

            # XML 오류 응답 체크
            if response_text.startswith("<OpenAPI_ServiceResponse>"):
                return self._handle_xml_error_response(response_text)

            # JSON 응답 처리
            try:
                data = response.json()
            except json.JSONDecodeError as e:
                self.logger.error(f"JSON 파싱 실패: {e}")
                return None

            # API 응답 검증
            if "response" not in data:
                self.logger.warning(f"잘못된 응답 형식: {endpoint}")
                return None

            response_data = data["response"]
            header = response_data.get("header", {})
            result_code = header.get("resultCode", "")

            if result_code != "0000":
                error_msg = header.get("resultMsg", "Unknown error")
                self.logger.error(f"API 오류 [{result_code}]: {error_msg}")
                return None

            body = response_data.get("body")
            if not body:
                self.logger.debug(f"응답 body가 비어있음: {endpoint}")
                return None

            # 성공 시 rate limit 카운터 초기화
            self.rate_limit_count = 0
            return body

        except requests.exceptions.Timeout:
            self.logger.error(f"API 호출 타임아웃: {endpoint}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f"API 호출 오류: {endpoint}, {str(e)}")
        except Exception as e:
            self.logger.error(f"예상치 못한 오류: {endpoint}, {str(e)}")

        # 재시도 로직
        if retry_count < max_retries:
            retry_delay = min(30, 5 * (retry_count + 1))  # 점진적 증가
            self.logger.info(
                f"🔄 {retry_delay}초 후 재시도 ({retry_count + 1}/{max_retries})"
            )
            time.sleep(retry_delay)
            return self.make_request(endpoint, params, retry_count + 1, max_retries)

        return None

    def fetch_paginated_data(self, endpoint: str, params: Dict[str, Any]) -> List[Dict]:
        """페이징 처리된 API 데이터 수집"""
        all_items = []
        page_no = 1
        num_of_rows = params.get("numOfRows", 100)

        while True:
            params["pageNo"] = page_no
            params["numOfRows"] = num_of_rows

            body = self.make_request(endpoint, params.copy())
            if not body:
                break

            total_count = body.get("totalCount", 0)
            if total_count == 0:
                self.logger.info(f"수집할 데이터가 없습니다: {endpoint}")
                break

            items_data = body.get("items", {})
            if not items_data or "item" not in items_data:
                break

            page_items = items_data["item"]
            if isinstance(page_items, dict):
                page_items = [page_items]

            all_items.extend(page_items)

            self.logger.info(
                f"[{endpoint}] 페이지 {page_no}: {len(page_items)}개 수집 "
                f"(전체: {len(all_items)}/{total_count})"
            )

            # 마지막 페이지 확인
            if len(all_items) >= total_count or len(page_items) < num_of_rows:
                break

            page_no += 1

        self.logger.info(f"[{endpoint}] 총 {len(all_items)}개 데이터 수집 완료")
        return all_items

    def fetch_single_item(
        self, endpoint: str, params: Dict[str, Any]
    ) -> Optional[Dict]:
        """단일 아이템 조회"""
        body = self.make_request(endpoint, params)
        if not body:
            return None

        items = body.get("items", {}).get("item")
        if not items:
            return None

        if isinstance(items, list):
            return items[0] if items else None
        return items

    @abstractmethod
    def get_request_stats(self) -> Dict[str, Any]:
        """API 요청 통계 반환 (하위 클래스에서 구현)"""
        pass

    def reset_daily_counter(self):
        """일일 요청 카운터 초기화"""
        self.daily_request_count = 0
        self.rate_limit_count = 0
        self.logger.info("일일 API 요청 카운터가 초기화되었습니다.")
