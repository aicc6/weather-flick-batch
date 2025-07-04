"""
여행지 정보 동기화 배치 작업

외부 관광 API와 여행지 정보를 동기화하는 작업
실행 주기: 매일 새벽 3시
"""

from typing import Dict, Any
from app.core.logger import get_logger


async def destination_sync_task() -> Dict[str, Any]:
    """여행지 정보 동기화 작업 실행 함수"""
    logger = get_logger(__name__)
    logger.warning(
        "destination_sync_job: destinations 테이블이 존재하지 않아 작업을 건너뜁니다."
    )
    return {
        "processed_records": 0,
        "new_destinations": 0,
        "updated_destinations": 0,
        "failed_destinations": 0,
        "status": "skipped",
        "message": "destinations 테이블이 존재하지 않아 작업을 건너뜀",
    }
