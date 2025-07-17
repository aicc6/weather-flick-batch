"""
배치 작업 로그 실시간 스트리밍을 위한 WebSocket 라우터
"""

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query, Header, HTTPException
from typing import Optional, Dict, Set
import asyncio
import logging
import json
from datetime import datetime

from app.api.config import settings
from app.api.services.job_manager_db import JobManagerDB

logger = logging.getLogger(__name__)
router = APIRouter()

# WebSocket 연결 관리
class ConnectionManager:
    def __init__(self):
        # job_id -> Set[WebSocket]
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self.lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket, job_id: str):
        await websocket.accept()
        async with self.lock:
            if job_id not in self.active_connections:
                self.active_connections[job_id] = set()
            self.active_connections[job_id].add(websocket)
            logger.info(f"WebSocket 연결 추가: job_id={job_id}, 현재 연결 수={len(self.active_connections[job_id])}")

    async def disconnect(self, websocket: WebSocket, job_id: str):
        async with self.lock:
            if job_id in self.active_connections:
                self.active_connections[job_id].discard(websocket)
                if not self.active_connections[job_id]:
                    del self.active_connections[job_id]
                logger.info(f"WebSocket 연결 제거: job_id={job_id}")

    async def send_log(self, job_id: str, log_data: dict):
        """특정 작업의 모든 연결에 로그 전송"""
        async with self.lock:
            if job_id in self.active_connections:
                disconnected = set()
                for websocket in self.active_connections[job_id]:
                    try:
                        await websocket.send_json(log_data)
                    except Exception as e:
                        logger.error(f"WebSocket 전송 실패: {e}")
                        disconnected.add(websocket)
                
                # 실패한 연결 제거
                for websocket in disconnected:
                    self.active_connections[job_id].discard(websocket)

    async def broadcast_job_update(self, job_id: str, update_data: dict):
        """작업 상태 업데이트 브로드캐스트"""
        await self.send_log(job_id, {
            "type": "job_update",
            "timestamp": datetime.utcnow().isoformat(),
            "data": update_data
        })

manager = ConnectionManager()

@router.websocket("/jobs/{job_id}/logs/stream")
async def websocket_endpoint(
    websocket: WebSocket,
    job_id: str,
    api_key: Optional[str] = Query(None)
):
    """
    작업 로그 실시간 스트리밍 WebSocket 엔드포인트
    
    연결 URL: ws://localhost:9090/api/ws/jobs/{job_id}/logs/stream?api_key={api_key}
    """
    # API 키 검증
    if api_key != settings.API_KEY:
        await websocket.close(code=4001, reason="Invalid API key")
        return

    await manager.connect(websocket, job_id)
    
    try:
        # 기존 로그 전송
        job_manager = JobManagerDB()
        logs = await job_manager.get_job_logs(job_id, page=1, size=100)
        
        if logs:
            # 기존 로그를 역순으로 전송 (오래된 것부터)
            for log in reversed(logs.logs):
                await websocket.send_json({
                    "type": "log",
                    "timestamp": log.timestamp.isoformat(),
                    "level": log.level.value,
                    "message": log.message,
                    "details": log.details,
                    "historical": True
                })

        # 클라이언트 메시지 대기 (연결 유지)
        while True:
            # ping/pong을 통한 연결 유지
            try:
                message = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                if message == "ping":
                    await websocket.send_text("pong")
            except asyncio.TimeoutError:
                # 타임아웃 시 ping 전송
                await websocket.send_json({"type": "ping"})
            except WebSocketDisconnect:
                break
                
    except WebSocketDisconnect:
        logger.info(f"WebSocket 연결 종료: job_id={job_id}")
    except Exception as e:
        logger.error(f"WebSocket 오류: {e}")
    finally:
        await manager.disconnect(websocket, job_id)

# 로그 전송을 위한 헬퍼 함수 (JobManagerDB에서 사용)
async def send_realtime_log(job_id: str, level: str, message: str, details: Optional[dict] = None):
    """실시간 로그 전송"""
    log_data = {
        "type": "log",
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "message": message,
        "details": details,
        "historical": False
    }
    await manager.send_log(job_id, log_data)

async def send_job_progress_update(job_id: str, progress: float, current_step: Optional[str] = None):
    """작업 진행률 업데이트 전송"""
    update_data = {
        "progress": progress,
        "current_step": current_step,
        "timestamp": datetime.utcnow().isoformat()
    }
    await manager.broadcast_job_update(job_id, update_data)

async def send_job_status_update(job_id: str, status: str, error_message: Optional[str] = None):
    """작업 상태 업데이트 전송"""
    update_data = {
        "status": status,
        "error_message": error_message,
        "timestamp": datetime.utcnow().isoformat()
    }
    await manager.broadcast_job_update(job_id, update_data)