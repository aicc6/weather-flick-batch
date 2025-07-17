"""
간단한 배치 API 서버 (테스트용)
"""

import os
import sys
from pathlib import Path

# 프로젝트 루트 경로를 Python 경로에 추가
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from app.api.routers import batch
from app.api.services.simple_job_manager import SimpleJobManager

# FastAPI 앱 생성
app = FastAPI(title="Batch Job API (Simple)", version="1.0.0")

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 간단한 Job Manager 인스턴스 생성
job_manager = SimpleJobManager()

# 라우터에 job_manager 주입
batch.job_manager = job_manager

# 라우터 등록
app.include_router(batch.router, prefix="/api/batch", tags=["batch"])

@app.get("/")
async def root():
    return {"message": "Batch Job API (Simple) is running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=9090)