"""
API 인증 모듈
"""

from fastapi import HTTPException, Security
from fastapi.security import APIKeyHeader
from app.api.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """API 키 검증"""
    if not api_key:
        raise HTTPException(
            status_code=401,
            detail="API 키가 필요합니다",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    
    if api_key != settings.API_KEY:
        raise HTTPException(
            status_code=403,
            detail="유효하지 않은 API 키입니다",
        )
    
    return api_key