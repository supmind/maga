# -*- coding: utf-8 -*-
from fastapi import Security, HTTPException, status
from fastapi.security import APIKeyHeader

from config import Settings

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

settings = Settings()
API_KEY = settings.scheduler_api_key

async def get_api_key(api_key_header: str = Security(API_KEY_HEADER)):
    """
    一个 FastAPI 依赖项，用于验证 API 密钥。
    从 'X-API-Key' 请求头中获取密钥，并与配置中的密钥进行比较。
    """
    if not api_key_header or api_key_header != API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="无效的 API Key 或未提供",
        )
    return api_key_header
