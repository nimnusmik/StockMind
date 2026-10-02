"""
FastAPI 의존성 주입
데이터베이스 세션, Redis 클라이언트, 인증 제공
"""
from typing import Generator
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
import redis.asyncio as redis

from api.config import settings
from api.data.database import SessionLocal
from api.utils.auth import decode_access_token


# HTTP Bearer 토큰 스키마
security = HTTPBearer()


def get_db() -> Generator[Session, None, None]:
    """
    데이터베이스 세션 의존성

    Yields:
        Session: SQLAlchemy 데이터베이스 세션
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def get_redis() -> redis.Redis:
    """
    Redis 클라이언트 의존성

    Returns:
        Redis: Redis 비동기 클라이언트
    """
    redis_client = redis.from_url(
        settings.REDIS_URL,
        encoding="utf-8",
        decode_responses=True
    )
    try:
        yield redis_client
    finally:
        await redis_client.close()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> dict:
    """
    JWT 토큰으로 현재 사용자 인증

    Args:
        credentials: HTTP Bearer 토큰
        db: 데이터베이스 세션

    Returns:
        dict: 사용자 정보 (user_id, api_key 등)

    Raises:
        HTTPException: 인증 실패 시
    """
    token = credentials.credentials
    payload = decode_access_token(token)

    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="유효하지 않은 인증 토큰입니다",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload


def validate_symbol(symbol: str) -> str:
    """
    종목 심볼 유효성 검사

    Args:
        symbol: 종목 심볼 (예: AAPL)

    Returns:
        str: 대문자로 변환된 심볼

    Raises:
        HTTPException: 지원하지 않는 종목일 경우
    """
    symbol = symbol.upper()
    if symbol not in settings.SUPPORTED_SYMBOLS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"지원하지 않는 종목입니다. 지원 종목: {', '.join(settings.SUPPORTED_SYMBOLS)}"
        )
    return symbol
