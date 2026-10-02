"""
JWT 인증 유틸리티
토큰 생성 및 검증
"""
from datetime import datetime, timedelta
from typing import Optional, Dict
from jose import JWTError, jwt
from passlib.context import CryptContext

from api.config import settings


# 비밀번호 해싱
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    JWT 액세스 토큰 생성

    Args:
        data: 토큰에 포함할 데이터 (예: {"sub": "user_id"})
        expires_delta: 만료 시간 (None이면 기본값 사용)

    Returns:
        str: JWT 토큰
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})

    encoded_jwt = jwt.encode(
        to_encode,
        settings.SECRET_KEY,
        algorithm=settings.ALGORITHM
    )

    return encoded_jwt


def decode_access_token(token: str) -> Optional[Dict]:
    """
    JWT 토큰 디코딩 및 검증

    Args:
        token: JWT 토큰

    Returns:
        Optional[Dict]: 토큰 payload 또는 None (유효하지 않을 경우)
    """
    try:
        payload = jwt.decode(
            token,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM]
        )
        return payload
    except JWTError:
        return None


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    비밀번호 검증

    Args:
        plain_password: 평문 비밀번호
        hashed_password: 해시된 비밀번호

    Returns:
        bool: 일치 여부
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """
    비밀번호 해싱

    Args:
        password: 평문 비밀번호

    Returns:
        str: 해시된 비밀번호
    """
    return pwd_context.hash(password)


def create_api_key() -> str:
    """
    API 키 생성

    Returns:
        str: 생성된 API 키 (UUID 형식)
    """
    import uuid
    return str(uuid.uuid4())


def hash_api_key(api_key: str) -> str:
    """
    API 키 해싱 (데이터베이스 저장용)

    Args:
        api_key: API 키

    Returns:
        str: 해시된 API 키
    """
    return get_password_hash(api_key)
