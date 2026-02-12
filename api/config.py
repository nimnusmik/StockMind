"""
API 설정 파일
환경 변수를 통해 데이터베이스, Redis, JWT 설정 관리
"""
import os
from typing import List
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """애플리케이션 설정"""

    # API 기본 설정
    API_V1_PREFIX: str = "/api/v1"
    PROJECT_NAME: str = "StockMind Trading Signal API"
    VERSION: str = "1.0.0"
    DESCRIPTION: str = "AI 기반 주식 매매 신호 및 감성 분석 API"

    # CORS 설정
    BACKEND_CORS_ORIGINS: List[str] = ["http://localhost:3000", "http://localhost:8000"]

    # 데이터베이스 설정
    DATABASE_URL: str = os.getenv(
        "DATABASE_URL",
        "postgresql://user:password@localhost:5432/stockmind"
    )

    # Redis 설정
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    # JWT 인증 설정
    SECRET_KEY: str = os.getenv("API_SECRET_KEY", "your-secret-key-change-in-production")
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60 * 24  # 24시간

    # ML 모델 설정
    MODEL_DIR: str = os.getenv("MODEL_DIR", "/app/api/models")
    FEATURES_DIR: str = os.getenv("FEATURES_DIR", "/app/news/features")
    METADATA_DIR: str = os.getenv("METADATA_DIR", "/app/news/metadata")

    # 지원 종목
    SUPPORTED_SYMBOLS: List[str] = [
        "AAPL", "GOOG", "META", "TSLA",
        "MSFT", "AMZN", "NVDA", "NFLX"
    ]

    # 캐시 TTL 설정 (초 단위)
    CACHE_TTL_SENTIMENT_COMMUNITY: int = 3600  # 1시간
    CACHE_TTL_SENTIMENT_NEWS: int = 86400  # 24시간
    CACHE_TTL_PREDICTION: int = 300  # 5분
    CACHE_TTL_SIGNAL: int = 300  # 5분
    CACHE_TTL_BUZZ: int = 900  # 15분

    # 감성 분석 가중치
    NEWS_SENTIMENT_WEIGHT: float = 0.6
    COMMUNITY_SENTIMENT_WEIGHT: float = 0.4

    # 매매 신호 임계값
    SIGNAL_BUY_PRICE_THRESHOLD: float = 2.0  # +2%
    SIGNAL_SELL_PRICE_THRESHOLD: float = -2.0  # -2%
    SIGNAL_BUY_SENTIMENT_THRESHOLD: float = 0.3
    SIGNAL_SELL_SENTIMENT_THRESHOLD: float = -0.3

    class Config:
        case_sensitive = True
        env_file = ".env"


# 싱글톤 인스턴스
settings = Settings()
