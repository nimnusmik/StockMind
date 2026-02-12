"""
감성 분석 API 라우터
커뮤니티, 뉴스, 통합 감성 조회
"""
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from datetime import datetime
import redis.asyncio as redis

from api.dependencies import get_db, get_redis, validate_symbol
from api.schemas.sentiment import (
    CommunitySentimentResponse,
    NewsSentimentResponse,
    CombinedSentimentResponse
)
from api.services.sentiment_service import SentimentService
from api.services.cache_service import CacheService


router = APIRouter(prefix="/sentiment", tags=["Sentiment Analysis"])


@router.get("/{symbol}/community", response_model=CommunitySentimentResponse)
async def get_community_sentiment(
    symbol: str = Depends(validate_symbol),
    timerange: str = Query("24h", regex="^(1h|4h|24h|7d)$", description="시간 범위"),
    db: Session = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis)
):
    """
    커뮤니티 댓글 감성 분석

    **데이터 소스:** Yahoo Finance 커뮤니티 댓글

    **시간 범위:**
    - 1h: 최근 1시간
    - 4h: 최근 4시간
    - 24h: 최근 24시간 (기본값)
    - 7d: 최근 7일

    **감성 분석 방식:**
    - 규칙 기반 키워드 매칭
    - 긍정/부정/중립 비율 계산
    - 트렌딩 키워드 추출

    **캐싱:** 1시간 TTL
    """
    cache_service = CacheService(redis_client)

    # 캐시 확인
    cached = await cache_service.get_sentiment_community(symbol, timerange)
    if cached:
        cached['timestamp'] = datetime.fromisoformat(cached['timestamp'])
        return CommunitySentimentResponse(**cached)

    # 시간 범위 변환
    hours_map = {"1h": 1, "4h": 4, "24h": 24, "7d": 168}
    hours = hours_map[timerange]

    # 감성 계산
    sentiment_service = SentimentService(db)
    result = sentiment_service.calculate_community_sentiment(symbol, hours)

    response_data = {
        "symbol": symbol,
        "timerange": timerange,
        "timestamp": datetime.utcnow(),
        **result
    }

    # 캐싱
    await cache_service.set_sentiment_community(symbol, timerange, response_data)

    return CommunitySentimentResponse(**response_data)


@router.get("/{symbol}/news", response_model=NewsSentimentResponse)
async def get_news_sentiment(
    symbol: str = Depends(validate_symbol),
    db: Session = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis)
):
    """
    뉴스 감성 분석

    **데이터 소스:** 금융 뉴스 (FinBERT 분석)

    **특징:**
    - FinBERT로 정확한 금융 감성 분석
    - DistilBART 요약 임베딩 (384차원)
    - KeyBERT 키워드 추출

    **캐싱:** 24시간 TTL
    """
    cache_service = CacheService(redis_client)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # 캐시 확인
    cached = await cache_service.get_sentiment_news(symbol, today)
    if cached:
        cached['timestamp'] = datetime.fromisoformat(cached['timestamp'])
        return NewsSentimentResponse(**cached)

    # 감성 계산
    sentiment_service = SentimentService(db)
    result = sentiment_service.calculate_news_sentiment(symbol)

    if result is None:
        # 뉴스 데이터가 없을 경우 기본값
        result = {
            "sentiment_score": 0.0,
            "article_count": 0,
            "positive_ratio": 0.0,
            "negative_ratio": 0.0,
            "neutral_ratio": 1.0,
            "date": today
        }

    response_data = {
        "symbol": symbol,
        "timestamp": datetime.utcnow(),
        "top_keywords": [],  # TODO: KeyBERT 키워드 추출
        **result
    }

    # 캐싱
    await cache_service.set_sentiment_news(symbol, today, response_data)

    return NewsSentimentResponse(**response_data)


@router.get("/{symbol}/combined", response_model=CombinedSentimentResponse)
async def get_combined_sentiment(
    symbol: str = Depends(validate_symbol),
    db: Session = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis)
):
    """
    통합 감성 분석 (뉴스 + 커뮤니티 융합)

    **이중 감성 융합:**
    - 뉴스 감성: 60% (기관 투자자 관점)
    - 커뮤니티 감성: 40% (개인 투자자 관점)

    **신뢰도 계산:**
    - 데이터 볼륨 기반
    - 뉴스 20개 + 댓글 200개 = 100% 신뢰도

    **장점:**
    - 시장 참여자 전체 감성 파악
    - 기관과 개인의 감성 차이 확인
    - 더 균형 잡힌 투자 결정

    **캐싱:** 1시간 TTL (동적 데이터)
    """
    cache_service = CacheService(redis_client)

    # 캐시 확인
    cached = await cache_service.get(cache_service.sentiment_combined_key(symbol))
    if cached:
        cached['timestamp'] = datetime.fromisoformat(cached['timestamp'])
        return CombinedSentimentResponse(**cached)

    # 통합 감성 계산
    sentiment_service = SentimentService(db)
    result = sentiment_service.calculate_composite_sentiment(symbol)

    response_data = {
        "symbol": symbol,
        "timestamp": datetime.utcnow(),
        **result
    }

    # 캐싱
    await cache_service.set(
        cache_service.sentiment_combined_key(symbol),
        response_data,
        ttl=3600  # 1시간
    )

    return CombinedSentimentResponse(**response_data)
