"""
매매 신호 API 라우터
BUY/HOLD/SELL 신호 생성 및 이력 조회
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import redis.asyncio as redis

from api.dependencies import get_db, get_redis, validate_symbol
from api.schemas.signal import (
    TradingSignalResponse,
    SignalHistoryResponse,
    SignalAccuracyResponse
)
from api.services.sentiment_service import SentimentService
from api.services.signal_service import SignalService
from api.services.ml_service import ml_service
from api.services.cache_service import CacheService
from api.data.repositories.signal_repo import SignalRepository


router = APIRouter(prefix="/signals", tags=["Trading Signals"])


@router.get("/{symbol}", response_model=TradingSignalResponse)
async def get_trading_signal(
    symbol: str = Depends(validate_symbol),
    db: Session = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis)
):
    """
    종목의 매매 신호 조회 (핵심 엔드포인트)

    **알고리즘:**
    1. 가격 예측 (RandomForest ML 모델)
    2. 감성 분석 (뉴스 60% + 커뮤니티 40%)
    3. 신호 생성 (BUY/HOLD/SELL)
    4. 신뢰도 및 근거 제공

    **신호 결정 로직:**
    - BUY: 예측 상승 > +2% AND 감성 > 0.3
    - SELL: 예측 하락 < -2% AND 감성 < -0.3
    - HOLD: 그 외

    **캐싱:** 5분 TTL
    """
    # 캐시 서비스 초기화
    cache_service = CacheService(redis_client)

    # 1. 캐시 확인
    cached_signal = await cache_service.get_signal(symbol)
    if cached_signal:
        cached_signal['timestamp'] = datetime.fromisoformat(cached_signal['timestamp'])
        return TradingSignalResponse(**cached_signal)

    # 2. 가격 예측
    prediction = ml_service.predict_price(symbol)
    if prediction is None:
        raise HTTPException(
            status_code=404,
            detail=f"모델 또는 특징 데이터를 찾을 수 없습니다: {symbol}"
        )

    predicted_price = prediction['predicted_price']
    model_confidence = prediction['model_confidence']

    # 현재 가격 (예측 가격의 98%로 가정, 실제로는 API에서 가져와야 함)
    current_price = predicted_price * 0.98

    # 3. 감성 분석
    sentiment_service = SentimentService(db)
    composite_sentiment = sentiment_service.calculate_composite_sentiment(symbol)

    sentiment_score = composite_sentiment['composite_score']
    news_sentiment = composite_sentiment['news_sentiment']
    community_sentiment = composite_sentiment['community_sentiment']

    # 4. 신호 생성
    signal_service = SignalService(db)
    signal = signal_service.generate_trading_signal(
        symbol=symbol,
        predicted_price=predicted_price,
        current_price=current_price,
        sentiment_score=sentiment_score,
        news_sentiment=news_sentiment,
        community_sentiment=community_sentiment
    )

    # 5. 캐싱 (5분)
    await cache_service.set_signal(symbol, signal)

    return TradingSignalResponse(**signal)


@router.get("/{symbol}/history", response_model=SignalHistoryResponse)
def get_signal_history(
    symbol: str = Depends(validate_symbol),
    days: int = Query(30, ge=1, le=365, description="조회 기간 (일)"),
    db: Session = Depends(get_db)
):
    """
    종목의 신호 이력 조회

    **파라미터:**
    - days: 조회 기간 (1 ~ 365일)

    **사용처:** 백테스팅, 신호 패턴 분석
    """
    signal_repo = SignalRepository(db)
    signals = signal_repo.get_recent_signals(symbol, days)

    return SignalHistoryResponse(
        symbol=symbol,
        signals=signals,
        total_count=len(signals)
    )


@router.get("/{symbol}/accuracy", response_model=SignalAccuracyResponse)
def get_signal_accuracy(
    symbol: str = Depends(validate_symbol),
    days: int = Query(30, ge=1, le=365, description="분석 기간 (일)"),
    db: Session = Depends(get_db)
):
    """
    신호 정확도 통계 조회

    **계산 방식:**
    - BUY 신호: 실제로 상승했는지
    - SELL 신호: 실제로 하락했는지
    - HOLD 신호: 변화가 ±2% 미만인지

    **사용처:** 모델 성능 모니터링, 신뢰도 평가
    """
    signal_repo = SignalRepository(db)
    accuracy = signal_repo.get_signal_accuracy(symbol, days)

    return SignalAccuracyResponse(
        symbol=symbol,
        period_days=days,
        **accuracy
    )


@router.delete("/{symbol}/cache")
async def invalidate_signal_cache(
    symbol: str = Depends(validate_symbol),
    redis_client: redis.Redis = Depends(get_redis)
):
    """
    종목의 신호 캐시 무효화

    **사용처:** 수동 캐시 갱신, 디버깅

    **주의:** 다음 요청 시 재계산 수행 (느림)
    """
    cache_service = CacheService(redis_client)
    await cache_service.delete(cache_service.signal_key(symbol))

    return {
        "message": f"{symbol} 신호 캐시가 무효화되었습니다",
        "symbol": symbol
    }
