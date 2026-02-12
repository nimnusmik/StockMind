"""
커뮤니티 버즈 지표 API 라우터
댓글 볼륨, 트렌드, 이상 활동 탐지
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
import redis.asyncio as redis

from api.dependencies import get_db, get_redis, validate_symbol
from api.schemas.prediction import BuzzIndicatorsResponse
from api.data.repositories.comment_repo import CommentRepository
from api.services.cache_service import CacheService


router = APIRouter(prefix="/buzz", tags=["Community Buzz"])


@router.get("/{symbol}", response_model=BuzzIndicatorsResponse)
async def get_buzz_indicators(
    symbol: str = Depends(validate_symbol),
    db: Session = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis)
):
    """
    커뮤니티 버즈 지표 조회

    **측정 항목:**
    - 현재 댓글 볼륨 (24시간)
    - 시간별 변화율 (Hour-over-Hour)
    - 일별 변화율 (Day-over-Day)
    - 감성 속도 (Sentiment Velocity)
    - 이상 활동 탐지
    - 버즈 점수 (0 ~ 100)

    **활동 수준:**
    - LOW: 평균 이하
    - NORMAL: 평균 수준
    - HIGH: 평균 이상
    - VERY_HIGH: 이상 활동 감지

    **사용처:**
    - 개인 투자자 심리 모니터링
    - 모멘텀 조기 발견
    - 이상 거래 감지

    **캐싱:** 15분 TTL
    """
    cache_service = CacheService(redis_client)

    # 캐시 확인
    cached = await cache_service.get_buzz(symbol)
    if cached:
        cached['timestamp'] = datetime.fromisoformat(cached['timestamp'])
        return BuzzIndicatorsResponse(**cached)

    comment_repo = CommentRepository(db)

    # 현재 볼륨 (24시간)
    current_volume = comment_repo.get_comment_count(symbol, hours=24)

    # 이전 시간 볼륨 (24-25시간 전)
    prev_hour_volume = comment_repo.get_comment_count(symbol, hours=25) - current_volume

    # 이전 날 볼륨 (24-48시간 전)
    prev_day_volume = comment_repo.get_comment_count(symbol, hours=48) - current_volume

    # 시간별 변화율
    hour_over_hour_change = (
        ((current_volume - prev_hour_volume) / prev_hour_volume * 100)
        if prev_hour_volume > 0 else 0.0
    )

    # 일별 변화율
    day_over_day_change = (
        ((current_volume - prev_day_volume) / prev_day_volume * 100)
        if prev_day_volume > 0 else 0.0
    )

    # 감성 속도 계산 (간단한 구현)
    # 실제로는 시간별 감성 변화를 측정해야 함
    sentiment_velocity = 0.0  # TODO: 구현

    # 이상 활동 탐지
    # 볼륨이 평균 대비 200% 이상이면 이상 활동
    unusual_activity = abs(hour_over_hour_change) > 200

    # 활동 수준 결정
    if unusual_activity:
        activity_level = "VERY_HIGH"
    elif current_volume > prev_day_volume * 1.5:
        activity_level = "HIGH"
    elif current_volume > prev_day_volume * 0.8:
        activity_level = "NORMAL"
    else:
        activity_level = "LOW"

    # 버즈 점수 (0 ~ 100)
    # 볼륨, 변화율, 활동 수준 종합
    buzz_score = min(100.0, (
        (current_volume / 10) +  # 볼륨 기여도
        abs(hour_over_hour_change) / 2 +  # 변화율 기여도
        (30 if unusual_activity else 0)  # 이상 활동 보너스
    ))

    # 시간별 볼륨 (최근 24시간)
    hourly_volume_data = comment_repo.get_hourly_comment_volume(symbol, hours=24)
    hourly_volume = {
        item['hour'].strftime("%H:%M"): item['count']
        for item in hourly_volume_data
    }

    response_data = {
        "symbol": symbol,
        "current_volume": current_volume,
        "hour_over_hour_change": round(hour_over_hour_change, 2),
        "day_over_day_change": round(day_over_day_change, 2),
        "sentiment_velocity": round(sentiment_velocity, 3),
        "unusual_activity": unusual_activity,
        "activity_level": activity_level,
        "buzz_score": round(buzz_score, 1),
        "hourly_volume": hourly_volume,
        "timestamp": datetime.utcnow()
    }

    # 캐싱
    await cache_service.set_buzz(symbol, response_data)

    return BuzzIndicatorsResponse(**response_data)


@router.get("/{symbol}/volume-trend")
def get_volume_trend(
    symbol: str = Depends(validate_symbol),
    hours: int = 24,
    db: Session = Depends(get_db)
):
    """
    댓글 볼륨 트렌드 조회

    **파라미터:**
    - hours: 조회 시간 범위 (기본 24시간)

    **반환:**
    - 시간별 댓글 볼륨
    - 평균 볼륨
    - 최대/최소 볼륨

    **사용처:**
    - 볼륨 패턴 분석
    - 활동 시간대 파악
    """
    comment_repo = CommentRepository(db)
    hourly_data = comment_repo.get_hourly_comment_volume(symbol, hours)

    if not hourly_data:
        return {
            "symbol": symbol,
            "trend": [],
            "average_volume": 0,
            "max_volume": 0,
            "min_volume": 0
        }

    volumes = [item['count'] for item in hourly_data]

    return {
        "symbol": symbol,
        "trend": [
            {
                "hour": item['hour'].isoformat(),
                "count": item['count']
            }
            for item in hourly_data
        ],
        "average_volume": round(sum(volumes) / len(volumes), 2),
        "max_volume": max(volumes),
        "min_volume": min(volumes),
        "total_hours": len(hourly_data)
    }


@router.get("/{symbol}/activity-comparison")
def compare_activity(
    symbol: str = Depends(validate_symbol),
    db: Session = Depends(get_db)
):
    """
    다른 종목 대비 활동 비교

    **반환:**
    - 종목별 24시간 댓글 볼륨
    - 상대적 순위
    - 평균 대비 비율

    **사용처:**
    - 종목 인기도 비교
    - 관심 종목 발견
    """
    from api.config import settings

    comment_repo = CommentRepository(db)
    volumes = {}

    for stock in settings.SUPPORTED_SYMBOLS:
        volumes[stock] = comment_repo.get_comment_count(stock, hours=24)

    # 순위 계산
    sorted_volumes = sorted(volumes.items(), key=lambda x: x[1], reverse=True)
    rank = next((i + 1 for i, (s, _) in enumerate(sorted_volumes) if s == symbol), 0)

    # 평균 계산
    avg_volume = sum(volumes.values()) / len(volumes)
    relative_ratio = (volumes[symbol] / avg_volume * 100) if avg_volume > 0 else 0

    return {
        "symbol": symbol,
        "volume_24h": volumes[symbol],
        "rank": rank,
        "total_symbols": len(volumes),
        "average_volume": round(avg_volume, 2),
        "relative_ratio": round(relative_ratio, 2),
        "all_volumes": sorted_volumes
    }
