"""
현재 주가 조회 서비스
TwelveData API를 통해 실시간 현재가를 가져오고 Redis에 1분간 캐싱
"""
import httpx
from typing import Optional

from api.config import settings
from api.utils.logger import api_logger

# TwelveData 현재가 엔드포인트
_PRICE_URL = "https://api.twelvedata.com/price"

# Redis 캐시 키 prefix
_CACHE_KEY_PREFIX = "current_price:"

# 캐시 TTL (초)
_CACHE_TTL = 60


async def get_current_price(symbol: str, cache_service) -> Optional[float]:
    """
    종목의 현재가를 반환한다.
    1. Redis 캐시에서 먼저 조회 (TTL 60초)
    2. 없으면 TwelveData API 호출
    3. API 키가 없거나 호출 실패 시 None 반환

    Args:
        symbol: 종목 심볼 (예: "AAPL")
        cache_service: CacheService 인스턴스

    Returns:
        float: 현재가 (USD), 조회 실패 시 None
    """
    cache_key = f"{_CACHE_KEY_PREFIX}{symbol}"

    # 캐시 확인
    cached = await cache_service.get(cache_key)
    if cached is not None:
        return float(cached)

    # API 키 미설정 시 조기 반환
    if not settings.TWELVEDATA_API_KEY:
        api_logger.warning(f"⚠️ TWELVEDATA_API_KEY 미설정 — {symbol} 현재가 조회 불가")
        return None

    # TwelveData API 호출
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(
                _PRICE_URL,
                params={
                    "symbol": symbol,
                    "apikey": settings.TWELVEDATA_API_KEY,
                },
            )
            resp.raise_for_status()
            data = resp.json()

        if "price" not in data:
            api_logger.error(f"❌ TwelveData 응답 오류 ({symbol}): {data.get('message', data)}")
            return None

        price = float(data["price"])
        api_logger.info(f"✅ TwelveData 현재가 조회 성공: {symbol} = ${price}")

        # 캐싱 (60초)
        await cache_service.set(cache_key, price, ttl=_CACHE_TTL)
        return price

    except httpx.TimeoutException:
        api_logger.error(f"❌ TwelveData 타임아웃: {symbol}")
        return None
    except Exception as e:
        api_logger.error(f"❌ TwelveData 호출 실패 ({symbol}): {e}")
        return None
