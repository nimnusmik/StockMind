"""
Redis 캐싱 서비스
ML 연산 결과 캐싱으로 성능 최적화
"""
import json
from typing import Optional, Any
import redis.asyncio as redis

from api.config import settings


class CacheService:
    """Redis 캐싱 비즈니스 로직"""

    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client

    async def get(self, key: str) -> Optional[Any]:
        """
        캐시에서 값 가져오기

        Args:
            key: 캐시 키

        Returns:
            Optional[Any]: 캐시된 값 또는 None
        """
        try:
            value = await self.redis.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            print(f"⚠️  캐시 조회 실패: {key}, 오류: {e}")
            return None

    async def set(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None
    ) -> bool:
        """
        캐시에 값 저장

        Args:
            key: 캐시 키
            value: 저장할 값
            ttl: TTL (초 단위, None이면 무제한)

        Returns:
            bool: 성공 여부
        """
        try:
            serialized = json.dumps(value, default=str)
            if ttl:
                await self.redis.setex(key, ttl, serialized)
            else:
                await self.redis.set(key, serialized)
            return True
        except Exception as e:
            print(f"⚠️  캐시 저장 실패: {key}, 오류: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """
        캐시에서 값 삭제

        Args:
            key: 캐시 키

        Returns:
            bool: 성공 여부
        """
        try:
            await self.redis.delete(key)
            return True
        except Exception as e:
            print(f"⚠️  캐시 삭제 실패: {key}, 오류: {e}")
            return False

    async def exists(self, key: str) -> bool:
        """
        캐시 키 존재 여부 확인

        Args:
            key: 캐시 키

        Returns:
            bool: 존재 여부
        """
        try:
            return await self.redis.exists(key) > 0
        except Exception as e:
            print(f"⚠️  캐시 존재 확인 실패: {key}, 오류: {e}")
            return False

    async def get_ttl(self, key: str) -> Optional[int]:
        """
        캐시 TTL 조회

        Args:
            key: 캐시 키

        Returns:
            Optional[int]: 남은 TTL (초) 또는 None
        """
        try:
            ttl = await self.redis.ttl(key)
            return ttl if ttl > 0 else None
        except Exception as e:
            print(f"⚠️  TTL 조회 실패: {key}, 오류: {e}")
            return None

    # 도메인별 캐시 키 생성기

    def sentiment_community_key(self, symbol: str, timerange: str) -> str:
        """커뮤니티 감성 캐시 키"""
        return f"sentiment:community:{symbol}:{timerange}"

    def sentiment_news_key(self, symbol: str, date: str) -> str:
        """뉴스 감성 캐시 키"""
        return f"sentiment:news:{symbol}:{date}"

    def sentiment_combined_key(self, symbol: str) -> str:
        """통합 감성 캐시 키"""
        return f"sentiment:combined:{symbol}"

    def prediction_key(self, symbol: str, date: str) -> str:
        """가격 예측 캐시 키"""
        return f"prediction:{symbol}:{date}"

    def signal_key(self, symbol: str) -> str:
        """매매 신호 캐시 키"""
        return f"signal:{symbol}"

    def buzz_key(self, symbol: str) -> str:
        """버즈 지표 캐시 키"""
        return f"buzz:{symbol}"

    # 도메인별 캐시 작업

    async def get_sentiment_community(
        self,
        symbol: str,
        timerange: str
    ) -> Optional[Any]:
        """커뮤니티 감성 캐시 조회"""
        key = self.sentiment_community_key(symbol, timerange)
        return await self.get(key)

    async def set_sentiment_community(
        self,
        symbol: str,
        timerange: str,
        value: Any
    ) -> bool:
        """커뮤니티 감성 캐시 저장"""
        key = self.sentiment_community_key(symbol, timerange)
        ttl = settings.CACHE_TTL_SENTIMENT_COMMUNITY
        return await self.set(key, value, ttl)

    async def get_sentiment_news(
        self,
        symbol: str,
        date: str
    ) -> Optional[Any]:
        """뉴스 감성 캐시 조회"""
        key = self.sentiment_news_key(symbol, date)
        return await self.get(key)

    async def set_sentiment_news(
        self,
        symbol: str,
        date: str,
        value: Any
    ) -> bool:
        """뉴스 감성 캐시 저장"""
        key = self.sentiment_news_key(symbol, date)
        ttl = settings.CACHE_TTL_SENTIMENT_NEWS
        return await self.set(key, value, ttl)

    async def get_signal(self, symbol: str) -> Optional[Any]:
        """매매 신호 캐시 조회"""
        key = self.signal_key(symbol)
        return await self.get(key)

    async def set_signal(self, symbol: str, value: Any) -> bool:
        """매매 신호 캐시 저장"""
        key = self.signal_key(symbol)
        ttl = settings.CACHE_TTL_SIGNAL
        return await self.set(key, value, ttl)

    async def get_prediction(
        self,
        symbol: str,
        date: str
    ) -> Optional[Any]:
        """가격 예측 캐시 조회"""
        key = self.prediction_key(symbol, date)
        return await self.get(key)

    async def set_prediction(
        self,
        symbol: str,
        date: str,
        value: Any
    ) -> bool:
        """가격 예측 캐시 저장"""
        key = self.prediction_key(symbol, date)
        ttl = settings.CACHE_TTL_PREDICTION
        return await self.set(key, value, ttl)

    async def get_buzz(self, symbol: str) -> Optional[Any]:
        """버즈 지표 캐시 조회"""
        key = self.buzz_key(symbol)
        return await self.get(key)

    async def set_buzz(self, symbol: str, value: Any) -> bool:
        """버즈 지표 캐시 저장"""
        key = self.buzz_key(symbol)
        ttl = settings.CACHE_TTL_BUZZ
        return await self.set(key, value, ttl)

    async def invalidate_symbol_cache(self, symbol: str):
        """종목 관련 모든 캐시 무효화"""
        patterns = [
            f"sentiment:*:{symbol}:*",
            f"prediction:{symbol}:*",
            f"signal:{symbol}",
            f"buzz:{symbol}"
        ]

        for pattern in patterns:
            try:
                keys = await self.redis.keys(pattern)
                if keys:
                    await self.redis.delete(*keys)
                    print(f"🗑️  캐시 무효화: {pattern} ({len(keys)}개 키)")
            except Exception as e:
                print(f"⚠️  캐시 무효화 실패: {pattern}, 오류: {e}")

    async def get_cache_stats(self) -> dict:
        """캐시 통계 조회"""
        try:
            info = await self.redis.info()
            return {
                "total_keys": await self.redis.dbsize(),
                "used_memory": info.get('used_memory_human', 'N/A'),
                "connected_clients": info.get('connected_clients', 0),
                "uptime_seconds": info.get('uptime_in_seconds', 0)
            }
        except Exception as e:
            print(f"⚠️  캐시 통계 조회 실패: {e}")
            return {}
