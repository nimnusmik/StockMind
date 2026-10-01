"""
감성 분석 서비스
커뮤니티 + 뉴스 이중 감성 융합
"""
from datetime import datetime
from typing import Dict, List, Optional
from sqlalchemy.orm import Session
from collections import Counter
import re

from api.config import settings
from api.data.repositories.comment_repo import CommentRepository
from api.data.repositories.news_repo import NewsRepository

# 모듈 레벨 키워드 목록 (historical 라우터에서도 import 가능)
_POSITIVE_KEYWORDS = [
    'buy', 'bullish', 'growth', 'profit', 'up', 'gain', 'success',
    'strong', 'rally', 'boom', 'rise', 'positive', 'good', 'great',
    'excellent', 'moon', 'rocket', '🚀', '📈', '💎'
]
_NEGATIVE_KEYWORDS = [
    'sell', 'bearish', 'loss', 'down', 'decline', 'crash', 'fail',
    'weak', 'drop', 'fall', 'negative', 'bad', 'poor', 'terrible',
    'dump', '📉', '⚠️', '💀'
]


def classify_text_sentiment(text: str) -> str:
    """
    규칙 기반 텍스트 감성 분류

    Returns:
        str: "positive" | "negative" | "neutral"
    """
    t = text.lower()
    has_pos = any(kw in t for kw in _POSITIVE_KEYWORDS)
    has_neg = any(kw in t for kw in _NEGATIVE_KEYWORDS)
    if has_pos and not has_neg:
        return "positive"
    if has_neg and not has_pos:
        return "negative"
    return "neutral"


class SentimentService:
    """감성 분석 비즈니스 로직"""

    def __init__(self, db: Session, news_repo: Optional[NewsRepository] = None):
        self.db = db
        self.comment_repo = CommentRepository(db)
        self.news_repo = news_repo or NewsRepository()  # 테스트 시 Mock 주입 가능

    def calculate_community_sentiment(
        self,
        symbol: str,
        hours: int = 24
    ) -> Dict:
        """
        커뮤니티 댓글 감성 계산

        간단한 규칙 기반 감성 분석:
        - 긍정 키워드: buy, bullish, growth, profit, up, gain, success, etc.
        - 부정 키워드: sell, bearish, loss, down, decline, crash, fail, etc.

        Args:
            symbol: 종목 심볼
            hours: 시간 범위

        Returns:
            Dict: 감성 분석 결과
        """
        # 최근 댓글 가져오기
        comments = self.comment_repo.get_recent_comments(symbol, hours=hours)

        if not comments:
            return {
                "sentiment_score": 0.0,
                "comment_count": 0,
                "positive_ratio": 0.0,
                "negative_ratio": 0.0,
                "neutral_ratio": 0.0,
                "trending_keywords": []
            }

        positive_count = 0
        negative_count = 0
        neutral_count = 0
        all_words = []

        for comment in comments:
            text = comment.comment_text.lower()
            all_words.extend(re.findall(r'\b\w+\b', text))

            # 긍정/부정 키워드 매칭
            has_positive = any(keyword in text for keyword in _POSITIVE_KEYWORDS)
            has_negative = any(keyword in text for keyword in _NEGATIVE_KEYWORDS)

            if has_positive and not has_negative:
                positive_count += 1
            elif has_negative and not has_positive:
                negative_count += 1
            elif has_positive and has_negative:
                # 둘 다 있으면 중립
                neutral_count += 1
            else:
                neutral_count += 1

        total = len(comments)
        positive_ratio = positive_count / total if total > 0 else 0
        negative_ratio = negative_count / total if total > 0 else 0
        neutral_ratio = neutral_count / total if total > 0 else 0

        # 감성 점수 계산 (-1 ~ 1)
        sentiment_score = positive_ratio - negative_ratio

        # 트렌딩 키워드 (상위 10개, 일반적인 단어 제외)
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'is', 'it', 'of'}
        filtered_words = [word for word in all_words if word not in stop_words and len(word) > 2]
        trending_keywords = [word for word, _ in Counter(filtered_words).most_common(10)]

        return {
            "sentiment_score": round(sentiment_score, 3),
            "comment_count": total,
            "positive_ratio": round(positive_ratio, 3),
            "negative_ratio": round(negative_ratio, 3),
            "neutral_ratio": round(neutral_ratio, 3),
            "trending_keywords": trending_keywords
        }

    def calculate_news_sentiment(
        self,
        symbol: str,
        date: Optional[datetime] = None
    ) -> Optional[Dict]:
        """
        뉴스 감성 계산

        Args:
            symbol: 종목 심볼
            date: 특정 날짜 (None이면 최신)

        Returns:
            Optional[Dict]: 뉴스 감성 결과 또는 None
        """
        sentiment_info = self.news_repo.get_latest_sentiment(symbol)

        if sentiment_info is None:
            return None

        return {
            "sentiment_score": sentiment_info.get('sentiment_score', 0.0),
            "article_count": sentiment_info.get('article_count', 0),
            "positive_ratio": sentiment_info.get('positive_ratio', 0.0),
            "negative_ratio": sentiment_info.get('negative_ratio', 0.0),
            "neutral_ratio": sentiment_info.get('neutral_ratio', 0.0),
            "date": sentiment_info.get('date')
        }

    def calculate_composite_sentiment(
        self,
        symbol: str,
        hours: int = 24
    ) -> Dict:
        """
        통합 감성 계산 (뉴스 + 커뮤니티 융합)

        가중치:
        - 뉴스: 60% (기관 투자자 관점)
        - 커뮤니티: 40% (개인 투자자 관점)

        Args:
            symbol: 종목 심볼
            hours: 커뮤니티 시간 범위

        Returns:
            Dict: 통합 감성 결과
        """
        # 뉴스 감성
        news_sentiment = self.calculate_news_sentiment(symbol)
        news_score = news_sentiment['sentiment_score'] if news_sentiment else 0.0
        news_count = news_sentiment['article_count'] if news_sentiment else 0

        # 커뮤니티 감성
        community_sentiment = self.calculate_community_sentiment(symbol, hours)
        community_score = community_sentiment['sentiment_score']
        community_count = community_sentiment['comment_count']

        # 가중 평균
        news_weight = settings.NEWS_SENTIMENT_WEIGHT
        community_weight = settings.COMMUNITY_SENTIMENT_WEIGHT

        composite_score = (
            news_score * news_weight +
            community_score * community_weight
        )

        # 신뢰도 계산 (데이터 볼륨 기반)
        # 뉴스 20개 + 댓글 200개 = 신뢰도 100%
        confidence = min(100.0, (news_count + (community_count / 10)) / 20 * 100)

        return {
            "composite_score": round(composite_score, 3),
            "confidence_level": round(confidence, 2),
            "news_sentiment": round(news_score, 3),
            "community_sentiment": round(community_score, 3),
            "news_weight": news_weight,
            "community_weight": community_weight,
            "breakdown": {
                "news_article_count": news_count,
                "community_comment_count": community_count,
                "data_quality": "high" if confidence > 70 else "medium" if confidence > 40 else "low"
            }
        }

    def calculate_sentiment_velocity(self, symbol: str) -> float:
        """
        감성 속도 계산 (현재 1시간 감성 - 이전 1시간 감성)

        Returns:
            float: -2.0 ~ 2.0 범위의 속도값
        """
        current_comments = self.comment_repo.get_comments_in_window(symbol, 1, 0)
        prev_comments = self.comment_repo.get_comments_in_window(symbol, 2, 1)

        def _score(comments) -> float:
            if not comments:
                return 0.0
            pos = sum(1 for c in comments if classify_text_sentiment(c.comment_text) == "positive")
            neg = sum(1 for c in comments if classify_text_sentiment(c.comment_text) == "negative")
            return (pos - neg) / len(comments)

        return round(_score(current_comments) - _score(prev_comments), 3)

    def get_sentiment_trend(
        self,
        symbol: str,
        hours: int = 24
    ) -> List[Dict]:
        """
        시간별 감성 트렌드

        Args:
            symbol: 종목 심볼
            hours: 시간 범위

        Returns:
            List[Dict]: 시간별 감성 데이터
        """
        from datetime import timedelta
        hourly_volume = self.comment_repo.get_hourly_comment_volume(symbol, hours)

        trend = []
        for item in hourly_volume:
            hour_start = item['hour']
            hour_end = hour_start + timedelta(hours=1)

            # 해당 시간대 댓글만 조회해 감성 계산
            comments = self.comment_repo.get_comments_by_date_range(
                symbol, hour_start, hour_end
            )

            if comments:
                pos = sum(1 for c in comments if classify_text_sentiment(c.comment_text) == "positive")
                neg = sum(1 for c in comments if classify_text_sentiment(c.comment_text) == "negative")
                score = round((pos - neg) / len(comments), 3)
            else:
                score = 0.0

            trend.append({
                "timestamp": hour_start,
                "sentiment_score": score,
                "volume": item['count']
            })

        return trend
