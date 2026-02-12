"""
감성 분석 서비스
커뮤니티 + 뉴스 이중 감성 융합
"""
from datetime import datetime
from typing import Optional, Dict, List
from sqlalchemy.orm import Session
from collections import Counter
import re

from api.config import settings
from api.data.repositories.comment_repo import CommentRepository
from api.data.repositories.news_repo import NewsRepository


class SentimentService:
    """감성 분석 비즈니스 로직"""

    def __init__(self, db: Session):
        self.db = db
        self.comment_repo = CommentRepository(db)
        self.news_repo = NewsRepository()

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

        # 긍정/부정 키워드 정의
        positive_keywords = [
            'buy', 'bullish', 'growth', 'profit', 'up', 'gain', 'success',
            'strong', 'rally', 'boom', 'rise', 'positive', 'good', 'great',
            'excellent', 'moon', 'rocket', '🚀', '📈', '💎'
        ]

        negative_keywords = [
            'sell', 'bearish', 'loss', 'down', 'decline', 'crash', 'fail',
            'weak', 'drop', 'fall', 'negative', 'bad', 'poor', 'terrible',
            'dump', '📉', '⚠️', '💀'
        ]

        positive_count = 0
        negative_count = 0
        neutral_count = 0
        all_words = []

        for comment in comments:
            text = comment.comment_text.lower()
            all_words.extend(re.findall(r'\b\w+\b', text))

            # 긍정/부정 키워드 매칭
            has_positive = any(keyword in text for keyword in positive_keywords)
            has_negative = any(keyword in text for keyword in negative_keywords)

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
        confidence = min(100.0, (news_count + community_count / 10) / 20 * 100)

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
        hourly_volume = self.comment_repo.get_hourly_comment_volume(symbol, hours)

        # 각 시간대의 댓글로 감성 계산 (간단한 구현)
        trend = []
        for item in hourly_volume:
            # 실제로는 각 시간대의 댓글을 분석해야 하지만, 여기서는 전체 감성 사용
            overall_sentiment = self.calculate_community_sentiment(symbol, 1)
            trend.append({
                "timestamp": item['hour'],
                "sentiment_score": overall_sentiment['sentiment_score'],
                "volume": item['count']
            })

        return trend
