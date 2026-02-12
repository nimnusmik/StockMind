"""
감성 분석 API 스키마
Pydantic 모델 정의
"""
from typing import Optional, List, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class CommunitySentimentResponse(BaseModel):
    """커뮤니티 감성 응답"""
    symbol: str = Field(..., description="종목 심볼")
    sentiment_score: float = Field(..., ge=-1, le=1, description="감성 점수 (-1 ~ 1)")
    timerange: str = Field(..., description="시간 범위 (예: 24h, 7d)")
    comment_count: int = Field(..., description="분석된 댓글 수")
    positive_ratio: float = Field(..., ge=0, le=1, description="긍정 비율")
    negative_ratio: float = Field(..., ge=0, le=1, description="부정 비율")
    neutral_ratio: float = Field(..., ge=0, le=1, description="중립 비율")
    trending_keywords: Optional[List[str]] = Field(None, description="트렌딩 키워드")
    timestamp: datetime = Field(..., description="분석 시각")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "sentiment_score": 0.45,
                "timerange": "24h",
                "comment_count": 523,
                "positive_ratio": 0.62,
                "negative_ratio": 0.23,
                "neutral_ratio": 0.15,
                "trending_keywords": ["earnings", "iPhone", "growth"],
                "timestamp": "2026-02-12T10:30:00"
            }
        }


class NewsSentimentResponse(BaseModel):
    """뉴스 감성 응답"""
    symbol: str = Field(..., description="종목 심볼")
    sentiment_score: float = Field(..., ge=-1, le=1, description="감성 점수 (-1 ~ 1)")
    date: str = Field(..., description="뉴스 날짜 (YYYY-MM-DD)")
    article_count: int = Field(..., description="분석된 기사 수")
    positive_ratio: float = Field(..., ge=0, le=1, description="긍정 비율")
    negative_ratio: float = Field(..., ge=0, le=1, description="부정 비율")
    neutral_ratio: float = Field(..., ge=0, le=1, description="중립 비율")
    top_keywords: Optional[List[str]] = Field(None, description="주요 키워드")
    timestamp: datetime = Field(..., description="분석 시각")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "sentiment_score": 0.38,
                "date": "2026-02-11",
                "article_count": 15,
                "positive_ratio": 0.60,
                "negative_ratio": 0.20,
                "neutral_ratio": 0.20,
                "top_keywords": ["revenue", "AI", "innovation"],
                "timestamp": "2026-02-12T10:30:00"
            }
        }


class CombinedSentimentResponse(BaseModel):
    """통합 감성 응답 (뉴스 + 커뮤니티)"""
    symbol: str = Field(..., description="종목 심볼")
    composite_score: float = Field(..., ge=-1, le=1, description="통합 감성 점수 (-1 ~ 1)")
    confidence_level: float = Field(..., ge=0, le=100, description="신뢰도 (0 ~ 100)")
    news_sentiment: float = Field(..., description="뉴스 감성 점수")
    community_sentiment: float = Field(..., description="커뮤니티 감성 점수")
    news_weight: float = Field(..., description="뉴스 가중치")
    community_weight: float = Field(..., description="커뮤니티 가중치")
    breakdown: Dict[str, Any] = Field(..., description="세부 분석")
    timestamp: datetime = Field(..., description="분석 시각")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "composite_score": 0.42,
                "confidence_level": 78.5,
                "news_sentiment": 0.38,
                "community_sentiment": 0.45,
                "news_weight": 0.6,
                "community_weight": 0.4,
                "breakdown": {
                    "news_article_count": 15,
                    "community_comment_count": 523,
                    "data_quality": "high"
                },
                "timestamp": "2026-02-12T10:30:00"
            }
        }


class SentimentTrendPoint(BaseModel):
    """감성 트렌드 데이터 포인트"""
    timestamp: datetime = Field(..., description="시각")
    sentiment_score: float = Field(..., description="감성 점수")
    volume: int = Field(..., description="데이터 볼륨")


class SentimentTrendResponse(BaseModel):
    """감성 트렌드 응답"""
    symbol: str = Field(..., description="종목 심볼")
    timerange: str = Field(..., description="시간 범위")
    trend: List[SentimentTrendPoint] = Field(..., description="트렌드 데이터")
    average_sentiment: float = Field(..., description="평균 감성")
    sentiment_volatility: float = Field(..., description="감성 변동성")
