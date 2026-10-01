"""
가격 예측 API 스키마
Pydantic 모델 정의
"""
from typing import Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field


class PricePredictionResponse(BaseModel):
    """가격 예측 응답"""
    symbol: str = Field(..., description="종목 심볼")
    predicted_price: float = Field(..., gt=0, description="예측 가격")
    current_price: Optional[float] = Field(None, description="현재 가격")
    predicted_change_pct: float = Field(..., description="예측 변동률 (%)")
    prediction_date: str = Field(..., description="예측 날짜 (YYYY-MM-DD)")
    confidence_interval: Dict[str, float] = Field(..., description="신뢰 구간")
    model_confidence: float = Field(..., ge=0, le=100, description="모델 신뢰도")
    features_used: Dict[str, Any] = Field(..., description="사용된 특징")
    timestamp: datetime = Field(..., description="예측 생성 시각")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "predicted_price": 218.20,
                "current_price": 213.75,
                "predicted_change_pct": 2.08,
                "prediction_date": "2026-02-13",
                "confidence_interval": {
                    "lower": 215.50,
                    "upper": 220.90
                },
                "model_confidence": 76.5,
                "features_used": {
                    "news_sentiment": 0.58,
                    "community_sentiment": 0.68,
                    "embedding_dimension": 384,
                    "total_features": 392
                },
                "timestamp": "2026-02-12T10:30:00"
            }
        }


class BuzzIndicatorsResponse(BaseModel):
    """커뮤니티 버즈 지표 응답"""
    symbol: str = Field(..., description="종목 심볼")
    current_volume: int = Field(..., description="현재 댓글 볼륨 (24h)")
    hour_over_hour_change: float = Field(..., description="시간별 변화율 (%)")
    day_over_day_change: float = Field(..., description="일별 변화율 (%)")
    sentiment_velocity: float = Field(..., description="감성 속도 (변화율)")
    unusual_activity: bool = Field(..., description="이상 활동 감지")
    activity_level: str = Field(..., description="활동 수준 (LOW/NORMAL/HIGH/VERY_HIGH)")
    buzz_score: float = Field(..., ge=0, le=100, description="버즈 점수 (0 ~ 100)")
    hourly_volume: Optional[Dict[str, int]] = Field(None, description="시간별 볼륨")
    timestamp: datetime = Field(..., description="분석 시각")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "current_volume": 523,
                "hour_over_hour_change": 15.3,
                "day_over_day_change": 8.7,
                "sentiment_velocity": 0.12,
                "unusual_activity": True,
                "activity_level": "HIGH",
                "buzz_score": 78.5,
                "hourly_volume": {
                    "09:00": 45,
                    "10:00": 52,
                    "11:00": 68
                },
                "timestamp": "2026-02-12T10:30:00"
            }
        }


class HistoricalCommentResponse(BaseModel):
    """댓글 이력 응답"""
    symbol: str = Field(..., description="종목 심볼")
    total_comments: int = Field(..., description="전체 댓글 수")
    date_range: Dict[str, str] = Field(..., description="날짜 범위")
    comments: list = Field(..., description="댓글 목록")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "total_comments": 12969,
                "date_range": {
                    "start": "2025-07-01",
                    "end": "2026-02-12"
                },
                "comments": [
                    {
                        "time": "2026-02-12T09:45:00",
                        "text": "Great earnings report!",
                        "sentiment": "positive"
                    }
                ]
            }
        }
