"""
매매 신호 API 스키마
Pydantic 모델 정의
"""
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from enum import Enum


class SignalType(str, Enum):
    """신호 타입"""
    BUY = "BUY"
    HOLD = "HOLD"
    SELL = "SELL"


class SignalStrength(str, Enum):
    """신호 강도"""
    STRONG = "STRONG"
    MODERATE = "MODERATE"
    WEAK = "WEAK"


class TradingSignalResponse(BaseModel):
    """매매 신호 응답"""
    symbol: str = Field(..., description="종목 심볼")
    signal: SignalType = Field(..., description="매매 신호 (BUY/HOLD/SELL)")
    strength: SignalStrength = Field(..., description="신호 강도")
    confidence: float = Field(..., ge=0, le=100, description="신뢰도 (0 ~ 100)")
    predicted_price: float = Field(..., gt=0, description="예측 가격")
    predicted_change_pct: float = Field(..., description="예측 변동률 (%)")
    current_price: Optional[float] = Field(None, description="현재 가격")
    sentiment_score: float = Field(..., ge=-1, le=1, description="통합 감성 점수")
    news_sentiment: Optional[float] = Field(None, description="뉴스 감성")
    community_sentiment: Optional[float] = Field(None, description="커뮤니티 감성")
    supporting_factors: List[str] = Field(..., description="근거 요인")
    risk_factors: Optional[List[str]] = Field(None, description="리스크 요인")
    timestamp: datetime = Field(..., description="생성 시각")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "signal": "BUY",
                "strength": "STRONG",
                "confidence": 78.5,
                "predicted_price": 218.20,
                "predicted_change_pct": 2.18,
                "current_price": 213.75,
                "sentiment_score": 0.62,
                "news_sentiment": 0.58,
                "community_sentiment": 0.68,
                "supporting_factors": [
                    "강력한 긍정 감성 (0.62)",
                    "예상 상승률 +2.18%",
                    "뉴스 및 커뮤니티 모두 긍정적",
                    "높은 신뢰도 (78.5%)"
                ],
                "risk_factors": [
                    "시장 변동성 주의"
                ],
                "timestamp": "2026-02-12T10:30:00"
            }
        }


class SignalHistoryItem(BaseModel):
    """신호 이력 항목"""
    id: int
    signal_date: datetime
    signal_type: SignalType
    signal_strength: Optional[SignalStrength]
    confidence: float
    predicted_price: float
    actual_price: Optional[float]
    predicted_change_pct: float
    sentiment_score: float

    class Config:
        from_attributes = True


class SignalHistoryResponse(BaseModel):
    """신호 이력 응답"""
    symbol: str = Field(..., description="종목 심볼")
    signals: List[SignalHistoryItem] = Field(..., description="신호 이력")
    total_count: int = Field(..., description="전체 신호 수")


class SignalAccuracyResponse(BaseModel):
    """신호 정확도 응답"""
    symbol: str = Field(..., description="종목 심볼")
    period_days: int = Field(..., description="분석 기간 (일)")
    total_signals: int = Field(..., description="전체 신호 수")
    buy_signals: int = Field(..., description="매수 신호 수")
    sell_signals: int = Field(..., description="매도 신호 수")
    hold_signals: int = Field(..., description="보유 신호 수")
    signals_with_actual_price: int = Field(..., description="실제 가격이 있는 신호 수")
    correct_predictions: int = Field(..., description="정확한 예측 수")
    accuracy: float = Field(..., ge=0, le=100, description="정확도 (%)")

    class Config:
        json_schema_extra = {
            "example": {
                "symbol": "AAPL",
                "period_days": 30,
                "total_signals": 28,
                "buy_signals": 12,
                "sell_signals": 8,
                "hold_signals": 8,
                "signals_with_actual_price": 25,
                "correct_predictions": 18,
                "accuracy": 72.0
            }
        }
