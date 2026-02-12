"""
매매 신호 생성 서비스
BUY/HOLD/SELL 결정 로직
"""
from typing import Dict, List
from datetime import datetime
from sqlalchemy.orm import Session

from api.config import settings
from api.schemas.signal import SignalType, SignalStrength
from api.data.repositories.signal_repo import SignalRepository


class SignalService:
    """매매 신호 생성 비즈니스 로직"""

    def __init__(self, db: Session):
        self.db = db
        self.signal_repo = SignalRepository(db)

    def generate_trading_signal(
        self,
        symbol: str,
        predicted_price: float,
        current_price: float,
        sentiment_score: float,
        news_sentiment: float = 0.0,
        community_sentiment: float = 0.0
    ) -> Dict:
        """
        매매 신호 생성

        알고리즘:
        - predicted_change > +2% AND sentiment > 0.3 → BUY
        - predicted_change < -2% AND sentiment < -0.3 → SELL
        - 그 외 → HOLD

        Args:
            symbol: 종목 심볼
            predicted_price: 예측 가격
            current_price: 현재 가격
            sentiment_score: 통합 감성 점수
            news_sentiment: 뉴스 감성
            community_sentiment: 커뮤니티 감성

        Returns:
            Dict: 매매 신호 정보
        """
        # 예측 변동률 계산
        predicted_change_pct = (
            (predicted_price - current_price) / current_price * 100
        )

        # 신호 결정
        signal_type = self._determine_signal_type(
            predicted_change_pct,
            sentiment_score
        )

        # 신호 강도 계산
        signal_strength = self._calculate_signal_strength(
            predicted_change_pct,
            sentiment_score
        )

        # 신뢰도 계산
        confidence = self._calculate_confidence(
            predicted_change_pct,
            sentiment_score,
            news_sentiment,
            community_sentiment
        )

        # 근거 요인
        supporting_factors = self._get_supporting_factors(
            signal_type,
            predicted_change_pct,
            sentiment_score,
            confidence
        )

        # 리스크 요인
        risk_factors = self._get_risk_factors(
            signal_type,
            predicted_change_pct,
            sentiment_score
        )

        signal_data = {
            "symbol": symbol,
            "signal": signal_type,
            "strength": signal_strength,
            "confidence": confidence,
            "predicted_price": round(predicted_price, 2),
            "predicted_change_pct": round(predicted_change_pct, 2),
            "current_price": round(current_price, 2),
            "sentiment_score": round(sentiment_score, 3),
            "news_sentiment": round(news_sentiment, 3),
            "community_sentiment": round(community_sentiment, 3),
            "supporting_factors": supporting_factors,
            "risk_factors": risk_factors,
            "timestamp": datetime.utcnow()
        }

        # 신호 이력 저장
        self._save_signal_history(signal_data)

        return signal_data

    def _determine_signal_type(
        self,
        predicted_change_pct: float,
        sentiment_score: float
    ) -> SignalType:
        """
        신호 타입 결정

        Args:
            predicted_change_pct: 예측 변동률
            sentiment_score: 감성 점수

        Returns:
            SignalType: BUY/HOLD/SELL
        """
        buy_threshold = settings.SIGNAL_BUY_PRICE_THRESHOLD
        sell_threshold = settings.SIGNAL_SELL_PRICE_THRESHOLD
        buy_sentiment = settings.SIGNAL_BUY_SENTIMENT_THRESHOLD
        sell_sentiment = settings.SIGNAL_SELL_SENTIMENT_THRESHOLD

        # BUY 조건: 예측 상승 + 긍정 감성
        if predicted_change_pct > buy_threshold and sentiment_score > buy_sentiment:
            return SignalType.BUY

        # SELL 조건: 예측 하락 + 부정 감성
        elif predicted_change_pct < sell_threshold and sentiment_score < sell_sentiment:
            return SignalType.SELL

        # 그 외: HOLD
        else:
            return SignalType.HOLD

    def _calculate_signal_strength(
        self,
        predicted_change_pct: float,
        sentiment_score: float
    ) -> SignalStrength:
        """
        신호 강도 계산

        Args:
            predicted_change_pct: 예측 변동률
            sentiment_score: 감성 점수

        Returns:
            SignalStrength: STRONG/MODERATE/WEAK
        """
        # 변동률과 감성의 절대값 합산
        strength_score = abs(predicted_change_pct) + abs(sentiment_score) * 10

        if strength_score > 5:
            return SignalStrength.STRONG
        elif strength_score > 3:
            return SignalStrength.MODERATE
        else:
            return SignalStrength.WEAK

    def _calculate_confidence(
        self,
        predicted_change_pct: float,
        sentiment_score: float,
        news_sentiment: float,
        community_sentiment: float
    ) -> float:
        """
        신호 신뢰도 계산

        Args:
            predicted_change_pct: 예측 변동률
            sentiment_score: 통합 감성
            news_sentiment: 뉴스 감성
            community_sentiment: 커뮤니티 감성

        Returns:
            float: 신뢰도 (0 ~ 100)
        """
        # 기본 신뢰도: 50
        confidence = 50.0

        # 예측 변동률이 클수록 +
        confidence += min(20, abs(predicted_change_pct) * 3)

        # 감성 점수가 강할수록 +
        confidence += min(15, abs(sentiment_score) * 15)

        # 뉴스와 커뮤니티 감성이 일치하면 +
        if (news_sentiment > 0 and community_sentiment > 0) or \
           (news_sentiment < 0 and community_sentiment < 0):
            confidence += 15

        return min(100.0, confidence)

    def _get_supporting_factors(
        self,
        signal_type: SignalType,
        predicted_change_pct: float,
        sentiment_score: float,
        confidence: float
    ) -> List[str]:
        """
        신호 근거 요인

        Args:
            signal_type: 신호 타입
            predicted_change_pct: 예측 변동률
            sentiment_score: 감성 점수
            confidence: 신뢰도

        Returns:
            List[str]: 근거 목록
        """
        factors = []

        if signal_type == SignalType.BUY:
            factors.append(f"예상 상승률 +{predicted_change_pct:.2f}%")
            if sentiment_score > 0.5:
                factors.append(f"강력한 긍정 감성 ({sentiment_score:.2f})")
            elif sentiment_score > 0.3:
                factors.append(f"긍정적 감성 ({sentiment_score:.2f})")

        elif signal_type == SignalType.SELL:
            factors.append(f"예상 하락률 {predicted_change_pct:.2f}%")
            if sentiment_score < -0.5:
                factors.append(f"강력한 부정 감성 ({sentiment_score:.2f})")
            elif sentiment_score < -0.3:
                factors.append(f"부정적 감성 ({sentiment_score:.2f})")

        else:  # HOLD
            factors.append(f"예측 변동률 {predicted_change_pct:.2f}%")
            factors.append("명확한 매수/매도 신호 부재")

        if confidence > 75:
            factors.append(f"높은 신뢰도 ({confidence:.1f}%)")
        elif confidence > 50:
            factors.append(f"중간 신뢰도 ({confidence:.1f}%)")

        return factors

    def _get_risk_factors(
        self,
        signal_type: SignalType,
        predicted_change_pct: float,
        sentiment_score: float
    ) -> List[str]:
        """
        리스크 요인

        Args:
            signal_type: 신호 타입
            predicted_change_pct: 예측 변동률
            sentiment_score: 감성 점수

        Returns:
            List[str]: 리스크 목록
        """
        risks = []

        # 예측 변동률과 감성이 불일치하면 리스크
        if (predicted_change_pct > 0 and sentiment_score < 0) or \
           (predicted_change_pct < 0 and sentiment_score > 0):
            risks.append("예측과 감성 불일치")

        # 예측 변동률이 크면 변동성 경고
        if abs(predicted_change_pct) > 5:
            risks.append("높은 가격 변동성 예상")

        # 감성이 중립에 가까우면
        if abs(sentiment_score) < 0.2:
            risks.append("시장 감성 불확실")

        if not risks:
            risks.append("시장 변동성 일반적 주의")

        return risks

    def _save_signal_history(self, signal_data: Dict):
        """
        신호 이력 저장

        Args:
            signal_data: 신호 데이터
        """
        try:
            history_data = {
                "stock_symbol": signal_data["symbol"],
                "signal_date": signal_data["timestamp"],
                "signal_type": signal_data["signal"].value,
                "signal_strength": signal_data["strength"].value,
                "confidence": signal_data["confidence"],
                "predicted_price": signal_data["predicted_price"],
                "predicted_change_pct": signal_data["predicted_change_pct"],
                "sentiment_score": signal_data["sentiment_score"],
                "news_sentiment": signal_data.get("news_sentiment"),
                "community_sentiment": signal_data.get("community_sentiment"),
                "supporting_factors": ", ".join(signal_data["supporting_factors"])
            }
            self.signal_repo.create_signal(history_data)
        except Exception as e:
            print(f"⚠️  신호 이력 저장 실패: {e}")
