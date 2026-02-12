"""
ML 모델 추론 서비스
가격 예측 및 신뢰도 계산
"""
from typing import Optional, Dict
from datetime import datetime, timedelta
import numpy as np

from api.ml.model_registry import model_registry
from api.ml.feature_engineering import feature_engineer


class MLService:
    """ML 모델 추론 비즈니스 로직"""

    def __init__(self):
        self.model_registry = model_registry
        self.feature_engineer = feature_engineer

    def predict_price(
        self,
        symbol: str,
        date: Optional[datetime] = None
    ) -> Optional[Dict]:
        """
        주가 예측

        Args:
            symbol: 종목 심볼
            date: 특정 날짜 (None이면 최신)

        Returns:
            Optional[Dict]: 예측 결과 또는 None
        """
        # 모델 로딩
        model = self.model_registry.get_model(symbol)
        if model is None:
            print(f"⚠️  모델을 찾을 수 없습니다: {symbol}")
            return None

        # 특징 생성
        features_info = self.feature_engineer.get_features_with_metadata(symbol, date)
        if features_info is None:
            print(f"⚠️  특징을 생성할 수 없습니다: {symbol}")
            return None

        features = features_info['features']

        # 특징 유효성 검사
        if not self.feature_engineer.validate_features(features):
            print(f"⚠️  유효하지 않은 특징: {symbol}")
            return None

        # 예측 수행
        try:
            # 특징을 2D 배열로 reshape
            features_2d = features.reshape(1, -1)

            # 가격 예측
            predicted_price = model.predict(features_2d)[0]

            # 신뢰 구간 계산 (RandomForest의 개별 트리 예측 활용)
            confidence_interval = self._calculate_confidence_interval(
                model, features_2d, predicted_price
            )

            # 모델 신뢰도
            model_confidence = self._calculate_model_confidence(
                features_info, confidence_interval
            )

            return {
                "predicted_price": float(predicted_price),
                "confidence_interval": confidence_interval,
                "model_confidence": model_confidence,
                "features_used": {
                    "news_sentiment": features_info['sentiment']['sentiment_score'] if features_info.get('sentiment') else 0.0,
                    "embedding_dimension": 384,
                    "total_features": len(features)
                },
                "prediction_date": (date or datetime.utcnow()).strftime("%Y-%m-%d")
            }

        except Exception as e:
            print(f"❌ 가격 예측 실패: {symbol}, 오류: {e}")
            return None

    def _calculate_confidence_interval(
        self,
        model,
        features: np.ndarray,
        predicted_price: float,
        confidence_level: float = 0.95
    ) -> Dict[str, float]:
        """
        신뢰 구간 계산

        RandomForest의 개별 트리 예측을 활용하여 신뢰 구간 추정

        Args:
            model: RandomForest 모델
            features: 특징 벡터
            predicted_price: 예측 가격
            confidence_level: 신뢰 수준 (기본 95%)

        Returns:
            Dict: 신뢰 구간 (lower, upper)
        """
        try:
            # 모든 트리의 예측값
            tree_predictions = np.array([
                tree.predict(features)[0]
                for tree in model.estimators_
            ])

            # 표준 편차 계산
            std = np.std(tree_predictions)

            # 신뢰 구간 (정규분포 가정)
            z_score = 1.96  # 95% 신뢰 수준
            margin = z_score * std

            return {
                "lower": float(predicted_price - margin),
                "upper": float(predicted_price + margin)
            }

        except Exception as e:
            print(f"⚠️  신뢰 구간 계산 실패: {e}")
            # 기본값: ±5%
            return {
                "lower": float(predicted_price * 0.95),
                "upper": float(predicted_price * 1.05)
            }

    def _calculate_model_confidence(
        self,
        features_info: Dict,
        confidence_interval: Dict[str, float]
    ) -> float:
        """
        모델 신뢰도 계산

        Args:
            features_info: 특징 정보
            confidence_interval: 신뢰 구간

        Returns:
            float: 모델 신뢰도 (0 ~ 100)
        """
        # 기본 신뢰도
        confidence = 60.0

        # 신뢰 구간이 좁을수록 신뢰도 증가
        interval_width = confidence_interval['upper'] - confidence_interval['lower']
        predicted_price = (confidence_interval['upper'] + confidence_interval['lower']) / 2

        relative_width = interval_width / predicted_price

        if relative_width < 0.05:  # 5% 미만
            confidence += 20
        elif relative_width < 0.10:  # 10% 미만
            confidence += 10

        # 특징 품질에 따른 신뢰도 조정
        sentiment_info = features_info.get('sentiment')
        if sentiment_info and sentiment_info.get('article_count', 0) > 10:
            confidence += 10

        return min(100.0, confidence)

    def get_model_info(self, symbol: str) -> Optional[Dict]:
        """
        모델 정보 조회

        Args:
            symbol: 종목 심볼

        Returns:
            Optional[Dict]: 모델 정보 또는 None
        """
        return self.model_registry.get_model_info(symbol)

    def preload_models(self):
        """모든 모델 사전 로딩"""
        self.model_registry.preload_all_models()

    def get_feature_importance(self) -> Dict:
        """
        특징 중요도 정보

        Returns:
            Dict: 특징 구성 정보
        """
        return self.feature_engineer.get_feature_importance_info()


# 전역 인스턴스
ml_service = MLService()
