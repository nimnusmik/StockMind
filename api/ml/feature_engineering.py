"""
특징 공학 (Feature Engineering)
392차원 특징 벡터 생성
"""
import numpy as np
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime

from api.data.repositories.news_repo import NewsRepository
from api.data.repositories.comment_repo import CommentRepository


class FeatureEngineer:
    """
    특징 공학 클래스
    뉴스 임베딩 + 감성 + 키워드 = 392차원 특징 생성
    """

    def __init__(self):
        self.news_repo = NewsRepository()
        self.feature_dim = 392

    def generate_features(
        self,
        symbol: str,
        date: Optional[datetime] = None
    ) -> Optional[np.ndarray]:
        """
        392차원 특징 벡터 생성

        특징 구성:
        - 뉴스 요약 임베딩: 384차원 (SentenceTransformer)
        - 감성 인코딩: 3차원 (positive/neutral/negative)
        - 키워드 빈도: 5차원

        Args:
            symbol: 종목 심볼
            date: 특정 날짜 (None이면 최신)

        Returns:
            Optional[np.ndarray]: 392차원 특징 벡터 또는 None
        """
        # 뉴스 리포지토리에서 특징 벡터 가져오기
        features = self.news_repo.get_feature_vector(symbol, date)

        if features is None:
            return None

        # 차원 검증
        if len(features) != self.feature_dim:
            print(f"⚠️  특징 차원 불일치: {len(features)} (기대값: {self.feature_dim})")
            # 패딩 또는 자르기로 조정
            if len(features) < self.feature_dim:
                # 부족하면 0으로 패딩
                features = np.pad(
                    features,
                    (0, self.feature_dim - len(features)),
                    mode='constant',
                    constant_values=0
                )
            else:
                # 초과하면 자르기
                features = features[:self.feature_dim]

        return features

    def get_features_with_metadata(
        self,
        symbol: str,
        date: Optional[datetime] = None
    ) -> Optional[Dict]:
        """
        특징 벡터와 메타데이터 함께 반환

        Args:
            symbol: 종목 심볼
            date: 특정 날짜

        Returns:
            Optional[Dict]: 특징 및 메타데이터 또는 None
        """
        # 특징 벡터 생성
        features = self.generate_features(symbol, date)

        if features is None:
            return None

        # 뉴스 감성 정보
        sentiment_info = self.news_repo.get_latest_sentiment(symbol)

        return {
            "features": features,
            "feature_dim": len(features),
            "symbol": symbol,
            "date": sentiment_info.get('date') if sentiment_info else None,
            "sentiment": sentiment_info
        }

    def validate_features(self, features: np.ndarray) -> bool:
        """
        특징 벡터 유효성 검사

        Args:
            features: 특징 벡터

        Returns:
            bool: 유효 여부
        """
        # 차원 확인
        if len(features) != self.feature_dim:
            return False

        # NaN 확인
        if np.isnan(features).any():
            return False

        # Inf 확인
        if np.isinf(features).any():
            return False

        return True

    def normalize_features(self, features: np.ndarray) -> np.ndarray:
        """
        특징 정규화 (선택적)

        Args:
            features: 특징 벡터

        Returns:
            np.ndarray: 정규화된 특징
        """
        # Min-Max 정규화
        min_val = features.min()
        max_val = features.max()

        if max_val - min_val == 0:
            return features

        normalized = (features - min_val) / (max_val - min_val)
        return normalized

    def extract_embedding_features(self, features: np.ndarray) -> np.ndarray:
        """
        임베딩 특징만 추출 (처음 384차원)

        Args:
            features: 전체 특징 벡터

        Returns:
            np.ndarray: 임베딩 특징 (384차원)
        """
        return features[:384]

    def extract_sentiment_features(self, features: np.ndarray) -> np.ndarray:
        """
        감성 특징만 추출 (384-386 인덱스)

        Args:
            features: 전체 특징 벡터

        Returns:
            np.ndarray: 감성 특징 (3차원)
        """
        return features[384:387]

    def extract_keyword_features(self, features: np.ndarray) -> np.ndarray:
        """
        키워드 특징만 추출 (387-391 인덱스)

        Args:
            features: 전체 특징 벡터

        Returns:
            np.ndarray: 키워드 특징 (5차원)
        """
        return features[387:392]

    def get_feature_importance_info(self) -> Dict:
        """
        특징 중요도 정보 반환

        Returns:
            Dict: 특징 구성 정보
        """
        return {
            "total_dimensions": self.feature_dim,
            "components": {
                "embedding": {
                    "dimensions": 384,
                    "description": "뉴스 요약 임베딩 (SentenceTransformer)"
                },
                "sentiment": {
                    "dimensions": 3,
                    "description": "감성 인코딩 (positive/neutral/negative)"
                },
                "keywords": {
                    "dimensions": 5,
                    "description": "키워드 빈도"
                }
            }
        }


# 전역 인스턴스
feature_engineer = FeatureEngineer()
