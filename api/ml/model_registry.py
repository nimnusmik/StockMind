"""
ML 모델 레지스트리
싱글톤 패턴으로 모델 캐싱 및 관리
"""
import os
import joblib
from typing import Dict, Optional
from sklearn.ensemble import RandomForestRegressor

from api.config import settings


class ModelRegistry:
    """
    ML 모델 레지스트리 (싱글톤)
    RandomForest 모델을 메모리에 캐싱하여 재사용
    """
    _instance: Optional['ModelRegistry'] = None
    _models: Dict[str, RandomForestRegressor] = {}

    def __new__(cls):
        """싱글톤 인스턴스 생성"""
        if cls._instance is None:
            cls._instance = super(ModelRegistry, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """초기화"""
        if not hasattr(self, 'initialized'):
            self.model_dir = settings.MODEL_DIR
            self.initialized = True

    @classmethod
    def get_instance(cls) -> 'ModelRegistry':
        """
        싱글톤 인스턴스 가져오기

        Returns:
            ModelRegistry: 레지스트리 인스턴스
        """
        if cls._instance is None:
            cls._instance = ModelRegistry()
        return cls._instance

    def load_model(self, symbol: str) -> Optional[RandomForestRegressor]:
        """
        종목의 ML 모델 로딩 (캐시 활용)

        Args:
            symbol: 종목 심볼

        Returns:
            Optional[RandomForestRegressor]: 모델 또는 None
        """
        # 이미 로딩된 모델이 있으면 반환
        if symbol in self._models:
            return self._models[symbol]

        # 모델 파일 경로
        model_path = os.path.join(self.model_dir, f"{symbol}_rf_model.pkl")

        # 모델 파일이 없으면 None 반환
        if not os.path.exists(model_path):
            print(f"⚠️  모델 파일을 찾을 수 없습니다: {model_path}")
            return None

        try:
            # 모델 로딩 및 캐싱
            model = joblib.load(model_path)
            self._models[symbol] = model
            print(f"✅ 모델 로딩 완료: {symbol}")
            return model
        except Exception as e:
            print(f"❌ 모델 로딩 실패: {symbol}, 오류: {e}")
            return None

    def get_model(self, symbol: str) -> Optional[RandomForestRegressor]:
        """
        종목의 ML 모델 가져오기 (load_model의 별칭)

        Args:
            symbol: 종목 심볼

        Returns:
            Optional[RandomForestRegressor]: 모델 또는 None
        """
        return self.load_model(symbol)

    def preload_all_models(self):
        """모든 종목의 모델 사전 로딩"""
        print("🔄 모든 모델 사전 로딩 시작...")
        for symbol in settings.SUPPORTED_SYMBOLS:
            self.load_model(symbol)
        print(f"✅ {len(self._models)}개 모델 로딩 완료")

    def get_loaded_models(self) -> list:
        """
        현재 로딩된 모델 목록

        Returns:
            list: 로딩된 종목 심볼 목록
        """
        return list(self._models.keys())

    def clear_cache(self):
        """모델 캐시 초기화"""
        self._models.clear()
        print("🗑️  모델 캐시 초기화 완료")

    def reload_model(self, symbol: str) -> Optional[RandomForestRegressor]:
        """
        종목 모델 재로딩

        Args:
            symbol: 종목 심볼

        Returns:
            Optional[RandomForestRegressor]: 재로딩된 모델 또는 None
        """
        # 기존 캐시 제거
        if symbol in self._models:
            del self._models[symbol]

        # 재로딩
        return self.load_model(symbol)

    def get_model_info(self, symbol: str) -> Optional[Dict]:
        """
        모델 정보 가져오기

        Args:
            symbol: 종목 심볼

        Returns:
            Optional[Dict]: 모델 정보 또는 None
        """
        model = self.get_model(symbol)

        if model is None:
            return None

        return {
            "symbol": symbol,
            "model_type": type(model).__name__,
            "n_estimators": getattr(model, 'n_estimators', None),
            "max_depth": getattr(model, 'max_depth', None),
            "n_features_in": getattr(model, 'n_features_in_', None),
            "is_cached": symbol in self._models
        }


# 전역 인스턴스
model_registry = ModelRegistry.get_instance()
