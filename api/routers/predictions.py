"""
가격 예측 API 라우터
ML 모델 기반 주가 예측
"""
from fastapi import APIRouter, Depends
from datetime import datetime
import redis.asyncio as redis

from api.dependencies import get_redis, validate_symbol
from api.schemas.prediction import PricePredictionResponse
from api.services.ml_service import ml_service
from api.services.cache_service import CacheService


router = APIRouter(prefix="/predictions", tags=["Price Predictions"])


@router.get("/{symbol}/price", response_model=PricePredictionResponse)
async def predict_stock_price(
    symbol: str = Depends(validate_symbol),
    redis_client: redis.Redis = Depends(get_redis)
):
    """
    주가 예측

    **ML 모델:** RandomForest Regressor

    **입력 특징 (392차원):**
    - 뉴스 요약 임베딩: 384차원 (SentenceTransformer)
    - 감성 인코딩: 3차원 (positive/neutral/negative)
    - 키워드 빈도: 5차원 (KeyBERT)

    **출력:**
    - 예측 가격
    - 예측 변동률 (%)
    - 신뢰 구간 (95%)
    - 모델 신뢰도 (0 ~ 100)

    **학습 데이터:**
    - 기간: 62-67일
    - 종목별 개별 모델
    - 일별 뉴스 데이터 기반

    **캐싱:** 5분 TTL
    """
    cache_service = CacheService(redis_client)
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # 캐시 확인
    cached = await cache_service.get_prediction(symbol, today)
    if cached:
        cached['timestamp'] = datetime.fromisoformat(cached['timestamp'])
        return PricePredictionResponse(**cached)

    # 가격 예측
    prediction = ml_service.predict_price(symbol)

    if prediction is None:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=404,
            detail=f"모델 또는 특징 데이터를 찾을 수 없습니다: {symbol}"
        )

    predicted_price = prediction['predicted_price']

    # 현재 가격 (간단히 예측 가격의 98%로 가정)
    # 실제로는 외부 API (예: TwelveData, Alpha Vantage)에서 가져와야 함
    current_price = predicted_price * 0.98

    # 변동률 계산
    predicted_change_pct = ((predicted_price - current_price) / current_price) * 100

    response_data = {
        "symbol": symbol,
        "predicted_price": round(predicted_price, 2),
        "current_price": round(current_price, 2),
        "predicted_change_pct": round(predicted_change_pct, 2),
        "prediction_date": prediction['prediction_date'],
        "confidence_interval": prediction['confidence_interval'],
        "model_confidence": prediction['model_confidence'],
        "features_used": prediction['features_used'],
        "timestamp": datetime.utcnow()
    }

    # 캐싱
    await cache_service.set_prediction(symbol, today, response_data)

    return PricePredictionResponse(**response_data)


@router.get("/{symbol}/model-info")
def get_model_info(symbol: str = Depends(validate_symbol)):
    """
    ML 모델 정보 조회

    **반환 정보:**
    - 모델 타입 (RandomForestRegressor)
    - 트리 개수 (n_estimators)
    - 최대 깊이 (max_depth)
    - 입력 특징 차원 (392)
    - 캐싱 여부

    **사용처:** 디버깅, 모델 검증
    """
    model_info = ml_service.get_model_info(symbol)

    if model_info is None:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=404,
            detail=f"모델을 찾을 수 없습니다: {symbol}"
        )

    return model_info


@router.get("/feature-importance")
def get_feature_importance():
    """
    특징 중요도 정보

    **반환 정보:**
    - 전체 특징 차원: 392
    - 구성 요소:
      - 임베딩: 384차원 (뉴스 요약)
      - 감성: 3차원 (positive/neutral/negative)
      - 키워드: 5차원 (주요 키워드 빈도)

    **사용처:** 모델 해석, 특징 엔지니어링 검증
    """
    return ml_service.get_feature_importance()


@router.post("/preload-models")
def preload_all_models():
    """
    모든 종목의 모델 사전 로딩

    **효과:**
    - 첫 예측 요청 속도 향상
    - 메모리에 모든 모델 캐싱

    **사용 시점:**
    - 서버 시작 시
    - 모델 업데이트 후

    **주의:** 메모리 사용량 증가
    """
    ml_service.preload_models()

    from api.ml.model_registry import model_registry
    loaded_models = model_registry.get_loaded_models()

    return {
        "message": "모든 모델이 로딩되었습니다",
        "loaded_models": loaded_models,
        "count": len(loaded_models)
    }
