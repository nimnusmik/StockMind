"""
가격 예측 API 라우터
ML 모델 기반 주가 예측
"""
from fastapi import APIRouter, Depends
from datetime import datetime
import redis.asyncio as redis
from api.config import settings

from api.dependencies import get_redis, validate_symbol, get_current_user
from api.schemas.prediction import PricePredictionResponse
from api.services.ml_service import ml_service
from api.services.cache_service import CacheService
from api.services.price_service import get_current_price


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

    # 모델 출력 = 변동률(%)
    predicted_rate = prediction['predicted_price']

    # 현재 가격 (TwelveData API)
    current_price = await get_current_price(symbol, cache_service)
    if current_price is None:
        from fastapi import HTTPException
        raise HTTPException(
            status_code=503,
            detail=f"현재 주가 조회 실패: {symbol}. TWELVEDATA_API_KEY 설정을 확인하세요."
        )

    # 실제 예측 가격 계산 (현재가 × (1 + 변동률/100))
    predicted_price = current_price * (1 + predicted_rate / 100)
    predicted_change_pct = predicted_rate

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


@router.post("/models/reload")
async def reload_models(
    redis_client: redis.Redis = Depends(get_redis),
    _user: dict = Depends(get_current_user)
):
    """
    ML 모델 핫 리로드 (재시작 없이 갱신)

    **동작:**
    1. 예측·신호 Redis 캐시 무효화 (구 예측값 제거)
    2. 메모리 모델 캐시 초기화
    3. 디스크에서 최신 모델 파일 재로딩

    **사용 시점:**
    - `train_model.py` 실행 후 즉시 반영
    - API 재시작 불필요

    **인증 필요**
    """
    from api.ml.model_registry import model_registry
    from api.services.cache_service import CacheService

    # 1. 예측·신호 캐시 무효화
    cache_service = CacheService(redis_client)
    for symbol in settings.SUPPORTED_SYMBOLS:
        await cache_service.invalidate_symbol_cache(symbol)

    # 2. 모델 재로딩
    model_registry.clear_cache()
    model_registry.preload_all_models()
    loaded = model_registry.get_loaded_models()

    return {
        "message": "모델 핫 리로드 완료 (캐시 무효화 포함)",
        "loaded_models": loaded,
        "count": len(loaded)
    }


@router.post("/preload-models")
def preload_all_models(_user: dict = Depends(get_current_user)):
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
