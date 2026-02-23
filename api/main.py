"""
StockMind Trading Signal API
FastAPI 메인 애플리케이션
"""
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
import time
import redis as redis_sync

from api.config import settings
from api.routers import sentiment, signals, predictions, buzz, historical
from api.utils.logger import api_logger, log_request, log_response, log_error
from api.data.database import init_db, engine
from sqlalchemy import text
from api.ml.model_registry import model_registry


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    애플리케이션 라이프사이클 관리
    시작 시 초기화, 종료 시 정리
    """
    # 시작 시
    api_logger.info("🚀 StockMind API 시작 중...")

    # 환경 변수 기본값 경고
    if "password@localhost" in settings.DATABASE_URL:
        api_logger.warning("⚠️ DATABASE_URL이 기본값으로 설정됨. 프로덕션에서는 환경 변수를 설정하세요.")
    if not settings.TWELVEDATA_API_KEY:
        api_logger.warning("⚠️ TWELVEDATA_API_KEY 미설정 — 현재가 조회 불가 (신호/예측 엔드포인트 503 반환)")

    # 데이터베이스 초기화
    try:
        init_db()
        api_logger.info("✅ 데이터베이스 연결 완료")
    except Exception as e:
        api_logger.error(f"❌ 데이터베이스 초기화 실패: {e}")

    # ML 모델 사전 로딩 (선택적)
    try:
        model_registry.preload_all_models()
        api_logger.info("✅ ML 모델 로딩 완료")
    except Exception as e:
        api_logger.warning(f"⚠️ ML 모델 로딩 실패: {e}")

    api_logger.info("✅ StockMind API 준비 완료")

    yield

    # 종료 시
    api_logger.info("🛑 StockMind API 종료 중...")


# FastAPI 앱 생성
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description=settings.DESCRIPTION,
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc"
)


# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["Authorization", "Content-Type"],
)


# 요청/응답 로깅 미들웨어
@app.middleware("http")
async def logging_middleware(request: Request, call_next):
    """모든 요청/응답 로깅"""
    start_time = time.time()

    # 요청 로깅
    log_request(
        endpoint=request.url.path,
        method=request.method,
        params=dict(request.query_params)
    )

    # 요청 처리
    try:
        response = await call_next(request)
        duration_ms = (time.time() - start_time) * 1000

        # 응답 로깅
        log_response(
            endpoint=request.url.path,
            status_code=response.status_code,
            duration_ms=duration_ms
        )

        return response

    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        log_error(endpoint=request.url.path, error=e)

        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error"}
        )


# 전역 예외 핸들러
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """유효성 검사 에러 핸들러"""
    api_logger.warning(f"⚠️ Validation Error: {exc.errors()}")
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": exc.errors()}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """일반 예외 핸들러"""
    log_error(endpoint=request.url.path, error=exc)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error"}
    )


# 라우터 등록
app.include_router(sentiment.router, prefix=settings.API_V1_PREFIX)
app.include_router(signals.router, prefix=settings.API_V1_PREFIX)
app.include_router(predictions.router, prefix=settings.API_V1_PREFIX)
app.include_router(buzz.router, prefix=settings.API_V1_PREFIX)
app.include_router(historical.router, prefix=settings.API_V1_PREFIX)


# 헬스 체크 엔드포인트
@app.get("/")
def root():
    """API 루트"""
    return {
        "name": settings.PROJECT_NAME,
        "version": settings.VERSION,
        "status": "running",
        "docs": "/docs",
        "supported_symbols": settings.SUPPORTED_SYMBOLS
    }


@app.get("/health")
def health_check():
    """헬스 체크 — DB 및 Redis 연결 상태 포함"""
    checks = {}

    # DB 연결 확인
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        checks["database"] = "ok"
    except Exception as e:
        api_logger.error(f"❌ DB 헬스 체크 실패: {e}")
        checks["database"] = "error"

    # Redis 연결 확인
    try:
        r = redis_sync.from_url(settings.REDIS_URL, socket_connect_timeout=2)
        r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        api_logger.error(f"❌ Redis 헬스 체크 실패: {e}")
        checks["redis"] = "error"

    overall = "healthy" if all(v == "ok" for v in checks.values()) else "degraded"
    http_status = status.HTTP_200_OK if overall == "healthy" else status.HTTP_503_SERVICE_UNAVAILABLE

    return JSONResponse(
        status_code=http_status,
        content={
            "status": overall,
            "timestamp": time.time(),
            "checks": checks,
        }
    )


@app.get("/api/v1")
def api_info():
    """API 정보"""
    return {
        "version": "v1",
        "endpoints": {
            "sentiment": "/api/v1/sentiment/{symbol}/...",
            "signals": "/api/v1/signals/{symbol}",
            "predictions": "/api/v1/predictions/{symbol}/price",
            "buzz": "/api/v1/buzz/{symbol}",
            "historical": "/api/v1/historical/{symbol}/comments"
        },
        "supported_symbols": settings.SUPPORTED_SYMBOLS,
        "documentation": "/docs"
    }


# 서버 실행 (개발용)
if __name__ == "__main__":
    import uvicorn

    api_logger.info("🚀 개발 서버 시작...")

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
