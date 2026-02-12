# Implementation Plan: Stock Sentiment & Trading Signal API MVP

## Context

수집된 Yahoo Finance 커뮤니티 데이터(12,969개 댓글)와 기존 뉴스 분석 파이프라인(FinBERT 감성분석, RandomForest 가격 예측)을 활용하여 **주식 매매 신호 API**를 구축합니다.

### 현재 보유 자산
- **PostgreSQL DB**: 8개 기술주(AAPL, GOOG, META, TSLA, MSFT, AMZN, NVDA, NFLX) 커뮤니티 댓글
- **뉴스 분석 파이프라인**: FinBERT 감성분석, DistilBART 요약, KeyBERT 키워드 추출
- **ML 모델**: RandomForest 가격 예측 모델 (62-67일 학습 데이터)
- **Feature CSVs**: 종목별 일별 뉴스 감성 + 임베딩 데이터

### 목표
백엔드 API MVP를 구축하여:
1. 커뮤니티 + 뉴스 이중 감성 분석 제공
2. AI 기반 매매 신호 생성 (BUY/HOLD/SELL)
3. 가격 예측 및 신뢰도 제공
4. 배포 가능한 Docker 기반 아키텍처

## Recommended Architecture

### Tech Stack
- **Framework**: FastAPI (async 지원, 자동 문서화, 타입 안전성)
- **Database**: PostgreSQL (기존 DB 활용)
- **Caching**: Redis (ML 연산 결과 캐싱)
- **ML Serving**: 메모리 내 모델 로딩 (joblib)
- **Authentication**: JWT 토큰
- **Deployment**: Docker Compose (기존 setup 확장)

### API Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     FastAPI Server                      │
├─────────────────────────────────────────────────────────┤
│  Routers (Endpoints)                                    │
│  ├─ /api/v1/sentiment/{symbol}/community              │
│  ├─ /api/v1/sentiment/{symbol}/news                   │
│  ├─ /api/v1/sentiment/{symbol}/combined               │
│  ├─ /api/v1/signals/{symbol}           (핵심)         │
│  ├─ /api/v1/predictions/{symbol}/price                │
│  ├─ /api/v1/buzz/{symbol}                             │
│  └─ /api/v1/historical/{symbol}/comments              │
├─────────────────────────────────────────────────────────┤
│  Services (Business Logic)                              │
│  ├─ SentimentService - 감성 계산 & 집계               │
│  ├─ SignalService - 매매 신호 생성                    │
│  ├─ MLService - 모델 추론                             │
│  └─ CacheService - Redis 캐싱                         │
├─────────────────────────────────────────────────────────┤
│  Data Layer                                             │
│  ├─ CommentRepository - PostgreSQL 쿼리               │
│  ├─ NewsRepository - Feature CSV 로딩                 │
│  └─ SignalRepository - 신호 이력 저장                 │
├─────────────────────────────────────────────────────────┤
│  ML Layer                                               │
│  ├─ ModelRegistry - RandomForest 모델 캐싱            │
│  └─ FeatureEngineering - 392차원 특징 생성            │
└─────────────────────────────────────────────────────────┘
         ↓                    ↓                    ↓
   PostgreSQL              Redis               ML Models
   (comments)           (cache)              (.pkl files)
```

## Core Endpoints

### 1. Sentiment Endpoints

**GET /api/v1/sentiment/{symbol}/community**
- 커뮤니티 댓글 감성 집계 (1h, 4h, 24h, 7d 범위)
- 반환: sentiment_score (-1~1), volume, trending_keywords

**GET /api/v1/sentiment/{symbol}/combined** (중요)
- 뉴스(60%) + 커뮤니티(40%) 가중 평균 감성
- 반환: composite_score, confidence_level, breakdown

### 2. Trading Signal Endpoint (핵심)

**GET /api/v1/signals/{symbol}**
- 매매 신호 생성: BUY/HOLD/SELL
- 알고리즘:
  ```python
  if predicted_change > +2% AND sentiment > 0.3:
      signal = "BUY"
  elif predicted_change < -2% AND sentiment < -0.3:
      signal = "SELL"
  else:
      signal = "HOLD"
  ```
- 반환:
  ```json
  {
    "signal": "BUY",
    "strength": "STRONG/MODERATE/WEAK",
    "confidence": 78.5,
    "predicted_price": 218.20,
    "predicted_change_pct": 2.18,
    "sentiment_score": 0.62,
    "supporting_factors": [...]
  }
  ```

### 3. Prediction Endpoint

**GET /api/v1/predictions/{symbol}/price**
- RandomForest로 다음날 가격 예측
- 입력: 392차원 특징 (뉴스 임베딩 + 감성 + 키워드)
- 반환: predicted_price, predicted_change_pct, confidence_interval

### 4. Community Buzz Endpoint

**GET /api/v1/buzz/{symbol}**
- 댓글 볼륨 트렌드 (hour-over-hour, day-over-day)
- 감성 속도 (sentiment velocity)
- 이상 탐지 (unusual activity spikes)

## Key Implementation Details

### 1. Dual Sentiment Fusion Algorithm

```python
def calculate_composite_sentiment(symbol: str, date: str):
    # 뉴스 감성 (기관 투자자 관점)
    news_sentiment = get_news_sentiment(symbol, date)
    news_weight = 0.6

    # 커뮤니티 감성 (개인 투자자 관점)
    community_sentiment = aggregate_community_sentiment(symbol, date)
    community_weight = 0.4

    # 가중 평균
    composite = (news_sentiment * news_weight) +
                (community_sentiment * community_weight)

    # 신뢰도 (데이터 볼륨 기반)
    news_count = get_news_article_count(symbol, date)
    comment_count = get_comment_count(symbol, date)
    confidence = min(1.0, (news_count + comment_count/10) / 20)

    return composite, confidence
```

### 2. Model Loading Strategy

```python
class ModelRegistry:
    """Singleton pattern for caching trained models"""
    _models = {}

    def get_model(self, symbol: str):
        if symbol not in self._models:
            model_path = f"/app/api/models/{symbol}_rf_model.pkl"
            self._models[symbol] = joblib.load(model_path)
        return self._models[symbol]
```

### 3. Caching Strategy

**Redis Cache Keys:**
- `sentiment:community:{symbol}:{timerange}` - TTL: 1시간
- `sentiment:news:{symbol}:{date}` - TTL: 24시간
- `prediction:{symbol}:{date}` - TTL: 5분
- `buzz:{symbol}` - TTL: 15분

### 4. Feature Engineering (392 dimensions)

```python
def generate_features(symbol: str, date: str):
    # 뉴스 요약 임베딩 (384차원)
    summary_vector = sentence_transformer.encode(summaries)

    # 감성 인코딩 (3차원: positive/neutral/negative)
    sentiment_encoded = one_hot_encode(sentiments)

    # 키워드 빈도 (5차원)
    keyword_vector = extract_keyword_frequencies(keywords)

    # 결합: 384 + 3 + 5 = 392
    return np.concatenate([summary_vector, sentiment_encoded, keyword_vector])
```

## File Structure

```
StockMind/
├── api/                          # 신규 생성
│   ├── main.py                   # FastAPI 앱 엔트리
│   ├── config.py                 # 환경 설정
│   ├── dependencies.py           # 인증, DB 세션, 캐시
│   │
│   ├── models/                   # ML 모델 파일
│   │   ├── AAPL_rf_model.pkl
│   │   └── ... (8개 종목)
│   │
│   ├── routers/                  # API 엔드포인트
│   │   ├── sentiment.py          # 감성 API
│   │   ├── signals.py            # 매매 신호 API
│   │   ├── predictions.py        # 가격 예측 API
│   │   ├── buzz.py               # 커뮤니티 버즈 API
│   │   └── historical.py         # 히스토리 데이터 API
│   │
│   ├── services/                 # 비즈니스 로직
│   │   ├── sentiment_service.py  # 감성 계산
│   │   ├── signal_service.py     # 신호 생성
│   │   ├── ml_service.py         # 모델 추론
│   │   └── cache_service.py      # Redis 캐싱
│   │
│   ├── data/                     # 데이터 접근 계층
│   │   ├── database.py           # SQLAlchemy 설정
│   │   ├── models.py             # ORM 모델
│   │   └── repositories/
│   │       ├── comment_repo.py   # 댓글 쿼리
│   │       ├── news_repo.py      # 뉴스 특징 로딩
│   │       └── signal_repo.py    # 신호 이력
│   │
│   ├── schemas/                  # Pydantic 스키마
│   │   ├── sentiment.py
│   │   ├── signal.py
│   │   └── prediction.py
│   │
│   ├── ml/                       # ML 유틸리티
│   │   ├── model_registry.py     # 모델 로딩 & 캐싱
│   │   └── feature_engineering.py
│   │
│   ├── utils/
│   │   ├── auth.py               # JWT 인증
│   │   └── logger.py
│   │
│   ├── Dockerfile
│   └── requirements.txt
│
├── community/                    # 기존 (수정 없음)
├── news/                         # 기존 (재사용)
├── chart_pattern/                # 기존 (미사용)
└── docker-compose.yml            # 업데이트 (API + Redis 추가)
```

## Critical Files to Create

### 1. API Entry Point
**`/api/main.py`**
- FastAPI 앱 초기화
- 모든 라우터 등록
- CORS 설정
- 에러 핸들러

### 2. Core Services
**`/api/services/sentiment_service.py`**
- `calculate_community_sentiment()` - PostgreSQL 쿼리 & 집계
- `calculate_news_sentiment()` - Feature CSV 로딩
- `calculate_composite_sentiment()` - 가중 평균 융합

**`/api/services/signal_service.py`**
- `generate_trading_signal()` - BUY/HOLD/SELL 결정 로직
- `calculate_signal_strength()` - STRONG/MODERATE/WEAK 판단
- `get_supporting_factors()` - 근거 생성

**`/api/services/ml_service.py`**
- `predict_price()` - RandomForest 모델 추론
- `load_features()` - 392차원 특징 로딩
- `calculate_confidence()` - 예측 신뢰도 계산

### 3. Data Access Layer
**`/api/data/repositories/comment_repo.py`**
```python
def get_recent_comments(symbol: str, hours: int = 24):
    """최근 N시간 댓글 조회 (인덱스 최적화)"""
    cutoff = datetime.utcnow() - timedelta(hours=hours)
    return session.query(Comment)\
        .filter(Comment.stock_symbol == symbol)\
        .filter(Comment.comment_time >= cutoff)\
        .order_by(Comment.comment_time.desc())\
        .limit(1000).all()
```

**`/api/data/repositories/news_repo.py`**
```python
def load_news_features(symbol: str, date: str):
    """Feature CSV 로딩"""
    csv_path = f"/app/news/features/{symbol}/{date}.csv"
    return pd.read_csv(csv_path)
```

### 4. ML Infrastructure
**`/api/ml/model_registry.py`**
```python
class ModelRegistry:
    _instance = None
    _models = {}

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = ModelRegistry()
        return cls._instance

    def load_model(self, symbol: str):
        if symbol not in self._models:
            path = f"/app/api/models/{symbol}_rf_model.pkl"
            self._models[symbol] = joblib.load(path)
        return self._models[symbol]
```

### 5. API Routers
**`/api/routers/signals.py`**
```python
@router.get("/signals/{symbol}", response_model=SignalResponse)
async def get_trading_signal(
    symbol: str,
    db: Session = Depends(get_db),
    redis: Redis = Depends(get_redis)
):
    # 1. 캐시 확인
    cached = await redis.get(f"signal:{symbol}")
    if cached:
        return json.loads(cached)

    # 2. 가격 예측
    prediction = ml_service.predict_price(symbol)

    # 3. 감성 분석
    sentiment = sentiment_service.get_composite_sentiment(symbol)

    # 4. 신호 생성
    signal = signal_service.generate_signal(
        symbol, prediction, sentiment
    )

    # 5. 캐싱 (5분)
    await redis.setex(f"signal:{symbol}", 300, json.dumps(signal))

    return signal
```

### 6. Docker Configuration
**`docker-compose.yml` 업데이트**
```yaml
services:
  db:  # 기존 유지
    ...

  redis:  # 신규 추가
    image: redis:7-alpine
    ports:
      - "6379:6379"

  api:  # 신규 추가
    build: ./api
    ports:
      - "8000:8000"
    depends_on:
      - db
      - redis
    environment:
      - DATABASE_URL=postgresql://user:password@db:5432/stockmind
      - REDIS_URL=redis://redis:6379/0
      - API_SECRET_KEY=${API_SECRET_KEY}
    volumes:
      - ./news/features:/app/features:ro
      - ./news/metadata:/app/metadata:ro
```

## Database Schema Extensions

```sql
-- API 키 관리
CREATE TABLE api_keys (
    id SERIAL PRIMARY KEY,
    key_hash TEXT NOT NULL UNIQUE,
    user_id TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    expires_at TIMESTAMP,
    rate_limit INT DEFAULT 100
);

-- 신호 이력 (백테스팅용)
CREATE TABLE signal_history (
    id SERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) NOT NULL,
    signal_date DATE NOT NULL,
    signal_type VARCHAR(10),  -- BUY/HOLD/SELL
    confidence FLOAT,
    predicted_price FLOAT,
    actual_price FLOAT,
    sentiment_score FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_signal_history ON signal_history(stock_symbol, signal_date DESC);
```

## MVP Scope

### Phase 1 (Weeks 1-2): 핵심 기능
- ✅ Sentiment endpoints (community, news, combined)
- ✅ Trading signals endpoint (BUY/HOLD/SELL)
- ✅ Price prediction endpoint
- ✅ Buzz indicators endpoint
- ✅ JWT authentication
- ✅ Redis caching
- ✅ Docker deployment

### Phase 2 (Weeks 3-4): 강화
- Signal performance tracking
- Historical accuracy metrics
- Webhook notifications
- Rate limiting per API key

### Defer to Future
- Real-time FinBERT inference
- Options sentiment
- Social media expansion (Twitter, Reddit)
- Automated trading integration

## Value Propositions

### 1. 이중 감성 융합 (Dual Sentiment)
- **기관 관점**: 뉴스 감성 (FinBERT)
- **개인 관점**: 커뮤니티 감성 (Yahoo Finance)
- 시장 참여자 전체 감성 파악 가능

### 2. AI 기반 매매 신호
- RandomForest 가격 예측 + 감성 분석
- 단순 감성 점수가 아닌 실행 가능한 신호
- 신뢰도 및 근거 제공

### 3. 실시간 커뮤니티 펄스
- 개인 투자자 심리 실시간 모니터링
- 기관보다 빠른 감성 전환 포착
- 이상 활동 탐지 (모멘텀 조기 발견)

## Verification Steps

### 1. 환경 설정
```bash
cd /Users/sunminkim/Desktop/projects/StockMind/api
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 모델 준비
```bash
# news 모듈에서 모델 학습 (8개 종목)
cd ../news
python train_model.py

# 모델 파일을 api/models/로 복사
cp models/*.pkl ../api/models/
```

### 3. Docker 실행
```bash
cd ..
docker-compose up -d  # PostgreSQL + Redis 시작
```

### 4. API 서버 실행
```bash
cd api
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 5. API 테스트
```bash
# Swagger UI 열기
open http://localhost:8000/docs

# 신호 조회 테스트
curl http://localhost:8000/api/v1/signals/AAPL

# 감성 조회 테스트
curl http://localhost:8000/api/v1/sentiment/AAPL/combined

# 버즈 지표 테스트
curl http://localhost:8000/api/v1/buzz/AAPL
```

### 6. 결과 검증
- ✅ 각 엔드포인트가 JSON 응답 반환
- ✅ signal이 BUY/HOLD/SELL 중 하나
- ✅ sentiment_score가 -1~1 범위
- ✅ confidence가 0~100 범위
- ✅ predicted_price가 현실적 값

### 7. 성능 테스트
```bash
# 응답 시간 확인 (<500ms 목표)
ab -n 100 -c 10 http://localhost:8000/api/v1/signals/AAPL

# Redis 캐싱 확인
redis-cli keys "signal:*"
redis-cli ttl "signal:AAPL"
```

## Implementation Notes

- 기존 `news/` 모듈의 FinBERT, RandomForest 재사용
- `community/` 모듈의 PostgreSQL DB 활용
- 신규 코드는 `api/` 디렉토리에 격리
- Docker Compose로 통합 배포 환경 구축
- MVP는 8개 기술주에 집중 (확장 가능한 구조)
