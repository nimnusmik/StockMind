# StockMind Trading Signal API

AI 기반 주식 매매 신호 및 감성 분석 REST API

## 🎯 핵심 기능

### 1. 이중 감성 분석 (Dual Sentiment)
- **뉴스 감성** (60%): FinBERT 기반 금융 뉴스 분석
- **커뮤니티 감성** (40%): Yahoo Finance 댓글 분석
- 기관 투자자 + 개인 투자자 관점 통합

### 2. AI 매매 신호 생성
- **BUY/HOLD/SELL** 신호 자동 생성
- RandomForest 모델 기반 가격 예측
- 신뢰도 및 근거 제공

### 3. 실시간 커뮤니티 모니터링
- 댓글 볼륨 트렌드
- 이상 활동 탐지
- 감성 속도 측정

## 📊 지원 종목

AAPL, GOOG, META, TSLA, MSFT, AMZN, NVDA, NFLX (8개 기술주)

## 🚀 빠른 시작

### 1. 환경 설정

```bash
# 환경 변수 설정
cp .env.example .env
# .env 파일 편집하여 시크릿 키 변경
```

### 2. Docker로 실행 (권장)

```bash
# 모든 서비스 시작 (PostgreSQL + Redis + API)
docker-compose up -d

# 로그 확인
docker-compose logs -f api
```

API 접속: http://localhost:8000

### 3. 로컬 개발 환경

```bash
# Python 가상환경 생성
cd api
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt

# 데이터베이스 및 Redis 시작 (Docker)
docker-compose up -d db redis

# API 서버 실행
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

### 4. ML 모델 준비

```bash
# 뉴스 파이프라인에서 모델 학습
cd ../news
python train_model.py

# 모델 파일 복사
cp models/*.pkl ../api/models/
```

## 📡 API 엔드포인트

### 핵심 엔드포인트

| 엔드포인트 | 설명 | 메서드 |
|-----------|------|--------|
| `/api/v1/signals/{symbol}` | 매매 신호 조회 | GET |
| `/api/v1/sentiment/{symbol}/combined` | 통합 감성 분석 | GET |
| `/api/v1/predictions/{symbol}/price` | 가격 예측 | GET |
| `/api/v1/buzz/{symbol}` | 커뮤니티 버즈 지표 | GET |

### 전체 엔드포인트

자세한 API 문서는 Swagger UI 참조:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

## 🔧 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│                     FastAPI Server                      │
├─────────────────────────────────────────────────────────┤
│  Routers (Endpoints)                                    │
│  ├─ sentiment.py    - 감성 분석 API                    │
│  ├─ signals.py      - 매매 신호 API (핵심)            │
│  ├─ predictions.py  - 가격 예측 API                    │
│  ├─ buzz.py         - 커뮤니티 버즈 API                │
│  └─ historical.py   - 이력 데이터 API                  │
├─────────────────────────────────────────────────────────┤
│  Services (Business Logic)                              │
│  ├─ sentiment_service.py  - 감성 계산 & 집계          │
│  ├─ signal_service.py     - 매매 신호 생성            │
│  ├─ ml_service.py         - 모델 추론                 │
│  └─ cache_service.py      - Redis 캐싱                │
├─────────────────────────────────────────────────────────┤
│  Data Layer                                             │
│  ├─ comment_repo.py  - PostgreSQL 쿼리                │
│  ├─ news_repo.py     - Feature CSV 로딩               │
│  └─ signal_repo.py   - 신호 이력 저장                 │
├─────────────────────────────────────────────────────────┤
│  ML Layer                                               │
│  ├─ model_registry.py        - 모델 캐싱              │
│  └─ feature_engineering.py   - 특징 생성              │
└─────────────────────────────────────────────────────────┘
         ↓                    ↓                    ↓
   PostgreSQL              Redis               ML Models
   (comments)           (cache)              (.pkl files)
```

## 💡 사용 예제

### 매매 신호 조회

```bash
curl http://localhost:8000/api/v1/signals/AAPL
```

```json
{
  "symbol": "AAPL",
  "signal": "BUY",
  "strength": "STRONG",
  "confidence": 78.5,
  "predicted_price": 218.20,
  "predicted_change_pct": 2.18,
  "sentiment_score": 0.62,
  "supporting_factors": [
    "강력한 긍정 감성 (0.62)",
    "예상 상승률 +2.18%",
    "높은 신뢰도 (78.5%)"
  ]
}
```

### 통합 감성 분석

```bash
curl http://localhost:8000/api/v1/sentiment/AAPL/combined
```

```json
{
  "symbol": "AAPL",
  "composite_score": 0.42,
  "confidence_level": 78.5,
  "news_sentiment": 0.38,
  "community_sentiment": 0.45,
  "news_weight": 0.6,
  "community_weight": 0.4
}
```

### 커뮤니티 버즈

```bash
curl http://localhost:8000/api/v1/buzz/AAPL
```

```json
{
  "symbol": "AAPL",
  "current_volume": 523,
  "hour_over_hour_change": 15.3,
  "unusual_activity": true,
  "activity_level": "HIGH",
  "buzz_score": 78.5
}
```

## 🔐 인증 (예정)

현재 MVP는 인증 없이 작동합니다. Phase 2에서 JWT 인증이 추가될 예정입니다.

## 📈 성능 최적화

### 캐싱 전략

| 데이터 타입 | TTL | Redis 키 패턴 |
|------------|-----|---------------|
| 커뮤니티 감성 | 1시간 | `sentiment:community:{symbol}:{timerange}` |
| 뉴스 감성 | 24시간 | `sentiment:news:{symbol}:{date}` |
| 매매 신호 | 5분 | `signal:{symbol}` |
| 가격 예측 | 5분 | `prediction:{symbol}:{date}` |
| 버즈 지표 | 15분 | `buzz:{symbol}` |

### 응답 시간 목표

- 캐시 히트: < 50ms
- 캐시 미스: < 500ms
- ML 예측: < 300ms

## 🗂️ 프로젝트 구조

```
api/
├── main.py                   # FastAPI 앱 엔트리
├── config.py                 # 환경 설정
├── dependencies.py           # 의존성 주입
├── requirements.txt          # Python 패키지
├── Dockerfile               # Docker 이미지
│
├── routers/                 # API 엔드포인트
│   ├── sentiment.py
│   ├── signals.py
│   ├── predictions.py
│   ├── buzz.py
│   └── historical.py
│
├── services/                # 비즈니스 로직
│   ├── sentiment_service.py
│   ├── signal_service.py
│   ├── ml_service.py
│   └── cache_service.py
│
├── data/                    # 데이터 접근
│   ├── database.py
│   ├── models.py
│   └── repositories/
│
├── schemas/                 # Pydantic 스키마
│   ├── sentiment.py
│   ├── signal.py
│   └── prediction.py
│
├── ml/                      # ML 유틸리티
│   ├── model_registry.py
│   └── feature_engineering.py
│
├── utils/                   # 유틸리티
│   ├── auth.py
│   └── logger.py
│
├── models/                  # ML 모델 파일
│   ├── README.md
│   └── *.pkl
│
└── migrations/              # DB 마이그레이션
    └── 001_api_tables.sql
```

## 🧪 테스트

```bash
# API 서버 헬스 체크
curl http://localhost:8000/health

# 전체 종목 요약
curl http://localhost:8000/api/v1/historical/all-symbols/summary

# 특정 종목 신호
for symbol in AAPL GOOG META TSLA; do
  echo "=== $symbol ===" curl "http://localhost:8000/api/v1/signals/$symbol" | jq '.signal'
done
```

## 📝 개발 가이드

### 새 엔드포인트 추가

1. `routers/` 에 라우터 파일 생성
2. 필요한 경우 `services/` 에 비즈니스 로직 추가
3. `schemas/` 에 Pydantic 모델 정의
4. `main.py` 에 라우터 등록

### 로깅

```python
from api.utils.logger import api_logger

api_logger.info("📊 데이터 처리 중...")
api_logger.error("❌ 에러 발생")
```

## 🐛 트러블슈팅

### 모델을 찾을 수 없음

```bash
# 모델 파일 확인
ls api/models/*.pkl

# 모델 학습 및 복사
cd news && python train_model.py
cp models/*.pkl ../api/models/
```

### 데이터베이스 연결 실패

```bash
# PostgreSQL 상태 확인
docker-compose ps db

# 로그 확인
docker-compose logs db

# 재시작
docker-compose restart db
```

### Redis 연결 실패

```bash
# Redis 상태 확인
docker-compose ps redis
docker-compose logs redis

# Redis 테스트
redis-cli ping
```

## 📚 참고 자료

- [FastAPI 공식 문서](https://fastapi.tiangolo.com/)
- [Pydantic 문서](https://docs.pydantic.dev/)
- [scikit-learn RandomForest](https://scikit-learn.org/stable/modules/generated/sklearn.ensemble.RandomForestRegressor.html)
- [Redis 캐싱 전략](https://redis.io/docs/manual/patterns/)

## 🚧 로드맵

### Phase 1 (현재 - MVP)
- ✅ 감성 분석 API
- ✅ 매매 신호 API
- ✅ 가격 예측 API
- ✅ 커뮤니티 버즈 API
- ✅ Redis 캐싱
- ✅ Docker 배포

### Phase 2 (계획)
- [ ] JWT 인증
- [ ] API 키 관리
- [ ] Rate Limiting
- [ ] 신호 정확도 추적
- [ ] Webhook 알림

### Phase 3 (미래)
- [ ] 실시간 FinBERT 추론
- [ ] WebSocket 스트리밍
- [ ] 대시보드 UI
- [ ] 멀티 종목 비교
- [ ] 소셜 미디어 확장

## 📄 라이선스

MIT License

## 👤 개발자

StockMind Team
