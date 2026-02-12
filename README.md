# StockMind - AI Stock Trading Signal Platform

<div align="center">

**커뮤니티 감성 + 뉴스 분석 → AI 매매 신호**

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://www.python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-green.svg)](https://fastapi.tiangolo.com)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue.svg)](https://www.postgresql.org)
[![Redis](https://img.shields.io/badge/Redis-7-red.svg)](https://redis.io)

</div>

## 📋 프로젝트 개요

StockMind는 Yahoo Finance 커뮤니티 감성과 금융 뉴스 분석을 결합하여 **AI 기반 매매 신호(BUY/HOLD/SELL)**를 제공하는 플랫폼입니다.

**핵심 기능:**
- 🤖 RandomForest 기반 가격 예측 (MAE 0.24~1.27%)
- 📊 이중 감성 분석 (뉴스 60% + 커뮤니티 40%)
- 🎯 자동 매매 신호 생성 (BUY/HOLD/SELL)
- 📈 커뮤니티 버즈 지표 (활동량, 감성 속도)
- 🚀 REST API (FastAPI + Redis 캐싱)

**대상 종목:** AAPL, GOOG, META, TSLA, MSFT, AMZN, NVDA, NFLX

## 🏗️ 아키텍처

```
┌──────────────────────────────────────────────────────────┐
│                    FastAPI Backend                       │
│                  (localhost:8001)                        │
├──────────────────────────────────────────────────────────┤
│  /api/v1/signals/{symbol}      → BUY/HOLD/SELL 신호    │
│  /api/v1/predictions/{symbol}  → 가격 예측             │
│  /api/v1/sentiment/{symbol}    → 감성 분석             │
│  /api/v1/buzz/{symbol}         → 커뮤니티 활동 지표     │
└──────────────────────────────────────────────────────────┘
                    ↓           ↓           ↓
         ┌──────────────┐  ┌─────────┐  ┌───────────────┐
         │ PostgreSQL   │  │  Redis  │  │  ML Models    │
         │ (12,969개    │  │ (캐싱)  │  │  (8 stocks)   │
         │  댓글)       │  │         │  │  .pkl files   │
         └──────────────┘  └─────────┘  └───────────────┘
```

## 🚀 빠른 시작

### 1. 전체 시스템 실행

```bash
# 모든 서비스 시작 (PostgreSQL, Redis, FastAPI)
docker-compose up -d

# API 문서 접속
open http://localhost:8001/docs
```

### 2. API 테스트

```bash
# 매매 신호 조회
curl http://localhost:8001/api/v1/signals/AAPL | jq

# 가격 예측 조회
curl http://localhost:8001/api/v1/predictions/TSLA/price | jq

# 복합 감성 분석
curl http://localhost:8001/api/v1/sentiment/NVDA/combined | jq
```

### 3. 커뮤니티 크롤러 실행

```bash
cd community
python3 src/main.py
```

### 4. ML 모델 학습

```bash
cd news/code
echo "last" | python3 train_model.py
```

## 📊 API 엔드포인트

### 매매 신호 (핵심)

**`GET /api/v1/signals/{symbol}`**

```json
{
  "signal": "BUY",
  "strength": "MODERATE",
  "confidence": 68.3,
  "predicted_change_pct": 2.5,
  "sentiment_score": 0.42,
  "supporting_factors": ["예측 변동률 2.5%", "긍정 감성 우세"]
}
```

**신호 알고리즘:**
- `BUY`: 예측 상승률 >+2% AND 감성 점수 >0.3
- `SELL`: 예측 하락률 <-2% AND 감성 점수 <-0.3
- `HOLD`: 그 외

### 기타 주요 엔드포인트

| 엔드포인트 | 설명 | 캐시 TTL |
|-----------|------|----------|
| `/predictions/{symbol}/price` | ML 가격 예측 | 5분 |
| `/sentiment/{symbol}/community` | 커뮤니티 감성 | 1시간 |
| `/sentiment/{symbol}/news` | 뉴스 감성 | 24시간 |
| `/sentiment/{symbol}/combined` | 복합 감성 | 1시간 |
| `/buzz/{symbol}` | 활동 지표 | 15분 |
| `/historical/{symbol}/comments` | 댓글 이력 | - |

## 📁 프로젝트 구조

```
StockMind/
├── api/                          # FastAPI 백엔드
│   ├── main.py                   # 앱 엔트리
│   ├── routers/                  # API 라우터
│   ├── services/                 # 비즈니스 로직
│   ├── ml/                       # ML 모델 레지스트리
│   └── models/                   # 학습된 .pkl 파일
│
├── community/                    # 커뮤니티 크롤러
│   ├── src/
│   │   ├── crawler.py           # Yahoo Finance 크롤러
│   │   ├── main.py              # 실행 파일
│   │   └── migrate_csv_to_db.py # CSV → DB 마이그레이션
│   └── init_db.sql              # DB 스키마
│
├── news/                         # 뉴스 분석 파이프라인
│   └── code/
│       ├── 1st_stock_graph.py   # 주가 데이터 수집
│       ├── 2nd_create_csv_with_link.py  # 뉴스 링크 크롤링
│       ├── 3rd_add_content_in_csv.py    # 본문 추출
│       ├── 4th_analysis.py      # NLP 분석
│       ├── 5th_make_metadata.py # 메타데이터 생성
│       └── train_model.py       # ML 모델 학습
│
├── docker-compose.yml           # 통합 Docker 설정
└── README.md
```

## 🔧 기술 스택

| 카테고리 | 기술 |
|---------|------|
| **Backend** | FastAPI, Uvicorn |
| **Database** | PostgreSQL 15, SQLAlchemy |
| **Cache** | Redis 7 |
| **ML** | scikit-learn (RandomForest), sentence-transformers |
| **NLP** | FinBERT, DistilBART, KeyBERT |
| **Crawler** | Playwright, BeautifulSoup |
| **Deploy** | Docker, Docker Compose |

## 📈 ML 모델 성능

| 종목 | MAE (%) | 특징 차원 | 학습 데이터 |
|-----|---------|----------|-----------|
| GOOG | 0.24 | 392D | 49일 |
| NVDA | 0.61 | 392D | 65일 |
| AAPL | 0.48 | 392D | 62일 |
| NFLX | 0.46 | 392D | 58일 |
| TSLA | 0.54 | 392D | 51일 |
| META | 0.64 | 392D | 59일 |
| AMZN | 0.94 | 392D | 64일 |
| MSFT | 1.27 | 392D | 63일 |

**특징 구성:** 384D 임베딩 + 3D 감성 + 5D 키워드 = 392D

## 💾 데이터

### 커뮤니티 댓글 (PostgreSQL)

```sql
SELECT stock_symbol, COUNT(*) FROM comments GROUP BY stock_symbol;
```

| 종목 | 댓글 수 | 기간 |
|-----|--------|------|
| AMZN | 2,404 | 2025-05 ~ 2025-07 |
| GOOG | 2,348 | 2025-05 ~ 2025-07 |
| NVDA | 1,810 | 2025-07 |
| MSFT | 1,769 | 2025-04 ~ 2025-07 |
| TSLA | 1,678 | 2025-08 |
| META | 1,646 | 2025-05 ~ 2025-07 |
| NFLX | 1,279 | 2025-05 ~ 2025-07 |
| AAPL | 35 | 2025-07 |

**총 12,969개 댓글**

### 뉴스 데이터

- 8개 종목 × 49~65일 = 약 460개 뉴스 데이터셋
- 각 데이터: 요약, 감성 점수, 키워드, 임베딩 벡터

## 🔐 환경 설정

### 필수 환경 변수 (api/.env)

```bash
DATABASE_URL=postgresql://user:password@localhost:5433/stockmind
REDIS_URL=redis://localhost:6379/0
API_SECRET_KEY=your-secret-key-here
```

### Docker 포트 매핑

- **PostgreSQL**: `localhost:5433` → `container:5432`
- **Redis**: `localhost:6379`
- **FastAPI**: `localhost:8001` → `container:8000`

## 📖 사용 예시

### Python 클라이언트

```python
import requests

# 매매 신호 조회
response = requests.get("http://localhost:8001/api/v1/signals/AAPL")
signal = response.json()

if signal["signal"] == "BUY" and signal["confidence"] > 70:
    print(f"🚀 강력한 매수 신호! 신뢰도: {signal['confidence']}%")
    print(f"예측 상승률: {signal['predicted_change_pct']}%")
```

### cURL

```bash
# 여러 종목 동시 조회
for symbol in AAPL GOOG TSLA NVDA; do
  echo "=== $symbol ==="
  curl -s "http://localhost:8001/api/v1/signals/$symbol" | jq '.signal, .confidence'
done
```

## 🛠️ 개발 가이드

### 로컬 개발 환경

```bash
# API 개발
cd api
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
uvicorn main:app --reload --port 8001

# 크롤러 개발
cd community
pip install -r requirements.txt
python3 src/main.py
```

### DB 마이그레이션 실행

```bash
# API 테이블 생성
docker exec stockmind-db psql -U user -d stockmind -f /app/migrations/001_api_tables.sql
```

### 새로운 종목 추가

1. `community/src/config.py`에 종목 추가
2. `news/code/` 파이프라인 실행
3. `train_model.py`로 모델 학습
4. API 재시작

## ⚠️ 주의사항

- **교육/연구 목적**: 이 프로젝트는 실제 투자 조언이 아닙니다
- **데이터 최신성**: 현재 데이터는 2025년 중반 기준으로 historical data입니다
- **크롤링 정책**: Yahoo Finance 이용약관을 준수하세요
- **리스크 관리**: 실제 투자 시 본인의 판단과 리스크 관리가 필요합니다

## 📝 라이센스

MIT License - 자세한 내용은 [LICENSE](LICENSE) 파일 참조

## 🙏 기여

이슈와 풀 리퀘스트는 언제나 환영합니다!

---

<div align="center">
Made with ❤️ by StockMind Team
</div>
