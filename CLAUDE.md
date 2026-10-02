# CLAUDE.md

## 프로젝트 개요

Yahoo Finance 커뮤니티 댓글(사람 군중)이 다음 날 주가(변동성·방향)를 예측하는지 정직하게 검증하는 연구 프로젝트.
최종 목표: 사람 군중 vs AI 군중(LLM 에이전트 여러 개) 비교.

## 구조

- **community/** — 현재 쓰는 코드와 데이터 (이 경로는 launchd가 참조하므로 옮기지 말 것)
  - `collect.py` — 커뮤니티 수집기. Yahoo 자체 커뮤니티 GraphQL(`GetContentByAssociatedContentId`, 로그인 불필요) → SQLite `data/community.db`. 7/1까지 backfill 후 증분.
    launchd `com.sunmin.stockmind`가 매시 23분 실행, `data/.collect.lock`으로 중복 실행 방지. 로그 `data/collect.log`.
  - `feed_query.graphql` — 수집기가 쓰는 GraphQL 쿼리
  - `prices.py` — yfinance 일별 주가 (수집 종목 + SPY) → `data/prices.csv`
  - `minute_bars.py` — 1분 봉 누적 → `data/prices_1m.csv.gz` (yfinance가 30일치만 줘서 매일 받음). launchd `com.sunmin.stockmind.minute` 매일 14:47, 로그 `data/minute.log`
  - `sentiment.py` — 글별 감정(cardiffnlp twitter-roberta) → `data/sentiment.csv`. torch가 커서 `uv run --no-project --with transformers --with torch --with pandas python sentiment.py`
  - `exp1_burst*.py`, `exp2_sentiment.py`, `exp3_resolution.py` — 실험 (Morstatter 2026 "The Conversation Turns First"의 주식판). 성능 판단은 시간 분할 결과 기준
  - `data/export/` — 엑셀용 CSV 스냅샷
- **docs/** — 교수 컨택 자료
- **legacy/** — 옛 StockMind 앱(FastAPI·Next.js·뉴스 파이프라인·Playwright 크롤러)과 2025년 7월 데이터. 현재 미사용, 참고용.
  옛 성능 수치(정확도 75% 등)는 데이터 누수로 무효.

## 실행

```bash
cd community
python3 collect.py              # 15종목 증분 수집
python3 collect.py GME AMC      # 일부만
python3 collect.py --summary    # 종목별 개수
python3 prices.py               # 주가 갱신
```

## 규칙

- 종목: 초대형 기술주 8개(AAPL GOOG META TSLA MSFT AMZN NVDA NFLX) + 개인투자자 인기주 7개(GME AMC PLTR SOFI RIVN COIN HOOD)
- DB 시간(`created_at`)은 UTC. 하루 경계는 뉴욕 장마감(16:00 ET) 기준으로 끊어 미래 정보 누수를 막는다.
- 성능 주장은 시간 분할, t→t+1, 기준선("항상 HOLD", 랜덤) 비교 후에만.
- 비공식 API라 요청 간격을 둔다(`DELAY`). 원본 댓글 데이터는 공개 저장소에 올리지 않는다(`data/`는 .gitignore).
- 코드 주석·로그는 한국어.
