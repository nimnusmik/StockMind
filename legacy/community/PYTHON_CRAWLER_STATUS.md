# Python Playwright 크롤러 실행 상태

## ✅ 전환 완료

TypeScript 크롤러에서 Python Playwright 크롤러로 성공적으로 전환하였습니다.

## 실행 환경 설정

### 1. Python 가상환경
```bash
cd /Users/sunminkim/Desktop/projects/StockMind/community
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium
```

### 2. PostgreSQL 데이터베이스
```bash
# 로컬 PostgreSQL 사용 (port 5432)
psql -U sunminkim -h localhost -d postgres

# 사용자 및 데이터베이스 생성
CREATE USER "user" WITH PASSWORD 'password';
CREATE DATABASE stockmind OWNER "user";

# 스키마 초기화
psql -U user -h localhost -d stockmind -f init_db.sql
```

### 3. 크롤러 설정 수정

**crawler_dblinked.py 수정사항:**

1. **DB 호스트 환경변수 지원 추가:**
```python
# Docker 또는 로컬 PostgreSQL 자동 감지
db_host = os.getenv('DB_HOST', 'db')
```

2. **DB 스키마 수정 (올바른 컬럼명으로 변경):**
```python
# 기존 (잘못됨):
INSERT INTO comments (id, symbol, timestamp, user, content)

# 수정 (정확함):
INSERT INTO comments (stock_symbol, comment_time, comment_text, comment_hash)
```

3. **충돌 처리 개선:**
```python
ON CONFLICT (stock_symbol, comment_time, comment_hash) DO NOTHING
```

## 크롤러 실행

### 실행 명령어
```bash
cd /Users/sunminkim/Desktop/projects/StockMind/community
DB_HOST=localhost source venv/bin/activate && python src/main_dblinked.py
```

### 실행 옵션
- **headless=False**: 브라우저 창 표시 (디버깅용)
- **headless=True**: 백그라운드 실행 (프로덕션)

## 수집 진행 상황

### AAPL (현재 실행 중)
- **수집 댓글 수**: 405+ 개
- **수집 기간**: Feb 12, 2026 → Feb 5, 2026 (현재)
- **목표 기간**: Aug 1, 2025까지
- **상태**: 진행 중 ✅

### 전체 대상 종목 (8개)
```
AAPL, GOOG, META, TSLA, MSFT, AMZN, NVDA, NFLX
```

## 크롤러 동작 방식

### 1. 페이지 로딩
```
https://finance.yahoo.com/quote/{SYMBOL}/community/
```

### 2. iframe 탐지
```
openweb.jac.yahoosandbox.com/2.0.0/safeframe.html
```

### 3. 정렬 변경
- "Sort by" 버튼 클릭
- "Newest" 옵션 선택
- 최신 댓글부터 수집

### 4. 댓글 수집 루프
```
while True:
    1. 현재 표시된 댓글 파싱
    2. cutoff_date 이전 댓글이면 중단
    3. "Show More Comments" 버튼 클릭
    4. 새 댓글 로딩 대기
    5. 반복
```

### 5. 데이터 저장
- **CSV**: `data/{SYMBOL}_comments_{날짜}_temp_{개수}.csv`
- **PostgreSQL**: `comments` 테이블
- **배치 크기**: 100개마다 DB 저장

## 로그 파일

### 위치
```
/Users/sunminkim/Desktop/AIStages/StockMind/community/logs/{SYMBOL}/
```

### 최근 로그
```
logs/AAPL/20260212_1429.log (35KB, 진행 중)
```

### 로그 내용
- 페이지 로딩 상태
- iframe 탐지 결과
- 댓글 수집 진행률
- "Show More" 버튼 클릭 결과
- 데이터 저장 메시지
- 에러 및 경고

## CSV 파일 출력

### 최근 파일
```
data/AAPL_comments_202602_temp_1700.csv (166KB, 2347 rows)
```

### CSV 형식
```csv
time,text,stock_symbol
"12 Feb, 2026 10:30 AM","Comment text here...",AAPL
```

## 데이터베이스 스키마

```sql
CREATE TABLE comments (
    id SERIAL PRIMARY KEY,
    stock_symbol VARCHAR(10) NOT NULL,
    comment_time TIMESTAMP WITH TIME ZONE NOT NULL,
    comment_text TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    comment_hash TEXT NOT NULL,
    UNIQUE (stock_symbol, comment_time, comment_hash)
);

CREATE INDEX idx_stock_time ON comments (stock_symbol, comment_time DESC);
```

### 중복 제거
- `comment_hash`: MD5 hash of comment text
- UNIQUE constraint: `(stock_symbol, comment_time, comment_hash)`
- 동일한 댓글은 자동으로 skip

## 성능 및 안정성

### 장점 ✅
1. **실제 브라우저 사용**: Yahoo Finance의 JavaScript를 정상 실행
2. **동적 iframe 처리**: Playwright가 iframe 자동 감지
3. **WAF 우회**: 실제 브라우저이므로 bot 감지 우회
4. **안정적인 클릭**: "Show More" 버튼 자동 클릭
5. **User-Agent 로테이션**: 차단 방지

### 속도
- **초당 처리**: 약 3-5개 댓글
- **AAPL 전체**: 약 5-10분 예상 (cutoff까지)
- **전체 8종목**: 약 40-80분 예상

## 모니터링

### 실시간 로그 확인
```bash
tail -f /Users/sunminkim/Desktop/AIStages/StockMind/community/logs/AAPL/20260212_1429.log
```

### 데이터베이스 확인
```bash
psql -U user -h localhost -d stockmind -c \
  "SELECT stock_symbol, COUNT(*) as count FROM comments GROUP BY stock_symbol;"
```

### CSV 파일 확인
```bash
ls -lht /Users/sunminkim/Desktop/AIStages/StockMind/community/data/*.csv | head
```

## 문제 해결

### Yahoo Finance 변경 시
- iframe 셀렉터 변경 가능 → `utils.py` 수정
- "Show More" 버튼 텍스트 변경 → 다중 패턴 시도 (이미 구현됨)
- "Sort by" 위치 변경 → 셀렉터 업데이트

### 속도 개선
- `headless=True`: 백그라운드 실행으로 리소스 절약
- 여러 인스턴스 병렬 실행 (종목별)
- 프록시 사용 (옵션)

### 데이터 검증
```bash
# 중복 체크
psql -U user -h localhost -d stockmind -c \
  "SELECT stock_symbol, comment_time, comment_text, COUNT(*)
   FROM comments
   GROUP BY stock_symbol, comment_time, comment_text
   HAVING COUNT(*) > 1;"

# 날짜 범위 확인
psql -U user -h localhost -d stockmind -c \
  "SELECT stock_symbol,
          MIN(comment_time) as earliest,
          MAX(comment_time) as latest,
          COUNT(*) as total
   FROM comments
   GROUP BY stock_symbol;"
```

## 다음 단계

### 1. 크롤러 완료 대기
현재 실행 중인 크롤러가 모든 종목을 완료할 때까지 대기

### 2. 데이터 검증
- CSV 파일 개수 및 크기 확인
- 데이터베이스 레코드 수 확인
- 날짜 범위 검증

### 3. 자동화 설정 (선택)
- Airflow DAG 수정 (현재 비활성)
- Cron job 설정
- Docker 컨테이너 재설정

### 4. 데이터 분석 파이프라인 연결
- news 모듈과 통합
- ML 모델 학습 준비

## 비교: TypeScript vs Python

| 항목 | TypeScript | Python Playwright |
|------|-----------|-------------------|
| 구현 | ✅ 완료 | ✅ 완료 및 실행 중 |
| Yahoo 접근 | ❌ 429/404 | ✅ 성공 |
| 브라우저 | Axios (HTTP only) | Playwright (실제 브라우저) |
| iframe 처리 | ❌ 불가 | ✅ 자동 처리 |
| JavaScript 실행 | ❌ 없음 | ✅ 전체 실행 |
| 데이터 수집 | 0개 (차단됨) | 405+ 개 (진행 중) |
| 프로덕션 사용 | ⚠️ Puppeteer 필요 | ✅ 즉시 사용 가능 |

## 결론

Python Playwright 크롤러가 성공적으로 작동하고 있으며, Yahoo Finance에서 실제 데이터를 수집하고 있습니다.

- ✅ DB 스키마 수정 완료
- ✅ 로컬 PostgreSQL 연결 성공
- ✅ AAPL 댓글 405+ 개 수집 중
- ✅ CSV 및 DB 저장 기능 작동
- ✅ 자동 "Show More" 클릭 작동

**TypeScript 크롤러는 참고용 구현**으로 유지하고, **프로덕션에서는 Python Playwright 크롤러**를 사용하는 것을 권장합니다.
