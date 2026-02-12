# StockMind API - 빠른 시작 가이드

## 🚀 5분 안에 시작하기

### 1단계: 환경 변수 설정

```bash
cp .env.example .env
```

`.env` 파일을 열고 시크릿 키를 변경하세요:
```bash
API_SECRET_KEY=your-secure-random-key-here
```

시크릿 키 생성 (선택):
```bash
openssl rand -hex 32
```

### 2단계: ML 모델 준비

```bash
# 뉴스 파이프라인에서 모델 학습
cd news
python train_model.py

# 모델 파일을 API 디렉토리로 복사
cp models/*.pkl ../api/models/

cd ..
```

**참고**: 모델 학습은 시간이 걸릴 수 있습니다. 테스트를 위해 이 단계를 건너뛰고 나중에 실행할 수 있습니다.

### 3단계: Docker로 모든 서비스 시작

```bash
docker-compose up -d
```

이 명령어로 다음 서비스가 시작됩니다:
- ✅ PostgreSQL (포트 5432)
- ✅ Redis (포트 6379)
- ✅ FastAPI 서버 (포트 8000)

### 4단계: API 테스트

#### 헬스 체크
```bash
curl http://localhost:8000/health
```

**예상 응답:**
```json
{"status": "healthy", "timestamp": 1739368800.0}
```

#### API 정보
```bash
curl http://localhost:8000/api/v1
```

#### 매매 신호 조회
```bash
curl http://localhost:8000/api/v1/signals/AAPL | jq
```

**예상 응답:**
```json
{
  "symbol": "AAPL",
  "signal": "BUY",
  "strength": "STRONG",
  "confidence": 78.5,
  "predicted_price": 218.20,
  "supporting_factors": [...]
}
```

### 5단계: Swagger UI에서 탐색

브라우저에서 열기:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## 📊 주요 API 엔드포인트

### 매매 신호 (핵심)
```bash
GET /api/v1/signals/{symbol}
```
BUY/HOLD/SELL 신호 및 신뢰도

### 통합 감성 분석
```bash
GET /api/v1/sentiment/{symbol}/combined
```
뉴스 + 커뮤니티 이중 감성

### 가격 예측
```bash
GET /api/v1/predictions/{symbol}/price
```
ML 모델 기반 가격 예측

### 커뮤니티 버즈
```bash
GET /api/v1/buzz/{symbol}
```
댓글 볼륨, 트렌드, 이상 활동

---

## 🔍 로그 확인

```bash
# API 서버 로그
docker-compose logs -f api

# 데이터베이스 로그
docker-compose logs -f db

# Redis 로그
docker-compose logs -f redis

# 모든 로그
docker-compose logs -f
```

---

## 🛑 서비스 중지

```bash
docker-compose down
```

데이터도 함께 삭제:
```bash
docker-compose down -v
```

---

## 🔄 서비스 재시작

```bash
docker-compose restart api
```

---

## 💡 유용한 명령어

### 8개 종목 모두 신호 확인
```bash
for symbol in AAPL GOOG META TSLA MSFT AMZN NVDA NFLX; do
  echo "=== $symbol ==="
  curl -s "http://localhost:8000/api/v1/signals/$symbol" | jq -r '.signal'
done
```

### Redis 캐시 확인
```bash
docker exec -it stockmind-redis redis-cli

# Redis 셸에서
KEYS *
TTL signal:AAPL
GET signal:AAPL
```

### PostgreSQL 데이터 확인
```bash
docker exec -it stockmind-db psql -U user -d stockmind

# PostgreSQL 셸에서
\dt                                    # 테이블 목록
SELECT COUNT(*) FROM comments;        # 댓글 수
SELECT * FROM signal_history LIMIT 5; # 신호 이력
```

### 성능 테스트
```bash
# Apache Bench로 부하 테스트
ab -n 100 -c 10 http://localhost:8000/api/v1/signals/AAPL
```

---

## ❓ 문제 해결

### "모델을 찾을 수 없습니다" 에러

모델 파일이 없는 경우입니다:

```bash
cd news
python train_model.py
cp models/*.pkl ../api/models/
docker-compose restart api
```

### 데이터베이스 연결 실패

```bash
# PostgreSQL 상태 확인
docker-compose ps db

# 재시작
docker-compose restart db

# 로그 확인
docker-compose logs db
```

### Redis 연결 실패

```bash
# Redis 상태 확인
docker-compose ps redis

# 재시작
docker-compose restart redis
```

### 포트 이미 사용 중

포트를 변경하려면 `docker-compose.yml` 수정:

```yaml
services:
  api:
    ports:
      - "8001:8000"  # 8001로 변경
```

---

## 📚 더 알아보기

- **API 상세 문서**: `api/README.md`
- **구현 계획**: `docs/target-site/API-Plan.md`
- **구현 요약**: `docs/target-site/API-Implementation-Summary.md`
- **프로젝트 가이드**: `CLAUDE.md`

---

## 🎯 다음 단계

1. ✅ API 테스트 완료
2. [ ] 실제 데이터로 ML 모델 학습
3. [ ] 프론트엔드 개발 (대시보드)
4. [ ] 프로덕션 배포 준비

---

**즐거운 트레이딩 되세요! 📈**
