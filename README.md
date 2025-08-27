# Yahoo Finance Community Sentiment Stock Prediction

## 프로젝트 개요

이 프로젝트는 야후 파이낸스의 커뮤니티 댓글을 분석하여 주식 가격의 상승/하락을 예측하는 머신러닝 프로젝트입니다. 대형 기술주 8개 종목의 커뮤니티 감정을 실시간으로 수집하고 분석하여 투자 인사이트를 제공합니다.

## 대상 종목

```python
stocks = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
```

- **AAPL**: Apple Inc.
- **GOOG**: Alphabet Inc.
- **META**: Meta Platforms Inc.
- **TSLA**: Tesla Inc.
- **MSFT**: Microsoft Corporation
- **AMZN**: Amazon.com Inc.
- **NVDA**: NVIDIA Corporation
- **NFLX**: Netflix Inc.

## 데이터 수집 구조

### 수집 데이터 형식
```csv
time,text,stock_symbol
"12 Jul, 2025 11:48 PM","Under Cook - Apple car epic fail - AI epic fail - Apple maps (2012), Apple Vision Pro's Lack of Traction (2024)","AAPL"
```

### 데이터 필드
- **time**: 댓글 작성 시간
- **text**: 댓글 내용
- **stock_symbol**: 해당 종목 심볼

## 프로젝트 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Yahoo Finance │    │   Data Pipeline │    │   ML Pipeline   │
│   Web Scraping  │───▶│   (Airflow)     │───▶│   Prediction    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw CSV Data  │    │   Database      │    │   Prediction    │
│                 │    │   (PostgreSQL)  │    │   Results API   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 기술 스택

### 데이터 수집
- **Python**: 웹 스크래핑 및 데이터 처리
- **BeautifulSoup/Selenium**: Yahoo Finance 댓글 크롤링
- **Pandas**: 데이터 전처리

### 데이터베이스
- **PostgreSQL**: 댓글 데이터 저장
- **SQLAlchemy**: ORM

### 자동화 및 파이프라인
- **Apache Airflow**: 데이터 수집 및 모델링 자동화
- **Docker**: 컨테이너화

### 머신러닝
- **Scikit-learn**: 기본 ML 모델
- **Transformers**: 자연어 처리 (BERT, RoBERTa)
- **TensorFlow/PyTorch**: 딥러닝 모델

### API 및 서빙
- **FastAPI**: 예측 결과 API
- **Redis**: 캐싱
- **Grafana**: 모니터링 대시보드

## 프로젝트 단계

### Phase 1: 데이터 수집 및 저장 (완료)
- [x] Yahoo Finance 댓글 크롤링
- [x] CSV 파일로 초기 데이터 저장
- [x] 데이터베이스 마이그레이션

### Phase 2: 자동화 파이프라인 구축 (진행 중)
- [ ] Airflow DAG 설계
- [ ] 일일 데이터 수집 자동화
- [ ] 데이터 품질 검증
- [ ] 에러 핸들링 및 알림

### Phase 3: 머신러닝 모델 개발
- [ ] 텍스트 전처리 및 특성 추출
- [ ] 감성 분석 모델 구축
- [ ] 주가 예측 모델 개발
- [ ] 모델 성능 평가 및 최적화

### Phase 4: 서빙 시스템 구축
- [ ] 실시간 예측 API 개발
- [ ] 웹 대시보드 구축
- [ ] 알림 시스템 구현

## 설치 및 실행

### 환경 설정
```bash
# 저장소 클론
git clone https://github.com/your-username/yahoo-finance-stock-prediction.git
cd yahoo-finance-stock-prediction

# 가상환경 생성 및 활성화
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 또는 venv\Scripts\activate  # Windows

# 의존성 설치
pip install -r requirements.txt
```

### 데이터베이스 설정
```bash
# PostgreSQL 설치 및 설정
createdb stock_prediction_db

# 환경 변수 설정
export DATABASE_URL="postgresql://username:password@localhost/stock_prediction_db"
```

### 실행
```bash
# 데이터 수집
python scripts/scrape_comments.py

# Airflow 시작
airflow webserver --port 8080
airflow scheduler
```

## 라이센스

이 프로젝트는 MIT 라이센스 하에 있습니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

## 주의사항

- 이 프로젝트는 교육 및 연구 목적으로만 사용되어야 합니다
- 실제 투자 결정에 사용하기 전에 충분한 검증이 필요합니다
- Yahoo Finance의 이용약관을 준수하여 크롤링을 수행하세요
- 예측 결과는 투자 조언이 아닙니다
