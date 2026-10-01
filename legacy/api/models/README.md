# ML Models Directory

이 디렉토리에는 종목별 RandomForest 가격 예측 모델이 저장됩니다.

## 모델 준비

### 1. 뉴스 파이프라인에서 모델 학습

```bash
cd ../news
python train_model.py
```

학습 완료 후 `news/models/` 디렉토리에 모델 파일들이 생성됩니다:
- `AAPL_rf_model.pkl`
- `GOOG_rf_model.pkl`
- `META_rf_model.pkl`
- `TSLA_rf_model.pkl`
- `MSFT_rf_model.pkl`
- `AMZN_rf_model.pkl`
- `NVDA_rf_model.pkl`
- `NFLX_rf_model.pkl`

### 2. 모델 파일 복사

```bash
cp ../news/models/*.pkl ./
```

또는 Docker 볼륨으로 마운트 (docker-compose.yml 참조).

## 모델 정보

- **모델 타입**: RandomForestRegressor
- **입력 특징**: 392차원
  - 뉴스 요약 임베딩: 384차원 (SentenceTransformer)
  - 감성 인코딩: 3차원 (positive/neutral/negative)
  - 키워드 빈도: 5차원
- **학습 데이터**: 62-67일 뉴스 데이터
- **출력**: 다음날 주가 예측

## 파일 구조

```
models/
├── README.md           # 이 파일
├── AAPL_rf_model.pkl   # Apple 모델
├── GOOG_rf_model.pkl   # Google 모델
├── META_rf_model.pkl   # Meta 모델
├── TSLA_rf_model.pkl   # Tesla 모델
├── MSFT_rf_model.pkl   # Microsoft 모델
├── AMZN_rf_model.pkl   # Amazon 모델
├── NVDA_rf_model.pkl   # NVIDIA 모델
└── NFLX_rf_model.pkl   # Netflix 모델
```

## 모델 업데이트

모델을 재학습한 후:

1. 새 모델 파일을 이 디렉토리에 복사
2. API 서버 재시작 또는 `/api/v1/predictions/preload-models` 엔드포인트 호출

```bash
# 로컬 개발
curl -X POST http://localhost:8000/api/v1/predictions/preload-models

# Docker
docker-compose restart api
```

## 주의사항

⚠️ 모델 파일은 Git에 커밋하지 마세요 (.gitignore에 추가)
⚠️ 프로덕션 환경에서는 모델 버전 관리 필요 (MLflow 등)
