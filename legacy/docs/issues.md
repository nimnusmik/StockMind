# StockMind 프로젝트 문제점 분석 보고서

> 분석일: 2026-02-23
> 최종 업데이트: 2026-02-23
> 총 발견 문제: 31개 (CRITICAL 14 / HIGH 10 / MEDIUM 7)
> **처리 현황: 29개 완료 / 2개 잔존**

---

## 목차

1. [코드 품질 / 버그](#1-코드-품질--버그)
2. [보안 이슈](#2-보안-이슈)
3. [아키텍처 / 설계](#3-아키텍처--설계)
4. [성능](#4-성능-이슈)
5. [운영 / 배포](#5-운영--배포)
6. [데이터 파이프라인](#6-데이터-파이프라인)
7. [잔존 이슈 목록](#7-잔존-이슈-목록)

---

## 1. 코드 품질 / 버그

### ~~BUG-01~~ ✅ 현재 주가를 하드코딩으로 대체

- **완료:** `api/services/price_service.py` 신규 생성. TwelveData API 연동, Redis 60초 캐싱.
- `signals.py`, `predictions.py`에서 `predicted_price * 0.98` 제거 → 실제 현재가 사용.
- 조회 실패 시 503 반환.

---

### ~~BUG-02~~ ✅ DB 저장 실패 시 롤백 없음

- **완료:** `community/src/crawler.py` 중간 저장(라인 ~251)·최종 저장(라인 ~291) 두 곳에 `self.db_engine.rollback()` 추가.

---

### ~~BUG-03~~ ✅ 미구현 기능들 (TODO 항목)

- **완료:**

| TODO | 구현 내용 |
|------|---------|
| `sentiment_velocity` | `SentimentService.calculate_sentiment_velocity()` — 현재 1h vs 이전 1h 감성 차이 |
| `top_keywords` | `NewsRepository.get_top_keywords()` — feature CSV `keywords` 컬럼 빈도 집계 |
| 댓글 sentiment | `classify_text_sentiment()` — 모듈 레벨 규칙 기반 분류 함수, historical에 적용 |

---

### ~~BUG-04~~ ✅ bare `except`로 모든 예외 무시

- **완료:** `community/src/utils.py` → `except Exception:` 으로 변경.

---

### ~~BUG-05~~ ✅ `logger` 미정의로 NameError 가능

- **완료:** `community/src/utils.py` 상단에 `import logging` + `logger = logging.getLogger(__name__)` 추가.

---

### ~~BUG-06~~ ✅ 시간별 감성 트렌드 계산 오류

- **완료:** `get_sentiment_trend`에서 각 시간대(`hour_start ~ hour_end`)의 댓글만 조회해 `classify_text_sentiment()`로 개별 계산. 시간대별 독립 감성 점수 반환.

---

### ~~BUG-07~~ ✅ 신뢰도 계산의 연산 순서 오류

- **완료:** `confidence = min(100.0, (news_count + (community_count / 10)) / 20 * 100)` — 명시적 괄호 추가.

---

### ~~BUG-08~~ ✅ CSV 중복 저장 race condition

- **파일:** `community/src/crawler.py` (라인 ~227)
- **완료:** `csv_header_written` 플래그를 루프 전 1회만 평가. 이후 저장마다 플래그를 `True`로 고정 → race condition 제거.

---

### ~~BUG-09~~ ✅ `np.argmax(dates)` 날짜 비교 오류

- **완료:** `news/code/train_model.py` → `pd.to_datetime(dates).argmax()` 로 변경.

---

### ~~BUG-10~~ ✅ 크롤러 JS 가비지 컬렉션 가정

- **파일:** `community/src/crawler.py` (라인 ~240)
- **심각도:** MEDIUM
- **완료:** `try/except Exception: pass` 로 감싸 silent fail 처리.

---

## 2. 보안 이슈

### ~~SEC-01~~ ✅ 시크릿 키 Git 저장소에 노출

- **완료:** `.env`가 이미 `.gitignore`에 포함됨 확인. `.env.example` 신규 생성.

---

### ~~SEC-02~~ ✅ DB 비밀번호 소스 코드 하드코딩

- **완료:** `community/src/crawler.py`, `migrate_csv_to_db.py` → `os.getenv('DB_PASSWORD')` 로 변경.

---

### ~~SEC-03~~ ✅ docker-compose.yml에 비밀번호 평문 저장

- **완료:** 모든 자격증명을 `${POSTGRES_USER}`, `${POSTGRES_PASSWORD}`, `${POSTGRES_DB}` 환경 변수 참조로 변경.

---

### ~~SEC-04~~ ✅ 기본 시크릿 키 사용 위험

- **완료:** `api/config.py`에서 `SECRET_KEY` 기본값 제거. `AliasChoices("API_SECRET_KEY", "SECRET_KEY")`로 매핑 — 미설정 시 앱 시작 불가.

---

### ~~SEC-05~~ ✅ API 인증이 관리 엔드포인트에 미적용

- **완료:** 관리 엔드포인트에 `Depends(get_current_user)` 적용:
  - `DELETE /signals/{symbol}/cache`
  - `POST /predictions/preload-models`
  - `POST /predictions/models/reload` (신규)
- 데이터 조회 엔드포인트(GET)는 기존 프론트엔드 호환성 유지를 위해 오픈.

---

### ~~SEC-06~~ ✅ CORS 과도한 허용

- **파일:** `api/main.py` (라인 ~62)
- **완료:** `allow_methods=["GET", "POST", "DELETE"]`, `allow_headers=["Authorization", "Content-Type"]` 으로 제한.

---

### ~~SEC-07~~ ✅ 필수 환경 변수 검증 부재

- **파일:** `api/config.py`, `api/main.py`
- **완료:** `api/main.py` lifespan에서 `DATABASE_URL` 기본값 감지 시 경고 로그 출력. `TWELVEDATA_API_KEY` 미설정 시 경고 출력. `SECRET_KEY`는 SEC-04에서 필수화 완료.

---

### ~~SEC-08~~ ✅ 예외 메시지에서 민감 정보 누출 가능

- **파일:** `api/main.py` (라인 ~104)
- **완료:** 미들웨어 및 전역 핸들러 모두 `{"detail": "Internal server error"}` 고정 반환. 상세 오류는 `log_error()`로 내부 로그에만 기록.

---

## 3. 아키텍처 / 설계

### ~~ARCH-01~~ ✅ 실시간 가격 데이터 소스 없음

- **완료:** BUG-01과 동일. `price_service.py` 로 TwelveData API 연동 완료.

---

### ~~ARCH-02~~ ✅ Repository 직접 인스턴스화 (DI 없음)

- **파일:** `api/services/sentiment_service.py`
- **완료:** `SentimentService.__init__`에 `news_repo: Optional[NewsRepository] = None` 파라미터 추가. 미주입 시 기본 인스턴스 생성, 테스트 시 Mock 주입 가능.

---

### ARCH-03 🟡 싱글톤 모델 레지스트리

- **파일:** `api/ml/model_registry.py`
- **심각도:** MEDIUM
- **내용:** 클래스 변수로 모델 캐싱, 테스트 격리 어려움.
- **참고:** OPS-02 모델 핫 리로드는 완료되어 운영 편의성은 해결됨. 테스트 격리는 잔존.

---

### ~~ARCH-04~~ ✅ requirements.txt 미작성

- **완료:** 루트 `requirements.txt` 버전 고정 (community용), `news/requirements.txt` 신규 생성. `api/requirements.txt`는 기존에 이미 작성됨.

---

### ARCH-05 🟢 에러 복구 메커니즘 미흡 (위험도 낮음 — 유지)

- **파일:** `community/src/crawler.py` (라인 ~321)
- **심각도:** MEDIUM
- **현황:** 3회 재시도 후 실패 시 `None` 반환. 호출자(`collect_comments_optimized`)는 항상 리스트를 반환하므로 None 전파 없음. 실질적 위험 낮아 현행 유지.

---

### ~~ARCH-06~~ ✅ 뉴스 파이프라인 6단계가 완전 수동

- **완료:** `news/run_pipeline.sh` 생성. `set -e`로 중간 실패 시 즉시 중단. 단일 종목 또는 전체 8종목 실행 지원.

---

## 4. 성능 이슈

### ~~PERF-01~~ ✅ 전체 기간 댓글 메모리 로드

- **완료:**
  - `GET /historical/{symbol}/comments` — `get_comments_by_date_range`에 `limit` 파라미터 추가, DB 레벨 LIMIT 적용.
  - `GET /historical/{symbol}/date-range` — `get_date_range()` 메서드(MIN/MAX 단일 집계 쿼리) 추가, 전체 로드 제거.

---

### ~~PERF-02~~ ✅ N+1 쿼리 문제

- **완료:** `CommentRepository.get_comment_counts_batch()` 추가 (GROUP BY 단일 쿼리). `buzz.py`의 8번 루프 쿼리 → 1회 호출로 교체.

---

### ~~PERF-03~~ ✅ 캐시 무효화 전략 없음

- **파일:** `api/routers/predictions.py`
- **완료:** `POST /predictions/models/reload` 엔드포인트에서 모델 재로딩 전 모든 종목의 예측·신호 캐시를 `cache_service.invalidate_symbol_cache(symbol)`로 일괄 무효화.

---

## 5. 운영 / 배포

### ~~OPS-01~~ ✅ 헬스 체크가 실제 서비스 상태 미반영

- **완료:** `GET /health` — SQLAlchemy `SELECT 1` + Redis `ping` 결과 포함. 실패 시 503 반환.

---

### ~~OPS-02~~ ✅ 모델 재학습 후 API 재시작 필요

- **완료:** `POST /api/v1/predictions/models/reload` 신규 추가 (인증 필요).
  - `model_registry.clear_cache()` → `preload_all_models()` 순서로 최신 `.pkl` 파일 반영.
  - `news/run_pipeline.sh` 실행 후 이 엔드포인트 호출로 무중단 갱신 가능.

---

## 6. 데이터 파이프라인

### ~~DATA-01~~ ✅ 크롤러 커트오프 날짜 이후에도 계속 실행

- **파일:** `community/src/crawler.py` (라인 ~170)
- **완료:** `max_consecutive_old` 임계값 조정 (정렬 성공 시 1회, 실패 시 10회). 커트오프 이전 댓글이 연속 감지되면 즉시 조기 break.

---

### ~~DATA-02~~ ✅ 학습 데이터 유효성 검사 부족

- **파일:** `news/code/train_model.py`
- **완료:** `train_and_evaluate`에서 `np.isfinite(X).all(axis=1)` 마스크로 NaN/Inf 샘플 제거. 유효 샘플 0개 시 `ValueError` 발생으로 조기 중단.

---

## 7. 잔존 이슈 목록

완료되지 않은 2개 이슈 요약:

| 이슈 | 심각도 | 영역 | 한 줄 설명 |
|------|--------|------|-----------|
| ARCH-03 | 🟡 MED | 설계 | 싱글톤 레지스트리 테스트 격리 — `clear_cache()` 존재로 운영 영향 없음 |
| ARCH-05 | 🟢 LOW | 설계 | 크롤러 None 반환 — 호출자가 이미 처리, 실질적 위험 낮음 |

---

*완료 이슈는 ~~취소선~~ + ✅ 표기. 이 문서는 코드 수정 완료 기준으로 업데이트됨.*
