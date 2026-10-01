# legacy (미사용, 참고용)

2026-09-29 현재 연구 코드(`../community/`)와 분리해 옮긴 옛 StockMind 앱과 데이터.

- `api/` `frontend/` `news/` `docker-compose.yml` `.env` — 옛 매매신호 웹앱 (FastAPI + Next.js + 뉴스 NLP + RandomForest)
- `community/` — 옛 Playwright 크롤러(OpenWeb iframe 대상, Yahoo 교체 후 0개 수집) 및 venv
- `data_2025/` — 옛 크롤러가 모은 2025년 7월 댓글 CSV
- `docs/` — 옛 문제점 보고서, 대상 사이트 분석
- `CLAUDE_old.md` `README_old.md` — 옛 문서

옛 성능 수치(정확도 75%, MAE 0.24%)는 학습 날짜로 채점·장중 뉴스 누수 때문에 무효.
