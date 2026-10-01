#!/bin/bash
# 뉴스 파이프라인 전체 실행 스크립트
# 사용법: ./run_pipeline.sh [TICKER]
#   TICKER 미지정 시 전체 8개 종목 실행
#
# 실행 전 venv 활성화:
#   source venv/bin/activate

set -e  # 오류 발생 시 즉시 중단

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)/code"
TICKERS="${1:-AAPL GOOG META TSLA MSFT AMZN NVDA NFLX}"

echo "======================================"
echo "🚀 StockMind 뉴스 파이프라인 시작"
echo "대상 종목: $TICKERS"
echo "======================================"

# 1단계: 주가 데이터 수집
echo ""
echo "📈 [1/6] 주가 데이터 수집 (TwelveData API)..."
python3 "$SCRIPT_DIR/1st_stock_graph.py"
echo "✅ 1단계 완료"

# 2단계: 뉴스 링크 크롤링
echo ""
echo "🔗 [2/6] 뉴스 링크 크롤링..."
python3 "$SCRIPT_DIR/2nd_create_csv_with_link.py"
echo "✅ 2단계 완료"

# 3단계: 뉴스 본문 추출
echo ""
echo "📄 [3/6] 뉴스 본문 추출..."
python3 "$SCRIPT_DIR/3rd_add_content_in_csv.py"
echo "✅ 3단계 완료"

# 4단계: NLP 분석 (DistilBART 요약 + FinBERT 감성 + KeyBERT 키워드)
echo ""
echo "🧠 [4/6] NLP 분석 (요약 / 감성 / 키워드)..."
python3 "$SCRIPT_DIR/4th_analysis.py"
echo "✅ 4단계 완료"

# 5단계: 메타데이터 생성
echo ""
echo "📦 [5/6] 메타데이터 생성..."
python3 "$SCRIPT_DIR/5th_make_metadata.py"
echo "✅ 5단계 완료"

# 6단계: RandomForest 모델 학습
echo ""
echo "🌲 [6/6] RandomForest 모델 학습..."
echo "random" | python3 "$SCRIPT_DIR/train_model.py"
echo "✅ 6단계 완료"

echo ""
echo "======================================"
echo "🎉 파이프라인 완료"
echo "모델 저장 위치: api/models/"
echo "API 재시작 필요: docker-compose restart api"
echo "======================================"
