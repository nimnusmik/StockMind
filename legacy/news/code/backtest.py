#!/usr/bin/env python3
"""
metadata 기반 오프라인 백테스트
과거 뉴스/가격 데이터로 신호 정확도 검증

사용법:
  cd news/code && python backtest.py
  # 또는
  python news/code/backtest.py  # 프로젝트 루트에서
"""
import os
import sys
import json
import numpy as np
import pandas as pd
import joblib
from pathlib import Path
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

# 프로젝트 루트 기준 경로
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
FEATURES_DIR = SCRIPT_DIR.parent / "features"
METADATA_DIR = SCRIPT_DIR.parent / "metadata"
MODEL_DIR = PROJECT_ROOT / "api" / "models"

# 신호 임계값 (api/config와 동일)
BUY_PRICE_THRESHOLD = 2.0   # +2%
SELL_PRICE_THRESHOLD = -2.0  # -2%
BUY_SENTIMENT_THRESHOLD = 0.3
SELL_SENTIMENT_THRESHOLD = -0.3

TICKERS = ["AAPL", "GOOG", "META", "TSLA", "MSFT", "AMZN", "NVDA", "NFLX"]


def load_backtest_data(metadata_path: Path, features_base: Path):
    """
    metadata + feature CSV에서 백테스트용 데이터 로드
    Returns: list of (date, features, actual_rate, news_sentiment)
    """
    with open(metadata_path, "r") as f:
        metadata = json.load(f)

    summary_model = SentenceTransformer("all-MiniLM-L6-v2")
    samples = []

    for date, info in sorted(metadata.items()):
        news_files = info["news"] if isinstance(info["news"], list) else [info["news"]]
        combined_df = pd.DataFrame()

        for file in news_files:
            feature_path = features_base / file
            if not feature_path.exists():
                continue
            df = pd.read_csv(feature_path)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        if combined_df.empty or info.get("rate") is None:
            continue

        # 1. 뉴스 임베딩 (384차원)
        summary_embeddings = summary_model.encode(combined_df["summary"].tolist())
        summary_vector = np.mean(summary_embeddings, axis=0)

        # 2. 감성 인코딩 (3차원)
        sentiment_encoded = pd.get_dummies(combined_df["sentiment"])
        for s in ["positive", "neutral", "negative"]:
            if s not in sentiment_encoded.columns:
                sentiment_encoded[s] = 0
        sentiment_vec = sentiment_encoded[["positive", "neutral", "negative"]].sum().values

        # 3. 뉴스 감성 점수 (-1 ~ 1) - 신호 결정용
        total = sentiment_vec.sum()
        if total > 0:
            news_sentiment = (sentiment_vec[0] - sentiment_vec[2]) / total
        else:
            news_sentiment = 0.0

        # 4. 키워드 (5차원)
        all_keywords = combined_df["keywords"].str.split(", ").explode()
        keyword_counts = all_keywords.value_counts()
        top_kw = list(all_keywords.unique())[:5]
        keyword_vec = keyword_counts.reindex(top_kw, fill_value=0).values
        if len(keyword_vec) < 5:
            keyword_vec = np.pad(keyword_vec, (0, 5 - len(keyword_vec)), constant_values=0)

        # 5. 392차원 특징
        full_vector = np.concatenate([summary_vector, sentiment_vec, keyword_vec])

        samples.append({
            "date": date,
            "features": full_vector,
            "actual_rate": info["rate"],
            "price": info["price"],
            "news_sentiment": news_sentiment,
        })

    return samples


def determine_signal(predicted_rate: float, composite_sentiment: float) -> str:
    """
    BUY/HOLD/SELL 결정 (api/signal_service와 동일 로직)
    백테스트에서는 커뮤니티 감성 없음 → 뉴스 감성만 사용 (composite = news_sentiment)
    """
    if predicted_rate > BUY_PRICE_THRESHOLD and composite_sentiment > BUY_SENTIMENT_THRESHOLD:
        return "BUY"
    if predicted_rate < SELL_PRICE_THRESHOLD and composite_sentiment < SELL_SENTIMENT_THRESHOLD:
        return "SELL"
    return "HOLD"


def is_signal_correct(signal: str, actual_rate: float) -> bool:
    """
    신호가 실제 결과와 맞는지 판단
    - BUY: 실제 상승 (rate > 0)
    - SELL: 실제 하락 (rate < 0)
    - HOLD: 변동 ±2% 미만
    """
    if signal == "BUY":
        return actual_rate > 0
    if signal == "SELL":
        return actual_rate < 0
    # HOLD
    return abs(actual_rate) < 2.0


def run_backtest(ticker: str, test_ratio: float = 0.2) -> dict:
    """
    단일 종목 백테스트
    test_ratio: 마지막 N%를 테스트 (0.2 = 최근 20% 날짜)
    """
    metadata_path = METADATA_DIR / f"{ticker}_metadata.json"
    model_path = MODEL_DIR / f"{ticker}_rf_model.pkl"

    if not metadata_path.exists():
        return {"error": f"metadata 없음: {metadata_path}"}
    if not model_path.exists():
        return {"error": f"모델 없음: {model_path}"}

    samples = load_backtest_data(metadata_path, FEATURES_DIR)
    if len(samples) < 5:
        return {"error": f"데이터 부족: {len(samples)}일"}

    model = joblib.load(model_path)

    # 마지막 test_ratio 비율만 테스트 (시간순)
    n_test = max(1, int(len(samples) * test_ratio))
    test_samples = samples[-n_test:]

    results = []
    for s in test_samples:
        features = s["features"].reshape(1, -1)
        pred_rate = model.predict(features)[0]
        # 백테스트: 커뮤니티 감성 없음 → 뉴스 감성 = 통합 감성
        composite_sentiment = s["news_sentiment"]
        signal = determine_signal(pred_rate, composite_sentiment)
        correct = is_signal_correct(signal, s["actual_rate"])

        results.append({
            "date": s["date"],
            "signal": signal,
            "predicted_rate": pred_rate,
            "actual_rate": s["actual_rate"],
            "sentiment": composite_sentiment,
            "correct": correct,
        })

    total = len(results)
    correct_count = sum(1 for r in results if r["correct"])
    by_signal = {"BUY": 0, "SELL": 0, "HOLD": 0}
    correct_by_signal = {"BUY": 0, "SELL": 0, "HOLD": 0}

    for r in results:
        by_signal[r["signal"]] += 1
        if r["correct"]:
            correct_by_signal[r["signal"]] += 1

    return {
        "ticker": ticker,
        "total": total,
        "correct": correct_count,
        "accuracy": round(correct_count / total * 100, 1) if total > 0 else 0,
        "by_signal": by_signal,
        "correct_by_signal": correct_by_signal,
        "mae": round(np.mean([abs(r["predicted_rate"] - r["actual_rate"]) for r in results]), 2),
        "details": results,
    }


def main(tickers=None, test_ratio: float = 0.2):
    tickers = tickers or TICKERS
    print("=" * 60)
    print("📊 StockMind metadata 기반 오프라인 백테스트")
    print("=" * 60)
    print(f"features: {FEATURES_DIR}")
    print(f"metadata: {METADATA_DIR}")
    print(f"models:   {MODEL_DIR}")
    print()

    all_results = []
    for ticker in tqdm(tickers, desc="종목"):
        result = run_backtest(ticker, test_ratio=test_ratio)
        all_results.append(result)

        if "error" in result:
            print(f"⚠️  {ticker}: {result['error']}")
            continue

        print(f"\n✅ {ticker}")
        print(f"   테스트: {result['total']}일 | 정확: {result['correct']} | 정확도: {result['accuracy']}%")
        print(f"   MAE: {result['mae']}%")
        print(f"   BUY: {result['correct_by_signal']['BUY']}/{result['by_signal']['BUY']} | "
              f"SELL: {result['correct_by_signal']['SELL']}/{result['by_signal']['SELL']} | "
              f"HOLD: {result['correct_by_signal']['HOLD']}/{result['by_signal']['HOLD']}")

    valid = [r for r in all_results if "error" not in r]
    if valid:
        total_days = sum(r["total"] for r in valid)
        total_correct = sum(r["correct"] for r in valid)
        avg_acc = total_correct / total_days * 100 if total_days > 0 else 0
        avg_mae = np.mean([r["mae"] for r in valid])

        print("\n" + "=" * 60)
        print("📈 전체 요약")
        print("=" * 60)
        print(f"총 테스트: {total_days}일 | 정확: {total_correct} | 평균 정확도: {avg_acc:.1f}%")
        print(f"평균 MAE: {avg_mae:.2f}%")
        print()

    return all_results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="metadata 기반 오프라인 백테스트")
    parser.add_argument("--ticker", type=str, help="단일 종목만 테스트 (예: AAPL)")
    parser.add_argument("--test-ratio", type=float, default=0.2, help="테스트 비율 (기본 0.2 = 최근 20%%)")
    args = parser.parse_args()

    tickers = [args.ticker.upper()] if args.ticker else TICKERS
    if args.ticker and tickers[0] not in TICKERS:
        print(f"⚠️  지원 종목: {TICKERS}")
        sys.exit(1)

    main(tickers=tickers, test_ratio=args.test_ratio)
