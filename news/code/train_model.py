import pandas as pd
import os
import json
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sentence_transformers import SentenceTransformer
from tqdm import tqdm

summary_model = SentenceTransformer('all-MiniLM-L6-v2')
tqdm.pandas()

def load_data(metadata_path):
    with open(metadata_path, "r") as f:
        metadata = json.load(f)

    X_features = []
    Y_labels = []
    date_list = []

    for date, info in metadata.items():
        news_files = info["news"] if isinstance(info["news"], list) else [info["news"]]
        combined_df = pd.DataFrame()

        for file in news_files:
            feature_path = os.path.join("../features", file)
            if not os.path.exists(feature_path):
                continue
            df = pd.read_csv(feature_path)
            combined_df = pd.concat([combined_df, df], ignore_index=True)

        # 필터 조건: 뉴스 없음 or 변동률 없음
        if combined_df.empty or info['rate'] is None:
            continue

        # 1. 뉴스 summary 임베딩 (평균)
        summary_embeddings = summary_model.encode(combined_df['summary'].tolist())
        summary_vector = np.mean(summary_embeddings, axis=0)  # shape: (384,)

        # 2. 감성 분석 결과 (one-hot 인코딩 후 합산)
        # 감정 one-hot → 항상 3차원 고정 (positive, neutral, negative 순서)
        sentiment_encoded = pd.get_dummies(combined_df['sentiment'])

        # 누락된 컬럼 보정
        for sentiment in ['positive', 'neutral', 'negative']:
            if sentiment not in sentiment_encoded.columns:
                sentiment_encoded[sentiment] = 0

        # 고정 순서로 정렬
        sentiment_encoded = sentiment_encoded[['positive', 'neutral', 'negative']].sum().values

        # 3. 키워드 등장 빈도 (5개 고정)
        all_keywords = combined_df['keywords'].str.split(", ").explode()
        keyword_counts = all_keywords.value_counts()
        top_keywords = all_keywords.unique()[:5]  # 5개 고정
        keyword_vector = keyword_counts.reindex(top_keywords, fill_value=0).values  # shape: (5,)

        # 4. 최종 벡터 연결
        full_vector = np.concatenate([summary_vector, sentiment_encoded, keyword_vector])

        X_features.append(full_vector)
        Y_labels.append(info['rate'])
        date_list.append(date)

    return np.array(X_features), np.array(Y_labels), np.array(date_list)


def train_and_evaluate(cache_file, mode):
    X, y, dates = load_data(cache_file)

    if mode == "random":
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    elif mode == "last":
        last_index = np.argmax(dates)
        X_test = X[last_index:last_index+1]
        y_test = y[last_index:last_index+1]
        X_train = np.delete(X, last_index, axis=0)
        y_train = np.delete(y, last_index, axis=0)
    else:
        raise ValueError("Invalid mode. Choose 'random' or 'last'.")

    model = RandomForestRegressor()
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)

    mae = mean_absolute_error(y_test, y_pred)
    print(f"✅ 모델 평가 완료 - Mode: {mode}")
    print(f"실제값 (y_test): {y_test[0]:.2f}%")
    print(f"예측값 (y_pred): {y_pred[0]:.2f}%")
    print(f"MAE: {mae:.4f}%")

if __name__ == "__main__":
    mode = input('mode select (random, last) : ')
    tickers = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
    for ticker in tickers:
        print(f'❗️ {ticker} 대상 analysis 시작')
        cache_file = f'../metadata/{ticker}_metadata.json'
        train_and_evaluate(cache_file, mode)
