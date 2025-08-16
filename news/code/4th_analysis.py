import pandas as pd
from transformers import pipeline
from transformers.utils import logging
import textwrap
import re
import os
from keybert import KeyBERT

logging.set_verbosity_error()

kw_model = KeyBERT()

def summarize_articles(df, text_col='content', chunk_size=1000, max_len=130, min_len=30):
    summarizer = pipeline("summarization", model="sshleifer/distilbart-cnn-12-6")
    def summarize_text(text):
        if pd.isna(text) or len(text.strip()) == 0:
            return ""
        chunks = textwrap.wrap(text, chunk_size)
        summaries = []
        for chunk in chunks:
            try:
                summary = summarizer(chunk, max_length=max_len, min_length=min_len, do_sample=False)[0]['summary_text']
                summaries.append(summary)
            except Exception as e:
                summaries.append(f"[요약 오류: {e}]")
        return " ".join(summaries)
    df['summary'] = df[text_col].apply(summarize_text)
    return df

def analyze_sentiment(df, summary_col='summary'):
    sentiment_pipeline = pipeline("sentiment-analysis", model="ProsusAI/finbert")
    def get_sentiment(text):
        if pd.isna(text) or len(text.strip()) == 0:
            return "unknown"
        try:
            result = sentiment_pipeline(text[:512])[0]
            return result['label'].lower()
        except Exception as e:
            return f"error: {e}"
    df['sentiment'] = df[summary_col].apply(get_sentiment)
    return df

def extract_keywords(df, summary_col='summary', top_k=5):
    def get_keywords(text):
        if pd.isna(text) or len(text.strip()) == 0:
            return ""
        keywords = kw_model.extract_keywords(text, top_n=top_k)
        return ", ".join([kw for kw, _ in keywords])
    df['keywords'] = df[summary_col].apply(get_keywords)
    return df

def process_news_file(input_file):
    df = pd.read_csv(input_file)
    df.reset_index(drop=True, inplace=True)

    for col in ['summary', 'sentiment', 'keywords']:
        if col not in df.columns:
            df[col] = ""

    for date, group in df.groupby('date'):
        group = summarize_articles(group, text_col = 'content')
        group = analyze_sentiment(group, summary_col='summary')
        group = extract_keywords(group, summary_col='summary')

        output_path = f'../features/{ticker}/{date}.csv'

        if os.path.exists(output_path):
            existing_df = pd.read_csv(output_path)
            group = pd.concat([existing_df, group], ignore_index=True)
            # group.drop_duplicates(subset='summary', inplace=True)

        group[['summary', 'sentiment', 'keywords']].to_csv(output_path, index=False)
        print(f"📁 저장 완료: {output_path}")

if __name__ == "__main__":
    tickers = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
    for ticker in tickers:
        print(f'❗️ {ticker} 대상 analysis 시작')
        input_file = f'../temp/{ticker}_temp.csv'
        process_news_file(input_file)
