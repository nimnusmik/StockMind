# 1st.py - Twelve Data API + Golden/Death Cross 시각화
import requests
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
'../../')))
from privacy import twelve_data_api_key as api_key

def fetch_price_data(ticker, interval="1day", outputsize=3000):
    url = (
        f"https://api.twelvedata.com/time_series"
        f"?symbol={ticker}&interval={interval}&outputsize={outputsize}"
        f"&apikey={api_key}&format=JSON&dp=2"
    )
    response = requests.get(url)
    data = response.json()

    if "values" not in data:
        raise ValueError(f"❗ 데이터 수신 실패: {data.get('message', '알 수 없음')}")

    df = pd.DataFrame(data["values"])
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.set_index("datetime", inplace=True)
    df = df.sort_index()
    df = df.astype(float)
    return df

def plot_stock_crosses(ticker):
    df = fetch_price_data(ticker)

    df['MA20'] = df['close'].rolling(window=20).mean()
    df['MA60'] = df['close'].rolling(window=60).mean()
    df['MA120'] = df['close'].rolling(window=120).mean()

    golden_cross = (df['MA20'] > df['MA60']) & (df['MA20'].shift(1) <= df['MA60'].shift(1))
    death_cross = (df['MA20'] < df['MA60']) & (df['MA20'].shift(1) >= df['MA60'].shift(1))

    golden_dates = df[golden_cross].index
    death_dates = df[death_cross].index

    plt.figure(figsize=(14,6))
    plt.plot(df['close'], label='Close')
    plt.plot(df['MA20'], label='MA20', color='orange')
    plt.plot(df['MA60'], label='MA60', color='green')
    plt.plot(df['MA120'], label='MA120', color='red')

    plt.scatter(golden_dates, df.loc[golden_dates]['close'], label='Golden Cross', marker='^', color='green')
    plt.scatter(death_dates, df.loc[death_dates]['close'], label='Death Cross', marker='v', color='red')

    plt.title(f"{ticker} Moving Averages & Crosses")
    plt.xlabel("Date")
    plt.ylabel("Price (USD)")
    plt.grid(True)
    plt.legend()
    plt.tight_layout()

    today = datetime.now().date()
    output_file = f"../visualization/{ticker}_{today}_stock.png"
    plt.savefig(output_file)
    print(f"✅ 그래프 저장 완료: {output_file}")

if __name__ == "__main__":
    tickers = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
    for ticker in tickers:
        plot_stock_crosses(ticker)
