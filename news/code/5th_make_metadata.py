import requests
import datetime
import json
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from privacy import twelve_data_api_key as api_key


def fetch_price_cache(ticker, cache_file):
    today = datetime.datetime.now().date(); end_date = today.strftime('%Y-%m-%d')
    start_date = '2025-05-01'
    filtered_start_date = '2025-05-02'
    url = (
        f"https://api.twelvedata.com/time_series?apikey={api_key}"
        f"&symbol={ticker}&interval=1day&start_date={start_date}&end_date={end_date}"
        f"&timezone=UTC&dp=2&format=JSON"
    )
    start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')

    response = requests.get(url)
    data = response.json()

    if "values" not in data:
        print("❗ 데이터 수신 실패:", data.get("message", "알 수 없음"))
        return None

    # ✅ 날짜 기준값 생성
    all_dates = [(start_date + datetime.timedelta(days=i)).strftime('%Y-%m-%d') for i in range((today - start_date.date()).days + 1)]
    values = sorted(data["values"], key=lambda x: x["datetime"])
    valid_dates = set(entry["datetime"] for entry in values)
    holiday_dates = [d for d in all_dates if d not in valid_dates]

    # ✅ 거래일 기준으로 처리
    price_dict = {}
    previous_price = None
    pending_news_dates = []

    value_index = 0
    for current_date in all_dates:
        if current_date in valid_dates:
            # 거래일 처리
            entry = values[value_index]
            assert entry["datetime"] == current_date, f"순서 불일치: {entry['datetime']} vs {current_date}"
            close_price = float(entry["close"])

            # 휴일 뉴스 포함 + 오늘 뉴스도 포함
            news_files = [f"{ticker}/{d}.csv" for d in pending_news_dates]
            news_files.append(f"{ticker}/{current_date}.csv")

            rate = round((close_price - previous_price) / previous_price * 100, 2) if previous_price else None
            if current_date >= filtered_start_date:
                price_dict[current_date] = {
                    "price": close_price,
                    "rate": rate,
                    "news": news_files
                }

            previous_price = close_price
            pending_news_dates = []  # 초기화
            value_index += 1
        else:
            # 시장이 닫힌 날 → 뉴스 보류
            pending_news_dates.append(current_date)

    # ✅ 저장
    with open(cache_file, "w") as f:
        json.dump(price_dict, f, indent=2)

    print(f"✅ {len(price_dict)}일치 종가 및 변동률 데이터를 {cache_file}에 저장 완료")
    return price_dict


if __name__ == "__main__":
    tickers = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
    for ticker in tickers:
        print(f'❗️ {ticker} 대상 metadata.json 저장 시작')
        cache_file = f'../metadata/{ticker}_metadata.json'
        fetch_price_cache(ticker, cache_file)
