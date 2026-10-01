from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import datetime

now = datetime.datetime.now(); yesterday = now - datetime.timedelta(days=1)
now = now.strftime('%Y-%m-%d'); yesterday = yesterday.strftime('%Y-%m-%d')

def collect_yahoo_finance_news(ticker, scroll_times, stop_url):
    options = Options()
    options.binary_location = "/usr/bin/google-chrome" # WSL에서 실행할 때 적용
    options.add_argument("--blink-settings=imagesEnabled=false") # 불필요한 광고/이미지 안 불러오도록
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    base_url = f"https://finance.yahoo.com/quote/{ticker}/news?p={ticker}"
    driver.set_page_load_timeout(180) # 타임아웃 120 -> 180
    driver.get(base_url)
    time.sleep(3)

    for _ in range(scroll_times):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    news_blocks = soup.select('section[data-testid="storyitem"]')

    articles = []
    for block in news_blocks:
        try:
            link_tag = block.find('a')
            if link_tag:
                link = link_tag['href']

                if not link.startswith('https'):
                    link = 'https://finance.yahoo.com' + link

                if stop_url and link in stop_url:
                    print("🛑 기존 URL과 중복된 뉴스 발견 → 수집 중단")
                    break

                if '/news/' in link:
                    articles.append({'url': link})

        except Exception as e:
            print("❗ 기사 파싱 실패:", e)
            continue

    driver.quit()
    return articles

def save_articles_to_csv(new_articles, filename):
    if not new_articles:
        print("⚠️ 저장할 새로운 뉴스가 없습니다.")
        return

    new_df = pd.DataFrame(new_articles)
    new_df.to_csv(filename, index=False)
    print(f"✅ {len(new_df)}건의 뉴스가 {filename} 맨 위에 추가 저장되었습니다.")


if __name__ == "__main__":
    tickers = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
    for ticker in tickers:
        print(f'❗️ {ticker} 대상 뉴스기사 수집 시작')
        file_name = f'../temp/{ticker}_temp.csv'
        latest_url = None

        if os.path.exists(file_name):
            old_df = pd.read_csv(file_name)
            if not old_df.empty:
                latest_url = old_df.iloc[0:3]["url"].tolist()

        news = collect_yahoo_finance_news(ticker=ticker, scroll_times=10, stop_url=latest_url)

        print(f"\n✅ 최종 수집된 뉴스 개수: {len(news)}건")
        for i, article in enumerate(news, 1):
            print(f"[{i}] URL: {article['url']}")

        save_articles_to_csv(news, file_name)
