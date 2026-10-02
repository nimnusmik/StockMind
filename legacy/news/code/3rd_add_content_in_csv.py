import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import datetime

now = datetime.datetime.now(); now = now.strftime('%Y-%m-%d')

def get_article_content(driver, url):
    try:
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, 'html.parser')

        paragraphs = soup.select('p.yf-1090901')
        content = ' '.join([p.get_text(strip=True) for p in paragraphs]) if paragraphs else '본문 없음'
        time_tag = soup.select_one('time.byline-attr-meta-time')
        if time_tag and time_tag.has_attr('datetime'):
            utc_time_tag = time_tag['datetime']
            utc_time = datetime.datetime.fromisoformat(utc_time_tag.replace('Z', '+00:00'))
            kst_time = utc_time + datetime.timedelta(hours=9)

            if kst_time.hour >= 6:
                kst_time += datetime.timedelta(days=1)

            date_str = kst_time.strftime('%Y-%m-%d')
        else:
            date_str = datetime.datetime.now().strftime('%Y-%m-%d')

        return content, date_str

    except Exception as e:
        print(f"❗ 본문 또는 날짜 수집 실패 ({url}):", e)
        return "본문 수집 실패", datetime.datetime.now().strftime('%Y-%m-%d')

def enrich_articles_with_content(filename):
    df = pd.read_csv(filename)

    if 'content' not in df.columns:
        df['content'] = ""

    options = Options()
    options.binary_location = "/usr/bin/google-chrome" # WSL에서 실행할 때 적용
    options.add_argument("--blink-settings=imagesEnabled=false") # 불필요한 광고/이미지 안 불러오도록
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    i = 0
    while i < len(df):
        row = df.iloc[i]
        if isinstance(row['content'], str) and row['content'].strip():
            print(f"🛑 {i}번째 이후는 이미 본문 수집 완료 → 중단")
            break

        print(f"✅ {i}번 뉴스 본문 수집 ...", end = " ")
        content, date = get_article_content(driver, row['url'])
        df.at[i, 'content'] = content
        df.at[i, 'date'] = date
        time.sleep(1)
        print(f"완료")
        i += 1

    driver.quit()
    df.to_csv(filename, index=False)
    print(f"✅ 본문이 포함된 뉴스 {i}건이 {filename}에 저장되었습니다.")


if __name__ == "__main__":
    tickers = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
    for ticker in tickers:
        print(f'❗️ {ticker} 대상 본문 수집 시작')
        filename = f'../temp/{ticker}_temp.csv'
        enrich_articles_with_content(filename)
