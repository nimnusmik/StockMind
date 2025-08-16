import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time, datetime, traceback

def get_article_content(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'p.yf-1090901'))
        )
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
        print(f"❗ 재시도 실패 ({url}): {e}")
        traceback.print_exc()
        return "본문 수집 실패", datetime.datetime.now().strftime('%Y-%m-%d')

def retry_failed_articles(filename):
    df = pd.read_csv(filename)

    failed_indices = df[df['content'] == '본문 수집 실패'].index.tolist()
    if not failed_indices:
        print("✅ 본문 수집 실패 항목이 없습니다.")
        return

    print(f"🔁 본문 수집 실패된 뉴스 {len(failed_indices)}건 재시도 시작")

    options = Options()
    options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    for i in failed_indices:
        row = df.loc[i]
        print(f"🔄 {i}번 뉴스 재시도 중 ...", end=" ")
        content, date = get_article_content(driver, row['url'])
        df.at[i, 'content'] = content
        df.at[i, 'date'] = date
        time.sleep(2)
        print("완료")

    driver.quit()
    df.to_csv(filename, index=False)
    print(f"✅ 재시도 결과가 {filename}에 저장되었습니다.")

if __name__ == "__main__":
    tickers = ['AAPL', 'GOOG', 'META', 'TSLA', 'MSFT', 'AMZN', 'NVDA', 'NFLX']
    for ticker in tickers:
        print(f'❗️ {ticker} 대상 수집 실패한 본문 수집 시작')
        filename = f'../temp/{ticker}_temp.csv'
        retry_failed_articles(filename)
