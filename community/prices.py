"""일별 주가 받기 (yfinance). 댓글 수집 종목 + SPY(시장 기준).

실행: python3 prices.py   → data/prices.csv (날짜, 종목, 시가·고가·저가·종가·거래량)
날짜는 뉴욕 거래일 기준. 다시 실행하면 최신까지 덮어쓴다.
"""
from pathlib import Path

import yfinance as yf

from collect import TICKERS

OUT = Path(__file__).parent / "data" / "prices.csv"
START = "2026-06-01"  # 댓글(7/1~)보다 한 달 앞서 받아서 '전날 대비' 계산이 가능하게

df = yf.download(TICKERS + ["SPY"], start=START, auto_adjust=True, progress=False)
df = df.stack(level=1, future_stack=True).reset_index()  # 넓은 표 → 날짜·종목 한 줄씩
df.columns = [c.lower() for c in df.columns]
df = df.rename(columns={"ticker": "symbol"}).dropna(subset=["close"]).sort_values(["symbol", "date"])
df.to_csv(OUT, index=False)
print(f"{OUT.name}: {len(df)}줄, {df.symbol.nunique()}종목, {df.date.min():%Y-%m-%d} ~ {df.date.max():%Y-%m-%d}")
