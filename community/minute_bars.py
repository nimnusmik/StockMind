"""1분 봉 쌓기. yfinance는 1분 봉을 최근 30일까지만 주므로 매일 받아 누적해 둔다.

한 번에 최근 7일치를 받아 기존 파일과 합치고 중복 제거 → 맥이 며칠 잠들어도 빈 날이 안 생김.
실행: python3 minute_bars.py   → data/prices_1m.csv.gz
자동: launchd com.sunmin.stockmind.minute (매일 14:47, 미국 장 마감 후)
"""
from pathlib import Path

import pandas as pd
import yfinance as yf

from collect import TICKERS

OUT = Path(__file__).parent / "data" / "prices_1m.csv.gz"

raw = yf.download(TICKERS + ["SPY"], period="7d", interval="1m", auto_adjust=True, progress=False)
new = raw.stack(level=1, future_stack=True).reset_index()
new.columns = [c.lower() for c in new.columns]
new = new.rename(columns={"ticker": "symbol", "datetime": "start"}).dropna(subset=["close"])
new["start"] = pd.to_datetime(new.start, utc=True)

old = pd.read_csv(OUT, parse_dates=["start"]) if OUT.exists() else new.iloc[:0]
df = pd.concat([old, new]).drop_duplicates(["symbol", "start"], keep="last").sort_values(["symbol", "start"])
df.to_csv(OUT, index=False)
print(f"{pd.Timestamp.now():%Y-%m-%d %H:%M} 1분 봉 {len(df)}줄 (+{len(df) - len(old)}), "
      f"{df.start.min():%m-%d} ~ {df.start.max():%m-%d %H:%M} UTC")
