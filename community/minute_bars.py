"""Accumulate 1-minute bars. yfinance only serves the last 30 days of 1-minute data, so fetch daily and append.

Each run fetches the last 7 days and merges with the existing file, dropping duplicates -> no gaps even if the Mac sleeps for a few days.
Run: python3 minute_bars.py   -> data/prices_1m.csv.gz
Automated: launchd com.sunmin.stockmind.minute (daily 14:47, after US market close)
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
print(f"{pd.Timestamp.now():%Y-%m-%d %H:%M} 1-min bars: {len(df)} rows (+{len(df) - len(old)}), "
      f"{df.start.min():%m-%d} ~ {df.start.max():%m-%d %H:%M} UTC")
