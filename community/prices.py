"""Download daily prices (yfinance) for the collected tickers + SPY (market benchmark).

Run: python3 prices.py   -> data/prices.csv (date, symbol, OHLC, volume)
Dates are New York trading days. Re-running overwrites with the latest data.
"""
from pathlib import Path

import yfinance as yf

from collect import TICKERS

OUT = Path(__file__).parent / "data" / "prices.csv"
START = "2026-06-01"  # one month before the posts (Jul 1-) so day-over-day features have history

df = yf.download(TICKERS + ["SPY"], start=START, auto_adjust=True, progress=False)
df = df.stack(level=1, future_stack=True).reset_index()  # wide -> one row per date x symbol
df.columns = [c.lower() for c in df.columns]
df = df.rename(columns={"ticker": "symbol"}).dropna(subset=["close"]).sort_values(["symbol", "date"])
df.to_csv(OUT, index=False)
print(f"{OUT.name}: {len(df)} rows, {df.symbol.nunique()} symbols, {df.date.min():%Y-%m-%d} ~ {df.date.max():%Y-%m-%d}")
