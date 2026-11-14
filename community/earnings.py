"""Earnings release times for the collected tickers (yfinance get_earnings_dates) -> data/earnings.csv (symbol, time).

Keeps every release since 2026-06-01, not just one per ticker, so later earnings seasons are covered too.
Run: uv run --no-project --with yfinance --with lxml --with pandas python earnings.py
"""
from pathlib import Path

import pandas as pd
import yfinance as yf

from collect import TICKERS

rows = []
for s in TICKERS:
    t = yf.Ticker(s).get_earnings_dates(limit=12).index
    rows += [(s, x) for x in t[(t >= pd.Timestamp("2026-06-01", tz=t.tz)) & (t <= pd.Timestamp.now(tz=t.tz))]]
out = pd.DataFrame(rows, columns=["symbol", "time"]).sort_values(["symbol", "time"])
assert out.symbol.nunique() == len(TICKERS), "a ticker returned no earnings dates"
out.to_csv(Path(__file__).parent / "data" / "earnings.csv", index=False)
print(out.to_string(index=False))
