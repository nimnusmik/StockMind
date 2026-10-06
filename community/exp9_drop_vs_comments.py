"""Experiment 9: Is the bounce caused by comments, or just by the drop? — match on drop size.

Experiment 8's figure: NVDA rose over the 5 days after a bearish-tilt signal, but the price
had been falling before the signal. Sentiment follows price (drops -> more bear posts), so
the signal may just be a rebroadcast of "it fell". -> Compare against days with a similar
drop but NO signal, within the same ticker. Rules fixed before seeing results (2026-10-05).

- Signal = same as experiment 8 (bearish tilt + heat, N=1 — the value exp 8 chose on Jul-Aug)
- Drop size = 3-day cumulative excess return (vs SPY) up to today, quintiles within each ticker
- Outcomes = (1) next-day excess return (original hypothesis)
             (2) next-5-day cumulative (hypothesis formed AFTER seeing exp 8's figure -> exploratory)
- Effect = (signal-day mean - non-signal-day mean) within the same drop quintile,
  weighted by signal-day counts
- Main verdict = 15 tickers pooled (NVDA alone has too few signals); NVDA shown for reference
- 95% intervals = date-block bootstrap (tickers co-move on the same day)
Run: python3 exp9_drop_vs_comments.py

Results (2026-10-05, Jul 15 - Oct 2):
- Signals cluster in the deepest-drop quintile (33% vs 7-17% elsewhere) -> much of the
  signal is a rebroadcast of the drop itself.
- Next day: drop-matched difference 0.00% [-0.64, +0.71] -> original hypothesis unsupported.
- Next 5 days: 15 tickers +1.62% [-0.05, +3.20], but Sep-onward only -0.38% -> Jul-Aug-only effect.
- NVDA 5-day +4.09% [+1.51, +6.43]: 14 overlapping signal days, and the hypothesis was formed
  from the same data as the test -> not evidence.
- PRE-REGISTERED RE-TEST: judge "NVDA 5-day excess after signals > similar-drop days" using
  ONLY data collected after 2026-10-05.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
WARMUP, BINS, SPLIT = 10, 5, "2026-09-01"

# 1) posts -> trading day, stance (same pipeline as experiment 8)
posts = pd.read_sql("SELECT uuid, symbol, created_at FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts.merge(pd.read_csv(HERE / "data" / "stance.csv"), on="uuid")
posts["op"] = posts[["bear", "neu", "bull"]].to_numpy().argmax(1)  # 0 bear, 1 neutral, 2 bull
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
daily = posts.groupby(["symbol", "date"]).agg(
    n=("op", "size"), bear=("op", lambda x: (x == 0).sum()), bull=("op", lambda x: (x == 2).sum())).reset_index()
daily["bear_share"] = daily.bear / (daily.bear + daily.bull)

# 2) signal, drop size, outcomes
px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="inner").sort_values(["symbol", "date"]).reset_index(drop=True)
g = df.groupby("symbol")
past = lambda col: g[col].transform(lambda x: x.expanding(WARMUP).mean().shift(1))
df["signal"] = (df.bear_share > past("bear_share")) & (df.n > past("n"))
df["drop3"] = g.ex.transform(lambda x: x.rolling(3).sum())
df["next1"] = g.ex.shift(-1)
df["next5"] = g.ex.transform(lambda x: sum(x.shift(-k) for k in range(1, 6)))
df = df[past("n").notna()].dropna(subset=["drop3"])
df["bin"] = df.groupby("symbol").drop3.transform(lambda x: pd.qcut(x, BINS, labels=False))


def effect(d, col):
    d = d.dropna(subset=[col])
    m = d.groupby(["bin", "signal"])[col].mean().unstack()
    w = d[d.signal].groupby("bin").size()
    m = m.dropna()
    return ((m[True] - m[False]) * w.reindex(m.index)).sum() / w.reindex(m.index).sum()


def raw(d, col):
    d = d.dropna(subset=[col])
    return d[d.signal][col].mean() - d[~d.signal][col].mean()


def boot(d, col, B=2000):
    rng = np.random.default_rng(0)
    by = {k: v for k, v in d.groupby("date")}
    ds = list(by)
    vals = []
    for _ in range(B):
        s = pd.concat([by[x] for x in rng.choice(ds, len(ds))])
        vals.append(effect(s, col))
    return np.nanpercentile(vals, [2.5, 97.5])


rows = []
for name, d in [("15 tickers, full", df), ("15 tickers, Sep 1+", df[df.date >= SPLIT]), ("NVDA, full", df[df.symbol == "NVDA"])]:
    for col, label in [("next1", "next day"), ("next5", "next 5 days")]:
        lo, hi = boot(d, col)
        rows.append(dict(sample=name, outcome=label, signal_days=int(d.signal.sum()),
                         raw_diff_pct=round(raw(d, col) * 100, 2),
                         drop_matched_diff_pct=round(effect(d, col) * 100, 2),
                         ci95=f"[{lo * 100:+.2f}, {hi * 100:+.2f}]"))
print(f"period {df.date.min():%Y-%m-%d} ~ {df.date.max():%Y-%m-%d}")
print("raw_diff = signal - non-signal days / drop_matched_diff = within similar-drop days (the comments' share)\n")
print(pd.DataFrame(rows).to_string(index=False))

# Do signals really cluster on drop days? (premise of the rebroadcast explanation)
print("\nsignal rate by drop quintile (0 = deepest drop):")
print(df.groupby("bin").signal.mean().round(2).to_string())
