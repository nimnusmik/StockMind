"""Experiment 1 (daily stock version of the paper's experiment 3): do today's comments predict tomorrow's volume burst?

Target   = tomorrow's volume > the ticker's own 80th percentile of volume up to today (past only -> no leakage)
Compare  = trading history only / comment volume only / both, logistic regression
Eval     = leave one whole ticker out (the paper's leave-one-market-out)
A day (t) of comments = previous trading day 16:00 ET to day t 16:00 ET

Run: python3 exp1_burst.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from collect import TICKERS

HERE = Path(__file__).parent
START = "2026-07-02"
LARGE = ["AAPL", "GOOG", "META", "TSLA", "MSFT", "AMZN", "NVDA", "NFLX"]

# 1) posts -> per-trading-day comment and author counts
con = sqlite3.connect(HERE / "data" / "community.db")
posts = pd.read_sql("SELECT symbol, created_at, username FROM posts", con)
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]  # today may be mid-session; exclude
days = np.sort(prices.date.unique())

et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
daily = posts.groupby(["symbol", "date"]).agg(comments=("username", "size"), authors=("username", "nunique")).reset_index()

# 2) features, all relative to the ticker's own past mean (puts NVDA and COIN on one scale)
df = prices[prices.symbol.isin(TICKERS)].merge(daily, on=["symbol", "date"], how="left")
df[["comments", "authors"]] = df[["comments", "authors"]].fillna(0)
df = df.sort_values(["symbol", "date"])
g = df.groupby("symbol")
past_mean = lambda s: s.groupby(df.symbol).transform(lambda x: x.expanding(5).mean().shift(1))

df["logv"] = np.log1p(df.volume)
df["absr"] = g.close.pct_change().abs()
df["logc"] = np.log1p(df.comments)
df["loga"] = np.log1p(df.authors)
df["vol_rel"] = df.logv - past_mean(df.logv)
df["move_rel"] = df.absr / past_mean(df.absr)
df["com_rel"] = df.logc - past_mean(df.logc.where(df.date >= START))
df["auth_rel"] = df.loga - past_mean(df.loga.where(df.date >= START))

p80 = g.volume.transform(lambda x: x.expanding(15).quantile(0.8))
df["target"] = (g.volume.shift(-1) > p80).astype(float).where(g.volume.shift(-1).notna() & p80.notna())

SETS = {"trading": ["vol_rel", "move_rel"], "comments": ["com_rel", "auth_rel"]}
SETS["both"] = SETS["trading"] + SETS["comments"]
data = df[df.date >= START].dropna(subset=SETS["both"] + ["target"]).reset_index(drop=True)
y = data.target.astype(int).values

# 3) leave-one-ticker-out fit/predict
scores = {}
for name, cols in SETS.items():
    s = np.empty(len(data))
    for sym in data.symbol.unique():
        test = data.symbol == sym
        m = make_pipeline(StandardScaler(), LogisticRegression()).fit(data.loc[~test, cols], y[~test])
        s[test] = m.predict_proba(data.loc[test, cols])[:, 1]
    scores[name] = s


def per_ticker_ap(s):
    return pd.Series({sym: average_precision_score(y[data.symbol == sym], s[data.symbol == sym])
                      for sym in data.symbol.unique() if y[data.symbol == sym].any()})


rng = np.random.default_rng(0)
rand = np.mean([per_ticker_ap(rng.random(len(data))).mean() for _ in range(200)])

print(f"{len(data)} observations ({data.symbol.nunique()} tickers x {data.date.nunique()} trading days, "
      f"{data.date.min():%m-%d}~{data.date.max():%m-%d}), burst rate {y.mean():.3f}\n")
print(f"{'':9}{'PR-AUC(all)':>12}{'ROC-AUC':>9}{'mean-per-ticker PR-AUC':>23}")
ap = {}
for name, s in scores.items():
    ap[name] = per_ticker_ap(s)
    print(f"{name:9}{average_precision_score(y, s):12.3f}{roc_auc_score(y, s):9.3f}{ap[name].mean():23.3f}")
print(f"{'random':9}{y.mean():12.3f}{0.5:9.3f}{rand:23.3f}  <- baseline (200 random draws)\n")

t = pd.DataFrame(ap)
t["group"] = np.where(t.index.isin(LARGE), "large-cap", "retail")
print("mean per-ticker PR-AUC by group")
print(t.groupby("group")[list(SETS)].mean().round(3).to_string())
win = (t["comments"] > t["trading"])
print(f"\ntickers where comments beat trading: {win.sum()}/{len(t)} -> {', '.join(t.index[win])}")
