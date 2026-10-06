"""Experiment 3: do finer blocks (1 hour -> 30 minutes) make comment volume more useful?

Only two resolutions were pre-specified: 1h vs 30m (prevents trying many and keeping the best).
yfinance serves 30-minute bars for the last 60 days only, so both resolutions use the same 60 days.
Target and features as in experiment 1 (relative to the same ticker and slot's past; burst = above the past 80th percentile).
Eval = time split: train before Sep 15, test after (the first ~3 weeks build up history).
Run: python3 exp3_resolution.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from collect import TICKERS

HERE = Path(__file__).parent
NY = "America/New_York"
CUT = pd.Timestamp("2026-09-15", tz=NY)
SETS = {"trading": ["vol_rel", "move_rel"], "comments": ["com_rel", "auth_rel"]}
SETS["both"] = SETS["trading"] + SETS["comments"]

posts = pd.read_sql("SELECT symbol, created_at, username FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts["t"] = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert(NY)


def build(interval):
    raw = yf.download(TICKERS, period="59d", interval=interval, auto_adjust=True, progress=False)
    bars = raw.stack(level=1, future_stack=True).reset_index()
    bars.columns = [c.lower() for c in bars.columns]
    bars = bars.rename(columns={"ticker": "symbol", "datetime": "start"}).dropna(subset=["close"])
    bars["start"] = pd.to_datetime(bars.start, utc=True).dt.tz_convert(NY)
    bars = bars[bars.start < pd.Timestamp.now(tz=NY).normalize()].sort_values(["symbol", "start"]).reset_index(drop=True)
    bars["slot"] = bars.start.dt.strftime("%H:%M")

    parts = []
    for sym, b in bars.groupby("symbol"):
        p = posts[posts.symbol == sym]
        k = np.searchsorted(b.start.values, p.t.values, side="right") - 1
        q = p[k >= 0].reset_index(drop=True)
        q["start"] = b.start.iloc[k[k >= 0]].reset_index(drop=True)
        parts.append(q)
    cnt = pd.concat(parts).groupby(["symbol", "start"]).agg(
        comments=("username", "size"), authors=("username", "nunique")).reset_index()

    df = bars.merge(cnt, on=["symbol", "start"], how="left").fillna({"comments": 0, "authors": 0})
    key = [df.symbol, df.slot]
    past = lambda s: s.groupby(key).transform(lambda x: x.expanding(5).mean().shift(1))
    absr = df.groupby("symbol").close.pct_change().abs()
    df["vol_rel"] = np.log1p(df.volume) - past(np.log1p(df.volume))
    df["move_rel"] = absr / past(absr)
    df["com_rel"] = np.log1p(df.comments) - past(np.log1p(df.comments))
    df["auth_rel"] = np.log1p(df.authors) - past(np.log1p(df.authors))
    p80 = df.volume.groupby(key).transform(lambda x: x.expanding(15).quantile(0.8).shift(1))
    nv, np80 = df.groupby("symbol").volume.shift(-1), p80.groupby(df.symbol).shift(-1)
    df["target"] = (nv > np80).astype(float).where(nv.notna() & np80.notna())
    return df.dropna(subset=SETS["both"] + ["target"]).reset_index(drop=True)


def gain_ci(y, a, b, day, n=1000):
    f = lambda i: average_precision_score(y[i], a[i]) - average_precision_score(y[i], b[i])
    uniq, r = np.unique(day), np.random.default_rng(1)
    pos = {d: np.where(day == d)[0] for d in uniq}
    boot = [f(np.concatenate([pos[d] for d in r.choice(uniq, len(uniq))])) for _ in range(n)]
    return f(np.arange(len(y))), *np.percentile(boot, [2.5, 97.5])


built = {iv: build(iv) for iv in ["1h", "30m"]}
first = max(d.start.min() for d in built.values())  # align both resolutions to the same first day
print(f"shared period {first:%m-%d} ~, train until {CUT:%m-%d}, test from {CUT:%m-%d}\n")
print(f"{'res':5}{'blocks':>7}{'zero-comment share':>19}{'trading':>9}{'comments':>9}{'both':>8}{'random':>7}   both - trading (test all / overnight)")
for iv, d in built.items():
    d = d[d.start >= first].reset_index(drop=True)
    y = d.target.astype(int).values
    tr, te = (d.start < CUT).values, (d.start >= CUT).values
    sc = {}
    for name, cols in SETS.items():
        m = make_pipeline(StandardScaler(), LogisticRegression()).fit(d.loc[tr, cols], y[tr])
        sc[name] = m.predict_proba(d[cols])[:, 1]
    aps = "".join(f"{average_precision_score(y[te], sc[n][te]):8.3f}" for n in SETS)
    day = d.start.dt.date.values
    night = te & (d.slot == "15:30").values
    out = []
    for m in [te, night]:
        g, lo, hi = gain_ci(y[m], sc["both"][m], sc["trading"][m], day[m])
        out.append(f"{g:+.3f} [{lo:+.3f}, {hi:+.3f}]")
    zero = (d.comments[te] == 0).mean()
    print(f"{iv:5}{te.sum():7}{zero:19.2f}{aps[:8]:>9}{aps[8:]}{y[te].mean():7.3f}   {out[0]} / {out[1]}")
