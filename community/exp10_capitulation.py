"""Experiment 10: Anger dosimeter — on falling days, how much rage marks the bottom?

Refined hypothesis (2026-10-05): "Fine, the stock is falling. What I want to know is HOW MUCH
anger has to build up in the comments before the turn." Experiments 8-9 only tested on/off;
this is the first dose-response test. Rules fixed BEFORE seeing results (2026-10-05);
anything changed afterwards must be labelled post-hoc.

- Day boundary and returns = same as experiment 9 (16:00 ET cutoff, excess vs SPY)
- Sample = falling days only: 3-day cumulative excess return in the bottom third within the
  ticker. (We do NOT claim drop size is fully controlled — deeper drops inside the sample may
  still carry more rage, so mean drop size is reported per dose bin.)
- Three rage measures (each relative to the ticker's own past: expanding mean, shift(1), min 10 days):
    tox  = mean toxicity of the day's posts (Detoxify)
    bear = bear-post share (stance.csv)
    heat = post-count multiple of the ticker's usual volume
- Dose bins = falling days split into quintiles of the rage measure (pooled across tickers;
  measures are already relative, so pooling is fair)
- Two outcomes:
    next5  = next 5 trading days' cumulative excess return (primary)
    bottom = close never goes below today's close during the next 5 days (today was a local bottom)
- Verdict = (1) does next5 improve monotonically with dose (Spearman)
            (2) top bin minus the rest, date-block bootstrap 95%
- Full period is the main verdict (sample too small to split; Sep-onward shown for reference)
Run: python3 exp10_capitulation.py

Results (2026-10-05, 259 falling days, Jul 15 - Sep 25):
- No dose-response. tox rho=-0.03 (flat), heat rho=+0.07, bear rho=-0.14 (p=.02, OPPOSITE to the
  hypothesis, but drop size differs across bins (-3.8 to -5.4%) so it may be uncontrolled drop).
  Every top-vs-rest 95% interval contains 0.
- Bottom rate is unrelated to dose (0.21-0.38, no pattern).
- Reading: no rage threshold visible with these measures (toxicity, bear tilt, post count).
  Limitation: Detoxify measures aggression, not SURRENDER ("sold everything", "I give up",
  "it's over") — and folk wisdom says bottoms come with capitulation, not anger.
  Next candidate = a surrender measure (experiment 11).
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
WARMUP, BINS = 10, 5

# 1) posts -> trading day, stance + toxicity (same pipeline as experiment 9)
posts = pd.read_sql("SELECT uuid, symbol, created_at FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts.merge(pd.read_csv(HERE / "data" / "stance.csv"), on="uuid")
posts = posts.merge(pd.read_csv(HERE / "data" / "toxicity.csv", usecols=["uuid", "toxicity"]), on="uuid", how="left")
posts["op"] = posts[["bear", "neu", "bull"]].to_numpy().argmax(1)
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
daily = posts.groupby(["symbol", "date"]).agg(
    n=("op", "size"), bear_cnt=("op", lambda x: (x == 0).sum()), bull_cnt=("op", lambda x: (x == 2).sum()),
    tox=("toxicity", "mean")).reset_index()
daily["bear"] = daily.bear_cnt / (daily.bear_cnt + daily.bull_cnt)

# 2) returns, rage relative to the ticker's past, falling-day sample
px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="inner").sort_values(["symbol", "date"]).reset_index(drop=True)
g = df.groupby("symbol")
past = lambda col: g[col].transform(lambda x: x.expanding(WARMUP).mean().shift(1))
df["tox_rel"] = df.tox - past("tox")
df["bear_rel"] = df.bear - past("bear")
df["heat_rel"] = df.n / past("n")
df["drop3"] = g.ex.transform(lambda x: x.rolling(3).sum())
df["next5"] = g.ex.transform(lambda x: sum(x.shift(-k) for k in range(1, 6)))
cl = prices.pivot(index="date", columns="symbol", values="close").stack().rename("close").reset_index()
df = df.merge(cl, on=["date", "symbol"])
df["fwd_min"] = df.groupby("symbol").close.transform(lambda x: pd.concat([x.shift(-k) for k in range(1, 6)], axis=1).min(axis=1))
df["bottom"] = (df.fwd_min >= df.close).astype(float)
df = df.dropna(subset=["tox_rel", "drop3", "next5"])
df["falling"] = df.groupby("symbol").drop3.transform(lambda x: x <= x.quantile(1 / 3))
fall = df[df.falling].copy()


def dose(d, meas, B=2000):
    d = d.dropna(subset=[meas]).copy()
    d["q"] = pd.qcut(d[meas].rank(method="first"), BINS, labels=False)
    tab = d.groupby("q").agg(days=("next5", "size"), drop_pct=("drop3", lambda x: round(x.mean() * 100, 1)),
                             next5_pct=("next5", lambda x: round(x.mean() * 100, 2)),
                             bottom_rate=("bottom", lambda x: round(x.mean(), 2)))
    rho, p = spearmanr(d.q, d.next5)
    top = d[d.q == BINS - 1]
    rest = d[d.q < BINS - 1]
    diff = top.next5.mean() - rest.next5.mean()
    rng = np.random.default_rng(0)
    by = {k: v for k, v in d.groupby("date")}
    ds = list(by)
    vals = []
    for _ in range(B):
        s = pd.concat([by[x] for x in rng.choice(ds, len(ds))])
        t, r = s[s.q == BINS - 1], s[s.q < BINS - 1]
        vals.append(t.next5.mean() - r.next5.mean() if len(t) and len(r) else np.nan)
    lo, hi = np.nanpercentile(vals, [2.5, 97.5])
    return tab, rho, p, diff * 100, lo * 100, hi * 100


print(f"falling days {len(fall)} / all {len(df)}, period {df.date.min():%Y-%m-%d} ~ {df.date.max():%Y-%m-%d}")
print("bin 0 = least rage, 4 = most. next5 = next-5-day cumulative excess return; bottom_rate = no lower close within 5 days\n")
for meas, name in [("tox_rel", "toxicity"), ("bear_rel", "bear tilt"), ("heat_rel", "heat (post count)")]:
    tab, rho, p, diff, lo, hi = dose(fall, meas)
    print(f"=== {name} — falling days split into {name} quintiles ===")
    print(tab.to_string())
    print(f"monotonicity (Spearman) rho={rho:+.3f} (p={p:.2f}) / top bin - rest next5 diff {diff:+.2f}pp [{lo:+.2f}, {hi:+.2f}]\n")

sep = fall[fall.date >= "2026-09-01"]
tab, rho, p, diff, lo, hi = dose(sep, "tox_rel", B=1000)
print(f"[Reference: Sep 1+ only, toxicity] falling days {len(sep)}, top-rest {diff:+.2f}pp [{lo:+.2f}, {hi:+.2f}], rho={rho:+.3f}")
