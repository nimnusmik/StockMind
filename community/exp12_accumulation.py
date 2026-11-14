"""Experiment 12: Accumulated rage + delayed bounce — how much must build up, and WHEN does it pay?

Hypothesis (user, 2026-10-13): "sentiment turning doesn't move the price the NEXT day — rage has
to ACCUMULATE over several days, and the bounce comes days later." Experiment 10 tested a
single day's rage against a 5-day lump sum; this tests (a) multi-day accumulation as the dose
and (b) each lag separately to locate the bounce. Rules fixed BEFORE seeing results
(2026-10-13); anything changed afterwards must be labelled post-hoc.

- Pipeline, day boundary, excess returns, falling-day sample = identical to experiment 10.
- Dose (primary) = 3-day rolling mean of daily toxicity-vs-own-baseline (tox_rel).
  Secondary = 5-day rolling mean. (Accumulation: rage sustained over days, not one spike.)
- Outcomes = cumulative excess return at horizons 1..10 trading days, measured per horizon
  so we can see WHERE along the path the bounce (if any) appears.
- Verdict per horizon = top dose quintile minus the rest, date-block bootstrap 95% CI,
  plus Spearman across quintiles. "The bounce arrives at day k" only if the CI at k excludes 0.
- Caveat stated up front: with ~3 months of data and 10 horizons there is a multiplicity
  problem — one isolated CI excluding 0 is NOT a finding; look for a consistent run.
Run: python3 exp12_accumulation.py

Results (2026-10-13, 274 falling days, Jul 17 - Oct 2):
- Short lags (1-5 days): nothing. All CIs straddle 0, rho flat — consistent with the user's
  intuition that next-day is too early, but no bounce at 2-5 days either.
- Long lags (7-10 days): top-rest diff drifts up in BOTH measures (acc3 day10 +2.76pp
  [-0.35, +5.79]; acc5 day10 +3.42pp [+0.10, +6.49], the only CI excluding 0).
- Honest read: NOT a finding. One borderline CI out of 20 horizons is exactly the multiplicity
  trap named above, and rho ~ 0 means no monotonic dose-response (only top-vs-rest moves).
  But the direction is consistent (both measures drift up past day 7), so it earns a
  pre-registered re-test: judge acc5/day 8-10 ONLY on data collected after 2026-10-13.

Re-run same day after refreshing prices + toxicity through Oct 13 (301 falling days, Jul 16 -
Oct 9): the one borderline CI disappeared — acc5 day10 now +2.98pp [-0.05, +5.75], acc3 day10
+2.34pp [-0.44, +5.18]. Adding ~1 week of data SHRANK the effect, the classic signature of
noise. The upward drift past day 7 is still there in both measures but nothing excludes 0.
Verdict so far: no demonstrable threshold or delayed bounce; the 10/13 re-test stays open.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
WARMUP, BINS, HMAX = 10, 5, 10

# 1) posts -> trading day, toxicity (same pipeline as experiment 10)
posts = pd.read_sql("SELECT uuid, symbol, created_at FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts.merge(pd.read_csv(HERE / "data" / "toxicity.csv", usecols=["uuid", "toxicity"]), on="uuid")
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
daily = posts.groupby(["symbol", "date"]).agg(tox=("toxicity", "mean")).reset_index()

# 2) returns, accumulated rage, falling-day sample
px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="inner").sort_values(["symbol", "date"]).reset_index(drop=True)
g = df.groupby("symbol")
df["tox_rel"] = df.tox - g.tox.transform(lambda x: x.expanding(WARMUP).mean().shift(1))
df["acc3"] = g.tox_rel.transform(lambda x: x.rolling(3).mean())
df["acc5"] = g.tox_rel.transform(lambda x: x.rolling(5).mean())
df["drop3"] = g.ex.transform(lambda x: x.rolling(3).sum())
for k in range(1, HMAX + 1):
    df[f"c{k}"] = g.ex.transform(lambda x, k=k: sum(x.shift(-j) for j in range(1, k + 1)))
df = df.dropna(subset=["acc3", "drop3", "c1"])
df["falling"] = df.groupby("symbol").drop3.transform(lambda x: x <= x.quantile(1 / 3))
fall = df[df.falling].copy()


def horizon_scan(d, meas, B=2000):
    d = d.dropna(subset=[meas]).copy()
    d["q"] = pd.qcut(d[meas].rank(method="first"), BINS, labels=False)
    rng = np.random.default_rng(0)
    by = {k: v for k, v in d.groupby("date")}
    ds = list(by)
    boots = [pd.concat([by[x] for x in rng.choice(ds, len(ds))]) for _ in range(B)]
    rows = []
    for k in range(1, HMAX + 1):
        col = f"c{k}"
        dk = d.dropna(subset=[col])
        rho, p = spearmanr(dk.q, dk[col])
        diff = dk[dk.q == BINS - 1][col].mean() - dk[dk.q < BINS - 1][col].mean()
        vals = []
        for s in boots:
            s = s.dropna(subset=[col])
            t, r = s[s.q == BINS - 1], s[s.q < BINS - 1]
            vals.append(t[col].mean() - r[col].mean() if len(t) and len(r) else np.nan)
        lo, hi = np.nanpercentile(vals, [2.5, 97.5])
        rows.append([k, len(dk), round(rho, 3), round(p, 2), round(diff * 100, 2),
                     round(lo * 100, 2), round(hi * 100, 2), "YES" if lo > 0 else ("neg" if hi < 0 else "")])
    return pd.DataFrame(rows, columns=["day", "n", "rho", "p", "top-rest pp", "lo", "hi", "CI>0"])


print(f"falling days {len(fall)} / all {len(df)}, period {df.date.min():%Y-%m-%d} ~ {df.date.max():%Y-%m-%d}")
print("dose = accumulated rage (rolling mean of toxicity vs own baseline), top quintile vs rest")
print("c-k = cumulative excess return over the next k trading days\n")
for meas, name in [("acc3", "3-day accumulated toxicity (primary)"), ("acc5", "5-day accumulated toxicity")]:
    print(f"=== {name} ===")
    print(horizon_scan(fall, meas).to_string(index=False))
    print()
