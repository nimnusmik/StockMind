"""Experiment 13: Silence after rage — does the board going QUIET mark the bottom?

Motivation: experiments 10-12 show rage VOLUME predicts nothing. Remaining idea: true
surrender is not posting angrily, it is walking away. Detoxify cannot see people who stop
writing — but post counts can. Hypothesis: on falling days where rage has built up, a sudden
DROP in posting (the quiet after the storm) marks the bottom.

Rules fixed BEFORE seeing results (2026-10-13):
- Pipeline, day boundary, excess returns, falling-day sample = experiment 12.
- Primed sample = falling days whose 3-day accumulated toxicity (acc3, vs own baseline) is in
  the TOP HALF of falling days (rage must have built up first).
- Dose = heat = today's post count / the ticker's expanding-mean count (shift 1, min 10 days).
  LOW heat = silence. Quintile 0 = quietest.
- Outcomes = cumulative excess return at horizons 1..10; verdict = quietest quintile minus the
  rest, date-block bootstrap 95% CI, Spearman (hypothesis predicts NEGATIVE rho: quieter ->
  better), AND a global permutation p over the 10-horizon scan (max of quiet-rest under 500
  date permutations) so one stray cell cannot impress us again.
Run: python3 exp13_silence.py

Results (2026-10-13, 151 primed falling days, Jul 17 - Oct 9):
- Global permutation p = 0.778 -> FAILS clearly. Max quiet-rest +1.96pp is a typical
  no-effect value.
- One suggestive cell: day 1 rho = -0.212 (p=.01), quieter -> better NEXT day, the direction
  the hypothesis predicts — but quiet-rest CI straddles 0, no follow-through at days 2+, and
  with 10 cells scanned one nominal p=.01 is unremarkable. Descriptive only.
- Reading: the quiet-after-the-storm signal does not survive honest correction either.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
WARMUP, BINS, HMAX, NPERM = 10, 5, 10, 500

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
daily = posts.groupby(["symbol", "date"]).agg(n=("toxicity", "size"), tox=("toxicity", "mean")).reset_index()

px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="inner").sort_values(["symbol", "date"]).reset_index(drop=True)
g = df.groupby("symbol")
past = lambda col: g[col].transform(lambda x: x.expanding(WARMUP).mean().shift(1))
df["tox_rel"] = df.tox - past("tox")
df["acc3"] = g.tox_rel.transform(lambda x: x.rolling(3).mean())
df["heat"] = df.n / past("n")
df["drop3"] = g.ex.transform(lambda x: x.rolling(3).sum())
for k in range(1, HMAX + 1):
    df[f"c{k}"] = g.ex.transform(lambda x, k=k: sum(x.shift(-j) for j in range(1, k + 1)))
df = df.dropna(subset=["acc3", "heat", "drop3", "c1"])
df["falling"] = df.groupby("symbol").drop3.transform(lambda x: x <= x.quantile(1 / 3))
fall = df[df.falling].copy()
primed = fall[fall.acc3 >= fall.acc3.median()].reset_index(drop=True)
COLS = [f"c{k}" for k in range(1, HMAX + 1)]


def scan(d, B=2000):
    d = d.copy()
    d["q"] = pd.qcut(d.heat.rank(method="first"), BINS, labels=False)
    rng = np.random.default_rng(0)
    by = {k: v for k, v in d.groupby("date")}
    ds = list(by)
    boots = [pd.concat([by[x] for x in rng.choice(ds, len(ds))]) for _ in range(B)]
    rows = []
    for col in COLS:
        dk = d.dropna(subset=[col])
        rho, p = spearmanr(dk.q, dk[col])
        diff = dk[dk.q == 0][col].mean() - dk[dk.q > 0][col].mean()
        vals = []
        for s in boots:
            s = s.dropna(subset=[col])
            t, r = s[s.q == 0], s[s.q > 0]
            vals.append(t[col].mean() - r[col].mean() if len(t) and len(r) else np.nan)
        lo, hi = np.nanpercentile(vals, [2.5, 97.5])
        rows.append([int(col[1:]), len(dk), round(rho, 3), round(p, 2), round(diff * 100, 2),
                     round(lo * 100, 2), round(hi * 100, 2), "YES" if lo > 0 else ""])
    return pd.DataFrame(rows, columns=["day", "n", "rho", "p", "quiet-rest pp", "lo", "hi", "CI>0"])


def max_quiet(d):
    q = pd.qcut(d.heat.rank(method="first"), BINS, labels=False)
    best = -np.inf
    for col in COLS:
        v = d[col]
        m = v.notna()
        if (m & (q == 0)).sum() and (m & (q > 0)).sum():
            best = max(best, v[m & (q == 0)].mean() - v[m & (q > 0)].mean())
    return best


obs = max_quiet(primed)
dates = primed.date.unique()
heat_lut = primed.set_index(["symbol", "date"]).heat
rng = np.random.default_rng(1)
maxes = []
for _ in range(NPERM):
    sigma = dict(zip(dates, rng.permutation(dates)))
    perm = primed[["symbol", "date"] + COLS].copy()
    perm["heat"] = heat_lut.reindex(pd.MultiIndex.from_arrays([perm.symbol, perm.date.map(sigma)])).to_numpy()
    maxes.append(max_quiet(perm.dropna(subset=["heat"])))
gp = (np.array(maxes) >= obs).mean()

print(f"primed falling days (rage built up) {len(primed)} / falling {len(fall)}, "
      f"period {primed.date.min():%Y-%m-%d} ~ {primed.date.max():%Y-%m-%d}")
print("quintile 0 = quietest day vs the ticker's usual volume; hypothesis: quiet-rest > 0, rho < 0\n")
print(scan(primed).to_string(index=False))
print(f"\nglobal permutation: observed max quiet-rest {obs * 100:+.2f}pp, p = {gp:.3f} "
      f"({'PASSES' if gp < 0.05 else 'FAILS'})")
