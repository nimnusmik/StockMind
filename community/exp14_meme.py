"""Experiment 14: Meme tickers only — accumulated rage + delayed bounce where retail SETS the price.

Motivation: a standing objection to experiments 10-12 is that Yahoo board posters are not the
marginal price-setter for AAPL or MSFT. For the 7 retail favorites (GME AMC PLTR SOFI RIVN
COIN HOOD) that objection is weakest — retail flow demonstrably moves these names. So rerun
the experiment-12 scan on memes only.

Rules fixed BEFORE seeing results (2026-10-13):
- Identical to experiment 12 (doses acc3/acc5, falling-day sample, horizons 1..10, top
  quintile vs rest, date-block bootstrap CI, Spearman) restricted to the 7 meme tickers.
- Falling-day terciles and toxicity baselines are computed within the meme subset.
- Global permutation p over all 20 cells (max top-rest, 500 date permutations) is the primary
  verdict; per-cell CIs are descriptive only. Sample is ~half of exp 12, so power is low —
  a null here is weak evidence, but a PASS would be notable.
Run: python3 exp14_meme.py

Results (2026-10-13, 135 meme falling days, Jul 22 - Oct 9):
- Global permutation p = 0.422 -> FAILS. Observed max top-rest +5.97pp looks big, but meme
  returns are so volatile that the no-effect null routinely produces larger scans.
- The per-cell pattern is actually a W: acc3 days 4-8 NEGATIVE (more rage -> worse, rho up to
  -0.22, nominal p<.05 — momentum continuation, the OPPOSITE of the bounce hypothesis), then
  acc5 days 9-10 positive with CIs excluding 0 (+5.5/+6.0pp). Mutually contradictory cells in
  a 20-cell scan on 135 days = noise, which is exactly what the global p says.
- Reading: even where retail sets the price, accumulated rage shows no usable bounce signal.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
WARMUP, BINS, HMAX, NPERM = 10, 5, 10, 500
MEMES = ["GME", "AMC", "PLTR", "SOFI", "RIVN", "COIN", "HOOD"]

posts = pd.read_sql("SELECT uuid, symbol, created_at FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts[posts.symbol.isin(MEMES)]
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

px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px[MEMES].sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="inner").sort_values(["symbol", "date"]).reset_index(drop=True)
g = df.groupby("symbol")
df["tox_rel"] = df.tox - g.tox.transform(lambda x: x.expanding(WARMUP).mean().shift(1))
df["acc3"] = g.tox_rel.transform(lambda x: x.rolling(3).mean())
df["acc5"] = g.tox_rel.transform(lambda x: x.rolling(5).mean())
df["drop3"] = g.ex.transform(lambda x: x.rolling(3).sum())
for k in range(1, HMAX + 1):
    df[f"c{k}"] = g.ex.transform(lambda x, k=k: sum(x.shift(-j) for j in range(1, k + 1)))
df = df.dropna(subset=["acc3", "acc5", "drop3", "c1"])
df["falling"] = df.groupby("symbol").drop3.transform(lambda x: x <= x.quantile(1 / 3))
fall = df[df.falling].reset_index(drop=True)
COLS = [f"c{k}" for k in range(1, HMAX + 1)]


def scan(d, meas, B=2000):
    d = d.dropna(subset=[meas]).copy()
    d["q"] = pd.qcut(d[meas].rank(method="first"), BINS, labels=False)
    rng = np.random.default_rng(0)
    by = {k: v for k, v in d.groupby("date")}
    ds = list(by)
    boots = [pd.concat([by[x] for x in rng.choice(ds, len(ds))]) for _ in range(B)]
    rows = []
    for col in COLS:
        dk = d.dropna(subset=[col])
        rho, p = spearmanr(dk.q, dk[col])
        diff = dk[dk.q == BINS - 1][col].mean() - dk[dk.q < BINS - 1][col].mean()
        vals = []
        for s in boots:
            s = s.dropna(subset=[col])
            t, r = s[s.q == BINS - 1], s[s.q < BINS - 1]
            vals.append(t[col].mean() - r[col].mean() if len(t) and len(r) else np.nan)
        lo, hi = np.nanpercentile(vals, [2.5, 97.5])
        rows.append([int(col[1:]), len(dk), round(rho, 3), round(p, 2), round(diff * 100, 2),
                     round(lo * 100, 2), round(hi * 100, 2), "YES" if lo > 0 else ""])
    return pd.DataFrame(rows, columns=["day", "n", "rho", "p", "top-rest pp", "lo", "hi", "CI>0"])


def max_stat(d):
    best = -np.inf
    for meas in ("acc3", "acc5"):
        dd = d.dropna(subset=[meas])
        q = pd.qcut(dd[meas].rank(method="first"), BINS, labels=False)
        top = q == BINS - 1
        for col in COLS:
            v = dd[col]
            m = v.notna()
            if (m & top).sum() and (m & ~top).sum():
                best = max(best, v[m & top].mean() - v[m & ~top].mean())
    return best


obs = max_stat(fall)
dates = fall.date.unique()
dose = fall.set_index(["symbol", "date"])[["acc3", "acc5"]]
rng = np.random.default_rng(1)
maxes = []
for _ in range(NPERM):
    sigma = dict(zip(dates, rng.permutation(dates)))
    perm = fall[["symbol", "date"] + COLS].copy()
    perm[["acc3", "acc5"]] = dose.reindex(pd.MultiIndex.from_arrays([perm.symbol, perm.date.map(sigma)])).to_numpy()
    maxes.append(max_stat(perm.dropna(subset=["acc3", "acc5"])))
gp = (np.array(maxes) >= obs).mean()

print(f"meme falling days {len(fall)}, period {fall.date.min():%Y-%m-%d} ~ {fall.date.max():%Y-%m-%d}")
for meas, name in [("acc3", "3-day accumulated toxicity"), ("acc5", "5-day accumulated toxicity")]:
    print(f"\n=== {name} (memes only) ===")
    print(scan(fall, meas).to_string(index=False))
print(f"\nglobal permutation: observed max top-rest {obs * 100:+.2f}pp, p = {gp:.3f} "
      f"({'PASSES' if gp < 0.05 else 'FAILS'})")
