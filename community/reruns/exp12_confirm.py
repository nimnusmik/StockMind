"""Experiment 12 pre-registered confirmation (sealed on 2026-10-13, opened 2026-11-12).

Registered verdict (exp12_accumulation.py docstring): judge acc5, horizons day 8-10,
ONLY on falling days dated AFTER 2026-10-13. Same pipeline, same rules, no re-tuning.

Choices fixed at opening time, before seeing output:
- Baselines (tox expanding mean) and rolling doses use full history (past-only information).
- The judged sample = falling days with date > 2026-10-13; dose quintiles are assigned
  WITHIN this confirmation sample (same code applied to the new window).
- Verdict: "bounce confirmed" only if acc5 top-rest CI excludes 0 at day 8, 9 AND 10
  (a consistent run, per the multiplicity caveat in the original pre-registration).
  acc3 and days 1-7 are reported as context only.

Run: uv run --no-project --with pandas --with numpy --with scipy python reruns/exp12_confirm.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).resolve().parent.parent
WARMUP, BINS, HMAX = 10, 5, 10
CONFIRM_AFTER = pd.Timestamp("2026-10-13")

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
fall = df[df.falling & (df.date > CONFIRM_AFTER)].copy()


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


print(f"CONFIRMATION WINDOW: falling days after {CONFIRM_AFTER:%Y-%m-%d}")
print(f"falling days {len(fall)}, period {fall.date.min():%Y-%m-%d} ~ {fall.date.max():%Y-%m-%d}")
print("registered verdict metric: acc5, days 8-10, top quintile vs rest, date-block bootstrap 95% CI\n")
for meas, name in [("acc5", "5-day accumulated toxicity (REGISTERED)"), ("acc3", "3-day accumulated toxicity (context)")]:
    print(f"--- {name} ---")
    print(horizon_scan(fall, meas).to_string(index=False))
    print()

t = horizon_scan(fall, "acc5")
hit = t[t.day.isin([8, 9, 10])]["CI>0"].eq("YES").all()
print("REGISTERED VERDICT (acc5 day 8,9,10 all CI>0):", "CONFIRMED" if hit else "NOT CONFIRMED")
