"""Experiment 11: Surrender dosimeter — on falling days, do give-up posts mark the bottom?

Experiment 10 found no threshold using anger (toxicity, bear tilt, heat). Folk wisdom says
bottoms come when people SURRENDER, not when they are angry (capitulation) -> count give-up
language directly with a lexicon.
The lexicon was fixed on 2026-10-05 by reading a sample of posts only — without looking at
prices. Changes made after seeing results must be labelled post-hoc.
Design is identical to experiment 10 (falling-day sample, quintiles, next5 + bottom rate,
Spearman + top-vs-rest bootstrap).

Known limitation (seen while fixing the lexicon): "i'm out" also matches short-covering, and
"give up" sometimes refers to other people. Noise is diluted by the day-to-day relative
comparison but not removed. A precise version would use LLM labels (paid).
Run: python3 exp11_surrender.py

Results (2026-10-05, 259 falling days): no threshold with surrender either. rho=-0.05,
top bin - rest -1.29pp [-3.39, +0.72] — direction OPPOSITE to the hypothesis (highest-surrender
days do slightly worse, though the interval contains 0).
Extra limitation: only 0.9% of posts contain surrender language, so a day's share is driven
by 0-1 posts — the measurement itself is coarse.
"""
import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
WARMUP, BINS = 10, 5

# Surrender lexicon: first-person giving up / liquidation / exhaustion.
# Mockery ("bagholder") and anger are excluded — those were covered in experiment 10.
SURRENDER = re.compile("|".join([
    r"sold +(all|everything|my +(entire|whole|last|remaining))",
    r"sold +out\b", r"dump(ed)? +(all|everything|my)\b",
    r"i'?m +(out|done|gone)\b", r"i +am +(out|done)\b", r"done +with +this\b",
    r"(i|i'?ve)? ?(giv(e|ing)|gave) +up\b", r"throw(ing)? +in +the +towel",
    r"cut +(my +)?loss(es)?\b", r"took +(my|the) +loss", r"out +at +a +loss",
    r"can'?t +take +(it|this) +any ?more", r"had +enough\b", r"i +quit\b", r"i +surrender",
    r"never +(buying|touching|trusting).{0,20}again", r"lesson +learned",
    r"(time +to +)?mov(e|ing) +on\b", r"get(ting)? +out +(of|while)\b",
    r"exit(ed|ing)? +(my|the) +position", r"capitulat", r"wiped +out\b", r"lost +everything\b",
]))

# 1) posts -> trading day, surrender flag (day boundary matches experiments 9-10)
posts = pd.read_sql("SELECT uuid, symbol, created_at, body FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
clean = posts.body.fillna("").str.replace("\\", "", regex=False).str.lower()
posts["sur"] = clean.str.contains(SURRENDER).astype(float)
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
print(f"posts with surrender language: {posts.sur.sum():.0f} / {len(posts)} ({posts.sur.mean() * 100:.1f}%)")
daily = posts.groupby(["symbol", "date"]).agg(n=("sur", "size"), sur=("sur", "mean")).reset_index()

# 2) returns, surrender relative to the ticker's past, falling days (identical to exp 10)
px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="inner").sort_values(["symbol", "date"]).reset_index(drop=True)
g = df.groupby("symbol")
past = lambda col: g[col].transform(lambda x: x.expanding(WARMUP).mean().shift(1))
df["sur_rel"] = df.sur - past("sur")
df["drop3"] = g.ex.transform(lambda x: x.rolling(3).sum())
df["next5"] = g.ex.transform(lambda x: sum(x.shift(-k) for k in range(1, 6)))
cl = prices.pivot(index="date", columns="symbol", values="close").stack().rename("close").reset_index()
df = df.merge(cl, on=["date", "symbol"])
df["fwd_min"] = df.groupby("symbol").close.transform(lambda x: pd.concat([x.shift(-k) for k in range(1, 6)], axis=1).min(axis=1))
df["bottom"] = (df.fwd_min >= df.close).astype(float)
df = df.dropna(subset=["sur_rel", "drop3", "next5"])
df["falling"] = df.groupby("symbol").drop3.transform(lambda x: x <= x.quantile(1 / 3))
fall = df[df.falling].copy()


def dose(d, meas, B=2000):
    d = d.dropna(subset=[meas]).copy()
    d["q"] = pd.qcut(d[meas].rank(method="first"), BINS, labels=False)
    tab = d.groupby("q").agg(days=("next5", "size"), drop_pct=("drop3", lambda x: round(x.mean() * 100, 1)),
                             surrender_pp=(meas, lambda x: round(x.mean() * 100, 2)),
                             next5_pct=("next5", lambda x: round(x.mean() * 100, 2)),
                             bottom_rate=("bottom", lambda x: round(x.mean(), 2)))
    rho, p = spearmanr(d.q, d.next5)
    diff = d[d.q == BINS - 1].next5.mean() - d[d.q < BINS - 1].next5.mean()
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


print(f"falling days {len(fall)}, period {df.date.min():%Y-%m-%d} ~ {df.date.max():%Y-%m-%d}")
print("bin 0 = least surrender, 4 = most. surrender_pp = surrender-post share vs the ticker's usual (pp)\n")
tab, rho, p, diff, lo, hi = dose(fall, "sur_rel")
print(tab.to_string())
print(f"monotonicity (Spearman) rho={rho:+.3f} (p={p:.2f}) / top bin - rest next5 diff {diff:+.2f}pp [{lo:+.2f}, {hi:+.2f}]")
sep = fall[fall.date >= "2026-09-01"]
tab, rho, p, diff, lo, hi = dose(sep, "sur_rel", B=1000)
print(f"[Reference: Sep 1+ only] falling days {len(sep)}, top-rest {diff:+.2f}pp [{lo:+.2f}, {hi:+.2f}], rho={rho:+.3f}")
