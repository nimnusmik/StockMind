"""Experiment 15: how far does trading history alone get on next-hour volume bursts, and which part carries it?

Experiments 1 and 16 both ended at "recent volume already explains it". This experiment drops comments
entirely and measures that baseline properly, as a ladder: each step adds one kind of memory.

Target  = next bar's volume > the ticker's PAST 80th percentile for that time of day (same as experiment 1).
Ladder  = 0 random (burst rate)
          1 "a burst now means a burst next" (persistence, no model)
          2 this bar's volume vs usual                       (logistic)
          3 + this bar's price move vs usual                 (logistic)
          4 + memory: last 3 bars, today so far, the target slot yesterday   (logistic)
          5 step 4 features, gradient boosting (depth 3, 200 iters, lr 0.05: fixed in advance, same as exp 5)
Eval    = time split: train before Sep 1, test from Sep 1. PR-AUC (bursts are rare). Every "usual" is a
          past-only expanding mean for the same ticker and time slot, so nothing looks ahead.
          Gains between steps get a date-block bootstrap 95% CI (tickers move together on the same day).
Prereq  : exp1_burst_hourly.py (writes data/prices_1h.csv)
Run     : python3 exp15_volume_spike.py
"""
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

HERE = Path(__file__).parent
NY = "America/New_York"
CUT = pd.Timestamp("2026-09-01", tz=NY)

# 1) 1-hour bars, one row per ticker x bar
df = pd.read_csv(HERE / "data" / "prices_1h.csv")
df["start"] = pd.to_datetime(df.start, utc=True).dt.tz_convert(NY)
df = df.sort_values(["symbol", "start"]).reset_index(drop=True)
df["slot"] = df.start.dt.strftime("%H:%M")  # time of day: volume is U-shaped, so compare like with like
df["date"] = df.start.dt.date
key = [df.symbol, df.slot]
past_mean = lambda s: s.groupby(key).transform(lambda x: x.expanding(5).mean().shift(1))  # up to yesterday only

# 2) features, all relative to the same ticker and slot's own past
lv = np.log1p(df.volume)
df["vol_rel"] = lv - past_mean(lv)
absr = df.groupby("symbol").close.pct_change().abs()
df["move_rel"] = absr / past_mean(absr)
df["last3"] = df.groupby("symbol").vol_rel.transform(lambda x: x.rolling(3).mean())           # this bar and the 2 before
df["day_sofar"] = df.groupby(["symbol", "date"]).vol_rel.transform(lambda x: x.expanding().mean())
yday = df.groupby(key).vol_rel.shift(1)                       # this slot, one trading day earlier
df["next_slot_yday"] = yday.groupby(df.symbol).shift(-1)      # the slot we are predicting, yesterday

# 3) target: is the NEXT bar a burst? threshold = past 80th percentile of that slot (needs 15 past days)
p80 = df.volume.groupby(key).transform(lambda x: x.expanding(15).quantile(0.8).shift(1))
df["burst_now"] = (df.volume > p80).astype(float).where(p80.notna())
nv, np80 = df.groupby("symbol").volume.shift(-1), p80.groupby(df.symbol).shift(-1)
df["target"] = (nv > np80).astype(float).where(nv.notna() & np80.notna())

MEMORY = ["last3", "day_sofar", "next_slot_yday"]
LADDER = {
    "2 this bar's volume": ["vol_rel"],
    "3 + price move": ["vol_rel", "move_rel"],
    "4 + memory": ["vol_rel", "move_rel"] + MEMORY,
}
data = df.dropna(subset=LADDER["4 + memory"] + ["target", "burst_now"]).reset_index(drop=True)
y = data.target.astype(int).values
train, test = (data.start < CUT).values, (data.start >= CUT).values
assert data.start[train].max() < data.start[test].min()  # no overlap between the two periods

# 4) fit on the training period, score the test period
scores = {"1 burst now -> burst next": data.burst_now.values}
for name, cols in LADDER.items():
    m = make_pipeline(StandardScaler(), LogisticRegression()).fit(data.loc[train, cols], y[train])
    scores[name] = m.predict_proba(data[cols])[:, 1]
gb = HistGradientBoostingClassifier(max_depth=3, max_iter=200, learning_rate=0.05, random_state=0)
scores["5 step 4, boosting"] = gb.fit(data.loc[train, LADDER["4 + memory"]], y[train]).predict_proba(data[LADDER["4 + memory"]])[:, 1]

idx = np.where(test)[0]
day = data.date.values
ap = lambda name, i: average_precision_score(y[i], scores[name][i])


def gain_ci(a, b, i, n=1000):
    """PR-AUC(a) - PR-AUC(b) on rows i, with a date-block bootstrap 95% interval."""
    d, r = day[i], np.random.default_rng(0)
    pos = {x: i[d == x] for x in np.unique(d)}
    boot = [ap(a, k) - ap(b, k) for k in (np.concatenate([pos[x] for x in r.choice(list(pos), len(pos))]) for _ in range(n))]
    return ap(a, i) - ap(b, i), *np.percentile(boot, [2.5, 97.5])


print(f"{len(data)} bars, {data.symbol.nunique()} tickers | train {train.sum()} ({data.start[train].min():%m-%d}~{data.start[train].max():%m-%d})"
      f" | test {test.sum()} ({data.start[test].min():%m-%d}~{data.start[test].max():%m-%d}, {len(np.unique(day[idx]))} trading days)\n")
print("TEST PR-AUC (higher is better)")
print(f"  {y[idx].mean():.3f}  0 random (burst rate)")
names = list(scores)
for name in names:
    print(f"  {ap(name, idx):.3f}  {name}")
print("\ngain of each step over the one before (95% CI; above 0 = the step helps)")
for a, b in zip(names[1:], names[:-1]):
    g, lo, hi = gain_ci(a, b, idx)
    print(f"  {a:26} vs {b:26} {g:+.3f} [{lo:+.3f}, {hi:+.3f}]")

m = make_pipeline(StandardScaler(), LogisticRegression()).fit(data.loc[train, LADDER["4 + memory"]], y[train])
print("\nstep 4 logistic coefficients (standardized; bigger = matters more):")
print("  " + ", ".join(f"{c} {w:+.2f}" for c, w in zip(LADDER["4 + memory"], m[-1].coef_[0])))

print("\nstep 4 PR-AUC by the slot being left (15:30 = predicting tomorrow's open)")
for slot, g in data[test].groupby("slot"):
    i = g.index.values
    print(f"  {slot} -> next bar: {ap('4 + memory', i):.3f}  (burst rate {y[i].mean():.2f}, this-bar-only {ap(chr(50) + ' this bar' + chr(39) + 's volume', i):.3f})")
