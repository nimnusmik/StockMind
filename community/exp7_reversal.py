"""Experiment 7: do comments turn before the trend flips? (stock version of the paper's experiment 6, "leader reversal")

Trend (leader) = close above the 20-day moving average -> up (+1), below -> down (-1)
Reversal       = tomorrow's trend sign differs from today's (the MA is crossed)
Leader-side sentiment = trend sign x sentiment (vs the ticker's norm). Negative = mood opposing the current trend.
(1) Pattern: mean leader-side sentiment on pre-reversal days vs other days, per ticker + binomial test
    (including a comparison matched on distance to the MA)
(2) Prediction: market state (distance to MA, recent moves) vs + leader-side sentiment; train Jul-Aug -> test Sep, PR-AUC
A day = previous trading day 16:00 ET to 16:00 ET, days with >= 5 comments (same sample as experiments 4-6)
Run: python3 exp7_reversal.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import binomtest
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

HERE = Path(__file__).parent
START, CUT, MIN_POSTS, MA = "2026-07-02", pd.Timestamp("2026-09-01"), 5, 20

posts = pd.read_sql("SELECT uuid, symbol, created_at FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts.merge(pd.read_csv(HERE / "data" / "sentiment.csv"), on="uuid")
posts["s"] = posts.pos - posts.neg
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[(prices.date < pd.Timestamp.now().normalize()) & (prices.symbol != "SPY")]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
daily = posts.groupby(["symbol", "date"]).agg(n=("s", "size"), sent=("s", "mean")).reset_index()

df = prices.sort_values(["symbol", "date"]).merge(daily, on=["symbol", "date"], how="left").reset_index(drop=True)
g = df.groupby("symbol").close
df["ma"] = g.transform(lambda x: x.rolling(MA).mean())
df["side"] = np.sign(df.close - df.ma)
df["dist"] = (df.close / df.ma - 1).abs()  # distance to the MA (closer = easier to cross)
df["ret"] = g.pct_change()
df["move_lead"] = df.side * df.ret  # positive when today's move goes with the trend
df["ret5_lead"] = df.side * g.transform(lambda x: x.pct_change(5))
nxt = df.groupby("symbol").side.shift(-1)
df["reversal"] = (nxt != df.side).astype(float).where(nxt.notna() & (df.side != 0))
valid = df.n >= MIN_POSTS
df["sent_rel"] = df.sent - df.sent.where(valid).groupby(df.symbol).transform(lambda x: x.expanding(5).mean().shift(1))
df["lead_sent"] = df.side * df.sent_rel
STATE = ["dist", "move_lead", "ret5_lead"]
df = df[valid & (df.date >= START)].dropna(subset=STATE + ["lead_sent", "reversal"]).reset_index(drop=True)
y = df.reversal.astype(int).values
print(f"{len(df)} ticker-days, {y.sum()} reversals ({y.mean():.1%})\n")

# (1) pattern — is leader-side sentiment lower the day before a reversal?
pre, other = df[y == 1], df[y == 0]
print("(1) pattern: mean leader-side sentiment (negative = opposing the current trend)")
print(f"  pre-reversal {pre.lead_sent.mean():+.3f} ({len(pre)} days) / other {other.lead_sent.mean():+.3f} ({len(other)} days)")
for label, d in [("all", df), ("far from the MA only (top 50% distance)", df[df.dist > df.dist.median()])]:
    t = d.groupby(["symbol", "reversal"]).lead_sent.mean().unstack()
    t = t[d.groupby("symbol").reversal.sum().reindex(t.index) >= 2].dropna()  # tickers with >= 2 reversals only
    k = int((t[1.0] < t[0.0]).sum())
    p = binomtest(k, len(t), 0.5).pvalue if len(t) else float("nan")
    print(f"  [{label}] tickers more negative pre-reversal: {k}/{len(t)} (binomial p = {p:.3f}), median within-ticker diff {(t[1.0] - t[0.0]).median():+.3f}")

# (2) prediction — does adding leader-side sentiment to market state improve reversal prediction?
train, test = (df.date < CUT).values, (df.date >= CUT).values
yt = y[test]
print(f"\n(2) prediction: Sep test {test.sum()} days, {yt.sum()} reversals (baseline = reversal rate {yt.mean():.3f})")
tdays = np.unique(df.date[test])
pos = {d: np.where(df.date.values[test] == d)[0] for d in tdays}
rng = np.random.default_rng(0)
picks = [np.concatenate([pos[d] for d in rng.choice(tdays, len(tdays))]) for _ in range(1000)]
sc = {}
for name, cols in {"market state": STATE, "leader-side sentiment": ["lead_sent"], "state + sentiment": STATE + ["lead_sent"]}.items():
    m = make_pipeline(StandardScaler(), LogisticRegression()).fit(df.loc[train, cols], y[train])
    sc[name] = m.predict_proba(df.loc[test, cols])[:, 1]
    print(f"  {name:22} PR-AUC {average_precision_score(yt, sc[name]):.3f}  ROC-AUC {roc_auc_score(yt, sc[name]):.3f}")
d = [average_precision_score(yt[i], sc["state + sentiment"][i]) - average_precision_score(yt[i], sc["market state"][i])
     for i in picks if yt[i].any()]
print(f"  gain from sentiment (PR-AUC) {np.mean(d):+.3f} [{np.percentile(d, 2.5):+.3f}, {np.percentile(d, 97.5):+.3f}]")

# post-hoc check: sentiment follows same-day price (rho 0.31) — does the pattern survive after removing
# the part explained by that day's move?
from sklearn.linear_model import LinearRegression
fit = LinearRegression().fit(df.loc[train, ["move_lead"]], df.lead_sent[train])
df["lead_excess"] = df.lead_sent - fit.predict(df[["move_lead"]])
t = df.groupby(["symbol", "reversal"]).lead_excess.mean().unstack()
t = t[df.groupby("symbol").reversal.sum().reindex(t.index) >= 2].dropna()
k = int((t[1.0] < t[0.0]).sum())
print(f"\n[post-hoc] leader-side sentiment with the same-day move removed: pre-reversal {df.lead_excess[y == 1].mean():+.3f} / other {df.lead_excess[y == 0].mean():+.3f}")
print(f"  tickers more negative pre-reversal: {k}/{len(t)} (binomial p = {binomtest(k, len(t), 0.5).pvalue:.3f})")
