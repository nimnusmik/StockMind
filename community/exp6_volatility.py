"""Experiment 6: do comments help forecast tomorrow's volatility? — HAR baseline + comments

Volatility = Garman-Klass (daily variance from OHLC)
Baseline   = HAR (Corsi 2009, log version): yesterday / 5-day mean / 22-day mean log-vol -> tomorrow's log-vol
Added      = comment volume (posts and authors, vs usual), sentiment (vs usual, negative share), toxicity (max hostility)
Eval       = train Jul-Aug -> test Sep, linear regression, scored by QLIKE (lower is better) and log MSE;
             date-block bootstrap.
A day = previous trading day 16:00 ET to 16:00 ET, days with >= 5 comments (same sample as experiments 4-5)
Run: python3 exp6_volatility.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

HERE = Path(__file__).parent
START, CUT, MIN_POSTS = "2026-07-02", pd.Timestamp("2026-09-01"), 5
TOX = ["toxicity", "obscene", "threat", "insult", "identity_attack"]

# 1) posts -> trading day, sentiment and toxicity
posts = pd.read_sql("SELECT uuid, symbol, created_at, username FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts.merge(pd.read_csv(HERE / "data" / "sentiment.csv"), on="uuid").merge(pd.read_csv(HERE / "data" / "toxicity.csv"), on="uuid")
posts["s"] = posts.pos - posts.neg
posts["is_neg"] = (posts[["neg", "neu", "pos"]].values.argmax(1) == 0).astype(float)
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[(prices.date < pd.Timestamp.now().normalize()) & (prices.symbol != "SPY")]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
agg = {"n": ("s", "size"), "authors": ("username", "nunique"), "sent": ("s", "mean"), "neg_share": ("is_neg", "mean")}
agg.update({t: (t, "mean") for t in TOX})
daily = posts.groupby(["symbol", "date"]).agg(**agg).reset_index()

# 2) Garman-Klass volatility + HAR terms
p = prices.sort_values(["symbol", "date"]).copy()
hl, co = np.log(p.high / p.low), np.log(p.close / p.open)
p["gk"] = (0.5 * hl**2 - (2 * np.log(2) - 1) * co**2).clip(lower=1e-8)
p["lv"] = np.log(p.gk)
g = p.groupby("symbol").lv
p["lv_w"] = g.transform(lambda x: x.rolling(5).mean())
p["lv_m"] = g.transform(lambda x: x.rolling(22).mean())
p["target"] = g.shift(-1)

df = p.merge(daily, on=["symbol", "date"], how="left").sort_values(["symbol", "date"]).reset_index(drop=True)
valid = df.n >= MIN_POSTS
past = lambda s: s.where(valid).groupby(df.symbol).transform(lambda x: x.expanding(5).mean().shift(1))
df["com_rel"] = np.log1p(df.n) - past(np.log1p(df.n))
df["auth_rel"] = np.log1p(df.authors) - past(np.log1p(df.authors))
df["sent_rel"] = df.sent - past(df.sent)
for t in TOX:
    sd = df[t].where(valid).groupby(df.symbol).transform(lambda x: x.expanding(5).std().shift(1))
    df[t + "_z"] = (df[t] - past(df[t])) / sd
df["hostility"] = df[[t + "_z" for t in TOX]].max(axis=1)

HAR = ["lv", "lv_w", "lv_m"]
SETS = {
    "HAR (baseline)": HAR,
    "HAR + comments": HAR + ["com_rel", "auth_rel"],
    "HAR + sentiment": HAR + ["sent_rel", "neg_share"],
    "HAR + toxicity": HAR + ["hostility"],
    "HAR + all": HAR + ["com_rel", "auth_rel", "sent_rel", "neg_share", "hostility"],
}
df = df[valid & (df.date >= START)].dropna(subset=SETS["HAR + all"] + ["target"]).reset_index(drop=True)
train, test = (df.date < CUT).values, (df.date >= CUT).values
yt = df.target.values[test]


def qlike(lv_true, lv_pred):
    """QLIKE = true/pred - log(true/pred) - 1 (variance scale). 0 is perfect; lower is better."""
    r = np.exp(lv_true - lv_pred)
    return r - np.log(r) - 1


preds = {}
for name, cols in SETS.items():
    m = LinearRegression().fit(df.loc[train, cols], df.target[train])
    resid_var = np.var(df.target[train] - m.predict(df.loc[train, cols]))
    preds[name] = m.predict(df.loc[test, cols]) + 0.5 * resid_var  # log -> variance bias correction

naive = df.lv.values[test]  # naive "tomorrow = today" baseline
tdays = np.unique(df.date[test])
pos = {d: np.where(df.date.values[test] == d)[0] for d in tdays}
rng = np.random.default_rng(0)
picks = [np.concatenate([pos[d] for d in rng.choice(tdays, len(tdays))]) for _ in range(1000)]

print(f"{len(df)} ticker-days — train {train.sum()} (Jul-Aug), test {test.sum()} (Sep, {len(tdays)} trading days)\n")
print(f"{'model':16}{'QLIKE':>8}{'log MSE':>10}   QLIKE vs HAR (negative = better) 95% CI")
q_har = qlike(yt, preds["HAR (baseline)"])
print(f"{'tomorrow=today':16}{qlike(yt, naive).mean():8.3f}{np.mean((yt - naive) ** 2):10.3f}")
for name, pr in preds.items():
    q = qlike(yt, pr)
    line = f"{name:16}{q.mean():8.3f}{np.mean((yt - pr) ** 2):10.3f}"
    if name != "HAR (baseline)":
        d = q - q_har
        lo, hi = np.percentile([d[i].mean() for i in picks], [2.5, 97.5])
        line += f"   {d.mean():+.4f} [{lo:+.4f}, {hi:+.4f}]"
    print(line)

coef = LinearRegression().fit(df.loc[train, SETS["HAR + all"]], df.target[train])
print("\n'HAR + all' coefficients (training period): " + ", ".join(f"{c} {v:+.3f}" for c, v in zip(SETS["HAR + all"], coef.coef_)))

# post-hoc check (added after seeing results): did the Jul-Aug earnings season spike comments and volatility together,
# polluting the coefficients? refit with earnings +/- 2 days removed from training
earn = pd.read_csv(HERE / "data" / "earnings.csv")
earn["d"] = pd.to_datetime(earn.time.str[:10])
near = np.zeros(len(df), bool)
for _, e in earn.iterrows():
    near |= (df.symbol == e.symbol).values & (abs((df.date - e.d).dt.days) <= 2).values
tr = train & ~near
print(f"\n[post-hoc] removed {(train & near).sum()} earnings +/- 2 day rows from training (train {tr.sum()})")
pr = {}
for name, cols in SETS.items():
    m = LinearRegression().fit(df.loc[tr, cols], df.target[tr])
    pr[name] = m.predict(df.loc[test, cols]) + 0.5 * np.var(df.target[tr] - m.predict(df.loc[tr, cols]))
qh = qlike(yt, pr["HAR (baseline)"])
for name in list(SETS)[1:]:
    d = qlike(yt, pr[name]) - qh
    lo, hi = np.percentile([d[i].mean() for i in picks], [2.5, 97.5])
    print(f"  {name:16} vs HAR {d.mean():+.4f} [{lo:+.4f}, {hi:+.4f}]")
