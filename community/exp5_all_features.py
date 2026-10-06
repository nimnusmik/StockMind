"""Experiment 5: predicting tomorrow's direction with ALL comment features (linear + tree models)

Target = tomorrow's return vs SPY > 0. Ticker-day level, days with >= 5 comments.
Feature groups
  trading:   today's return, 5-day return, volume (vs usual), move size (vs usual)
  comments:  comment count, author count (vs usual)
  sentiment: mean, vs usual, positive share, negative share, excess sentiment (not explained by today's price)
  toxicity:  daily means of the 6 Detoxify scores + max hostility
Models = logistic regression / gradient boosting (depth 3, 200 iters, lr 0.05 — fixed in advance, no tuning)
Eval = train Jul-Aug, test Sep. Only 544 training rows, so many features risk overfitting.
Prereqs: sentiment.py, toxicity.py outputs. Run: python3 exp5_all_features.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

HERE = Path(__file__).parent
START, CUT, MIN_POSTS = "2026-07-02", pd.Timestamp("2026-09-01"), 5
TOX = ["toxicity", "severe_toxicity", "obscene", "threat", "insult", "identity_attack"]

# 1) posts -> trading day, attach sentiment and toxicity
con = sqlite3.connect(HERE / "data" / "community.db")
posts = pd.read_sql("SELECT uuid, symbol, created_at, username FROM posts", con)
posts = posts.merge(pd.read_csv(HERE / "data" / "sentiment.csv"), on="uuid").merge(
    pd.read_csv(HERE / "data" / "toxicity.csv"), on="uuid")
posts["s"] = posts.pos - posts.neg
lab = posts[["neg", "neu", "pos"]].values.argmax(1)
posts["is_pos"], posts["is_neg"] = (lab == 2).astype(float), (lab == 0).astype(float)

prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
agg = {"n": ("s", "size"), "authors": ("username", "nunique"), "sent": ("s", "mean"),
       "pos_share": ("is_pos", "mean"), "neg_share": ("is_neg", "mean")}
agg.update({t: (t, "mean") for t in TOX})
daily = posts.groupby(["symbol", "date"]).agg(**agg).reset_index()

# 2) trading features + vs-usual features (past information only)
px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
vol = prices[prices.symbol != "SPY"][["date", "symbol", "volume"]]
df = ex.merge(vol, on=["date", "symbol"]).merge(daily, on=["symbol", "date"], how="left").sort_values(["symbol", "date"])
g = df.groupby("symbol")
past = lambda s, k=5: s.groupby(df.symbol).transform(lambda x: x.expanding(k).mean().shift(1))
df["ex_next"] = g.ex.shift(-1)
df["ex5"] = g.ex.transform(lambda x: x.rolling(5).sum())
df["vol_rel"] = np.log(df.volume) - past(np.log(df.volume), 10)
df["move_rel"] = df.ex.abs() / past(df.ex.abs(), 10)
valid = df.n >= MIN_POSTS
v = lambda c: df[c].where(valid)
df["com_rel"] = np.log1p(df.n) - past(np.log1p(v("n")))
df["auth_rel"] = np.log1p(df.authors) - past(np.log1p(v("authors")))
df["sent_rel"] = df.sent - past(v("sent"))
for t in TOX:  # standardize vs the ticker's past -> the max = the day's most unusual toxicity type (the paper's overall hostility)
    sd = v(t).groupby(df.symbol).transform(lambda x: x.expanding(5).std().shift(1))
    df[t + "_z"] = (df[t] - past(v(t))) / sd
df["hostility"] = df[[t + "_z" for t in TOX if t != "severe_toxicity"]].max(axis=1)
df = df[valid & (df.date >= START)].reset_index(drop=True)

train, test = (df.date < CUT).values, (df.date >= CUT).values
fit = LinearRegression().fit(df.loc[train & df.sent_rel.notna(), ["ex"]], df.loc[train & df.sent_rel.notna(), "sent_rel"])
df["excess"] = df.sent_rel - fit.predict(df[["ex"]])

GROUPS = {
    "trading": ["ex", "ex5", "vol_rel", "move_rel"],
    "comments": ["com_rel", "auth_rel"],
    "sentiment": ["sent", "sent_rel", "pos_share", "neg_share", "excess"],
    "toxicity": TOX + ["hostility"],
}
SETS = {k: v for k, v in GROUPS.items()}
SETS["all comment features"] = GROUPS["comments"] + GROUPS["sentiment"] + GROUPS["toxicity"]
SETS["everything"] = sum(GROUPS.values(), [])
need = SETS["everything"] + ["ex_next"]
df = df.dropna(subset=need).reset_index(drop=True)
train, test = (df.date < CUT).values, (df.date >= CUT).values
y = (df.ex_next > 0).astype(int).values

MODELS = {
    "logistic": lambda: make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
    "boosting": lambda: HistGradientBoostingClassifier(max_depth=3, max_iter=200, learning_rate=0.05, random_state=0),
}
tdays = np.unique(df.date[test])
pos = {d: np.where(df.date.values[test] == d)[0] for d in tdays}
rng = np.random.default_rng(0)
picks = [np.concatenate([pos[d] for d in rng.choice(tdays, len(tdays))]) for _ in range(1000)]

print(f"{len(df)} ticker-days — train {train.sum()} (Jul-Aug), test {test.sum()} (Sep, {len(tdays)} trading days), up to {len(SETS['everything'])} features")
print(f"share of up days: train {y[train].mean():.3f}, test {y[test].mean():.3f}\n")
print(f"{'feature set':24}{'k':>4}" + "".join(f"{m + ' ROC':>14}{'95% CI':>17}" for m in MODELS))
yt = y[test]
for name, cols in SETS.items():
    row = f"{name:24}{len(cols):4}"
    for mname, make in MODELS.items():
        s = make().fit(df.loc[train, cols], y[train]).predict_proba(df.loc[test, cols])[:, 1]
        boot = [roc_auc_score(yt[i], s[i]) for i in picks if 0 < yt[i].mean() < 1]
        lo, hi = np.percentile(boot, [2.5, 97.5])
        row += f"{roc_auc_score(yt, s):13.3f}   [{lo:.3f}, {hi:.3f}]"
    print(row)
print(f"\nbaseline: 0.500 = coin flip. If the CI contains 0.5, we cannot claim it predicts.")
print(f"{len(SETS)} sets x {len(MODELS)} models = {len(SETS) * len(MODELS)} tests -> one may look good by chance")
