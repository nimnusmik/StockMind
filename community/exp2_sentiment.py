"""Experiment 2: does comment CONTENT (sentiment) help? — 1-hour blocks, two problems

A. Next-bar volume burst (same target as experiment 1) — does adding sentiment improve it?
B. Next-bar price direction (up or down) — stock version of the paper's experiment 4 (buy direction).
   Stocks have no buy/sell flow data, so the sign of the return stands in.

Sentiment = (pos prob - neg prob) per post, averaged per block. Blocks with no comments = 0 (neutral).
Eval = time split by default (train before Sep 1, test after); leave-ticker-out shown for reference.
Prereqs: exp1_burst_hourly.py (-> data/prices_1h.csv), sentiment.py (-> data/sentiment.csv)
Run: python3 exp2_sentiment.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

HERE = Path(__file__).parent
START = pd.Timestamp("2026-07-01 09:30", tz="America/New_York")
CUT = pd.Timestamp("2026-09-01", tz="America/New_York")

# 1) 1-hour bars + posts (with sentiment) -> blocks
bars = pd.read_csv(HERE / "data" / "prices_1h.csv")
bars["start"] = pd.to_datetime(bars.start, utc=True).dt.tz_convert("America/New_York")
bars = bars.sort_values(["symbol", "start"]).reset_index(drop=True)
bars["slot"] = bars.start.dt.strftime("%H:%M")

posts = pd.read_sql("SELECT uuid, symbol, created_at, username FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
sent = pd.read_csv(HERE / "data" / "sentiment.csv")
posts = posts.merge(sent, on="uuid", how="inner")
posts["s"] = posts.pos - posts.neg
lab = posts[["neg", "neu", "pos"]].values.argmax(1)
posts["is_pos"], posts["is_neg"] = (lab == 2).astype(float), (lab == 0).astype(float)
posts["t"] = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")

parts = []
for sym, b in bars.groupby("symbol"):
    p = posts[posts.symbol == sym]
    k = np.searchsorted(b.start.values, p.t.values, side="right") - 1
    q = p[k >= 0].reset_index(drop=True)
    q["start"] = b.start.iloc[k[k >= 0]].reset_index(drop=True)
    parts.append(q)
blk = pd.concat(parts).groupby(["symbol", "start"]).agg(
    comments=("uuid", "size"), authors=("username", "nunique"),
    sent=("s", "mean"), pos_share=("is_pos", "mean"), neg_share=("is_neg", "mean")).reset_index()

df = bars.merge(blk, on=["symbol", "start"], how="left")
has = df.comments.notna() & (df.start >= START)
df[["comments", "authors"]] = df[["comments", "authors"]].fillna(0)
df.loc[df.start < START, ["comments", "authors"]] = np.nan
df["net"] = df.pos_share - df.neg_share

# 2) features (past information only)
key = [df.symbol, df.slot]
past_slot = lambda s: s.groupby(key).transform(lambda x: x.expanding(5).mean().shift(1))
past_sym = lambda s: s.groupby(df.symbol).transform(lambda x: x.expanding(20).mean().shift(1))
df["ret"] = df.groupby("symbol").close.pct_change()
df["ret4"] = df.groupby("symbol").ret.transform(lambda x: x.rolling(4).sum())
df["vol_rel"] = np.log1p(df.volume) - past_slot(np.log1p(df.volume))
df["move_rel"] = df.ret.abs() / past_slot(df.ret.abs())
df["com_rel"] = np.log1p(df.comments) - past_slot(np.log1p(df.comments))
df["auth_rel"] = np.log1p(df.authors) - past_slot(np.log1p(df.authors))
df["sent_rel"] = df.sent - past_sym(df.sent.where(has))  # vs the ticker's usual mood
for c in ["sent", "net", "sent_rel"]:
    df[c] = df[c].where(has, 0.0).fillna(0.0)

# 3) targets
g = df.groupby("symbol")
p80 = df.volume.groupby(key).transform(lambda x: x.expanding(15).quantile(0.8).shift(1))
nv, np80 = g.volume.shift(-1), p80.groupby(df.symbol).shift(-1)
df["burst"] = (nv > np80).astype(float).where(nv.notna() & np80.notna())
nr = g.ret.shift(-1)
df["up"] = (nr > 0).astype(float).where(nr.notna() & (nr != 0))

TRADE_B, TRADE_D = ["vol_rel", "move_rel"], ["ret", "ret4"]
ATT, SENT = ["com_rel", "auth_rel"], ["sent", "sent_rel", "net"]
PROBLEMS = {
    "A. volume burst (PR-AUC)": ("burst", average_precision_score, {
        "trading": TRADE_B, "comments": ATT, "sentiment": SENT, "comments+sentiment": ATT + SENT,
        "trading+comments": TRADE_B + ATT, "trading+comments+sentiment": TRADE_B + ATT + SENT}),
    "B. price direction (ROC-AUC)": ("up", roc_auc_score, {
        "trading": TRADE_D, "comments": ATT, "sentiment": SENT,
        "trading+sentiment": TRADE_D + SENT, "trading+comments+sentiment": TRADE_D + ATT + SENT}),
}


def predict(d, y, cols, splits):
    s = np.full(len(d), np.nan)
    for tr, te in splits:
        m = make_pipeline(StandardScaler(), LogisticRegression()).fit(d.loc[tr, cols], y[tr])
        s[te] = m.predict_proba(d.loc[te, cols])[:, 1]
    return s


def gain_ci(y, s_new, s_base, metric, day, n=1000):
    """s_new - s_base score gain with a date-block bootstrap 95% interval (models fixed; only scoring resampled)."""
    f = lambda i: metric(y[i], s_new[i]) - metric(y[i], s_base[i])
    uniq, r = np.unique(day), np.random.default_rng(1)
    pos = {d: np.where(day == d)[0] for d in uniq}
    boot = [f(np.concatenate([pos[d] for d in r.choice(uniq, len(uniq))])) for _ in range(n)]
    return f(np.arange(len(y))), *np.percentile(boot, [2.5, 97.5])


for title, (target, metric, sets) in PROBLEMS.items():
    allcols = sorted({c for v in sets.values() for c in v})
    d = df.dropna(subset=allcols + [target]).reset_index(drop=True)
    y = d[target].astype(int).values
    syms, fut = d.symbol.values, (d.start >= CUT).values
    schemes = {"time split": [(~fut, fut)],
               "leave-ticker-out (ref)": [(syms != s, syms == s) for s in np.unique(syms)]}
    print(f"\n{title}  — {len(d)} observations, positive rate {y.mean():.3f}, test (Sep) {fut.sum()}")
    print(f"  {'features':28}" + "".join(f"{k:>24}" for k in schemes) + f"{'Sep overnight':>14}")
    res = {k: {n: predict(d, y, c, sp) for n, c in sets.items()} for k, sp in schemes.items()}
    night = fut & (d.slot == "15:30").values
    for n in sets:
        row = "".join(f"{metric(y[~np.isnan(res[k][n])], res[k][n][~np.isnan(res[k][n])]):24.3f}" for k in schemes)
        print(f"  {n:28}{row}{metric(y[night], res['time split'][n][night]):14.3f}")
    base = "trading"
    print(f"  {'random baseline':28}{(y[fut].mean() if target == 'burst' else 0.5):24.3f}")
    ts = res["time split"]
    day = d.start.dt.date.values
    for n in [k for k in sets if k.startswith("trading+")]:
        for lbl, m in [("Sep all", fut), ("Sep overnight", night)]:
            gch, lo, hi = gain_ci(y[m], ts[n][m], ts[base][m], metric, day[m])
            print(f"  {n} - trading ({lbl}): {gch:+.3f} [{lo:+.3f}, {hi:+.3f}]")
    if target == "up":
        acc = ((ts["trading+sentiment"][fut] > 0.5) == y[fut]).mean()
        print(f"  accuracy (Sep, trading+sentiment) {acc:.3f} vs always guess the majority {max(y[fut].mean(), 1 - y[fut].mean()):.3f}")
