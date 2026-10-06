"""Experiment 4: predicting tomorrow's direction with EXCESS sentiment (Tetlock-2007-style overreaction -> reversal)

Excess sentiment = the part of today's sentiment (vs the ticker's norm) not explained by today's return.
  The sentiment ~ today's-return regression is fit on the TRAINING period only; its residual is
  then computed for all days (prevents leakage).
Target = tomorrow's return vs SPY > 0
Eval = train Jul-Aug, test Sep. Success bar pre-set at 52-55% (above 60% would suggest leakage).
Run: python3 exp4_excess_sentiment.py -> figures/excess_sentiment.png
"""
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

HERE = Path(__file__).parent
START, CUT, MIN_POSTS = "2026-07-02", pd.Timestamp("2026-09-01"), 5

# 1) per ticker-day sentiment and returns (same definitions as plot_sentiment_deciles.py)
posts = pd.read_sql("SELECT uuid, symbol, created_at FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts.merge(pd.read_csv(HERE / "data" / "sentiment.csv"), on="uuid")
posts["s"] = posts.pos - posts.neg
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
daily = posts.groupby(["symbol", "date"]).agg(n=("s", "size"), sent=("s", "mean")).reset_index()

px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="left").sort_values(["symbol", "date"])
df["ex_next"] = df.groupby("symbol").ex.shift(-1)
valid = df.n >= MIN_POSTS
df["sent_rel"] = df.sent - df.sent.where(valid).groupby(df.symbol).transform(lambda x: x.expanding(5).mean().shift(1))
df = df[valid & (df.date >= START)].dropna(subset=["sent_rel", "ex", "ex_next"]).reset_index(drop=True)
train, test = (df.date < CUT).values, (df.date >= CUT).values

# 2) excess sentiment = sentiment - (what the training period says sentiment should be, given today's return)
fit = LinearRegression().fit(df.loc[train, ["ex"]], df.loc[train, "sent_rel"])
df["excess"] = df.sent_rel - fit.predict(df[["ex"]])
y = (df.ex_next > 0).astype(int).values

SETS = {"today's return": ["ex"], "sentiment": ["sent_rel"], "excess sentiment": ["excess"], "excess + today's return": ["excess", "ex"]}
day = df.date.values
rng = np.random.default_rng(0)
tdays = np.unique(day[test])
print(f"{len(df)} ticker-days — train {train.sum()} (Jul-Aug), test {test.sum()} (Sep, {len(tdays)} trading days)")
print(f"sentiment ~ today's-return slope {fit.coef_[0]:.2f} (training period)\n")
print(f"{'features':26}{'ROC-AUC':>8}{'95% CI':>18}{'accuracy':>9}")
for name, cols in SETS.items():
    m = make_pipeline(StandardScaler(), LogisticRegression()).fit(df.loc[train, cols], y[train])
    s = m.predict_proba(df.loc[test, cols])[:, 1]
    yt, dt = y[test], day[test]
    pos = {d: np.where(dt == d)[0] for d in tdays}
    boot = []
    for _ in range(1000):
        i = np.concatenate([pos[d] for d in rng.choice(tdays, len(tdays))])
        if 0 < yt[i].mean() < 1:
            boot.append(roc_auc_score(yt[i], s[i]))
    lo, hi = np.percentile(boot, [2.5, 97.5])
    print(f"{name:26}{roc_auc_score(yt, s):8.3f}   [{lo:.3f}, {hi:.3f}]{((s > 0.5) == yt).mean():9.3f}")
print(f"{'always guess the majority':26}{0.5:8.3f}{'':18}{max(y[test].mean(), 1 - y[test].mean()):9.3f}")

# 3) figure: tomorrow's return by excess-sentiment decile (train and test periods separately)
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb"
COLORS = {"train (Jul-Aug)": "#2a78d6", "test (Sep)": "#d6762a"}
edges = np.quantile(df.loc[train, "excess"], np.linspace(0, 1, 11))  # bin edges from the training period only
df["bin"] = np.clip(np.searchsorted(edges[1:-1], df.excess, side="right") + 1, 1, 10)
fig, ax = plt.subplots(figsize=(9, 5), facecolor=SURF)
ax.set_facecolor(SURF)
ax.axhline(0, color=INK2, lw=1, ls=(0, (3, 3)))
for (label, mask), dx in zip([("train (Jul-Aug)", train), ("test (Sep)", test)], [-0.12, 0.12]):
    m = df[mask].groupby("bin").ex_next.mean() * 100
    ax.plot(m.index + dx, m.values, color=COLORS[label], lw=2, marker="o", ms=8, mec=SURF, mew=2, label=label)
ax.set_xticks(range(1, 11))
ax.set_xticklabels(["more negative\nthan the price"] + [str(i) for i in range(2, 10)] + ["more positive\nthan the price"])
ax.set_ylabel("tomorrow's return (vs SPY, %)", color=INK2)
ax.set_title("Does sentiment that overshoots the price foretell a reversal tomorrow?", loc="left", fontsize=13, color=INK, fontweight="bold")
ax.legend(frameon=False, labelcolor=INK2)
ax.grid(axis="y", color=GRID, lw=0.8)
ax.set_axisbelow(True)
for s in ["top", "right"]:
    ax.spines[s].set_visible(False)
for s in ["left", "bottom"]:
    ax.spines[s].set_color(GRID)
ax.tick_params(colors=INK2)
fig.tight_layout()
fig.savefig(HERE / "figures" / "excess_sentiment.png", dpi=180, facecolor=SURF)
print("\nsaved: figures/excess_sentiment.png  (a reversal would show high on the left, low on the right)")
