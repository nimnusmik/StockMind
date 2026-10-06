"""How positive/negative must comments be before the price moves? — next-day price by sentiment decile

Sentiment = daily mean of (pos prob - neg prob), relative to the ticker's PAST mean (corrects boards that are
            pessimistic by nature)
A day = previous trading day 16:00 ET to 16:00 ET, days with >= 5 comments only
Return = ticker - SPY (removes the market-wide move)
Three panels: (1) next-day return (2) size of the next-day move (vs usual) (3) TODAY's return —
            if (3) slopes, sentiment follows the price
Error bars = date-block bootstrap 95% intervals (same-day tickers co-move)
Run: python3 plot_sentiment_deciles.py -> figures/sentiment_deciles.png
"""
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
FIG = HERE / "figures" / "sentiment_deciles.png"
START, MIN_POSTS, BINS = "2026-07-02", 5, 10

# 1) posts -> trading day, attach sentiment
posts = pd.read_sql("SELECT uuid, symbol, created_at, body FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
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

# 2) returns (vs SPY) + sentiment vs the ticker's usual
px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="left").sort_values(["symbol", "date"])
g = df.groupby("symbol")
df["ex_next"] = g.ex.shift(-1)
df["absx_next"] = df.ex_next.abs() / g.ex.transform(lambda x: x.abs().expanding(10).mean())
valid = df.n >= MIN_POSTS
df["sent_rel"] = df.sent - df.sent.where(valid).groupby(df.symbol).transform(lambda x: x.expanding(5).mean().shift(1))
df = df[valid & (df.date >= START)].dropna(subset=["sent_rel", "ex_next", "absx_next", "ex"]).reset_index(drop=True)
df["bin"] = pd.qcut(df.sent_rel, BINS, labels=False) + 1

# 3) decile means + date-block bootstrap
cols = {"ex_next": 100, "absx_next": 1, "ex": 100}  # returns in %
mean = df.groupby("bin")[list(cols)].mean() * pd.Series(cols)
dates = df.date.unique()
rng = np.random.default_rng(0)
by_date = {d: grp for d, grp in df.groupby("date")}
boot = [pd.concat([by_date[d] for d in rng.choice(dates, len(dates))]).groupby("bin")[list(cols)].mean() * pd.Series(cols)
        for _ in range(1000)]
lo = pd.concat(boot).groupby(level=0).quantile(0.025)
hi = pd.concat(boot).groupby(level=0).quantile(0.975)

# 4) figure — each panel measures something different, so y-axes are per panel
INK, INK2, GRID, SURF, DOT = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb", "#2a78d6"
fig, axes = plt.subplots(1, 3, figsize=(15, 5), facecolor=SURF)
panels = [("ex_next", "(1) next-day return (vs SPY, %)", 0),
          ("absx_next", "(2) size of next-day move (x usual)", 1),
          ("ex", "(3) TODAY's return (vs SPY, %) — reverse check", 0)]
x = mean.index.values
for ax, (c, title, ref) in zip(axes, panels):
    ax.set_facecolor(SURF)
    ax.axhline(ref, color=INK2, lw=1, ls=(0, (3, 3)))
    ax.vlines(x, lo[c], hi[c], color=DOT, lw=2, alpha=0.35)
    ax.plot(x, mean[c], color=DOT, lw=2, marker="o", ms=8, mfc=DOT, mec=SURF, mew=2)
    ax.set_title(title, loc="left", fontsize=11, color=INK)
    ax.set_xticks(x)
    ax.set_xticklabels(["most\nnegative"] + [str(i) for i in x[1:-1]] + ["most\npositive"])
    ax.grid(axis="y", color=GRID, lw=0.8)
    ax.set_axisbelow(True)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    for s in ["left", "bottom"]:
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2)
axes[1].set_xlabel("today's comment sentiment (vs the ticker's usual, deciles)", color=INK2)

r_next = spearmanr(df.sent_rel, df.ex_next)
r_today = spearmanr(df.sent_rel, df.ex)
fig.suptitle("How extreme must sentiment be before the next-day price moves?", x=0.04, ha="left", fontsize=15, color=INK, fontweight="bold")
fig.text(0.04, 0.89,
         f"Yahoo Finance community, 15 tickers, {df.date.min():%m-%d}~{df.date.max():%m-%d}, {len(df)} ticker-days (~{len(df) // BINS} per bin). "
         f"Dot = mean, bar = date-block bootstrap 95% CI. Rank corr: sentiment vs next day {r_next.statistic:.2f}, vs today {r_today.statistic:.2f}",
         ha="left", fontsize=9.5, color=INK2)
fig.tight_layout(rect=(0, 0, 1, 0.87))
fig.savefig(FIG, dpi=180, facecolor=SURF)

print(f"{len(df)} ticker-days")
print((mean.round(3)).assign(n=df.groupby("bin").size()).to_string())
print(f"\nsentiment vs next-day return rho = {r_next.statistic:.3f} (p = {r_next.pvalue:.3f})")
print(f"sentiment vs TODAY's return  rho = {r_today.statistic:.3f} (p = {r_today.pvalue:.3f})  <- does sentiment follow the price?")
print(f"saved: {FIG}")

# 5) read the most extreme posts (does the model misread slang or sarcasm?)
for b, label in [(1, "most negative bin"), (BINS, "most positive bin")]:
    keys = df.loc[df.bin == b, ["symbol", "date"]]
    sample = posts.merge(keys, on=["symbol", "date"]).sort_values("s", ascending=(b == 1)).head(8)
    print(f"\n[{label}] 8 most extreme posts by sentiment score")
    for _, r in sample.iterrows():
        print(f"  {r.s:+.2f} {r.symbol:5} {str(r.body).replace(chr(10), ' ')[:110]}")
