"""First chart: today's comment volume -> size of tomorrow's price move.

A day (t) of comments = previous trading day 16:00 ET to day t 16:00 ET (cut at the close to avoid
leakage; weekend posts go to Monday)
Tomorrow's move = |close(t+1) / close(t) - 1|
Baseline = today's move |r_t| (big-move days tend to be followed by big-move days anyway)

Run: python3 plot_comments_vs_move.py
Output: figures/comments_vs_next_move.png, data/daily_merged.csv (the table behind the chart)
"""
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
FIG = HERE / "figures" / "comments_vs_next_move.png"
START = "2026-07-02"  # the 7/1 window (from 6/30 16:00 ET) is excluded: some tickers' backfill does not cover it

# 1) posts -> counts per trading day
con = sqlite3.connect(HERE / "data" / "community.db")
posts = pd.read_sql("SELECT symbol, created_at FROM posts", con)
oldest = posts.groupby("symbol").created_at.min()
full = oldest[oldest < "2026-07-01"].index  # only tickers backfilled to 7/1 (new tickers join automatically once filled)
posts = posts[posts.symbol.isin(full)]

prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
days = np.sort(prices.date.unique())

et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)  # weekend/holiday posts -> next trading day
posts = posts[idx < len(days)]  # posts after the last close belong to a day with no price yet; exclude
posts["date"] = days[idx[idx < len(days)]]
counts = posts.groupby(["symbol", "date"]).size().rename("comments").reset_index()

# 2) prices -> today's and tomorrow's moves
p = prices[prices.symbol.isin(full)].sort_values(["symbol", "date"]).copy()
p["move_today"] = p.groupby("symbol").close.pct_change().abs()
p["move_next"] = p.groupby("symbol").move_today.shift(-1)

df = p.merge(counts, on=["symbol", "date"], how="left").fillna({"comments": 0})
df = df[(df.date >= START) & df.move_next.notna()].copy()

# 3) per ticker, scale to 'x times its usual' (puts NVDA's 300 posts/day and AMZN's 30 on one scale)
# ponytail: divided by the full-period average — fine for a descriptive chart; a predictive model must use past-window means only
g = df.groupby("symbol")
df["comments_x"] = df.comments / g.comments.transform("median")
df["today_x"] = df.move_today / g.move_today.transform("mean")
df["next_x"] = df.move_next / g.move_next.transform("mean")
df.to_csv(HERE / "data" / "daily_merged.csv", index=False)


def partial_spearman(x, y, z):
    """Rank correlation of x and y after removing the effect of z (today's move)."""
    rx, ry, rz = (pd.Series(v).rank().values for v in (x, y, z))
    res = lambda a: a - np.polyval(np.polyfit(rz, a, 1), rz)
    return spearmanr(res(rx), res(ry))


rho_c = spearmanr(df.comments_x, df.next_x)
rho_t = spearmanr(df.today_x, df.next_x)
rho_p = partial_spearman(df.comments_x, df.next_x, df.today_x)

# 4) figure: two panels sharing the y-axis (comments left, baseline right)
INK, INK2, GRID, SURF, DOT = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb", "#2a78d6"
fig, axes = plt.subplots(1, 2, figsize=(12, 5.4), sharey=True, facecolor=SURF)

panels = [("comments_x", "today's comments (x the ticker's usual)", rho_c, True),
          ("today_x", "today's price move (x the ticker's usual)", rho_t, False)]
for ax, (col, xlabel, rho, logx) in zip(axes, panels):
    ax.set_facecolor(SURF)
    ax.scatter(df[col], df.next_x, s=22, color=DOT, alpha=0.45, edgecolor=SURF, linewidth=0.8)
    # median trend line per bin (the drift of the point cloud)
    bins = pd.qcut(np.log(df[col].clip(lower=1e-3)), 8, duplicates="drop")
    trend = df.groupby(bins, observed=True).agg(x=(col, "median"), y=("next_x", "median"))
    ax.plot(trend.x, trend.y, color=INK, lw=2, marker="o", ms=5, mfc=INK, mec=SURF, mew=1.5, label="median per bin")
    if logx:
        ax.set_xscale("log")
        ax.set_xticks([0.3, 1, 3, 10])
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_minor_formatter(plt.NullFormatter())
    ax.axhline(1, color=INK2, lw=1, ls=(0, (3, 3)))
    ax.set_xlabel(xlabel, color=INK2)
    ax.text(0.02, 0.97, f"rank corr = {rho.statistic:.2f}", transform=ax.transAxes, va="top", color=INK, fontsize=11)
    ax.grid(color=GRID, lw=0.8)
    ax.set_axisbelow(True)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    for s in ["left", "bottom"]:
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2)

axes[0].set_ylabel("size of tomorrow's move (x the ticker's usual)", color=INK2)
axes[0].legend(loc="upper right", frameon=False, labelcolor=INK2)
n_days = df.date.nunique()
fig.suptitle("Do busy comment days precede big price moves?", x=0.07, ha="left", fontsize=15, color=INK, fontweight="bold")
fig.text(0.07, 0.905,
         f"Yahoo Finance community, {len(full)} tickers x {n_days} trading days (2026-07-02 ~ {df.date.max():%m-%d}); one dot = one ticker-day. "
         f"Rank corr of comments after removing today's move = {rho_p.statistic:.2f} (p = {rho_p.pvalue:.3f})",
         ha="left", fontsize=9.5, color=INK2)
fig.tight_layout(rect=(0.05, 0, 1, 0.88))
FIG.parent.mkdir(exist_ok=True)
fig.savefig(FIG, dpi=180, facecolor=SURF)

print(f"tickers: {', '.join(full)} / {len(df)} dots")
print(f"comments vs tomorrow's move          rho = {rho_c.statistic:.3f} (p = {rho_c.pvalue:.4f})")
print(f"today's move vs tomorrow's move      rho = {rho_t.statistic:.3f} (p = {rho_t.pvalue:.4f})  <- baseline")
print(f"comments after removing today's move rho = {rho_p.statistic:.3f} (p = {rho_p.pvalue:.4f})")
print(f"saved: {FIG}")
