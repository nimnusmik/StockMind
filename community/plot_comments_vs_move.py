"""첫 그래프: 오늘 댓글량 → 내일 주가 움직임 크기.

하루(t)의 댓글 = 전 거래일 16:00 ET ~ t일 16:00 ET (장 마감 기준으로 끊어 누수 방지, 주말 글은 월요일로)
내일 움직임 = |t+1일 종가 / t일 종가 - 1|
비교 기준선 = 오늘 움직임 |r_t| (주가는 크게 움직인 다음 날 또 크게 움직이는 경향이 원래 있다)

실행: python3 plot_comments_vs_move.py
출력: figures/comments_vs_next_move.png, data/daily_merged.csv (그래프에 쓴 표)
"""
import sqlite3
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from scipy.stats import spearmanr

HERE = Path(__file__).parent
FIG = HERE / "figures" / "comments_vs_next_move.png"
START = "2026-07-02"  # 7/1 창(6/30 16:00 ET~)은 일부 종목 backfill이 덜 덮어서 제외

# 1) 댓글 → 거래일별 개수
con = sqlite3.connect(HERE / "data" / "community.db")
posts = pd.read_sql("SELECT symbol, created_at FROM posts", con)
oldest = posts.groupby("symbol").created_at.min()
full = oldest[oldest < "2026-07-01"].index  # 7/1까지 채워진 종목만 (새 종목은 backfill 끝나면 자동 포함)
posts = posts[posts.symbol.isin(full)]

prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
days = np.sort(prices.date.unique())

et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)  # 주말·휴장일 글 → 다음 거래일
posts = posts[idx < len(days)]  # 마지막 장 마감 이후 글은 아직 주가가 없는 날 몫이라 제외
posts["date"] = days[idx[idx < len(days)]]
counts = posts.groupby(["symbol", "date"]).size().rename("comments").reset_index()

# 2) 주가 → 오늘·내일 움직임
p = prices[prices.symbol.isin(full)].sort_values(["symbol", "date"]).copy()
p["move_today"] = p.groupby("symbol").close.pct_change().abs()
p["move_next"] = p.groupby("symbol").move_today.shift(-1)

df = p.merge(counts, on=["symbol", "date"], how="left").fillna({"comments": 0})
df = df[(df.date >= START) & df.move_next.notna()].copy()

# 3) 종목마다 '평소 대비 몇 배'로 맞춤 (NVDA 하루 300개 vs AMZN 30개를 같은 눈금에)
# ponytail: 전체 기간 평균으로 나눔 — 그림(기술 통계)용. 예측 모델에서는 과거 창 평균만 써야 누수가 없다
g = df.groupby("symbol")
df["comments_x"] = df.comments / g.comments.transform("median")
df["today_x"] = df.move_today / g.move_today.transform("mean")
df["next_x"] = df.move_next / g.move_next.transform("mean")
df.to_csv(HERE / "data" / "daily_merged.csv", index=False)


def partial_spearman(x, y, z):
    """z(오늘 움직임) 효과를 뺀 뒤 x와 y의 순위 상관."""
    rx, ry, rz = (pd.Series(v).rank().values for v in (x, y, z))
    res = lambda a: a - np.polyval(np.polyfit(rz, a, 1), rz)
    return spearmanr(res(rx), res(ry))


rho_c = spearmanr(df.comments_x, df.next_x)
rho_t = spearmanr(df.today_x, df.next_x)
rho_p = partial_spearman(df.comments_x, df.next_x, df.today_x)

# 4) 그림: 같은 y축 두 칸 (왼쪽 댓글, 오른쪽 기준선)
plt.rcParams.update({"font.family": "AppleGothic", "axes.unicode_minus": False})
INK, INK2, GRID, SURF, DOT = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb", "#2a78d6"
fig, axes = plt.subplots(1, 2, figsize=(12, 5.4), sharey=True, facecolor=SURF)

panels = [("comments_x", "오늘 댓글량 (종목 평소 대비, 배)", rho_c, True),
          ("today_x", "오늘 주가 움직임 (종목 평소 대비, 배)", rho_t, False)]
for ax, (col, xlabel, rho, logx) in zip(axes, panels):
    ax.set_facecolor(SURF)
    ax.scatter(df[col], df.next_x, s=22, color=DOT, alpha=0.45, edgecolor=SURF, linewidth=0.8)
    # 구간별 중앙값 추세선 (점 구름의 흐름)
    bins = pd.qcut(np.log(df[col].clip(lower=1e-3)), 8, duplicates="drop")
    trend = df.groupby(bins, observed=True).agg(x=(col, "median"), y=("next_x", "median"))
    ax.plot(trend.x, trend.y, color=INK, lw=2, marker="o", ms=5, mfc=INK, mec=SURF, mew=1.5, label="구간별 중앙값")
    if logx:
        ax.set_xscale("log")
        ax.set_xticks([0.3, 1, 3, 10])
        ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"{v:g}"))
        ax.xaxis.set_minor_formatter(plt.NullFormatter())
    ax.axhline(1, color=INK2, lw=1, ls=(0, (3, 3)))
    ax.set_xlabel(xlabel, color=INK2)
    ax.text(0.02, 0.97, f"순위 상관 = {rho.statistic:.2f}", transform=ax.transAxes, va="top", color=INK, fontsize=11)
    ax.grid(color=GRID, lw=0.8)
    ax.set_axisbelow(True)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    for s in ["left", "bottom"]:
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2)

axes[0].set_ylabel("내일 주가 움직임 크기 (종목 평소 대비, 배)", color=INK2)
axes[0].legend(loc="upper right", frameon=False, labelcolor=INK2)
n_days = df.date.nunique()
fig.suptitle("오늘 댓글이 많으면 내일 주가가 크게 움직일까?", x=0.07, ha="left", fontsize=15, color=INK, fontweight="bold")
fig.text(0.07, 0.905,
         f"Yahoo Finance 커뮤니티 {len(full)}종목 × {n_days}거래일 (2026-07-02 ~ {df.date.max():%m-%d}), 점 하나 = 종목 하루. "
         f"오늘 움직임 효과를 뺀 뒤 댓글의 순위 상관 = {rho_p.statistic:.2f} (p = {rho_p.pvalue:.3f})",
         ha="left", fontsize=9.5, color=INK2)
fig.tight_layout(rect=(0.05, 0, 1, 0.88))
FIG.parent.mkdir(exist_ok=True)
fig.savefig(FIG, dpi=180, facecolor=SURF)

print(f"종목: {', '.join(full)} / 점 {len(df)}개")
print(f"댓글량 vs 내일 움직임        ρ = {rho_c.statistic:.3f} (p = {rho_c.pvalue:.4f})")
print(f"오늘 움직임 vs 내일 움직임   ρ = {rho_t.statistic:.3f} (p = {rho_t.pvalue:.4f})  ← 기준선")
print(f"오늘 움직임 뺀 뒤 댓글      ρ = {rho_p.statistic:.3f} (p = {rho_p.pvalue:.4f})")
print(f"저장: {FIG}")
