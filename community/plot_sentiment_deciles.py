"""얼마나 긍정/부정이어야 주가가 움직이나? — 감정 10등분별 다음 날 주가

감정 = 하루 댓글의 평균 (긍정 확률 - 부정 확률), 그 종목의 '과거' 평균 대비 (원래 비관적인 게시판 보정)
하루 = 전 거래일 16:00 ET ~ 당일 16:00 ET, 댓글 5개 이상인 날만
수익률 = 종목 - SPY (시장 전체 움직임 제거)
세 칸: ① 다음 날 수익률 ② 다음 날 움직임 크기(평소 대비) ③ 오늘 수익률 — ③이 기울면 감정이 주가를 뒤따라간다는 뜻
오차 막대 = 날짜 묶음 부트스트랩 95% 구간 (같은 날 종목들은 같이 움직이므로)
실행: python3 plot_sentiment_deciles.py → figures/sentiment_deciles.png
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

# 1) 글 → 거래일, 감정 붙이기
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

# 2) 수익률 (SPY 대비) + 감정의 '평소 대비'
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

# 3) 10등분별 평균 + 날짜 묶음 부트스트랩
cols = {"ex_next": 100, "absx_next": 1, "ex": 100}  # 수익률은 % 단위로
mean = df.groupby("bin")[list(cols)].mean() * pd.Series(cols)
dates = df.date.unique()
rng = np.random.default_rng(0)
by_date = {d: grp for d, grp in df.groupby("date")}
boot = [pd.concat([by_date[d] for d in rng.choice(dates, len(dates))]).groupby("bin")[list(cols)].mean() * pd.Series(cols)
        for _ in range(1000)]
lo = pd.concat(boot).groupby(level=0).quantile(0.025)
hi = pd.concat(boot).groupby(level=0).quantile(0.975)

# 4) 그림 — 칸마다 측정값이 달라 y축은 칸별 (같은 칸 안에선 한 축)
plt.rcParams.update({"font.family": "AppleGothic", "axes.unicode_minus": False})
INK, INK2, GRID, SURF, DOT = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb", "#2a78d6"
fig, axes = plt.subplots(1, 3, figsize=(15, 5), facecolor=SURF)
panels = [("ex_next", "① 다음 날 수익률 (SPY 대비, %)", 0),
          ("absx_next", "② 다음 날 움직임 크기 (평소 대비, 배)", 1),
          ("ex", "③ 오늘 수익률 (SPY 대비, %) — 역방향 점검", 0)]
x = mean.index.values
for ax, (c, title, ref) in zip(axes, panels):
    ax.set_facecolor(SURF)
    ax.axhline(ref, color=INK2, lw=1, ls=(0, (3, 3)))
    ax.vlines(x, lo[c], hi[c], color=DOT, lw=2, alpha=0.35)
    ax.plot(x, mean[c], color=DOT, lw=2, marker="o", ms=8, mfc=DOT, mec=SURF, mew=2)
    ax.set_title(title, loc="left", fontsize=11, color=INK)
    ax.set_xticks(x)
    ax.set_xticklabels(["가장\n부정"] + [str(i) for i in x[1:-1]] + ["가장\n긍정"])
    ax.grid(axis="y", color=GRID, lw=0.8)
    ax.set_axisbelow(True)
    for s in ["top", "right"]:
        ax.spines[s].set_visible(False)
    for s in ["left", "bottom"]:
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2)
axes[1].set_xlabel("오늘 댓글 감정 (종목 평소 대비, 10등분)", color=INK2)

r_next = spearmanr(df.sent_rel, df.ex_next)
r_today = spearmanr(df.sent_rel, df.ex)
fig.suptitle("얼마나 긍정/부정이어야 다음 날 주가가 움직일까?", x=0.04, ha="left", fontsize=15, color=INK, fontweight="bold")
fig.text(0.04, 0.89,
         f"Yahoo Finance 커뮤니티 15종목, {df.date.min():%m-%d}~{df.date.max():%m-%d}, 종목·하루 {len(df)}개 (칸당 약 {len(df) // BINS}개). "
         f"점 = 평균, 막대 = 날짜 묶음 부트스트랩 95% 구간. 순위상관: 감정↔다음 날 {r_next.statistic:.2f}, 감정↔오늘 {r_today.statistic:.2f}",
         ha="left", fontsize=9.5, color=INK2)
fig.tight_layout(rect=(0, 0, 1, 0.87))
fig.savefig(FIG, dpi=180, facecolor=SURF)

print(f"종목·하루 {len(df)}개")
print((mean.round(3)).assign(n=df.groupby("bin").size()).to_string())
print(f"\n감정 ↔ 다음 날 수익률 ρ = {r_next.statistic:.3f} (p = {r_next.pvalue:.3f})")
print(f"감정 ↔ 오늘 수익률     ρ = {r_today.statistic:.3f} (p = {r_today.pvalue:.3f})  ← 감정이 주가를 뒤따라가는지")
print(f"저장: {FIG}")

# 5) 극단 칸의 실제 글 확인 (모델이 은어·비꼼을 잘못 읽는지)
for b, label in [(1, "가장 부정 칸"), (BINS, "가장 긍정 칸")]:
    keys = df.loc[df.bin == b, ["symbol", "date"]]
    sample = posts.merge(keys, on=["symbol", "date"]).sort_values("s", ascending=(b == 1)).head(8)
    print(f"\n[{label}] 감정 점수가 가장 극단인 글 8개")
    for _, r in sample.iterrows():
        print(f"  {r.s:+.2f} {r.symbol:5} {str(r.body).replace(chr(10), ' ')[:110]}")
