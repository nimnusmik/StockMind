"""실험 4: '초과 감정'으로 내일 주가 방향 예측 (Tetlock 2007식 과잉반응 → 반전)

초과 감정 = 오늘 감정(종목 평소 대비) 중 오늘 수익률로 설명 안 되는 부분.
  감정 ~ 오늘 수익률 회귀를 '학습 기간'에서만 맞추고, 그 잔차를 모든 날에 계산 (누수 방지)
정답 = 내일 SPY 대비 수익률 > 0
평가 = 7~8월 학습, 9월 시험. 성공 기준은 미리 52~55% (60% 넘으면 누수 의심)
실행: python3 exp4_excess_sentiment.py → figures/excess_sentiment.png
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

# 1) 종목·하루 감정과 수익률 (plot_sentiment_deciles.py와 같은 정의)
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

# 2) 초과 감정 = 감정 - (학습 기간에서 배운 '오늘 수익률이면 이 정도 감정')
fit = LinearRegression().fit(df.loc[train, ["ex"]], df.loc[train, "sent_rel"])
df["excess"] = df.sent_rel - fit.predict(df[["ex"]])
y = (df.ex_next > 0).astype(int).values

SETS = {"오늘 수익률만": ["ex"], "감정만": ["sent_rel"], "초과 감정만": ["excess"], "초과 감정+오늘 수익률": ["excess", "ex"]}
day = df.date.values
rng = np.random.default_rng(0)
tdays = np.unique(day[test])
print(f"종목·하루 {len(df)}개 — 학습 {train.sum()} (07~08월), 시험 {test.sum()} (9월, {len(tdays)}거래일)")
print(f"감정 ~ 오늘 수익률 기울기 {fit.coef_[0]:.2f} (학습 기간)\n")
print(f"{'특징':22}{'ROC-AUC':>8}{'95% 구간':>18}{'정확도':>8}")
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
    print(f"{name:22}{roc_auc_score(yt, s):8.3f}   [{lo:.3f}, {hi:.3f}]{((s > 0.5) == yt).mean():8.3f}")
print(f"{'늘 많은 쪽으로 찍기':22}{0.5:8.3f}{'':18}{max(y[test].mean(), 1 - y[test].mean()):8.3f}")

# 3) 그림: 초과 감정 10등분별 내일 수익률 (학습·시험 기간 따로)
plt.rcParams.update({"font.family": "AppleGothic", "axes.unicode_minus": False})
INK, INK2, GRID, SURF = "#0b0b0b", "#52514e", "#e6e5e1", "#fcfcfb"
COLORS = {"학습 기간 (7~8월)": "#2a78d6", "시험 기간 (9월)": "#d6762a"}
edges = np.quantile(df.loc[train, "excess"], np.linspace(0, 1, 11))  # 칸 경계도 학습 기간에서만
df["bin"] = np.clip(np.searchsorted(edges[1:-1], df.excess, side="right") + 1, 1, 10)
fig, ax = plt.subplots(figsize=(9, 5), facecolor=SURF)
ax.set_facecolor(SURF)
ax.axhline(0, color=INK2, lw=1, ls=(0, (3, 3)))
for (label, mask), dx in zip([("학습 기간 (7~8월)", train), ("시험 기간 (9월)", test)], [-0.12, 0.12]):
    m = df[mask].groupby("bin").ex_next.mean() * 100
    ax.plot(m.index + dx, m.values, color=COLORS[label], lw=2, marker="o", ms=8, mec=SURF, mew=2, label=label)
ax.set_xticks(range(1, 11))
ax.set_xticklabels(["주가보다\n과하게 부정"] + [str(i) for i in range(2, 10)] + ["주가보다\n과하게 긍정"])
ax.set_ylabel("내일 수익률 (SPY 대비, %)", color=INK2)
ax.set_title("주가보다 과하게 반응한 댓글은 내일 반전을 예고할까?", loc="left", fontsize=13, color=INK, fontweight="bold")
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
print("\n저장: figures/excess_sentiment.png  (반전이면 왼쪽이 높고 오른쪽이 낮아야 함)")
