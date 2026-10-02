"""실험 7: 흐름이 꺾이기 전에 댓글이 먼저 돌아서나? (Morstatter 논문 실험 6 '리더 역전'의 주식판)

흐름(리더) = 종가가 20일 평균선 위면 상승(+1), 아래면 하락(-1)
역전       = 내일 흐름 부호가 오늘과 달라짐 (평균선 돌파)
리더 쪽 감정 = 흐름 부호 × 감정(종목 평소 대비). 음수 = 지금 흐름에 반대하는 분위기
① 패턴: 역전 전날 vs 다른 날의 리더 쪽 감정 평균, 종목별 비교 + 이항검정 (평균선과의 거리를 맞춘 비교 포함)
② 예측: 시장 상태(평균선 거리·최근 움직임) vs + 리더 쪽 감정, 7~8월 학습 → 9월 시험, PR-AUC
하루 = 전 거래일 16:00 ET ~ 당일 16:00 ET, 댓글 5개 이상인 날 (실험 4~6과 같은 표본)
실행: python3 exp7_reversal.py
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
df["dist"] = (df.close / df.ma - 1).abs()  # 평균선에서 얼마나 멀리 있나 (가까우면 뚫리기 쉬움)
df["ret"] = g.pct_change()
df["move_lead"] = df.side * df.ret  # 오늘 움직임이 흐름 방향이면 +
df["ret5_lead"] = df.side * g.transform(lambda x: x.pct_change(5))
nxt = df.groupby("symbol").side.shift(-1)
df["reversal"] = (nxt != df.side).astype(float).where(nxt.notna() & (df.side != 0))
valid = df.n >= MIN_POSTS
df["sent_rel"] = df.sent - df.sent.where(valid).groupby(df.symbol).transform(lambda x: x.expanding(5).mean().shift(1))
df["lead_sent"] = df.side * df.sent_rel
STATE = ["dist", "move_lead", "ret5_lead"]
df = df[valid & (df.date >= START)].dropna(subset=STATE + ["lead_sent", "reversal"]).reset_index(drop=True)
y = df.reversal.astype(int).values
print(f"종목·하루 {len(df)}개, 역전 {y.sum()}번 ({y.mean():.1%})\n")

# ① 패턴 — 역전 전날 리더 쪽 감정이 더 낮은가
pre, other = df[y == 1], df[y == 0]
print("① 패턴: 리더 쪽 감정 평균 (음수 = 지금 흐름에 반대)")
print(f"  역전 전날 {pre.lead_sent.mean():+.3f} ({len(pre)}일) / 다른 날 {other.lead_sent.mean():+.3f} ({len(other)}일)")
for label, d in [("전체", df), ("평균선에서 먼 날만 (거리 상위 50%)", df[df.dist > df.dist.median()])]:
    t = d.groupby(["symbol", "reversal"]).lead_sent.mean().unstack()
    t = t[d.groupby("symbol").reversal.sum().reindex(t.index) >= 2].dropna()  # 역전 2번 이상인 종목만
    k = int((t[1.0] < t[0.0]).sum())
    p = binomtest(k, len(t), 0.5).pvalue if len(t) else float("nan")
    print(f"  [{label}] 역전 전날이 더 부정적인 종목 {k}/{len(t)} (이항검정 p = {p:.3f}), 종목 안 차이 중앙값 {(t[1.0] - t[0.0]).median():+.3f}")

# ② 예측 — 시장 상태에 리더 쪽 감정을 더하면 역전을 더 잘 맞히나
train, test = (df.date < CUT).values, (df.date >= CUT).values
yt = y[test]
print(f"\n② 예측: 9월 시험 {test.sum()}일, 역전 {yt.sum()}번 (기준 = 역전 비율 {yt.mean():.3f})")
tdays = np.unique(df.date[test])
pos = {d: np.where(df.date.values[test] == d)[0] for d in tdays}
rng = np.random.default_rng(0)
picks = [np.concatenate([pos[d] for d in rng.choice(tdays, len(tdays))]) for _ in range(1000)]
sc = {}
for name, cols in {"시장 상태": STATE, "리더 쪽 감정만": ["lead_sent"], "시장 상태 + 감정": STATE + ["lead_sent"]}.items():
    m = make_pipeline(StandardScaler(), LogisticRegression()).fit(df.loc[train, cols], y[train])
    sc[name] = m.predict_proba(df.loc[test, cols])[:, 1]
    print(f"  {name:12} PR-AUC {average_precision_score(yt, sc[name]):.3f}  ROC-AUC {roc_auc_score(yt, sc[name]):.3f}")
d = [average_precision_score(yt[i], sc["시장 상태 + 감정"][i]) - average_precision_score(yt[i], sc["시장 상태"][i])
     for i in picks if yt[i].any()]
print(f"  감정 추가 효과 (PR-AUC) {np.mean(d):+.3f} [{np.percentile(d, 2.5):+.3f}, {np.percentile(d, 97.5):+.3f}]")

# 사후 점검: 감정은 그날 주가를 따라가므로(ρ 0.31), 역전 전날 '그날 움직임'으로 설명되는 부분을 뺀 뒤에도 남나
from sklearn.linear_model import LinearRegression
fit = LinearRegression().fit(df.loc[train, ["move_lead"]], df.lead_sent[train])
df["lead_excess"] = df.lead_sent - fit.predict(df[["move_lead"]])
t = df.groupby(["symbol", "reversal"]).lead_excess.mean().unstack()
t = t[df.groupby("symbol").reversal.sum().reindex(t.index) >= 2].dropna()
k = int((t[1.0] < t[0.0]).sum())
print(f"\n[사후 점검] 그날 움직임을 뺀 리더 쪽 감정: 역전 전날 {df.lead_excess[y == 1].mean():+.3f} / 다른 날 {df.lead_excess[y == 0].mean():+.3f}")
print(f"  역전 전날이 더 부정적인 종목 {k}/{len(t)} (이항검정 p = {binomtest(k, len(t), 0.5).pvalue:.3f})")
