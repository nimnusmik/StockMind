"""실험 5: 댓글 특징을 '전부' 넣고 내일 주가 방향 예측 (직선 모델 + 나무 모델)

정답 = 내일 SPY 대비 수익률 > 0. 종목·하루 단위, 댓글 5개 이상인 날.
특징 묶음
  거래기록: 오늘 수익률, 5일 수익률, 거래량(평소 대비), 움직임 크기(평소 대비)
  댓글량:   댓글 수, 작성자 수 (평소 대비)
  감정:     평균, 평소 대비, 긍정 비율, 부정 비율, 초과 감정(오늘 주가로 설명 안 되는 부분)
  악플:     Detoxify 6개 점수의 하루 평균 + 최대 적대성
모델 = 로지스틱 회귀 / 부스팅(깊이 3, 200회, 학습률 0.05 — 미리 고정, 튜닝 안 함)
평가 = 7~8월 학습, 9월 시험. 학습 544개뿐이라 특징 많으면 과적합 주의.
준비: sentiment.py, toxicity.py 결과. 실행: python3 exp5_all_features.py
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

# 1) 글 → 거래일, 감정·악플 붙이기
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

# 2) 거래기록 + 평소 대비 특징 (과거 정보만)
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
for t in TOX:  # 종목별 과거 대비 표준화 → 최대값 = 그날 가장 튄 악플 종류 (논문의 overall hostility)
    sd = v(t).groupby(df.symbol).transform(lambda x: x.expanding(5).std().shift(1))
    df[t + "_z"] = (df[t] - past(v(t))) / sd
df["hostility"] = df[[t + "_z" for t in TOX if t != "severe_toxicity"]].max(axis=1)
df = df[valid & (df.date >= START)].reset_index(drop=True)

train, test = (df.date < CUT).values, (df.date >= CUT).values
fit = LinearRegression().fit(df.loc[train & df.sent_rel.notna(), ["ex"]], df.loc[train & df.sent_rel.notna(), "sent_rel"])
df["excess"] = df.sent_rel - fit.predict(df[["ex"]])

GROUPS = {
    "거래기록": ["ex", "ex5", "vol_rel", "move_rel"],
    "댓글량": ["com_rel", "auth_rel"],
    "감정": ["sent", "sent_rel", "pos_share", "neg_share", "excess"],
    "악플": TOX + ["hostility"],
}
SETS = {k: v for k, v in GROUPS.items()}
SETS["댓글 전부(양+감정+악플)"] = GROUPS["댓글량"] + GROUPS["감정"] + GROUPS["악플"]
SETS["전부"] = sum(GROUPS.values(), [])
need = SETS["전부"] + ["ex_next"]
df = df.dropna(subset=need).reset_index(drop=True)
train, test = (df.date < CUT).values, (df.date >= CUT).values
y = (df.ex_next > 0).astype(int).values

MODELS = {
    "로지스틱": lambda: make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
    "부스팅": lambda: HistGradientBoostingClassifier(max_depth=3, max_iter=200, learning_rate=0.05, random_state=0),
}
tdays = np.unique(df.date[test])
pos = {d: np.where(df.date.values[test] == d)[0] for d in tdays}
rng = np.random.default_rng(0)
picks = [np.concatenate([pos[d] for d in rng.choice(tdays, len(tdays))]) for _ in range(1000)]

print(f"종목·하루 {len(df)}개 — 학습 {train.sum()} (07~08월), 시험 {test.sum()} (9월, {len(tdays)}거래일), 특징 최대 {len(SETS['전부'])}개")
print(f"내일 오른 비율: 학습 {y[train].mean():.3f}, 시험 {y[test].mean():.3f}\n")
print(f"{'특징 묶음':24}{'개수':>4}" + "".join(f"{m + ' ROC':>13}{'95% 구간':>17}" for m in MODELS))
yt = y[test]
for name, cols in SETS.items():
    row = f"{name:24}{len(cols):4}"
    for mname, make in MODELS.items():
        s = make().fit(df.loc[train, cols], y[train]).predict_proba(df.loc[test, cols])[:, 1]
        boot = [roc_auc_score(yt[i], s[i]) for i in picks if 0 < yt[i].mean() < 1]
        lo, hi = np.percentile(boot, [2.5, 97.5])
        row += f"{roc_auc_score(yt, s):13.3f}   [{lo:.3f}, {hi:.3f}]"
    print(row)
print(f"\n기준: 0.500 = 동전 던지기. 구간이 0.5를 포함하면 '맞힌다'고 말할 수 없음.")
print(f"묶음 {len(SETS)}개 × 모델 {len(MODELS)}개 = {len(SETS) * len(MODELS)}번 시험 → 하나쯤은 우연히 좋게 나올 수 있음")
