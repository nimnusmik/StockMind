"""실험 6: 댓글이 내일 변동성(얼마나 크게 출렁일까) 예측을 돕나? — HAR 기준선 + 댓글

변동성 = Garman-Klass (일봉 시가·고가·저가·종가로 계산한 하루 분산)
기준선 = HAR (Corsi 2009, 로그판): 어제 / 지난 5일 평균 / 지난 22일 평균 로그 변동성 → 내일 로그 변동성
추가   = 댓글량(댓글 수·작성자 수, 평소 대비), 감정(평소 대비·부정 비율), 악플(최대 적대성)
평가   = 7~8월 학습 → 9월 시험, 선형회귀, 점수 QLIKE(낮을수록 좋음)·로그 MSE. 날짜 묶음 부트스트랩.
하루 = 전 거래일 16:00 ET ~ 당일 16:00 ET 댓글, 댓글 5개 이상인 날 (실험 4·5와 같은 표본)
실행: python3 exp6_volatility.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

HERE = Path(__file__).parent
START, CUT, MIN_POSTS = "2026-07-02", pd.Timestamp("2026-09-01"), 5
TOX = ["toxicity", "obscene", "threat", "insult", "identity_attack"]

# 1) 글 → 거래일, 감정·악플
posts = pd.read_sql("SELECT uuid, symbol, created_at, username FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts.merge(pd.read_csv(HERE / "data" / "sentiment.csv"), on="uuid").merge(pd.read_csv(HERE / "data" / "toxicity.csv"), on="uuid")
posts["s"] = posts.pos - posts.neg
posts["is_neg"] = (posts[["neg", "neu", "pos"]].values.argmax(1) == 0).astype(float)
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[(prices.date < pd.Timestamp.now().normalize()) & (prices.symbol != "SPY")]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
agg = {"n": ("s", "size"), "authors": ("username", "nunique"), "sent": ("s", "mean"), "neg_share": ("is_neg", "mean")}
agg.update({t: (t, "mean") for t in TOX})
daily = posts.groupby(["symbol", "date"]).agg(**agg).reset_index()

# 2) Garman-Klass 변동성 + HAR 항
p = prices.sort_values(["symbol", "date"]).copy()
hl, co = np.log(p.high / p.low), np.log(p.close / p.open)
p["gk"] = (0.5 * hl**2 - (2 * np.log(2) - 1) * co**2).clip(lower=1e-8)
p["lv"] = np.log(p.gk)
g = p.groupby("symbol").lv
p["lv_w"] = g.transform(lambda x: x.rolling(5).mean())
p["lv_m"] = g.transform(lambda x: x.rolling(22).mean())
p["target"] = g.shift(-1)

df = p.merge(daily, on=["symbol", "date"], how="left").sort_values(["symbol", "date"]).reset_index(drop=True)
valid = df.n >= MIN_POSTS
past = lambda s: s.where(valid).groupby(df.symbol).transform(lambda x: x.expanding(5).mean().shift(1))
df["com_rel"] = np.log1p(df.n) - past(np.log1p(df.n))
df["auth_rel"] = np.log1p(df.authors) - past(np.log1p(df.authors))
df["sent_rel"] = df.sent - past(df.sent)
for t in TOX:
    sd = df[t].where(valid).groupby(df.symbol).transform(lambda x: x.expanding(5).std().shift(1))
    df[t + "_z"] = (df[t] - past(df[t])) / sd
df["hostility"] = df[[t + "_z" for t in TOX]].max(axis=1)

HAR = ["lv", "lv_w", "lv_m"]
SETS = {
    "HAR (기준선)": HAR,
    "HAR + 댓글량": HAR + ["com_rel", "auth_rel"],
    "HAR + 감정": HAR + ["sent_rel", "neg_share"],
    "HAR + 악플": HAR + ["hostility"],
    "HAR + 댓글 전부": HAR + ["com_rel", "auth_rel", "sent_rel", "neg_share", "hostility"],
}
df = df[valid & (df.date >= START)].dropna(subset=SETS["HAR + 댓글 전부"] + ["target"]).reset_index(drop=True)
train, test = (df.date < CUT).values, (df.date >= CUT).values
yt = df.target.values[test]


def qlike(lv_true, lv_pred):
    """QLIKE = 실제/예측 - log(실제/예측) - 1 (분산 단위). 0이 완벽, 낮을수록 좋음."""
    r = np.exp(lv_true - lv_pred)
    return r - np.log(r) - 1


preds = {}
for name, cols in SETS.items():
    m = LinearRegression().fit(df.loc[train, cols], df.target[train])
    resid_var = np.var(df.target[train] - m.predict(df.loc[train, cols]))
    preds[name] = m.predict(df.loc[test, cols]) + 0.5 * resid_var  # 로그→분산 편향 보정

naive = df.lv.values[test]  # '내일 = 오늘' 단순 기준
tdays = np.unique(df.date[test])
pos = {d: np.where(df.date.values[test] == d)[0] for d in tdays}
rng = np.random.default_rng(0)
picks = [np.concatenate([pos[d] for d in rng.choice(tdays, len(tdays))]) for _ in range(1000)]

print(f"종목·하루 {len(df)}개 — 학습 {train.sum()} (07~08월), 시험 {test.sum()} (9월, {len(tdays)}거래일)\n")
print(f"{'모델':16}{'QLIKE':>8}{'로그 MSE':>10}   HAR 대비 QLIKE 차이 (음수 = 더 좋음) 95% 구간")
q_har = qlike(yt, preds["HAR (기준선)"])
print(f"{'내일=오늘':16}{qlike(yt, naive).mean():8.3f}{np.mean((yt - naive) ** 2):10.3f}")
for name, pr in preds.items():
    q = qlike(yt, pr)
    line = f"{name:16}{q.mean():8.3f}{np.mean((yt - pr) ** 2):10.3f}"
    if name != "HAR (기준선)":
        d = q - q_har
        lo, hi = np.percentile([d[i].mean() for i in picks], [2.5, 97.5])
        line += f"   {d.mean():+.4f} [{lo:+.4f}, {hi:+.4f}]"
    print(line)

coef = LinearRegression().fit(df.loc[train, SETS["HAR + 댓글 전부"]], df.target[train])
print("\n'HAR + 댓글 전부' 계수 (학습 기간): " + ", ".join(f"{c} {v:+.3f}" for c, v in zip(SETS["HAR + 댓글 전부"], coef.coef_)))

# 사후 점검(결과를 본 뒤 추가): 7~8월 실적 시즌에 댓글·변동성이 같이 튀어 계수가 오염됐나? 학습에서 실적 ±2일 제외
earn = pd.read_csv(HERE / "data" / "earnings.csv")
earn["d"] = pd.to_datetime(earn.time.str[:10])
near = np.zeros(len(df), bool)
for _, e in earn.iterrows():
    near |= (df.symbol == e.symbol).values & (abs((df.date - e.d).dt.days) <= 2).values
tr = train & ~near
print(f"\n[사후 점검] 학습에서 실적 ±2일 {(train & near).sum()}개 제외 (학습 {tr.sum()}개)")
pr = {}
for name, cols in SETS.items():
    m = LinearRegression().fit(df.loc[tr, cols], df.target[tr])
    pr[name] = m.predict(df.loc[test, cols]) + 0.5 * np.var(df.target[tr] - m.predict(df.loc[tr, cols]))
qh = qlike(yt, pr["HAR (기준선)"])
for name in list(SETS)[1:]:
    d = qlike(yt, pr[name]) - qh
    lo, hi = np.percentile([d[i].mean() for i in picks], [2.5, 97.5])
    print(f"  {name:16} HAR 대비 {d.mean():+.4f} [{lo:+.4f}, {hi:+.4f}]")
