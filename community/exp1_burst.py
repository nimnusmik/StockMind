"""실험 1 (Morstatter 논문 실험 3의 주식·일 단위판): 오늘 댓글로 내일 거래량 급증을 맞히나?

정답 = 내일 거래량 > 그 종목의 '오늘까지' 거래량 80번째 백분위 (과거만 사용 → 누수 없음)
비교 = 거래기록만 / 댓글량만 / 둘 다, 로지스틱 회귀
평가 = 종목 하나씩 통째로 빼고 테스트 (논문의 leave-one-market-out)
하루(t) 댓글 = 전 거래일 16:00 ET ~ t일 16:00 ET

실행: python3 exp1_burst.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from collect import TICKERS

HERE = Path(__file__).parent
START = "2026-07-02"
LARGE = ["AAPL", "GOOG", "META", "TSLA", "MSFT", "AMZN", "NVDA", "NFLX"]

# 1) 댓글 → 거래일별 댓글 수·작성자 수
con = sqlite3.connect(HERE / "data" / "community.db")
posts = pd.read_sql("SELECT symbol, created_at, username FROM posts", con)
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]  # 오늘은 장중일 수 있어 제외
days = np.sort(prices.date.unique())

et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
daily = posts.groupby(["symbol", "date"]).agg(comments=("username", "size"), authors=("username", "nunique")).reset_index()

# 2) 종목별 특징 — 전부 '그 종목의 과거 평균 대비'로 맞춤 (NVDA와 COIN을 같은 눈금에)
df = prices[prices.symbol.isin(TICKERS)].merge(daily, on=["symbol", "date"], how="left")
df[["comments", "authors"]] = df[["comments", "authors"]].fillna(0)
df = df.sort_values(["symbol", "date"])
g = df.groupby("symbol")
past_mean = lambda s: s.groupby(df.symbol).transform(lambda x: x.expanding(5).mean().shift(1))

df["logv"] = np.log1p(df.volume)
df["absr"] = g.close.pct_change().abs()
df["logc"] = np.log1p(df.comments)
df["loga"] = np.log1p(df.authors)
df["vol_rel"] = df.logv - past_mean(df.logv)
df["move_rel"] = df.absr / past_mean(df.absr)
df["com_rel"] = df.logc - past_mean(df.logc.where(df.date >= START))
df["auth_rel"] = df.loga - past_mean(df.loga.where(df.date >= START))

p80 = g.volume.transform(lambda x: x.expanding(15).quantile(0.8))
df["target"] = (g.volume.shift(-1) > p80).astype(float).where(g.volume.shift(-1).notna() & p80.notna())

SETS = {"거래기록": ["vol_rel", "move_rel"], "댓글량": ["com_rel", "auth_rel"]}
SETS["둘 다"] = SETS["거래기록"] + SETS["댓글량"]
data = df[df.date >= START].dropna(subset=SETS["둘 다"] + ["target"]).reset_index(drop=True)
y = data.target.astype(int).values

# 3) 종목 하나씩 빼고 학습·예측
scores = {}
for name, cols in SETS.items():
    s = np.empty(len(data))
    for sym in data.symbol.unique():
        test = data.symbol == sym
        m = make_pipeline(StandardScaler(), LogisticRegression()).fit(data.loc[~test, cols], y[~test])
        s[test] = m.predict_proba(data.loc[test, cols])[:, 1]
    scores[name] = s


def per_ticker_ap(s):
    return pd.Series({sym: average_precision_score(y[data.symbol == sym], s[data.symbol == sym])
                      for sym in data.symbol.unique() if y[data.symbol == sym].any()})


rng = np.random.default_rng(0)
rand = np.mean([per_ticker_ap(rng.random(len(data))).mean() for _ in range(200)])

print(f"관측 {len(data)}개 (종목 {data.symbol.nunique()}개 × 거래일 {data.date.nunique()}일, "
      f"{data.date.min():%m-%d}~{data.date.max():%m-%d}), 급증 비율 {y.mean():.3f}\n")
print(f"{'':8}{'PR-AUC(전체)':>12}{'ROC-AUC':>9}{'종목평균 PR-AUC':>16}")
ap = {}
for name, s in scores.items():
    ap[name] = per_ticker_ap(s)
    print(f"{name:8}{average_precision_score(y, s):12.3f}{roc_auc_score(y, s):9.3f}{ap[name].mean():16.3f}")
print(f"{'랜덤':8}{y.mean():12.3f}{0.5:9.3f}{rand:16.3f}  ← 기준선 (랜덤 200회)\n")

t = pd.DataFrame(ap)
t["그룹"] = np.where(t.index.isin(LARGE), "대형주", "개인인기주")
print("그룹별 종목평균 PR-AUC")
print(t.groupby("그룹")[list(SETS)].mean().round(3).to_string())
win = (t["댓글량"] > t["거래기록"])
print(f"\n댓글량이 거래기록을 이긴 종목: {win.sum()}/{len(t)} → {', '.join(t.index[win])}")
