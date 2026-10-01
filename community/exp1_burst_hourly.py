"""실험 1-시간판: 지금 1시간 블록의 댓글로 다음 1시간 거래량 급증을 맞히나?

블록 = 정규장 1시간 봉(9:30, 10:30 … 15:30 ET, 하루 7개). 15:30 블록은 다음 날 9:30 개장 직전까지의 장외 댓글을 포함.
정답 = 다음 봉 거래량 > 그 종목·같은 시간대의 '과거' 거래량 80번째 백분위 (장중 U자 패턴 제거, 누수 없음)
특징 = 같은 종목·같은 시간대의 과거 평균 대비 값. 평가 = 종목 하나씩 통째로 빼고 테스트.

실행: python3 exp1_burst_hourly.py   (1시간 봉은 data/prices_1h.csv에 저장)
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from collect import TICKERS

HERE = Path(__file__).parent
START = pd.Timestamp("2026-07-01 09:30", tz="America/New_York")
LARGE = ["AAPL", "GOOG", "META", "TSLA", "MSFT", "AMZN", "NVDA", "NFLX"]

# 1) 1시간 봉 받기 (오늘 봉은 진행 중일 수 있어 제외)
raw = yf.download(TICKERS, start="2026-06-01", interval="1h", auto_adjust=True, progress=False)
bars = raw.stack(level=1, future_stack=True).reset_index()
bars.columns = [c.lower() for c in bars.columns]
bars = bars.rename(columns={"ticker": "symbol", "datetime": "start"}).dropna(subset=["close"])
bars["start"] = pd.to_datetime(bars.start, utc=True).dt.tz_convert("America/New_York")
bars = bars[bars.start < pd.Timestamp.now(tz="America/New_York").normalize()]
bars.to_csv(HERE / "data" / "prices_1h.csv", index=False)
bars = bars.sort_values(["symbol", "start"]).reset_index(drop=True)
bars["slot"] = bars.start.dt.strftime("%H:%M")

# 2) 댓글 → 그 시각을 포함하는 블록 (블록 k = k번째 봉 시작 ~ 다음 봉 시작)
con = sqlite3.connect(HERE / "data" / "community.db")
posts = pd.read_sql("SELECT symbol, created_at, username FROM posts", con)
posts["t"] = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
parts = []
for sym, b in bars.groupby("symbol"):
    p = posts[posts.symbol == sym]
    k = np.searchsorted(b.start.values, p.t.values, side="right") - 1
    q = p[k >= 0].reset_index(drop=True)
    q["start"] = b.start.iloc[k[k >= 0]].reset_index(drop=True)
    parts.append(q)
counts = pd.concat(parts).groupby(["symbol", "start"]).agg(
    comments=("username", "size"), authors=("username", "nunique")).reset_index()

df = bars.merge(counts, on=["symbol", "start"], how="left")
df[["comments", "authors"]] = df[["comments", "authors"]].fillna(0)
df["absr"] = df.groupby("symbol").close.pct_change().abs()
df.loc[df.start < START, ["comments", "authors"]] = np.nan  # 댓글 수집 전 구간은 0이 아니라 '모름'

# 3) 같은 종목·같은 시간대의 과거 평균 대비 (최소 5일 이력)
key = [df.symbol, df.slot]
past_mean = lambda s: s.groupby(key).transform(lambda x: x.expanding(5).mean().shift(1))
df["vol_rel"] = np.log1p(df.volume) - past_mean(np.log1p(df.volume))
df["move_rel"] = df.absr / past_mean(df.absr)
df["com_rel"] = np.log1p(df.comments) - past_mean(np.log1p(df.comments))
df["auth_rel"] = np.log1p(df.authors) - past_mean(np.log1p(df.authors))

p80 = df.volume.groupby(key).transform(lambda x: x.expanding(15).quantile(0.8).shift(1))
next_vol = df.groupby("symbol").volume.shift(-1)
next_p80 = p80.groupby(df.symbol).shift(-1)
df["target"] = (next_vol > next_p80).astype(float).where(next_vol.notna() & next_p80.notna())

SETS = {"거래기록": ["vol_rel", "move_rel"], "댓글량": ["com_rel", "auth_rel"]}
SETS["둘 다"] = SETS["거래기록"] + SETS["댓글량"]
data = df.dropna(subset=SETS["둘 다"] + ["target"]).reset_index(drop=True)
y = data.target.astype(int).values

# 4) 종목 하나씩 빼고 학습·예측
scores = {}
for name, cols in SETS.items():
    s = np.empty(len(data))
    for sym in data.symbol.unique():
        test = (data.symbol == sym).values
        m = make_pipeline(StandardScaler(), LogisticRegression()).fit(data.loc[~test, cols], y[~test])
        s[test] = m.predict_proba(data.loc[test, cols])[:, 1]
    scores[name] = s


def per_ticker_ap(s):
    return pd.Series({sym: average_precision_score(y[data.symbol == sym], s[data.symbol == sym])
                      for sym in data.symbol.unique() if y[data.symbol == sym].any()})


rng = np.random.default_rng(0)
rand = np.mean([per_ticker_ap(rng.random(len(data))).mean() for _ in range(200)])

print(f"관측 {len(data)}개 (종목 {data.symbol.nunique()}개, {data.start.min():%m-%d}~{data.start.max():%m-%d}), "
      f"급증 {y.sum()}번 (비율 {y.mean():.3f})\n")
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
win = t["댓글량"] > t["거래기록"]
print(f"\n댓글량이 거래기록을 이긴 종목: {win.sum()}/{len(t)} → {', '.join(t.index[win])}")
print("\n시간대별 PR-AUC (전체 종목 합침)")
for slot, grp in data.groupby("slot"):
    i = grp.index.values
    print(f"  {slot} 블록 → 다음 봉: " + "  ".join(f"{n} {average_precision_score(y[i], scores[n][i]):.3f}" for n in SETS)
          + f"  (급증 비율 {y[i].mean():.2f})")
