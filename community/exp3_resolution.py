"""실험 3: 블록을 잘게 쪼개면(1시간 → 30분) 댓글량이 더 쓸모 있어지나?

미리 정한 비교는 1시간 vs 30분 두 개뿐 (여러 개 해보고 좋은 것만 고르는 것 방지).
30분 봉은 최근 60일만 받을 수 있어서 두 단위 모두 같은 60일로 맞춘다.
정답·특징은 실험 1과 같음 (같은 종목·같은 시간대 과거 대비, 급증 = 과거 80분위 초과).
평가 = 시간 나누기: 9/15 이전 학습, 이후 시험 (앞쪽 약 3주는 과거 이력 쌓는 데 쓰임).
실행: python3 exp3_resolution.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
import yfinance as yf
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from collect import TICKERS

HERE = Path(__file__).parent
NY = "America/New_York"
CUT = pd.Timestamp("2026-09-15", tz=NY)
SETS = {"거래기록": ["vol_rel", "move_rel"], "댓글량": ["com_rel", "auth_rel"]}
SETS["둘 다"] = SETS["거래기록"] + SETS["댓글량"]

posts = pd.read_sql("SELECT symbol, created_at, username FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts["t"] = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert(NY)


def build(interval):
    raw = yf.download(TICKERS, period="59d", interval=interval, auto_adjust=True, progress=False)
    bars = raw.stack(level=1, future_stack=True).reset_index()
    bars.columns = [c.lower() for c in bars.columns]
    bars = bars.rename(columns={"ticker": "symbol", "datetime": "start"}).dropna(subset=["close"])
    bars["start"] = pd.to_datetime(bars.start, utc=True).dt.tz_convert(NY)
    bars = bars[bars.start < pd.Timestamp.now(tz=NY).normalize()].sort_values(["symbol", "start"]).reset_index(drop=True)
    bars["slot"] = bars.start.dt.strftime("%H:%M")

    parts = []
    for sym, b in bars.groupby("symbol"):
        p = posts[posts.symbol == sym]
        k = np.searchsorted(b.start.values, p.t.values, side="right") - 1
        q = p[k >= 0].reset_index(drop=True)
        q["start"] = b.start.iloc[k[k >= 0]].reset_index(drop=True)
        parts.append(q)
    cnt = pd.concat(parts).groupby(["symbol", "start"]).agg(
        comments=("username", "size"), authors=("username", "nunique")).reset_index()

    df = bars.merge(cnt, on=["symbol", "start"], how="left").fillna({"comments": 0, "authors": 0})
    key = [df.symbol, df.slot]
    past = lambda s: s.groupby(key).transform(lambda x: x.expanding(5).mean().shift(1))
    absr = df.groupby("symbol").close.pct_change().abs()
    df["vol_rel"] = np.log1p(df.volume) - past(np.log1p(df.volume))
    df["move_rel"] = absr / past(absr)
    df["com_rel"] = np.log1p(df.comments) - past(np.log1p(df.comments))
    df["auth_rel"] = np.log1p(df.authors) - past(np.log1p(df.authors))
    p80 = df.volume.groupby(key).transform(lambda x: x.expanding(15).quantile(0.8).shift(1))
    nv, np80 = df.groupby("symbol").volume.shift(-1), p80.groupby(df.symbol).shift(-1)
    df["target"] = (nv > np80).astype(float).where(nv.notna() & np80.notna())
    return df.dropna(subset=SETS["둘 다"] + ["target"]).reset_index(drop=True)


def gain_ci(y, a, b, day, n=1000):
    f = lambda i: average_precision_score(y[i], a[i]) - average_precision_score(y[i], b[i])
    uniq, r = np.unique(day), np.random.default_rng(1)
    pos = {d: np.where(day == d)[0] for d in uniq}
    boot = [f(np.concatenate([pos[d] for d in r.choice(uniq, len(uniq))])) for _ in range(n)]
    return f(np.arange(len(y))), *np.percentile(boot, [2.5, 97.5])


built = {iv: build(iv) for iv in ["1h", "30m"]}
first = max(d.start.min() for d in built.values())  # 두 단위의 시작일을 같게
print(f"공통 기간 {first:%m-%d} ~, 학습 ~{CUT:%m-%d}, 시험 {CUT:%m-%d}~\n")
print(f"{'단위':5}{'블록':>7}{'칸당 댓글 0인 비율':>14}{'거래기록':>9}{'댓글량':>8}{'둘 다':>8}{'랜덤':>7}   둘 다 - 거래기록 (시험 전체 / 장외)")
for iv, d in built.items():
    d = d[d.start >= first].reset_index(drop=True)
    y = d.target.astype(int).values
    tr, te = (d.start < CUT).values, (d.start >= CUT).values
    sc = {}
    for name, cols in SETS.items():
        m = make_pipeline(StandardScaler(), LogisticRegression()).fit(d.loc[tr, cols], y[tr])
        sc[name] = m.predict_proba(d[cols])[:, 1]
    aps = "".join(f"{average_precision_score(y[te], sc[n][te]):8.3f}" for n in SETS)
    day = d.start.dt.date.values
    night = te & (d.slot == "15:30").values
    out = []
    for m in [te, night]:
        g, lo, hi = gain_ci(y[m], sc["둘 다"][m], sc["거래기록"][m], day[m])
        out.append(f"{g:+.3f} [{lo:+.3f}, {hi:+.3f}]")
    zero = (d.comments[te] == 0).mean()
    print(f"{iv:5}{te.sum():7}{zero:14.2f}{aps[:8]:>9}{aps[8:]}{y[te].mean():7.3f}   {out[0]} / {out[1]}")
