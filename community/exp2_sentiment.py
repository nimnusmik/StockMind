"""실험 2: 댓글 '내용'(감정)이 도움이 되나? — 1시간 블록, 두 가지 문제

A. 다음 봉 거래량 급증 (실험 1과 같은 정답) — 감정을 더하면 나아지나
B. 다음 봉 주가 방향 (오를까 내릴까) — 논문 실험 4(매수 방향)의 주식판. 주식엔 매수/매도 흐름 데이터가 없어 수익률 부호로 대신

감정 = 글마다 (긍정 확률 - 부정 확률), 블록 평균. 댓글 없는 블록은 0(중립).
평가 = 기본은 시간 나누기(9/1 이전 학습, 이후 시험), 참고로 종목 빼기.
준비: exp1_burst_hourly.py(→ data/prices_1h.csv), sentiment.py(→ data/sentiment.csv)
실행: python3 exp2_sentiment.py
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

HERE = Path(__file__).parent
START = pd.Timestamp("2026-07-01 09:30", tz="America/New_York")
CUT = pd.Timestamp("2026-09-01", tz="America/New_York")

# 1) 1시간 봉 + 댓글(감정 포함) → 블록
bars = pd.read_csv(HERE / "data" / "prices_1h.csv")
bars["start"] = pd.to_datetime(bars.start, utc=True).dt.tz_convert("America/New_York")
bars = bars.sort_values(["symbol", "start"]).reset_index(drop=True)
bars["slot"] = bars.start.dt.strftime("%H:%M")

posts = pd.read_sql("SELECT uuid, symbol, created_at, username FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
sent = pd.read_csv(HERE / "data" / "sentiment.csv")
posts = posts.merge(sent, on="uuid", how="inner")
posts["s"] = posts.pos - posts.neg
lab = posts[["neg", "neu", "pos"]].values.argmax(1)
posts["is_pos"], posts["is_neg"] = (lab == 2).astype(float), (lab == 0).astype(float)
posts["t"] = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")

parts = []
for sym, b in bars.groupby("symbol"):
    p = posts[posts.symbol == sym]
    k = np.searchsorted(b.start.values, p.t.values, side="right") - 1
    q = p[k >= 0].reset_index(drop=True)
    q["start"] = b.start.iloc[k[k >= 0]].reset_index(drop=True)
    parts.append(q)
blk = pd.concat(parts).groupby(["symbol", "start"]).agg(
    comments=("uuid", "size"), authors=("username", "nunique"),
    sent=("s", "mean"), pos_share=("is_pos", "mean"), neg_share=("is_neg", "mean")).reset_index()

df = bars.merge(blk, on=["symbol", "start"], how="left")
has = df.comments.notna() & (df.start >= START)
df[["comments", "authors"]] = df[["comments", "authors"]].fillna(0)
df.loc[df.start < START, ["comments", "authors"]] = np.nan
df["net"] = df.pos_share - df.neg_share

# 2) 특징 (모두 과거 정보만)
key = [df.symbol, df.slot]
past_slot = lambda s: s.groupby(key).transform(lambda x: x.expanding(5).mean().shift(1))
past_sym = lambda s: s.groupby(df.symbol).transform(lambda x: x.expanding(20).mean().shift(1))
df["ret"] = df.groupby("symbol").close.pct_change()
df["ret4"] = df.groupby("symbol").ret.transform(lambda x: x.rolling(4).sum())
df["vol_rel"] = np.log1p(df.volume) - past_slot(np.log1p(df.volume))
df["move_rel"] = df.ret.abs() / past_slot(df.ret.abs())
df["com_rel"] = np.log1p(df.comments) - past_slot(np.log1p(df.comments))
df["auth_rel"] = np.log1p(df.authors) - past_slot(np.log1p(df.authors))
df["sent_rel"] = df.sent - past_sym(df.sent.where(has))  # 그 종목 평소 분위기 대비
for c in ["sent", "net", "sent_rel"]:
    df[c] = df[c].where(has, 0.0).fillna(0.0)

# 3) 정답
g = df.groupby("symbol")
p80 = df.volume.groupby(key).transform(lambda x: x.expanding(15).quantile(0.8).shift(1))
nv, np80 = g.volume.shift(-1), p80.groupby(df.symbol).shift(-1)
df["burst"] = (nv > np80).astype(float).where(nv.notna() & np80.notna())
nr = g.ret.shift(-1)
df["up"] = (nr > 0).astype(float).where(nr.notna() & (nr != 0))

TRADE_B, TRADE_D = ["vol_rel", "move_rel"], ["ret", "ret4"]
ATT, SENT = ["com_rel", "auth_rel"], ["sent", "sent_rel", "net"]
PROBLEMS = {
    "A. 거래량 급증 (PR-AUC)": ("burst", average_precision_score, {
        "거래기록": TRADE_B, "댓글량": ATT, "감정": SENT, "댓글량+감정": ATT + SENT,
        "거래기록+댓글량": TRADE_B + ATT, "거래기록+댓글량+감정": TRADE_B + ATT + SENT}),
    "B. 주가 방향 (ROC-AUC)": ("up", roc_auc_score, {
        "거래기록": TRADE_D, "댓글량": ATT, "감정": SENT,
        "거래기록+감정": TRADE_D + SENT, "거래기록+댓글량+감정": TRADE_D + ATT + SENT}),
}


def predict(d, y, cols, splits):
    s = np.full(len(d), np.nan)
    for tr, te in splits:
        m = make_pipeline(StandardScaler(), LogisticRegression()).fit(d.loc[tr, cols], y[tr])
        s[te] = m.predict_proba(d.loc[te, cols])[:, 1]
    return s


def gain_ci(y, s_new, s_base, metric, day, n=1000):
    """s_new - s_base 점수 차이와 날짜 묶음 부트스트랩 95% 구간 (모델 고정, 채점만 재표집)."""
    f = lambda i: metric(y[i], s_new[i]) - metric(y[i], s_base[i])
    uniq, r = np.unique(day), np.random.default_rng(1)
    pos = {d: np.where(day == d)[0] for d in uniq}
    boot = [f(np.concatenate([pos[d] for d in r.choice(uniq, len(uniq))])) for _ in range(n)]
    return f(np.arange(len(y))), *np.percentile(boot, [2.5, 97.5])


for title, (target, metric, sets) in PROBLEMS.items():
    allcols = sorted({c for v in sets.values() for c in v})
    d = df.dropna(subset=allcols + [target]).reset_index(drop=True)
    y = d[target].astype(int).values
    syms, fut = d.symbol.values, (d.start >= CUT).values
    schemes = {"시간 나누기": [(~fut, fut)],
               "종목 빼기(참고)": [(syms != s, syms == s) for s in np.unique(syms)]}
    print(f"\n{title}  — 관측 {len(d)}개, 정답 비율 {y.mean():.3f}, 시험(9월) {fut.sum()}개")
    print(f"  {'특징':18}" + "".join(f"{k:>14}" for k in schemes) + f"{'9월 장외 블록':>14}")
    res = {k: {n: predict(d, y, c, sp) for n, c in sets.items()} for k, sp in schemes.items()}
    night = fut & (d.slot == "15:30").values
    for n in sets:
        row = "".join(f"{metric(y[~np.isnan(res[k][n])], res[k][n][~np.isnan(res[k][n])]):14.3f}" for k in schemes)
        print(f"  {n:18}{row}{metric(y[night], res['시간 나누기'][n][night]):14.3f}")
    base = "거래기록"
    print(f"  {'랜덤 기준':18}{(y[fut].mean() if target == 'burst' else 0.5):14.3f}")
    ts = res["시간 나누기"]
    day = d.start.dt.date.values
    for n in [k for k in sets if k.startswith("거래기록+")]:
        for lbl, m in [("9월 전체", fut), ("9월 장외", night)]:
            gch, lo, hi = gain_ci(y[m], ts[n][m], ts[base][m], metric, day[m])
            print(f"  {n} - 거래기록 ({lbl}): {gch:+.3f} [{lo:+.3f}, {hi:+.3f}]")
    if target == "up":
        acc = ((ts["거래기록+감정"][fut] > 0.5) == y[fut]).mean()
        print(f"  정확도(9월, 거래기록+감정) {acc:.3f} vs 늘 다수 쪽으로 찍기 {max(y[fut].mean(), 1 - y[fut].mean()):.3f}")
