"""Experiment 8: Does the crowd mark the turn? — bearish tilt + heat for N straight days, then up?

Hypothesis (2026-10-05): "When NVDA's board tilts bearish with high posting heat for at least
N days in a row, the price rises over the next day or so." Rules below were fixed BEFORE
looking at results (2026-10-05). Anything changed afterwards must be labelled post-hoc.

- A day = previous trading day 16:00 ET to current day 16:00 ET
- Post stance = argmax of stance.py probabilities (bull / bear / neutral)
- Bear share = bear posts / (bull + bear posts)   (neutral excluded)
- Bearish tilt = bear share > the ticker's own past mean (expanding, up to the prior day, min 10 days)
- Heat         = post count > the ticker's own past mean
- Signal day t = 'tilt + heat' held for N consecutive days (t inclusive)
- N is not fixed; it is searched (user request): try N=1..7 on Jul-Aug (search window),
  pick the N with the largest (up-rate - baseline up-rate), requiring >= 5 signal days;
  then score ONLY that N on Sep 1 onward (confirmation window). Search-window numbers
  are selected-for and must not be used as evidence.
- Outcome  = next-day (t+1) return minus SPY > 0   (secondary: t+1..t+3 cumulative)
- Baseline = up-rate on non-signal days (NVDA rises often, so compare to this, not 50%)
- Main test is NVDA only. The other 14 tickers are reported for reference
  (testing all 15 would make 1-2 of them "work" by chance).

Caveat: consecutive signal days overlap and are not independent -> the number of distinct
tilt spells is reported too.
Figure: align each spell on the day the signal first fires (day N) and plot cumulative
excess returns +/-5 trading days around it. (Do NOT align on the day the tilt *ends*:
"ended" means bear posts fell the next day, which likely means the price rose — an
anchor chosen with future information.)
Run: python3 exp8_contrarian.py -> figures/contrarian_nvda.png
"""
import sqlite3
from math import comb
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).parent
FIG = HERE / "figures" / "contrarian_nvda.png"
MAIN, WARMUP, WIN, SPLIT, NS, MIN_SIG = "NVDA", 10, 5, "2026-09-01", range(1, 8), 5

# 1) posts -> trading day, attach stance (day boundary matches plot_sentiment_deciles.py)
posts = pd.read_sql("SELECT uuid, symbol, created_at FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
posts = posts.merge(pd.read_csv(HERE / "data" / "stance.csv"), on="uuid")
posts["op"] = posts[["bear", "neu", "bull"]].to_numpy().argmax(1)  # 0 bear, 1 neutral, 2 bull
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[prices.date < pd.Timestamp.now().normalize()]  # drop today's intraday price
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]
daily = posts.groupby(["symbol", "date"]).agg(
    n=("op", "size"), bear=("op", lambda x: (x == 0).sum()), bull=("op", lambda x: (x == 2).sum())).reset_index()
daily["bear_share"] = daily.bear / (daily.bear + daily.bull)

# 2) excess returns (vs SPY) + signal rules
px = prices.pivot(index="date", columns="symbol", values="close").pct_change()
ex = px.drop(columns="SPY").sub(px.SPY, axis=0).stack().rename("ex").reset_index()
df = ex.merge(daily, on=["symbol", "date"], how="inner").sort_values(["symbol", "date"]).reset_index(drop=True)
g = df.groupby("symbol")
past = lambda col: g[col].transform(lambda x: x.expanding(WARMUP).mean().shift(1))
df["cond"] = (df.bear_share > past("bear_share")) & (df.n > past("n"))
df["run"] = g.cond.transform(lambda c: c.groupby((~c).cumsum()).cumsum())  # consecutive days so far
df["ex_next"] = g.ex.shift(-1)
df["ex_next3"] = g.ex.transform(lambda x: x.shift(-1) + x.shift(-2) + x.shift(-3))
df = df[past("n").notna()]  # start once the past mean exists


def binom_p(k, n, p):  # one-sided: chance of >= k ups by luck
    return sum(comb(n, i) * p**i * (1 - p)**(n - i) for i in range(k, n + 1))


def report(d, name, N):
    d = d.assign(signal=d.run >= N)
    s, o = d[d.signal].dropna(subset=["ex_next"]), d[~d.signal].dropna(subset=["ex_next"])
    k, n, base = int((s.ex_next > 0).sum()), len(s), (o.ex_next > 0).mean()
    spells = int((d.run == N).sum())
    return dict(ticker=name, N=N, signal_days=n, spells=spells, ups=f"{k}/{n}" if n else "-",
                up_rate=round(k / n, 2) if n else np.nan, base_up_rate=round(base, 2),
                p_value=round(binom_p(k, n, base), 3) if n else np.nan,
                next_day_excess_pct=round(s.ex_next.mean() * 100, 2) if n else np.nan,
                base_pct=round(o.ex_next.mean() * 100, 2),
                next3_cum_pct=round(s.ex_next3.mean() * 100, 2) if n else np.nan)


train, test = df[df.date < SPLIT], df[df.date >= SPLIT]
print(f"search {train.date.min():%Y-%m-%d} ~ {train.date.max():%Y-%m-%d} / confirm {test.date.min():%Y-%m-%d} ~ {test.date.max():%Y-%m-%d}\n")
search = pd.DataFrame([report(train[train.symbol == MAIN], MAIN, N) for N in NS])
print("[Step 1 search: Jul-Aug, NVDA, N=1..7 — selected-for, not evidence]")
print(search.to_string(index=False))
ok = search[search.signal_days >= MIN_SIG]
if ok.empty:
    raise SystemExit(f"\nNo N with >= {MIN_SIG} signal days in the search window -> verdict deferred (retry with more data)")
BEST = int(ok.N[(ok.up_rate - ok.base_up_rate).idxmax()])
print(f"\n-> chosen N = {BEST} day(s)\n")
print("[Step 2 confirm: Sep 1 onward, NVDA, the chosen N only — the real test]")
print(pd.DataFrame([report(test[test.symbol == MAIN], MAIN, BEST)]).to_string(index=False))
print("\n[Reference: confirmation window, other tickers, same N — some may 'work' by chance]")
print(pd.DataFrame([report(d, s, BEST) for s, d in test[test.symbol != MAIN].groupby("symbol")]
                   + [report(test[test.symbol != MAIN], "14 pooled", BEST)]).to_string(index=False))

# Pre-Sep-1 spells in the figure were used to choose N — they are not evidence.
# 3) figure (full period, chosen N): NVDA cumulative excess return aligned on signal onset
d = df[df.symbol == MAIN].reset_index(drop=True)
d["signal"] = d.run >= BEST
ends = d.index[d.run == BEST]
fig, ax = plt.subplots(figsize=(8, 4.5))
paths = []
for e in ends:
    lo, hi = max(e - WIN, 0), min(e + WIN, len(d) - 1)
    cum = (d.ex[lo:hi + 1].cumsum() - d.ex[:e + 1][lo:].sum()) * 100  # day-0 close = 0
    x = np.arange(lo - e, hi - e + 1)
    ax.plot(x, cum.values, color="#9aa5b1", lw=1)
    ax.annotate(f"{d.date[e]:%m/%d}", (x[-1], cum.values[-1]), fontsize=8, color="#52606d")
    paths.append(pd.Series(cum.values, index=x))
if paths:
    ax.plot(pd.concat(paths, axis=1).sort_index().mean(1), color="#1f6feb", lw=2.5, label=f"mean ({len(paths)} spells)")
ax.axvline(0, color="#52606d", ls="--", lw=1)
ax.axhline(0, color="#52606d", lw=0.5)
ax.set(xlabel="trading days around signal onset (0); grey = one spell, label = onset date",
       ylabel="cumulative excess return (%, vs SPY)",
       title=f"{MAIN}: price around bearish-tilt+heat signals (N >= {BEST})")
ax.legend(frameon=False)
FIG.parent.mkdir(exist_ok=True)
fig.tight_layout()
fig.savefig(FIG, dpi=150)
print(f"\nfigure: {FIG}")
