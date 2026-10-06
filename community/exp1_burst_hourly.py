"""Experiment 1, hourly: do this hour's comments predict a volume burst in the next hour?

Blocks  = regular-session 1-hour bars (9:30, 10:30 ... 15:30 ET, 7 per day). The 15:30 block also
          holds after-hours comments up to the next day's 9:30 open.
Target  = next bar's volume > the ticker's PAST 80th percentile for that time of day
          (removes the intraday U-shape; no leakage)
Features = relative to the same ticker and time slot's past mean. Eval = leave one whole ticker out.

Run: python3 exp1_burst_hourly.py   (1-hour bars are saved to data/prices_1h.csv)
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

# 1) fetch 1-hour bars (exclude today: bars may still be forming)
raw = yf.download(TICKERS, start="2026-06-01", interval="1h", auto_adjust=True, progress=False)
bars = raw.stack(level=1, future_stack=True).reset_index()
bars.columns = [c.lower() for c in bars.columns]
bars = bars.rename(columns={"ticker": "symbol", "datetime": "start"}).dropna(subset=["close"])
bars["start"] = pd.to_datetime(bars.start, utc=True).dt.tz_convert("America/New_York")
bars = bars[bars.start < pd.Timestamp.now(tz="America/New_York").normalize()]
bars.to_csv(HERE / "data" / "prices_1h.csv", index=False)
bars = bars.sort_values(["symbol", "start"]).reset_index(drop=True)
bars["slot"] = bars.start.dt.strftime("%H:%M")

# 2) posts -> the block containing their timestamp (block k = k-th bar start to the next bar start)
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
df.loc[df.start < START, ["comments", "authors"]] = np.nan  # before collection started it is "unknown", not 0

# 3) relative to the same ticker and slot past mean (min 5 days of history)
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
df["next_start"] = df.groupby("symbol").start.shift(-1)

SETS = {"trading": ["vol_rel", "move_rel"], "comments": ["com_rel", "auth_rel"]}
SETS["both"] = SETS["trading"] + SETS["comments"]
data = df.dropna(subset=SETS["both"] + ["target"]).reset_index(drop=True)
y = data.target.astype(int).values

# 4) leave-one-ticker-out fit/predict
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

print(f"{len(data)} observations ({data.symbol.nunique()} tickers, {data.start.min():%m-%d}~{data.start.max():%m-%d}), "
      f"{y.sum()} bursts (rate {y.mean():.3f})\n")
print(f"{'':9}{'PR-AUC(all)':>12}{'ROC-AUC':>9}{'mean-per-ticker PR-AUC':>23}")
ap = {}
for name, s in scores.items():
    ap[name] = per_ticker_ap(s)
    print(f"{name:9}{average_precision_score(y, s):12.3f}{roc_auc_score(y, s):9.3f}{ap[name].mean():23.3f}")
print(f"{'random':9}{y.mean():12.3f}{0.5:9.3f}{rand:23.3f}  <- baseline (200 random draws)\n")

t = pd.DataFrame(ap)
t["group"] = np.where(t.index.isin(LARGE), "large-cap", "retail")
print("mean per-ticker PR-AUC by group")
print(t.groupby("group")[list(SETS)].mean().round(3).to_string())
win = t["comments"] > t["trading"]
print(f"\ntickers where comments beat trading: {win.sum()}/{len(t)} -> {', '.join(t.index[win])}")
print("\nPR-AUC by time slot (all tickers pooled)")
for slot, grp in data.groupby("slot"):
    i = grp.index.values
    print(f"  {slot} block -> next bar: " + "  ".join(f"{n} {average_precision_score(y[i], scores[n][i]):.3f}" for n in SETS)
          + f"  (burst rate {y[i].mean():.2f})")

# 5) is the overnight-block gain (both - trading) driven by earnings? re-score without earnings blocks
#    data/earnings.csv comes from yfinance: uv run --no-project --with yfinance --with lxml (see README)
earn = pd.read_csv(HERE / "data" / "earnings.csv")
earn["time"] = pd.to_datetime(earn.time, utc=True).dt.tz_convert("America/New_York")
night = data[data.slot == "15:30"]
hit_strict = pd.Series(False, index=night.index)  # blocks whose night contained an earnings release
hit_wide = pd.Series(False, index=night.index)    # plus blocks opening within 3 days after earnings
for _, e in earn.iterrows():
    m = night.symbol == e.symbol
    hit_strict |= m & (night.start < e.time) & (e.time < night.next_start + pd.Timedelta(hours=1))
    hit_wide |= m & (night.next_start >= e.time - pd.Timedelta(hours=18)) & (night.next_start <= e.time + pd.Timedelta(days=3))


def gain_with_ci(idx, sc=scores, n_boot=1000):
    """both - trading PR-AUC gain, with a date-block bootstrap 95% interval (models fixed; only scoring is resampled)."""
    gain = lambda i: average_precision_score(y[i], sc["both"][i]) - average_precision_score(y[i], sc["trading"][i])
    day = data.start.dt.date.values[idx]
    uniq = np.unique(day)
    r = np.random.default_rng(1)
    boot = [gain(np.concatenate([idx[day == d] for d in r.choice(uniq, len(uniq))])) for _ in range(n_boot)]
    return gain(idx), *np.percentile(boot, [2.5, 97.5])


print("\novernight block (15:30 -> next day 9:30 bar): PR-AUC gain from adding comments")
for label, keep in [("all", hit_strict | True), ("ex earnings night", ~hit_strict), ("ex 3 days post-earnings", ~hit_wide)]:
    idx = night.index.values[np.asarray(keep, dtype=bool)]
    g, lo, hi = gain_with_ci(idx)
    print(f"  {label:24} {len(idx):4} blocks, {y[idx].sum():3} bursts -> gain {g:+.3f}  95% CI [{lo:+.3f}, {hi:+.3f}]")

# 6) evaluation-scheme comparison: leave-ticker-out trains on other tickers' FUTURE dates, so also split by time
CUT = pd.Timestamp("2026-09-01", tz="America/New_York")  # train Jul-Aug, test Sep
past, future = (data.start < CUT).values, (data.start >= CUT).values
syms = data.symbol.values
schemes = {
    "leave-ticker-out": [(syms != s, syms == s) for s in np.unique(syms)],
    "time split": [(past, future)],
    "time + ticker": [(past & (syms != s), future & (syms == s)) for s in np.unique(syms)],
}
print(f"\nevaluation-scheme comparison (time split: train before {CUT:%m-%d}, test after)")
print(f"  {'scheme':16}{'trading':>8}{'comments':>9}{'both':>8}{'random':>7}   overnight gain (ex earnings night)")
for label, splits in schemes.items():
    sc = {}
    for name, cols in SETS.items():
        s = np.full(len(data), np.nan)
        for tr, te in splits:
            m = make_pipeline(StandardScaler(), LogisticRegression()).fit(data.loc[tr, cols], y[tr])
            s[te] = m.predict_proba(data.loc[te, cols])[:, 1]
        sc[name] = s
    ev = ~np.isnan(sc["trading"])
    aps = "".join(f"{average_precision_score(y[ev], sc[n][ev]):8.3f}" for n in SETS)
    idx = night.index.values[(~hit_strict).values & ev[night.index.values]]
    g, lo, hi = gain_with_ci(idx, sc)
    print(f"  {label:16}{aps}{y[ev].mean():7.3f}   {g:+.3f} [{lo:+.3f}, {hi:+.3f}] ({len(idx)} overnight blocks)")
