"""Experiment 16: similar-day (analog) forecast of next-day volume from what the board is talking about.

Question: if we find the past days whose posts *read* most like today's, does the average of what happened
after those days predict tomorrow's volume better than (a) the past mean, (b) yesterday's value,
(c) similar days found from counts only (post count, bear share), (d) randomly picked past days?

- A day = previous trading day 16:00 ET to current day 16:00 ET (same boundary as exp8).
- Day vector = mean post embedding (embed.py), days with < MIN_POSTS posts dropped.
- Target y(t) = log( volume(t+1) / mean volume(t-19..t) ). Neighbors are same-symbol days strictly before t,
  so every neighbor's target was already known at the close of day t.
- Settings (k, centering) are picked on days before SPLIT only; the verdict uses days from SPLIT on, once.

Run: python3 embed.py first (see its docstring), then  python3 exp16_similar_days.py
"""
import os
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).parent
SPLIT, MIN_POSTS, WARMUP, KS, N_RAND, SHOW = "2026-09-15", 20, 30, (3, 5, 10, 20), 1000, "NVDA"
rng = np.random.default_rng(0)

# 1) posts -> trading day (boundary copied from exp8), attach embedding row and stance
posts = pd.read_sql("SELECT uuid, symbol, created_at, body FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
ids = pd.read_csv(HERE / "data" / "embeddings_uuid.csv")
E = np.load(HERE / "data" / "embeddings.npy").astype(np.float32)
posts = posts.merge(ids.assign(row=np.arange(len(ids))), on="uuid")
posts = posts.merge(pd.read_csv(HERE / "data" / "stance.csv"), on="uuid", how="left")
op = posts[["bear", "neu", "bull"]].to_numpy()
posts["is_bear"], posts["is_bull"] = op.argmax(1) == 0, op.argmax(1) == 2
posts.loc[posts.bear.isna(), ["is_bear", "is_bull"]] = False
prices = pd.read_csv(HERE / "data" / "prices.csv", parse_dates=["date"])
prices = prices[(prices.date < pd.Timestamp.now().normalize()) & (prices.symbol != "SPY")]
days = np.sort(prices.date.unique())
et = pd.to_datetime(posts.created_at, utc=True).dt.tz_convert("America/New_York")
cal = et.dt.normalize().dt.tz_localize(None) + pd.to_timedelta((et.dt.hour >= 16).astype(int), unit="D")
idx = np.searchsorted(days, cal.values)
posts = posts[idx < len(days)].copy()
posts["date"] = days[idx[idx < len(days)]]

# 2) target and the "yesterday's value" baseline, on the full trading calendar
prices = prices.sort_values(["symbol", "date"])
g = prices.groupby("symbol").volume
prices["y"] = np.log(g.shift(-1) / g.transform(lambda v: v.rolling(20).mean()))
prices["y_last"] = prices.groupby("symbol").y.shift(1)  # = log(vol(t) / mean20(t-1)), known at close of t
if os.environ.get("RESID"):  # follow-up: target = what the mean of the last 3 days' values gets wrong
    gy = prices.groupby("symbol").y
    prices["y"] -= (gy.shift(1) + gy.shift(2) + gy.shift(3)) / 3
    prices["y_last"] = prices.groupby("symbol").y.shift(1)


def unit(v):
    return v / np.linalg.norm(v, axis=-1, keepdims=True)


# 3) walk forward per symbol
rows, cand_y = [], []
for sym, p in posts.groupby("symbol"):
    grp = p.groupby("date")
    d = grp.agg(n=("row", "size"), bear=("is_bear", "sum"), bull=("is_bull", "sum")).reset_index()
    d = d[d.n >= MIN_POSTS].merge(prices.loc[prices.symbol == sym, ["date", "y", "y_last"]], on="date").reset_index(drop=True)
    V = np.stack([E[p.row.values[grp.indices[t]]].mean(0) for t in d.date])
    F = np.column_stack([np.log(d.n), (d.bear / (d.bear + d.bull)).fillna(0.5)])  # counts-only features
    y = d.y.to_numpy()
    for i in range(len(d)):
        c = np.arange(i)[~np.isnan(y[:i])]  # past days only; their targets are known by day i
        if len(c) < WARMUP:
            continue
        assert d.date[c].max() < d.date[i]  # no look-ahead
        r = {"symbol": sym, "date": d.date[i], "y": y[i], "mean": y[c].mean(), "last": d.y_last[i], "zero": 0.0}
        for center in (0, 1):
            mu = V[c].mean(0) * center  # centering removes the symbol's usual topic, using past days only
            order = c[np.argsort(-(unit(V[c] - mu) @ unit(V[i] - mu)))]
            olds = order[order < i - 10]  # same ranking, but neighbors from the last 10 days are not allowed
            for k in KS:
                r[f"emb_c{center}_k{k}"] = y[order[:k]].mean()
                r[f"age_c{center}_k{k}"] = np.median(i - order[:k])  # how many days back the neighbors are
                r[f"old_c{center}_k{k}"] = y[olds[:k]].mean() if len(olds) >= k else np.nan
            if center == 0 and sym == SHOW:
                show = (d, order[:5], i, p, grp)  # last one wins = most recent day
        z = (F[c] - F[c].mean(0)) / (F[c].std(0) + 1e-9), (F[i] - F[c].mean(0)) / (F[c].std(0) + 1e-9)
        order = c[np.argsort(((z[0] - z[1]) ** 2).sum(1))]
        for k in KS:
            r[f"num_k{k}"] = y[order[:k]].mean()
            r[f"rec_k{k}"] = y[c[-k:]].mean()  # no text at all: just the k most recent days
        rows.append(r)
        cand_y.append(y[c])

df = pd.DataFrame(rows)
df["cand"] = cand_y
scored = df[df.y.notna()]
train, test = scored[scored.date < SPLIT], scored[scored.date >= SPLIT]
mae = lambda t, col: (t[col] - t.y).abs().mean()

# 4) pick settings on the train period only
emb_cols = [c for c in df if c.startswith("emb_")]
best = min(emb_cols, key=lambda c: mae(train, c))
k = int(best.split("_k")[1])
num = min([f"num_k{k}" for k in KS], key=lambda c: mae(train, c))
rec = min([f"rec_k{k}" for k in KS], key=lambda c: mae(train, c))
old = best.replace("emb_", "old_")
assert test[old].notna().all()
print(f"train days {len(train)} ({train.date.min().date()}..{train.date.max().date()}), "
      f"test days {len(test)} ({test.date.min().date()}..{test.date.max().date()}), symbols {test.symbol.nunique()}")
print("train MAE by setting:", {c: round(mae(train, c), 4) for c in emb_cols})
print(f"picked on train: {best} (counts-only: {num}, recent-days: {rec})")
print(f"median age of the text neighbors on test: {test[best.replace('emb_', 'age_')].median():.0f} days back "
      f"(candidates go back ~{test.cand.map(len).median():.0f} days)\n")

# 5) verdict on the test period
rand = np.stack([yc[rng.random((N_RAND, len(yc))).argpartition(k, axis=1)[:, :k]].mean(1) for yc in test.cand])
rand_mae = np.abs(rand - test.y.to_numpy()[:, None]).mean(0)  # one MAE per random repeat (random within symbol)
m = {name: mae(test, col) for name, col in
     [("similar days (text)", best), ("past mean", "mean"), ("yesterday's value", "last"), ("similar days (counts)", num), ("always 0", "zero"),
      ("most recent days, no text", rec), ("similar days (text), older than 10 days only", old)]}
m["random past days (avg of 1000)"] = rand_mae.mean()
print("TEST MAE of log volume ratio (lower is better)")
for name, v in sorted(m.items(), key=lambda kv: kv[1]):
    print(f"  {v:.4f}  {name}")
print(f"p(random past days do as well as text) = {((rand_mae <= m['similar days (text)']).sum() + 1) / (N_RAND + 1):.4f}")

# uncertainty of the gaps: resample whole trading dates (symbols on the same date move together)
err = test.assign(e=(test[best] - test.y).abs(), e_num=(test[num] - test.y).abs(), e_mean=(test["mean"] - test.y).abs(),
                  e_last=(test["last"] - test.y).abs(), e_rec=(test[rec] - test.y).abs(),
                  e_old=(test[old] - test.y).abs(), e_zero=test.y.abs()).groupby("date")[["e", "e_num", "e_mean", "e_last", "e_rec", "e_old", "e_zero"]].sum()
boot = np.stack([err.iloc[rng.integers(0, len(err), len(err))].sum().to_numpy() for _ in range(2000)]) / len(test)
for j, name in [(1, "counts"), (2, "past mean"), (3, "yesterday"), (4, "most recent days"), (6, "always 0")]:
    lo, hi = np.percentile(boot[:, 0] - boot[:, j], [2.5, 97.5])
    print(f"  text minus {name}: {(err.e.sum() - err.iloc[:, j].sum()) / len(test):+.4f}  (95% CI {lo:+.4f}..{hi:+.4f}; below 0 = text wins)")

lo, hi = np.percentile(boot[:, 5] - boot[:, 2], [2.5, 97.5])
print(f"  old-only text minus past mean: {(err.e_old.sum() - err.e_mean.sum()) / len(test):+.4f}  (95% CI {lo:+.4f}..{hi:+.4f})")

print("\nTEST MAE per symbol")
per = test.groupby("symbol").apply(lambda t: pd.Series(
    {"days": len(t), "text": mae(t, best), "counts": mae(t, num), "past_mean": mae(t, "mean"), "yesterday": mae(t, "last")}))
print(per.round(4).to_string())

# 6) what it looks like: most recent SHOW day, its 5 most similar past days, and what happened next (terminal only)
d, top, i, p, grp = show
print(f"\n{SHOW} {d.date[i].date()} ({d.n[i]} posts) -> forecast next-day volume x{np.exp(d.y.to_numpy()[top].mean()):.2f} of 20-day mean"
      + (f", actual x{np.exp(d.y[i]):.2f}" if pd.notna(d.y[i]) else " (actual not known yet)"))
for j in [i, *top]:
    rws = p.row.values[grp.indices[d.date[j]]]
    near = np.argsort(-(E[rws] @ unit(E[rws].mean(0))))[:3]  # posts closest to the day's average
    print(f"\n  {d.date[j].date()}  next-day volume x{np.exp(d.y[j]):.2f}" if j != i else f"\n  {d.date[j].date()}  (today)")
    for b in p.body.values[grp.indices[d.date[j]]][near]:
        print("    -", " ".join(str(b).split())[:140])
