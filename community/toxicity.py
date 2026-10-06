"""Toxicity scores per post (Detoxify, as in the paper: threat, insult, obscenity, identity attack, ...).

Run (torch is heavy, so use an ephemeral uv env):
  uv run --no-project --with detoxify --with pandas python toxicity.py
Output: data/toxicity.csv (uuid + 6 scores). Already-scored posts are skipped.
"""
import sqlite3
from pathlib import Path

import pandas as pd
import torch
from detoxify import Detoxify

HERE = Path(__file__).parent
OUT = HERE / "data" / "toxicity.csv"
BATCH = 64

posts = pd.read_sql("SELECT uuid, body FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
if OUT.exists():
    posts = posts[~posts.uuid.isin(pd.read_csv(OUT, usecols=["uuid"]).uuid)]
print(f"posts to score: {len(posts)}")

model = Detoxify("original", device="mps" if torch.backends.mps.is_available() else "cpu")
for start in range(0, len(posts), BATCH * 20):
    chunk = posts.iloc[start:start + BATCH * 20]
    rows = []
    for i in range(0, len(chunk), BATCH):
        text = chunk.body.iloc[i:i + BATCH].fillna("").str.slice(0, 1000).tolist()
        rows.append(pd.DataFrame(model.predict(text)))
    out = pd.concat(rows, ignore_index=True)
    out.insert(0, "uuid", chunk.uuid.values)
    out.to_csv(OUT, mode="a", header=not OUT.exists(), index=False, float_format="%.4f")
    print(f"  {start + len(chunk)}/{len(posts)}")
