"""Sentiment scores per post (same family as the paper: Cardiff NLP twitter-roberta; pos/neu/neg probabilities).

Run (torch is heavy, so use an ephemeral uv env instead of system python):
  uv run --no-project --with transformers --with torch --with pandas python sentiment.py
Output: data/sentiment.csv (uuid, neg, neu, pos). Already-scored posts are skipped, so re-running adds new posts only.
"""
import sqlite3
from pathlib import Path

import pandas as pd
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

HERE = Path(__file__).parent
OUT = HERE / "data" / "sentiment.csv"
MODEL = "cardiffnlp/twitter-roberta-base-sentiment-latest"
BATCH = 64

posts = pd.read_sql("SELECT uuid, body FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
if OUT.exists():
    posts = posts[~posts.uuid.isin(pd.read_csv(OUT, usecols=["uuid"]).uuid)]
print(f"posts to score: {len(posts)}")

device = "mps" if torch.backends.mps.is_available() else "cpu"
tok = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL).to(device).eval()
labels = [model.config.id2label[i].lower()[:3] for i in range(3)]  # neg, neu, pos

for start in range(0, len(posts), BATCH * 20):
    chunk = posts.iloc[start:start + BATCH * 20]
    probs = []
    for i in range(0, len(chunk), BATCH):
        text = chunk.body.iloc[i:i + BATCH].fillna("").str.slice(0, 1000).tolist()
        enc = tok(text, padding=True, truncation=True, max_length=128, return_tensors="pt").to(device)
        with torch.no_grad():
            probs.append(model(**enc).logits.softmax(-1).cpu())
    out = pd.DataFrame(torch.cat(probs).numpy(), columns=labels)
    out.insert(0, "uuid", chunk.uuid.values)
    out[["uuid", "neg", "neu", "pos"]].to_csv(OUT, mode="a", header=not OUT.exists(), index=False, float_format="%.4f")
    print(f"  {start + len(chunk)}/{len(posts)}")
