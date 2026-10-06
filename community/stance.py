"""Label each post's stance: bullish / bearish / neutral (FinTwitBERT, trained on stock tweets).

Different from sentiment.py (mood): "shorts are getting crushed lol" is negative in mood
but BULLISH in stance.
Benchmark on a 50-post NVDA answer key (data/stance/key50.tsv, hand-labelled):
78% agreement on bull/bear posts for this model vs 59% for the mood model.
Errors concentrate on sarcasm ("Bears/Clowns...", "Up $175k (short + puts)").

Run: uv run --no-project --with transformers --with torch --with pandas python stance.py
Output: data/stance.csv (uuid, bear, neu, bull). Already-labelled posts are skipped.
"""
import sqlite3
from pathlib import Path

import pandas as pd
import torch
from transformers import AutoModelForSequenceClassification, AutoTokenizer

HERE = Path(__file__).parent
OUT = HERE / "data" / "stance.csv"
MODEL = "StephanAkkerman/FinTwitBERT-sentiment"
BATCH = 64

posts = pd.read_sql("SELECT uuid, body FROM posts", sqlite3.connect(HERE / "data" / "community.db"))
if OUT.exists():
    posts = posts[~posts.uuid.isin(pd.read_csv(OUT, usecols=["uuid"]).uuid)]
print(f"posts to label: {len(posts)}")

device = "mps" if torch.backends.mps.is_available() else "cpu"
tok = AutoTokenizer.from_pretrained(MODEL)
model = AutoModelForSequenceClassification.from_pretrained(MODEL).to(device).eval()
names = {"BEARISH": "bear", "NEUTRAL": "neu", "BULLISH": "bull"}
labels = [names[model.config.id2label[i]] for i in range(3)]

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
    out[["uuid", "bear", "neu", "bull"]].to_csv(OUT, mode="a", header=not OUT.exists(), index=False, float_format="%.4f")
    print(f"  {start + len(chunk)}/{len(posts)}", flush=True)
