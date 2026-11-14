"""Sentence embedding per post (all-MiniLM-L6-v2, 384 dims, unit length).

Run (torch is heavy, so use an ephemeral uv env instead of system python):
  uv run --no-project --with sentence-transformers --with pandas python embed.py
Output: data/embeddings.npy (float16, one row per post) + data/embeddings_uuid.csv (same row order).
Already-embedded posts are skipped, so re-running adds new posts only.
"""
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer

HERE = Path(__file__).parent
NPY, IDS = HERE / "data" / "embeddings.npy", HERE / "data" / "embeddings_uuid.csv"

posts = pd.read_sql("SELECT uuid, body FROM posts WHERE body IS NOT NULL AND body != ''",
                    sqlite3.connect(HERE / "data" / "community.db"))
old_ids = pd.read_csv(IDS).uuid if IDS.exists() else pd.Series([], dtype=str, name="uuid")
old = np.load(NPY) if NPY.exists() else np.zeros((0, 384), dtype=np.float16)
assert len(old) == len(old_ids), "embeddings.npy and embeddings_uuid.csv are out of sync"
posts = posts[~posts.uuid.isin(old_ids)]
print(f"posts to embed: {len(posts)}")

if len(posts):
    model = SentenceTransformer("all-MiniLM-L6-v2")  # picks mps/cuda/cpu itself
    new = model.encode(posts.body.tolist(), batch_size=128, normalize_embeddings=True, show_progress_bar=True)
    np.save(NPY, np.vstack([old, new.astype(np.float16)]))
    pd.concat([old_ids, posts.uuid]).to_csv(IDS, index=False)
print(f"total embedded: {len(old) + len(posts)}")
