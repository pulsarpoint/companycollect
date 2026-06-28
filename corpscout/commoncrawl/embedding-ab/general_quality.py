#!/usr/bin/env python3
"""General-quality intrinsic analysis of a stored embeddings.parquet.

Measures how usable the vectors are for general (non-NACE) purposes WITHOUT page content:
  - anisotropy: mean pairwise cosine (0 = ideal spread; high = collapsed toward a task cone)
  - effective rank: how many of the embed_dim dimensions are actually used (higher = more spread)
  - nearest-neighbour examples: eyeball whether neighbours look related (by domain + url)

Read-only. No GPU, no ClickHouse, no re-fetch.

  uv run --with pyarrow,numpy python general_quality.py <embeddings.parquet> [N=8000]
"""
import sys
import random
import numpy as np
import pyarrow.parquet as pq

path = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 8000

batch = next(pq.ParquetFile(path).iter_batches(
    batch_size=N, columns=["root_domain", "source_url", "embedding"]))
dom = batch.column("root_domain").to_pylist()
url = batch.column("source_url").to_pylist()
V = np.array(batch.column("embedding").to_pylist(), dtype=np.float32)
V /= np.linalg.norm(V, axis=1, keepdims=True) + 1e-9
n, d = V.shape
print(f"vectors: {n} x {d}")

S = V @ V.T
off = S[~np.eye(n, dtype=bool)]
print(f"mean pairwise cosine (anisotropy): {off.mean():.4f}  median: {np.median(off):.4f}  p95: {np.percentile(off, 95):.4f}")
print("  0 = ideal spread for general similarity; high = collapsed toward a task cone")

Vc = V - V.mean(0)
s = np.linalg.svd(Vc, compute_uv=False)
ev = s * s
p = ev / ev.sum()
participation = (ev.sum() ** 2) / (ev * ev).sum()
entropy_rank = np.exp(-(p * np.log(p + 1e-12)).sum())
print(f"effective rank: participation={participation:.1f}  entropy={entropy_rank:.1f}  (of {d} dims; higher = more spread)")

random.seed(1)
for q in random.sample(range(n), 6):
    sims = S[q].copy()
    sims[q] = -1
    nn = np.argsort(-sims)[:5]
    print(f"\nQUERY  {dom[q]}   {url[q][:70]}")
    for j in nn:
        print(f"   {sims[j]:.3f}  {dom[j]:30.30s} {url[j][:60]}")
