#!/usr/bin/env python3
"""Intrinsic tests of a stored embeddings.parquet for the general 'neutral' purposes that can be measured
WITHOUT page content:

  - similar-domain : top-1 nearest-neighbour cosine distribution (tight neighbours = good)
  - near-duplicate : the high-cosine tail (share with NN > 0.95/0.97/0.99) and its separation from the bulk
  - clustering     : k-means silhouette (cosine) + cluster size balance

Confirming whether high-cosine neighbours are TRULY similar (vs just industry-similar) needs the page
content — see embed_neutral_block.py. Read-only. No GPU.

  uv run --with pyarrow,numpy,scikit-learn python intrinsic_purposes.py <embeddings.parquet> [N=8000] [K=50]
"""
import sys
import numpy as np
import pyarrow.parquet as pq
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

path = sys.argv[1]
N = int(sys.argv[2]) if len(sys.argv) > 2 else 8000
K = int(sys.argv[3]) if len(sys.argv) > 3 else 50

batch = next(pq.ParquetFile(path).iter_batches(batch_size=N, columns=["root_domain", "source_url", "embedding"]))
V = np.array(batch.column("embedding").to_pylist(), dtype=np.float32)
V /= np.linalg.norm(V, axis=1, keepdims=True) + 1e-9
n = V.shape[0]
S = V @ V.T
bulk = float(S[~np.eye(n, dtype=bool)].mean())
np.fill_diagonal(S, -1)
nn1 = S.max(1)

print(f"intrinsic tests (n={n})")
print("--- near-dup / similar (top-1 nearest-neighbour cosine) ---")
for p in (50, 75, 90, 95, 99):
    print(f"  p{p}: {np.percentile(nn1, p):.3f}")
print(f"  share NN>0.95: {(nn1 > 0.95).mean() * 100:.1f}%   >0.97: {(nn1 > 0.97).mean() * 100:.1f}%   >0.99: {(nn1 > 0.99).mean() * 100:.1f}%")
print(f"  bulk mean pairwise: {bulk:.3f}   -> separation (median NN - bulk): {np.median(nn1) - bulk:.3f}")

print(f"--- clustering (k={K}) ---")
km = KMeans(K, n_init=4, random_state=0).fit(V)
sil = silhouette_score(V, km.labels_, metric="cosine", sample_size=min(4000, n), random_state=0)
sz = np.bincount(km.labels_)
print(f"  silhouette(cosine): {sil:.3f}  (>0.1 weak, >0.25 ok, >0.5 strong)")
print(f"  cluster sizes min/median/max: {sz.min()}/{int(np.median(sz))}/{sz.max()}")
