#!/usr/bin/env python3
"""General-quality A/B: anisotropy, effective rank, near-dup tail, and clustering silhouette for the
INSTRUCTED and NEUTRAL vectors side by side. Lower anisotropy + higher effective rank = a more spread,
more general-purpose embedding (better for similarity / clustering / re-classification beyond industry).

  uv run python general_ab.py <instructed.parquet> <neutral.parquet> [N=8000]
"""
import sys
import numpy as np
import pyarrow.parquet as pq
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score

N = int(sys.argv[3]) if len(sys.argv) > 3 else 8000


def load(path):
    b = next(pq.ParquetFile(path).iter_batches(batch_size=N, columns=["root_domain", "embedding"]))
    ec = b.column("embedding")
    V = ec.flatten().to_numpy(zero_copy_only=False).reshape(len(ec), -1).astype(np.float32)
    V /= np.linalg.norm(V, axis=1, keepdims=True) + 1e-9
    return V


def stats(V, name):
    n, d = V.shape
    S = V @ V.T
    aniso = S[~np.eye(n, dtype=bool)].mean()
    Vc = V - V.mean(0)
    ev = np.linalg.svd(Vc, compute_uv=False) ** 2
    p = ev / ev.sum()
    erank = np.exp(-(p * np.log(p + 1e-12)).sum())
    np.fill_diagonal(S, -1)
    nn1 = S.max(1)
    km = KMeans(50, n_init=4, random_state=0).fit(V)
    sil = silhouette_score(V, km.labels_, metric="cosine", sample_size=min(4000, n), random_state=0)
    print(f"\n[{name}] n={n}")
    print(f"  anisotropy (mean pairwise cosine): {aniso:.4f}     (LOWER = more spread)")
    print(f"  effective rank (entropy):          {erank:.1f} / {d}   (HIGHER = more spread)")
    print(f"  near-dup tail: NN>0.97 {(nn1 > 0.97).mean() * 100:.1f}%   NN>0.99 {(nn1 > 0.99).mean() * 100:.1f}%   median NN {np.median(nn1):.3f}")
    print(f"  clustering silhouette (k=50):      {sil:.3f}")


stats(load(sys.argv[1]), "INSTRUCTED")
stats(load(sys.argv[2]), "NEUTRAL")
