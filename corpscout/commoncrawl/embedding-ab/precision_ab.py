#!/usr/bin/env python3
"""Precision A/B: does quantizing the stored page vectors (fp16 / int8) change NACE classification vs
fp32? Round-trips the stored fp32 vectors through fp16 and per-vector int8, re-classifies against the NACE
matrix (from ClickHouse), and reports how many top-1 picks change + the confidence margin shift.

No GPU, no re-fetch — runs entirely on existing embeddings.

  uv run python precision_ab.py <embeddings.parquet> [N=20000]
"""
import os
import sys
import json
import numpy as np
import pyarrow.parquet as pq
import requests

N = int(sys.argv[2]) if len(sys.argv) > 2 else 20000


def load(path, n):
    b = next(pq.ParquetFile(path).iter_batches(batch_size=n, columns=["root_domain", "embedding"]))
    ec = b.column("embedding")
    return ec.flatten().to_numpy(zero_copy_only=False).reshape(len(ec), -1).astype(np.float32)


def nace_matrix():
    ch = f"http://{os.environ['CLICKHOUSE_HOST']}:8123/"
    auth = (os.environ["CLICKHOUSE_USER"], os.environ["CLICKHOUSE_PASSWORD"])
    q = "SELECT embedding FROM corpscout.nace_category_embeddings FINAL ORDER BY code FORMAT JSONEachRow"
    r = requests.get(ch, params={"query": q}, auth=auth, timeout=180)
    r.raise_for_status()
    return np.array([json.loads(li)["embedding"] for li in r.text.splitlines() if li.strip()], dtype=np.float32)


def norm(V):
    return V / (np.linalg.norm(V, axis=1, keepdims=True) + 1e-9)


def classify(V, M):
    S = V @ M.T
    top = np.argpartition(-S, 2, axis=1)[:, :2]
    rows = np.arange(len(V))[:, None]
    top = top[rows, np.argsort(-S[rows, top], axis=1)]
    margin = S[np.arange(len(V)), top[:, 0]] - S[np.arange(len(V)), top[:, 1]]
    return top[:, 0], margin


def fp16(V):
    return V.astype(np.float16).astype(np.float32)


def int8(V):  # per-vector symmetric int8 (the standard embedding quantization)
    s = np.abs(V).max(1, keepdims=True) / 127.0
    return np.round(V / s).astype(np.int8).astype(np.float32) * s


V = norm(load(sys.argv[1], N))
M = norm(nace_matrix())
print(f"n={len(V)}  nace_codes={len(M)}  dim={V.shape[1]}")

base_top1, base_margin = classify(V, M)
print(f"fp32 baseline: mean margin {base_margin.mean():.4f}")
for name, Vq in [("fp16", norm(fp16(V))), ("int8", norm(int8(V)))]:
    t1, mg = classify(Vq, M)
    changed = int((t1 != base_top1).sum())
    self_cos = (V * Vq).sum(1)
    print(f"{name}: top-1 agreement {100 * (t1 == base_top1).mean():.3f}%   "
          f"changed {changed}/{len(V)}   mean self-cosine {self_cos.mean():.6f}   mean margin {mg.mean():.4f}")
