#!/usr/bin/env python3
"""Verify fp16 embedding parquet(s) were correctly converted from their fp32 siblings, before deleting any
fp32. For each fp32 input it checks the sibling `embeddings_fp16.parquet`:
  - exists and opens,
  - the `embedding` column is float16 (HALF_FLOAT),
  - row count matches the fp32,
  - a sample of vectors round-trips (max abs diff tiny, cosine ~1.0) — catches a corrupt/partial write.
Reads only footers + a small row sample, so it's fast and memory-tiny (safe on a small box).

  python verify_fp16.py <fp32.parquet> [more ...] [--rows 1000]
  find ../data/ -name embeddings.parquet | xargs python verify_fp16.py
"""
import os
import sys
import numpy as np
import pyarrow.parquet as pq

K = 1000
if "--rows" in sys.argv:
    j = sys.argv.index("--rows")
    K = int(sys.argv[j + 1])
    del sys.argv[j:j + 2]
files = sys.argv[1:]


def sample(path, k):
    b = next(pq.ParquetFile(path).iter_batches(batch_size=k, columns=["embedding"]))
    ec = b.column(0)
    return ec.flatten().to_numpy(zero_copy_only=False).astype(np.float32).reshape(len(ec), -1)


ok = missing = bad = 0
for f32 in files:
    stem = os.path.splitext(f32)[0]
    f16 = (stem[:-5] if stem.endswith("_fp32") else stem) + "_fp16.parquet"
    try:
        if not os.path.exists(f16):
            print(f"MISSING  not converted yet   {f32}")
            missing += 1
            continue
        n32 = pq.read_metadata(f32).num_rows
        n16 = pq.read_metadata(f16).num_rows
        etype = str(pq.read_schema(f16).field("embedding").type)
        if "halffloat" not in etype:
            print(f"BAD      embedding is {etype}  {f16}"); bad += 1; continue
        if n16 != n32:
            print(f"BAD      rows {n32} != {n16}    {f16}"); bad += 1; continue
        a = sample(f32, K)
        c = sample(f16, K)
        d = float(np.abs(a - c).max())
        cos = float((a[0] @ c[0]) / ((np.linalg.norm(a[0]) + 1e-9) * (np.linalg.norm(c[0]) + 1e-9)))
        if d > 1e-2 or cos < 0.9999:
            print(f"BAD      values diff={d:.3g} cos={cos:.5f}  {f16}"); bad += 1; continue
        print(f"OK       {n16} rows  maxdiff={d:.2g} cos={cos:.6f}  {os.path.dirname(f16)}")
        ok += 1
    except Exception as e:
        print(f"BAD      {type(e).__name__}: {str(e)[:80]}  {f16}")
        bad += 1

print(f"\nOK {ok}   MISSING(not converted) {missing}   BAD {bad}")
if bad:
    print("!! do NOT prune — fix the BAD ones (re-convert) first")
