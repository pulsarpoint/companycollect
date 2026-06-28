#!/usr/bin/env python3
"""Extraction cross-check: confirm the neutral block's Python text extraction matches the Go worker's.

Embeds a sample of the neutral block's saved TEXT *with* the classify instruction (Python), and compares
to the STORED instructed vectors for the same domains. High mean cosine (~0.95+) => same text => the
instructed-vs-neutral A/B is fair (differences are instruction, not extraction). Low => extraction differs
and the A/B is partly confounded (we'd need to re-embed instructed from the same text).

  uv run python crosscheck.py <stored_instructed.parquet> <neutral_with_text.parquet> [N=500]
"""
import os
import sys
import numpy as np
import pyarrow.parquet as pq
import requests

N = int(sys.argv[3]) if len(sys.argv) > 3 else 500
BASE = os.environ["COMMONCRAWL_EMBED_BASE_URL"].rstrip("/")
INSTR = "Classify the business into its industry category"
model = os.environ.get("COMMONCRAWL_EMBED_MODEL", "")
if not model or model == "auto":
    model = requests.get(BASE + "/models", timeout=30).json()["data"][0]["id"]

neutral = pq.read_table(sys.argv[2], columns=["root_domain", "text"]).to_pylist()[:N]
it = pq.read_table(sys.argv[1], columns=["root_domain", "embedding"])
idom = it.column("root_domain").to_pylist()
_ec = it.column("embedding").combine_chunks()  # fast numpy load (no to_pylist on 83k x 4096)
IV = _ec.flatten().to_numpy(zero_copy_only=False).reshape(len(_ec), -1).astype(np.float32)
pos = {}
for k, d in enumerate(idom):
    pos.setdefault(d, k)


def embed_instructed(texts):
    wrapped = [f"Instruct: {INSTR}\nQuery: {t}" for t in texts]
    r = requests.post(BASE + "/embeddings", json={"model": model, "input": wrapped}, timeout=180)
    r.raise_for_status()
    return np.array([d["embedding"] for d in r.json()["data"]], dtype=np.float32)


sample = [(r["root_domain"], r["text"]) for r in neutral if r["root_domain"] in pos]
sims = []
for i in range(0, len(sample), 32):
    chunk = sample[i:i + 32]
    PV = embed_instructed([t for _, t in chunk])
    for (d, _), pv in zip(chunk, PV):
        sv = IV[pos[d]]
        sims.append(float(pv @ sv / ((np.linalg.norm(pv) + 1e-9) * (np.linalg.norm(sv) + 1e-9))))

sims = np.array(sims)
print(f"extraction cross-check (n={len(sims)}): python-instructed vs stored-instructed cosine")
print(f"  mean {sims.mean():.4f}  median {np.median(sims):.4f}  p10 {np.percentile(sims, 10):.4f}")
print("  ~1.0 identical extraction; >0.95 good; <0.90 => extraction differs, A/B partly confounded")
