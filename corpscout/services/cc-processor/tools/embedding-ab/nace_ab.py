#!/usr/bin/env python3
"""NACE classification A/B: classify the INSTRUCTED (stored) vectors and the NEUTRAL vectors against the
SAME NACE matrix (from ClickHouse), aligned by domain, and compare top-1 code / division / top-3 overlap /
confidence margin. Answers: does a neutral page vector classify NACE about as well as the instructed one?

  uv run python nace_ab.py <instructed.parquet> <neutral.parquet>
"""
import os
import sys
import json
import numpy as np
import pyarrow.parquet as pq
import requests


def load(path):
    t = pq.read_table(path, columns=["root_domain", "embedding"])
    dom = t.column("root_domain").to_pylist()
    ec = t.column("embedding").combine_chunks()  # fast: flatten the list<float32> child to numpy, no to_pylist
    V = ec.flatten().to_numpy(zero_copy_only=False).reshape(len(ec), -1).astype(np.float32)
    V /= np.linalg.norm(V, axis=1, keepdims=True) + 1e-9
    return dom, V


def nace_matrix():
    ch = f"http://{os.environ['CLICKHOUSE_HOST']}:8123/"
    auth = (os.environ["CLICKHOUSE_USER"], os.environ["CLICKHOUSE_PASSWORD"])
    q = ("SELECT code, division, embedding FROM corpscout.nace_category_embeddings FINAL "
         "ORDER BY code FORMAT JSONEachRow")
    r = requests.get(ch, params={"query": q}, auth=auth, timeout=180)
    r.raise_for_status()
    codes, divs, mat = [], [], []
    for line in r.text.splitlines():
        if line.strip():
            o = json.loads(line)
            codes.append(o["code"]); divs.append(o["division"]); mat.append(o["embedding"])
    M = np.array(mat, dtype=np.float32)
    M /= np.linalg.norm(M, axis=1, keepdims=True) + 1e-9
    return np.array(codes), np.array(divs), M


def classify(V, M):
    S = V @ M.T
    rows = np.arange(len(V))[:, None]
    top = np.argpartition(-S, 3, axis=1)[:, :3]
    top = top[rows, np.argsort(-S[rows, top], axis=1)]
    score1 = S[np.arange(len(V)), top[:, 0]]
    margin = score1 - S[np.arange(len(V)), top[:, 1]]
    return top, top[:, 0], score1, margin


dom_i, Vi = load(sys.argv[1])
dom_n, Vn = load(sys.argv[2])
codes, divs, M = nace_matrix()
print(f"NACE codes: {len(codes)}   instructed rows: {len(dom_i)}   neutral rows: {len(dom_n)}")

pos = {}
for k, d in enumerate(dom_i):
    pos.setdefault(d, k)
keep = [(j, pos[d]) for j, d in enumerate(dom_n) if d in pos]
jn = np.array([j for j, _ in keep]); ji = np.array([i for _, i in keep])
print(f"aligned domains: {len(keep)}")

ti, t1i, si, mi = classify(Vi[ji], M)
tn, t1n, sn, mn = classify(Vn[jn], M)

top1 = (t1i == t1n).mean()
div = (divs[t1i] == divs[t1n]).mean()
ov = np.mean([len(set(ti[k]) & set(tn[k])) / 3 for k in range(len(keep))])

print(f"\n=== INSTRUCTED vs NEUTRAL agreement (n={len(keep)}) ===")
print(f"top-1 NACE code agreement:    {top1 * 100:.1f}%")
print(f"top-1 NACE division agreement:{div * 100:.1f}%")
print(f"top-3 overlap (mean jaccard): {ov * 100:.1f}%")
print(f"\n=== confidence (top-1 score & margin = top1-top2) ===")
print(f"instructed: mean score {si.mean():.3f}  mean margin {mi.mean():.3f}")
print(f"neutral:    mean score {sn.mean():.3f}  mean margin {mn.mean():.3f}")
print("  higher margin = sharper / more confident classification")

dis = np.where(t1i != t1n)[0][:10]
print("\n=== sample disagreements (domain: instructed_code -> neutral_code) ===")
for k in dis:
    print(f"  {dom_n[jn[k]]:32.32s} {codes[t1i[k]]:>7s} -> {codes[t1n[k]]:<7s}")
