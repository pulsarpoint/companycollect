#!/usr/bin/env python
"""Validation spike: does embedding-NN classify industry as well as the LLM?

Builds the NACE reference matrix M (2043 categories) from data/nace_source.duckdb,
captures real homepage texts from one WET file, then for each page compares:
  - LLM label (ground-truth proxy, industry_nace_hint)
  - embedding top-3 NACE (page vector @ M.T)
across two M variants (bare description vs hierarchy-augmented).

Run:
    export EMBED_BASE_URL=http://100.77.62.33:8000/v1
    export COMMONCRAWL_LLM_BASE_URL=http://100.77.62.33:8888/v1
    export COMMONCRAWL_LLM_BASE_MODEL=qwen3:6b   # or whatever :8888 serves
    uv run python scripts/spike_nace_embed.py --pages 60

Re-runs reuse the cached homepage texts (data/commoncrawl/spike_pages.parquet),
so you can iterate the embedding side offline without re-downloading/re-LLM'ing.
"""
import argparse
import os
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from openai import OpenAI

from commoncrawl_enrich import extract, segment

NACE_DB = "data/nace_source.duckdb"
PAGE_CACHE = Path("data/commoncrawl/spike_pages.parquet")
GT_CACHE = Path("data/commoncrawl/spike_gt.parquet")
EMBED_BATCH = 128
_PAGE_INSTRUCTION = "Classify the business into its industry category"


# ---- embedding client -------------------------------------------------------
def embed_client() -> tuple[OpenAI, str]:
    base = os.environ.get("EMBED_BASE_URL", "http://100.77.62.33:8000/v1")
    cl = OpenAI(base_url=base, api_key="x", timeout=120)
    model = cl.models.list().data[0].id
    return cl, model


def embed(cl: OpenAI, model: str, texts: list[str], *, instruction: str | None = None) -> np.ndarray:
    if instruction:
        texts = [f"Instruct: {instruction}\nQuery: {t}" for t in texts]
    out: list[list[float]] = []
    for i in range(0, len(texts), EMBED_BATCH):
        chunk = texts[i:i + EMBED_BATCH]
        out.extend(d.embedding for d in cl.embeddings.create(model=model, input=chunk).data)
    a = np.asarray(out, dtype="float32")
    a /= np.linalg.norm(a, axis=1, keepdims=True) + 1e-9
    return a


# ---- NACE reference matrices ------------------------------------------------
def load_nace(variant: str) -> tuple[list[str], list[str], list[str]]:
    """Return (codes, labels, texts-to-embed) for a variant ('bare' | 'hier')."""
    con = duckdb.connect(NACE_DB, read_only=True)
    rows = con.execute("""
        select c.code, c.description_en,
               sec.description_en as section_desc,
               par.description_en as parent_desc
        from nace_stage.nace_categories c
        left join nace_stage.nace_categories sec
               on sec.code = c.section_code and sec.is_current
        left join nace_stage.nace_categories par
               on par.code = c.parent_code and par.is_current
        where c.is_current and coalesce(c.description_en,'') <> ''
          and c.level <> 'section'   -- single-letter sections too coarse to be a label
        order by c.code
    """).fetchall()
    con.close()
    codes = [r[0] for r in rows]
    labels = [r[1] for r in rows]
    if variant == "bare":
        texts = [r[1] for r in rows]
    else:  # hier: section > parent > leaf
        texts = [" > ".join(p for p in (r[2], r[3], r[1]) if p and p != r[1]) + (" > " + r[1])
                 if (r[2] or r[3]) else r[1] for r in rows]
    return codes, labels, texts


# ---- homepage text capture (cached) -----------------------------------------
def capture_pages(n: int) -> list[dict]:
    if PAGE_CACHE.exists():
        cached = pq.read_table(PAGE_CACHE).to_pylist()
        if len(cached) >= n:
            print(f"using cached {len(cached)} pages from {PAGE_CACHE}")
            return cached[:n]
    wet_url = segment.first_wet_url()
    print(f"downloading homepage texts from {wet_url}")
    from warcio.archiveiterator import ArchiveIterator
    stream = segment._open_stream(wet_url, None)
    pages: list[dict] = []
    try:
        for rec in ArchiveIterator(stream):
            if rec.rec_type != "conversion":
                continue
            uri = rec.rec_headers.get_header("WARC-Target-URI") or ""
            if not segment._is_homepage(uri):
                continue
            text = rec.content_stream().read().decode("utf-8", "replace")
            if len(text.strip()) < 200:  # skip near-empty pages
                continue
            root, _ = segment._host(uri)
            pages.append({"url": uri, "root_domain": root, "text": text[:8000]})
            if len(pages) >= n:
                break
    finally:
        stream.close()
    PAGE_CACHE.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(pages), PAGE_CACHE)
    print(f"cached {len(pages)} pages -> {PAGE_CACHE}")
    return pages


# ---- LLM ground-truth -------------------------------------------------------
def llm_labels(pages: list[dict]) -> list[dict]:
    # Reuse cached ground-truth if it covers these pages (lets embed + LLM run in
    # separate GPU windows, since the DGX swaps one model in at a time).
    if GT_CACHE.exists():
        gt = pq.read_table(GT_CACHE).to_pylist()
        if len(gt) >= len(pages):
            print(f"using cached LLM ground-truth from {GT_CACHE}")
            return gt[:len(pages)]
    base = os.environ.get("COMMONCRAWL_LLM_BASE_URL")
    if not base:
        print("no COMMONCRAWL_LLM_BASE_URL and no cache -> skipping LLM ground-truth")
        return [{"llm_label": "", "llm_nace": ""} for _ in pages]
    from commoncrawl_enrich.llm import from_openai
    cl = OpenAI(base_url=base, api_key="x", timeout=120)
    model = cl.models.list().data[0].id  # auto-detect served chat model id
    print(f"LLM model: {model}")
    arm = from_openai(base_url=base, model=model, api_key="x", enable_thinking=False)
    out = []
    for i, p in enumerate(pages):
        try:
            g = arm.classify_industry(p["text"])
            out.append({"llm_label": g.label, "llm_nace": g.nace_hint})
            print(f"  llm {i + 1}/{len(pages)} {p['root_domain'][:30]:30} -> {g.nace_hint or '?':7} {g.label[:40]}")
        except Exception as exc:  # noqa: BLE001 - server may be down; degrade to embedding-only
            if i == 0:
                print(f"  LLM unreachable ({type(exc).__name__}) -> embedding-only, no scored ground-truth")
            out.append({"llm_label": "", "llm_nace": ""})
    if any(r["llm_nace"] or r["llm_label"] for r in out):
        GT_CACHE.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(out), GT_CACHE)
        print(f"cached LLM ground-truth -> {GT_CACHE}")
    return out


def div(code: str) -> str:
    """2-digit NACE division from messy codes: '63.12'->'63', 'C15.20'->'15',
    '63.12Z'->'63', '72'->'72', bare section letter 'S'->'S'."""
    import re as _re
    s = (code or "").strip()
    m = _re.search(r"\d{2}", s)          # first 2-digit run = division
    return m.group(0) if m else s[:1].upper()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--pages", type=int, default=60)
    args = ap.parse_args()

    pages = capture_pages(args.pages)
    gt = llm_labels(pages)
    try:
        cl, model = embed_client()
    except Exception as exc:  # noqa: BLE001 - embedding server may be swapped out
        print(f"\nembedding server unreachable ({type(exc).__name__}). "
              f"Ground-truth cached; re-run when :8000 embedding model is up.")
        return
    print(f"embedding model: {model}")
    P = embed(cl, model, [p["text"] for p in pages], instruction=_PAGE_INSTRUCTION)

    for variant in ("bare", "hier"):
        codes, labels, texts = load_nace(variant)
        M = embed(cl, model, texts)  # documents: no instruction prefix
        S = P @ M.T
        top = np.argsort(-S, axis=1)[:, :3]
        n_gt = sum(1 for g in gt if g["llm_nace"])
        top1_div = top3_div = 0
        print(f"\n===== variant={variant}  ({len(codes)} categories) =====")
        for i, p in enumerate(pages):
            cand = [codes[j] for j in top[i]]
            margin = float(S[i, top[i][0]] - S[i, top[i][1]])
            g = gt[i]["llm_nace"]
            t1 = div(g) and div(g) == div(cand[0])
            t3 = div(g) and div(g) in {div(c) for c in cand}
            top1_div += bool(t1)
            top3_div += bool(t3)
            flag = "  " if margin >= 0.05 else "~~"  # low-margin tail
            mark = "=" if t1 else ("3" if t3 else (" " if not g else "x"))
            print(f" {flag}{mark} {p['root_domain'][:26]:26} llm={g or '-':6} "
                  f"emb={cand[0]:6}({S[i, top[i][0]]:.2f},m{margin:.2f}) top3={','.join(cand)}")
        if n_gt:
            print(f"  -> division top1 {top1_div}/{n_gt} ({top1_div / n_gt:.0%})  "
                  f"top3 {top3_div}/{n_gt} ({top3_div / n_gt:.0%})")


if __name__ == "__main__":
    main()
