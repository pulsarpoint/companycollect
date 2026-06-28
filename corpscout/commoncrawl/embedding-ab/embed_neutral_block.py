#!/usr/bin/env python3
"""Re-embed one already-embedded part's pages WITHOUT the classify instruction (neutral / plain document
embeddings), saving a parallel parquet for instructed-vs-neutral A/B analysis.

Reads the WARC coords from the existing (instructed) embeddings.parquet, re-fetches the SAME pages from the
public CommonCrawl S3 bucket, extracts visible text the same way the Go worker does, and embeds them PLAIN
(no "Instruct:/Query:" wrapper). Saves per page: the NEUTRAL vector AND the extracted TEXT.

Fetch+parse runs in a PROCESS pool (HTML parsing is CPU-bound and GIL-throttled under threads), pipelined
into embedding so the GPU works from the first batch and the fetch overlaps it. GPU cost = one block of
embeds; no NACE, no ClickHouse.

  uv run --with pyarrow,requests,boto3,warcio,beautifulsoup4 python embed_neutral_block.py SRC OUT [LIMIT]
  env: FETCH_PROCS (default 48), COMMONCRAWL_EMBED_*, AWS_*
"""
import os
import sys
import gzip
import io
import time
import base64
import warnings
import concurrent.futures as cf
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import requests

warnings.filterwarnings("ignore")
MAX_CHARS = int(os.environ.get("COMMONCRAWL_EMBED_MAX_CHARS", "2000"))
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

# Per-worker-process S3 client (boto3 clients are not fork-safe to share; build lazily in each process).
_s3 = None


def _client():
    global _s3
    if _s3 is None:
        import boto3
        from botocore.config import Config
        _s3 = boto3.client("s3", region_name=AWS_REGION,
                           config=Config(retries={"max_attempts": 4}, max_pool_connections=4))
    return _s3


def fetch_text(r):
    from warcio.archiveiterator import ArchiveIterator
    from bs4 import BeautifulSoup
    try:
        off, ln = r["warc_offset"], r["warc_length"]
        raw = _client().get_object(Bucket="commoncrawl", Key=r["warc_filename"],
                                   Range=f"bytes={off}-{off + ln - 1}")["Body"].read()
        rec = next(ArchiveIterator(io.BytesIO(gzip.decompress(raw))))
        soup = BeautifulSoup(rec.content_stream().read(), "html.parser")
        for tag in soup(["script", "style", "noscript", "template"]):
            tag.decompose()
        return r, " ".join(soup.get_text(" ").split())[:MAX_CHARS]
    except Exception:
        return r, None


def main():
    SRC, OUT = sys.argv[1], sys.argv[2]
    LIMIT = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    BASE = os.environ["COMMONCRAWL_EMBED_BASE_URL"].rstrip("/")
    PROCS = int(os.environ.get("FETCH_PROCS", "48"))
    EMBED_BATCH = int(os.environ.get("COMMONCRAWL_EMBED_BATCH", "16"))
    EMBED_CONC = int(os.environ.get("COMMONCRAWL_EMBED_CONCURRENCY", "96"))

    model = os.environ.get("COMMONCRAWL_EMBED_MODEL", "")
    if not model or model == "auto":
        model = requests.get(BASE + "/models", timeout=30).json()["data"][0]["id"]
    print(f"model={model} max_chars={MAX_CHARS} procs={PROCS} embed_conc={EMBED_CONC} batch={EMBED_BATCH}", flush=True)

    cols = ["crawl_id", "root_domain", "subdomain", "source_url", "warc_filename", "warc_offset", "warc_length"]
    rows = pq.read_table(SRC, columns=cols).to_pylist()
    if LIMIT:
        rows = rows[:LIMIT]
    print(f"pages: {len(rows)}", flush=True)

    sess = requests.Session()  # urllib3 pool is thread-safe for concurrent posts
    sess.mount("http://", requests.adapters.HTTPAdapter(pool_maxsize=EMBED_CONC + 8))

    def _vec(e):
        # base64-encoded float32 (fast path) or a plain float list (fallback if server ignores the flag)
        if isinstance(e, str):
            return np.frombuffer(base64.b64decode(e), dtype=np.float32)
        return np.asarray(e, dtype=np.float32)

    def embed_pack(b):
        body = {"model": model, "input": [t for _, t in b], "encoding_format": "base64"}
        for a in range(5):
            try:
                resp = sess.post(BASE + "/embeddings", json=body, timeout=180)
                resp.raise_for_status()
                vecs = [_vec(d["embedding"]) for d in resp.json()["data"]]
                break
            except Exception:
                if a == 4:
                    raise
                time.sleep(2 * (a + 1))
        return [{
            "crawl_id": r["crawl_id"], "root_domain": r["root_domain"], "subdomain": r["subdomain"],
            "embedding": v.tolist(), "embed_dim": len(v), "text_len": len(text), "text": text,
            "source_url": r["source_url"], "warc_filename": r["warc_filename"],
            "warc_offset": r["warc_offset"], "warc_length": r["warc_length"],
        } for (r, text), v in zip(b, vecs)]

    out_rows, bad, t0 = [], 0, time.time()
    batch, pending = [], []

    # Fetch in PROCESSES (CPU-bound parse); embed via a THREAD pool keeping EMBED_CONC requests in flight
    # so vLLM batches them and the GPU stays saturated. Fetch + embed overlap.
    with cf.ProcessPoolExecutor(max_workers=PROCS) as fex, cf.ThreadPoolExecutor(max_workers=EMBED_CONC) as eex:
        for r, text in fex.map(fetch_text, rows, chunksize=64):
            if text is None:
                bad += 1
                continue
            batch.append((r, text))
            if len(batch) >= EMBED_BATCH:
                pending.append(eex.submit(embed_pack, batch))
                batch = []
        if batch:
            pending.append(eex.submit(embed_pack, batch))
        for f in cf.as_completed(pending):
            out_rows.extend(f.result())
            if len(out_rows) % 5000 < EMBED_BATCH:
                print(f"embedded {len(out_rows)} bad={bad} ({len(out_rows) / max(time.time() - t0, 1e-9):.0f}/s)", flush=True)

    schema = pa.schema([
        ("crawl_id", pa.string()), ("root_domain", pa.string()), ("subdomain", pa.string()),
        ("embedding", pa.list_(pa.float32())), ("embed_dim", pa.uint16()), ("text_len", pa.uint32()),
        ("text", pa.string()), ("source_url", pa.string()), ("warc_filename", pa.string()),
        ("warc_offset", pa.int64()), ("warc_length", pa.int64()),
    ])
    os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
    pq.write_table(pa.Table.from_pylist(out_rows, schema=schema), OUT)
    print(f"SAVED {len(out_rows)} neutral embeddings (+text) -> {OUT}", flush=True)


if __name__ == "__main__":
    main()
