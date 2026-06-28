#!/usr/bin/env python3
"""Re-embed one already-embedded part's pages WITHOUT the classify instruction (neutral / plain document
embeddings), saving a parallel parquet for instructed-vs-neutral A/B analysis.

Reads the WARC coords from the existing (instructed) embeddings.parquet, re-fetches the SAME pages from the
public CommonCrawl S3 bucket, extracts visible text the same way the Go worker does (strip script/style,
collapse whitespace, truncate to MAX_CHARS), and embeds them PLAIN (no "Instruct:/Query:" wrapper).

Saves, per page: the NEUTRAL vector AND the extracted TEXT — so every downstream purpose test (similar,
near-dup, cluster, re-classify, semantic search) can judge with real content, and the instructed-vs-neutral
comparison has its neutral side. GPU cost = one block of embeds; no NACE, no ClickHouse.

  uv run --with pyarrow,requests,boto3,warcio,beautifulsoup4 python embed_neutral_block.py SRC OUT [LIMIT]
"""
import os
import sys
import gzip
import io
import time
import concurrent.futures as cf
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import boto3
from botocore.config import Config
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup

SRC = sys.argv[1]
OUT = sys.argv[2]
LIMIT = int(sys.argv[3]) if len(sys.argv) > 3 else 0
MAX_CHARS = int(os.environ.get("COMMONCRAWL_EMBED_MAX_CHARS", "2000"))
BASE = os.environ["COMMONCRAWL_EMBED_BASE_URL"].rstrip("/")
FETCH_CONC = 48
EMBED_BATCH = 32

model = os.environ.get("COMMONCRAWL_EMBED_MODEL", "")
if not model or model == "auto":
    model = requests.get(BASE + "/models", timeout=30).json()["data"][0]["id"]
print(f"model={model} max_chars={MAX_CHARS} base={BASE}", flush=True)

s3 = boto3.client("s3", region_name=os.environ.get("AWS_REGION", "us-east-1"),
                  config=Config(retries={"max_attempts": 4}, max_pool_connections=FETCH_CONC * 2))

cols = ["crawl_id", "root_domain", "subdomain", "source_url", "warc_filename", "warc_offset", "warc_length"]
rows = pq.read_table(SRC, columns=cols).to_pylist()
if LIMIT:
    rows = rows[:LIMIT]
print(f"pages to embed: {len(rows)}", flush=True)


def fetch_text(r):
    try:
        off, ln = r["warc_offset"], r["warc_length"]
        raw = s3.get_object(Bucket="commoncrawl", Key=r["warc_filename"],
                            Range=f"bytes={off}-{off + ln - 1}")["Body"].read()
        rec = next(ArchiveIterator(io.BytesIO(gzip.decompress(raw))))
        soup = BeautifulSoup(rec.content_stream().read(), "html.parser")
        for tag in soup(["script", "style", "noscript", "template"]):
            tag.decompose()
        return r, " ".join(soup.get_text(" ").split())[:MAX_CHARS]
    except Exception:
        return r, None


fetched, bad = [], 0
with cf.ThreadPoolExecutor(max_workers=FETCH_CONC) as ex:
    for i, (r, text) in enumerate(ex.map(fetch_text, rows)):
        if text:
            fetched.append((r, text))
        else:
            bad += 1
        if (i + 1) % 5000 == 0:
            print(f"fetched {i + 1}/{len(rows)} ok={len(fetched)} bad={bad}", flush=True)
print(f"fetched ok={len(fetched)} bad={bad}", flush=True)


def embed_batch(texts):
    for attempt in range(5):
        try:
            resp = requests.post(BASE + "/embeddings", json={"model": model, "input": texts}, timeout=180)
            resp.raise_for_status()
            return [d["embedding"] for d in resp.json()["data"]]
        except Exception:
            if attempt == 4:
                raise
            time.sleep(2 * (attempt + 1))


out_rows, t0 = [], time.time()
for i in range(0, len(fetched), EMBED_BATCH):
    chunk = fetched[i:i + EMBED_BATCH]
    vecs = embed_batch([t for _, t in chunk])
    for (r, text), v in zip(chunk, vecs):
        out_rows.append({
            "crawl_id": r["crawl_id"], "root_domain": r["root_domain"], "subdomain": r["subdomain"],
            "embedding": v, "embed_dim": len(v), "text_len": len(text), "text": text,
            "source_url": r["source_url"], "warc_filename": r["warc_filename"],
            "warc_offset": r["warc_offset"], "warc_length": r["warc_length"],
        })
    if len(out_rows) % 5000 < EMBED_BATCH:
        print(f"embedded {len(out_rows)}/{len(fetched)} ({len(out_rows) / (time.time() - t0):.0f}/s)", flush=True)

schema = pa.schema([
    ("crawl_id", pa.string()), ("root_domain", pa.string()), ("subdomain", pa.string()),
    ("embedding", pa.list_(pa.float32())), ("embed_dim", pa.uint16()), ("text_len", pa.uint32()),
    ("text", pa.string()), ("source_url", pa.string()), ("warc_filename", pa.string()),
    ("warc_offset", pa.int64()), ("warc_length", pa.int64()),
])
os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
pq.write_table(pa.Table.from_pylist(out_rows, schema=schema), OUT)
print(f"SAVED {len(out_rows)} neutral embeddings (+text) -> {OUT}", flush=True)
