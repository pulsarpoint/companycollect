#!/usr/bin/env python
"""Validate index-driven enrichment on a small subset and measure throughput.

On the EC2 (us-east-1, instance role for S3, Tailscale -> DGX for embeddings):
    export COMMONCRAWL_EMBED_BASE_URL=http://100.77.62.33:8000/v1
    export COMMONCRAWL_REFS_DIR=/path/to/refs   # holds nace_reference.npz + page_type_prototypes.npz
    uv run python scripts/index_enrich_validate.py \
        --index-glob "'s3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2026-25/subset=warc/part-0000[0-3]-*.parquet'" \
        --crawl CC-MAIN-2026-25 --limit 10000

`--index-glob` is the DuckDB read_parquet source (quote-wrapped). For a cheap validation point
it at a FEW index parts (a complete scan of all 300 parts is the production/Athena path). The
worklist over a partial scan is fine for measuring fetch+enrich throughput on real domains.
"""
import argparse
import time

import boto3
import duckdb

from index_builder import worklist
from index_enrich import worker


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--crawl", default="CC-MAIN-2026-25")
    ap.add_argument("--index-glob", required=True,
                    help="DuckDB read_parquet source for the index parts (s3 glob / HTTP list)")
    ap.add_argument("--where", default="", help="extra SQL filter, e.g. url_host_tld='sk'")
    ap.add_argument("--limit", type=int, default=10000)
    ap.add_argument("--out", default="data/commoncrawl/index_validate.parquet")
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    # DuckDB picks up the EC2 instance-role credentials for s3:// reads.
    con.execute("CREATE SECRET (TYPE s3, PROVIDER credential_chain, REGION 'us-east-1');")

    t = time.monotonic()
    src = f"read_parquet([{args.index_glob}], hive_partitioning=true)"
    rows = worklist.run_worklist(con, src, where=args.where).fetchmany(args.limit)
    cols = ["root_domain", "url", "warc_filename", "warc_record_offset", "warc_record_length"]
    items = [dict(zip(cols, r)) for r in rows]
    print(f"worklist: {len(items)} domains in {time.monotonic()-t:.0f}s")
    if not items:
        print("no rows — widen --index-glob or relax --where")
        return

    t = time.monotonic()
    stats = worker.run_shard(items, s3=boto3.client("s3"),
                             classifier=worker._load_classifier(), crawl_id=args.crawl,
                             out_path=args.out)
    dt = time.monotonic() - t
    rate = stats["domains"] / max(dt, 1e-9)
    print(f"enriched {stats['domains']} domains ({stats['errors']} errors) in {dt:.0f}s "
          f"-> {rate:.1f} domains/s")
    print(f"projection to 40M domains: {40_000_000 / max(rate, 1e-9) / 86400:.1f} "
          f"worker-days single-stream  -> {args.out}")


if __name__ == "__main__":
    main()
