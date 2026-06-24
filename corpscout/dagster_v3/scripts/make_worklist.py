"""Generate an index-driven worklist shard from the CommonCrawl columnar index, off-AWS.

Anonymous S3 *LIST* is denied on the `commoncrawl` bucket (anonymous GET is fine), so DuckDB
globs (`part-00000-*.parquet`) fail off-AWS. Instead we resolve the exact parquet part
filenames from CommonCrawl's published HTTPS manifest (`cc-index-table.paths.gz`) and hand
DuckDB exact `https://data.commoncrawl.org/...` URLs — no glob, no credentials, no LIST.

One index part -> one restartable worklist shard (the warc subset has ~300 parts per crawl).

Usage:
  uv run python scripts/make_worklist.py --crawl CC-MAIN-2026-25 --part 0 \
      --out data/commoncrawl/shard0.parquet
  uv run python scripts/make_worklist.py --crawl CC-MAIN-2026-25 --parts 0-9 \
      --out data/commoncrawl/shard_0_9.parquet
"""
import argparse
import gzip
import urllib.request
from pathlib import Path

import duckdb

from index_enrich import worklist

BASE = "https://data.commoncrawl.org/"


def warc_part_urls(crawl: str) -> list[str]:
    """Exact HTTPS URLs of every warc-subset index parquet part, in part order."""
    manifest = f"{BASE}crawl-data/{crawl}/cc-index-table.paths.gz"
    raw = urllib.request.urlopen(manifest, timeout=60).read()
    paths = gzip.decompress(raw).decode().splitlines()
    parts = sorted(p for p in paths if "subset=warc" in p and p.endswith(".parquet"))
    if not parts:
        raise SystemExit(f"no warc parts in manifest {manifest}")
    return [BASE + p for p in parts]


def chosen_indices(part: str, parts: str) -> list[int]:
    if parts:
        lo, _, hi = parts.partition("-")
        return list(range(int(lo), int(hi or lo) + 1))
    return [int(part)]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--crawl", default="CC-MAIN-2026-25")
    ap.add_argument("--part", default="0", help="single part index (0-based)")
    ap.add_argument("--parts", default="", help="inclusive range, e.g. 0-9 (overrides --part)")
    ap.add_argument("--where", default="content_languages like '%eng%'")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    urls = warc_part_urls(args.crawl)
    idxs = chosen_indices(args.part, args.parts)
    selected = [urls[i] for i in idxs]
    src = "read_parquet(" + repr(selected) + ", hive_partitioning=true)"

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")
    n = worklist.build_worklist(con, src, args.out, where=args.where)
    print(f"worklist: {n} domains from parts {idxs} -> {args.out}")


if __name__ == "__main__":
    main()
