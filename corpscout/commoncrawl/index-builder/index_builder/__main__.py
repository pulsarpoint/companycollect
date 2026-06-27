"""Generate index-driven worklist shards from the CommonCrawl columnar index, off-AWS.

Anonymous S3 *LIST* is denied on the `commoncrawl` bucket (anonymous GET is fine), so DuckDB
globs (`part-00000-*.parquet`) fail off-AWS. Instead we resolve the exact parquet part
filenames from CommonCrawl's published HTTPS manifest (`cc-index-table.paths.gz`) and hand
DuckDB exact `https://data.commoncrawl.org/...` URLs — no glob, no credentials, no LIST.

One index part -> one restartable worklist shard (the warc subset has ~300 parts per crawl).
Shards are cached under <index-dir>/<crawl>/shard_<part>.parquet and skipped if present.

Usage:
  python -m index_builder --crawl CC-MAIN-2026-25 --list           # how many parts exist
  python -m index_builder --crawl CC-MAIN-2026-25 --part 0         # build shard 0 (skip if cached)
  python -m index_builder --crawl CC-MAIN-2026-25 --parts 0-9      # build a range, skip existing
  python -m index_builder --crawl CC-MAIN-2026-25 --part 0 --out x.parquet   # explicit output path
"""
import argparse
import gzip
import sys
import urllib.request
from pathlib import Path

import duckdb

from .worklist import build_worklist

BASE = "https://data.commoncrawl.org/"
# Worklist shards live under the gitignored commoncrawl/data/ — NEVER inside the code package.
# Anchored to this file's location so the default is the same regardless of the working directory.
DEFAULT_INDEX_DIR = Path(__file__).resolve().parents[2] / "data" / "index"


def warc_part_urls(crawl: str) -> list[str]:
    """Exact HTTPS URLs of every warc-subset index parquet part, in part order."""
    manifest = f"{BASE}crawl-data/{crawl}/cc-index-table.paths.gz"
    raw = urllib.request.urlopen(manifest, timeout=60).read()
    paths = gzip.decompress(raw).decode().splitlines()
    parts = sorted(p for p in paths if "subset=warc" in p and p.endswith(".parquet"))
    if not parts:
        raise SystemExit(f"no warc parts in manifest {manifest}")
    return [BASE + p for p in parts]


def chosen_parts(part: str, parts: str) -> list[int]:
    if parts:
        lo, _, hi = parts.partition("-")
        return list(range(int(lo), int(hi or lo) + 1))
    return [int(part)]


def _build(con, url: str, out: Path, mode: str, max_pages: int) -> None:
    """Build one shard atomically (tmp + rename) so a crash can't leave a half file."""
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_name(out.name + f".tmp.{out.stem}")
    src = "read_parquet(" + repr([url]) + ", hive_partitioning=true)"
    n = build_worklist(con, src, str(tmp), mode=mode, max_pages=max_pages)
    tmp.replace(out)
    print(f"{out}: {n} rows ({mode})")


def main() -> None:
    ap = argparse.ArgumentParser(
        prog="index_builder", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--crawl", default="CC-MAIN-2026-25")
    ap.add_argument("--mode", choices=["industry", "tech"], default="industry",
                    help="industry = 1 page/domain (homepage); tech = many pages/domain")
    ap.add_argument("--max-pages", type=int, default=25,
                    help="tech mode: max pages per domain (0 = uncapped = every 200/HTML page)")
    ap.add_argument("--part", default="", help="single 0-based part index")
    ap.add_argument("--parts", default="", help="inclusive range, e.g. 0-9 (overrides --part)")
    ap.add_argument("--list", action="store_true", help="print how many parts the crawl has, then exit")
    ap.add_argument("--index-dir", default=str(DEFAULT_INDEX_DIR), help="shard cache dir")
    ap.add_argument("--out", default="", help="explicit output path (single part only; bypasses the cache)")
    args = ap.parse_args()

    urls = warc_part_urls(args.crawl)
    if args.list:
        print(f"{len(urls)} parts available for {args.crawl}")
        return
    if not args.part and not args.parts:
        ap.error("one of --part / --parts / --list is required")

    parts = chosen_parts(args.part or "0", args.parts)
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs")

    if args.out:
        if len(parts) != 1:
            ap.error("--out is only valid with a single --part")
        _build(con, urls[parts[0]], Path(args.out), args.mode, args.max_pages)
        return

    index_dir = Path(args.index_dir)
    for p in parts:
        if p >= len(urls):
            print(f"[{args.crawl} {p}] no such part (crawl has {len(urls)})", file=sys.stderr)
            continue
        out = index_dir / args.crawl / f"shard_{args.mode}_{p}.parquet"
        if out.exists():
            print(f"{out}: cached, skip")
            continue
        _build(con, urls[p], out, args.mode, args.max_pages)


if __name__ == "__main__":
    main()
