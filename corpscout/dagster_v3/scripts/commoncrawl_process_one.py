#!/usr/bin/env python
"""Process the first WET file of the latest CommonCrawl crawl, end to end:

    download CommonCrawl WET -> S3 bucket  ->  process from S3 -> Parquet  ->  remove from S3

Three reusable functions (download / process / remove) the future loop will call per segment.

Run (LLM industry on, think-off):
    export $(grep -E '^COMMONCRAWL_LLM_(BASE_URL|BASE_MODEL)=' .env | xargs)
    export COMMONCRAWL_LLM_MODEL="$COMMONCRAWL_LLM_BASE_MODEL"
    uv run python scripts/commoncrawl_process_one.py --bucket crawls --limit 200
"""
import argparse
import os
import time

import boto3
import requests
from botocore.config import Config

from commoncrawl_enrich import segment


def make_s3(endpoint_url: str | None, profile: str | None):
    session = boto3.session.Session(profile_name=profile) if profile else boto3.session.Session()
    # MinIO/RustFS need path-style addressing + SigV4 (incl. for presigned URLs).
    return session.client("s3", endpoint_url=endpoint_url,
                          config=Config(signature_version="s3v4", s3={"addressing_style": "path"}))


def download_file(wet_url: str, bucket: str, key: str, s3) -> str:
    """Download a CommonCrawl WET file to a temp file, put it in the S3 bucket. Returns the s3:// path."""
    import os
    import tempfile

    fd, tmp_path = tempfile.mkstemp(suffix=".gz")
    os.close(fd)
    try:
        with requests.get(wet_url, stream=True, headers={"User-Agent": segment.USER_AGENT}, timeout=600) as resp:
            resp.raise_for_status()
            with open(tmp_path, "wb") as out:
                for chunk in resp.iter_content(1 << 20):
                    if chunk:
                        out.write(chunk)
        with open(tmp_path, "rb") as body:  # single PUT (no threaded multipart) — robust on MinIO
            s3.put_object(Bucket=bucket, Key=key, Body=body)
    finally:
        os.unlink(tmp_path)
    return f"s3://{bucket}/{key}"


def process_file(bucket: str, key: str, s3, out_dir: str, *, llm=None, limit: int | None = None,
                 industry_workers: int = 1) -> str:
    """Process the WET object at s3://bucket/key -> one Parquet of analysis. Returns its path."""
    import os
    import tempfile
    from pathlib import Path

    Path(out_dir).mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(suffix=".gz")
    os.close(fd)
    try:
        s3.download_file(bucket, key, tmp_path)  # fast parallel GET, then process locally
        out_parquet = Path(out_dir) / (Path(key).name.removesuffix(".gz") + ".parquet")
        stats = segment.process_wet_file(tmp_path, out_parquet, llm=llm, limit=limit,
                                         industry_workers=industry_workers)
        print(f"   wet stats: {stats}")
    finally:
        os.unlink(tmp_path)
    return str(out_parquet)


def remove_file(bucket: str, key: str, s3) -> None:
    """Remove the processed WET object from the S3 bucket."""
    s3.delete_object(Bucket=bucket, Key=key)


def _build_llm():
    base = os.environ.get("COMMONCRAWL_LLM_BASE_URL")
    if not base:
        return None
    from commoncrawl_enrich.llm import from_openai

    model = os.environ.get("COMMONCRAWL_LLM_BASE_MODEL") or os.environ.get("COMMONCRAWL_LLM_MODEL")
    return from_openai(base_url=base, model=model,
                       api_key=os.environ.get("COMMONCRAWL_LLM_API_KEY", "not-needed"),
                       enable_thinking=False)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bucket", default=os.environ.get("CORPSCOUT_S3_BUCKET", "crawls"),
                    help="S3 bucket to download the WET file into")
    ap.add_argument("--endpoint-url", default=os.environ.get("CORPSCOUT_S3_ENDPOINT"))
    ap.add_argument("--profile", default=(os.environ.get("AWS_PROFILE") or "minio"))
    ap.add_argument("--out-dir", default="data/commoncrawl/parquet")
    ap.add_argument("--segment-index", type=int, default=0)
    ap.add_argument("--limit", type=int, default=None, help="cap homepage records processed (dev)")
    ap.add_argument("--industry-workers", type=int, default=1,
                    help="concurrent LLM calls (1 = serial; this vLLM degrades under concurrency)")
    ap.add_argument("--no-industry", action="store_true")
    args = ap.parse_args()

    s3 = make_s3(args.endpoint_url, args.profile)
    wet_url = segment.first_wet_url(segment_index=args.segment_index)
    key = "wet/" + wet_url.rsplit("/", 1)[-1]
    print(f"WET: {wet_url}")

    t = time.monotonic()
    print(f"1) download -> s3://{args.bucket}/{key}")
    print("  ", download_file(wet_url, args.bucket, key, s3), f"({time.monotonic()-t:.0f}s)")

    t = time.monotonic()
    llm = None if args.no_industry else _build_llm()
    print(f"2) process (industry={'on' if llm else 'off'}, limit={args.limit})")
    parquet = process_file(args.bucket, key, s3, args.out_dir, llm=llm, limit=args.limit,
                           industry_workers=args.industry_workers)
    print("  ", parquet, f"({time.monotonic()-t:.0f}s)")

    print(f"3) remove s3://{args.bucket}/{key}")
    remove_file(args.bucket, key, s3)
    print("   done")


if __name__ == "__main__":
    main()
