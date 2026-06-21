import argparse
import json
import logging
import os
import time
from pathlib import Path

import duckdb
from dlt.sources.helpers import requests as dlt_requests

from dagster_v3.commoncrawl_enrich import enrich, index_client, metrics, parquet_out, warc
from dagster_v3.commoncrawl_enrich.llm import from_openai
from dagster_v3.commoncrawl_enrich.models import DomainTarget

LOGGER = logging.getLogger(__name__)


def load_targets(manifest_path: str, *, limit: int) -> list[DomainTarget]:
    rows = duckdb.connect().execute(
        "select root_domain, source_rank, open_page_rank "
        "from read_parquet(?) order by open_page_rank desc limit ?"
        if manifest_path.endswith(".parquet") else
        "select root_domain, source_rank, open_page_rank "
        "from read_csv_auto(?) order by open_page_rank desc limit ?",
        [manifest_path, limit],
    ).fetchall()
    return [DomainTarget(str(r[0]), int(r[1]), float(r[2])) for r in rows]


def run_pipeline(targets, *, out_dir, resolve, fetch, llm, crawl_id, max_workers) -> dict:
    start = time.monotonic()
    enrichments = enrich.enrich_domains(
        targets, resolve=resolve, fetch=fetch, llm=llm, crawl_id=crawl_id, max_workers=max_workers)
    parquet_out.write_parquet(enrichments, out_dir)
    report = metrics.build_report(enrichments, wall_clock_seconds=time.monotonic() - start)
    Path(out_dir, "metrics.json").write_text(json.dumps(report, indent=2))
    return report


def _build_duckdb_connection() -> duckdb.DuckDBPyConnection:
    # Kept for potential future use if DuckDB gains true anonymous S3 support.
    # The commoncrawl S3 bucket does not permit anonymous (unsigned) requests;
    # the primary resolver uses resolve_via_cdx_batch (credential-free CDX HTTP API).
    con = duckdb.connect()
    con.execute("install httpfs; load httpfs; set s3_region='us-east-1'; set s3_use_ssl=true;")
    return con


def main() -> None:
    parser = argparse.ArgumentParser(description="CommonCrawl enrichment Phase 0 spike runner")
    parser.add_argument("--manifest", required=True, help="Parquet/CSV: root_domain,source_rank,open_page_rank")
    parser.add_argument("--out", required=True, help="output directory for Parquet + metrics.json")
    parser.add_argument("--limit", type=int, default=10000)
    parser.add_argument("--crawl-id", default=index_client.DEFAULT_CRAWL_ID)
    parser.add_argument("--max-workers", type=int, default=16)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)

    targets = load_targets(args.manifest, limit=args.limit)
    # Use repo-standard dlt session (built-in 429/5xx retry/backoff).
    session = dlt_requests.Session()
    llm = from_openai(
        base_url=os.environ["COMMONCRAWL_LLM_BASE_URL"],
        model=os.environ["COMMONCRAWL_LLM_MODEL"],
        api_key=os.environ.get("COMMONCRAWL_LLM_API_KEY", "not-needed"),
        enable_thinking=True,
    )
    report = run_pipeline(
        targets, out_dir=args.out,
        # Resolve via credential-free CDX HTTP API (commoncrawl S3 bucket rejects anonymous
        # unsigned requests; DuckDB httpfs cannot sign-as-anonymous against that bucket).
        resolve=lambda domains, *, crawl_id: index_client.resolve_via_cdx_batch(
            domains, crawl_id=crawl_id, session=session, max_workers=args.max_workers,
        ),
        fetch=lambda record: warc.fetch_page(record, session=session),
        llm=llm, crawl_id=args.crawl_id, max_workers=args.max_workers,
    )
    LOGGER.info("Report: %s", json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
