"""Process a worklist shard: fetch each record, enrich, write one Parquet of domain rows."""
import argparse
import json
import logging
import os
from datetime import datetime, timezone

from index_enrich import classify, fetch, schema

LOGGER = logging.getLogger(__name__)


def run_shard(worklist: list[dict], *, s3, classifier, crawl_id: str, out_path,
              wappalyzer=None, source_run_id: str = "", resolved_at: datetime | None = None) -> dict:
    resolved_at = resolved_at or datetime.now(timezone.utc)
    domain_rows: list[tuple] = []
    errors = 0
    for item in worklist:
        try:
            html, headers = fetch.fetch_warc_record(
                s3, item["warc_filename"], int(item["warc_record_offset"]),
                int(item["warc_record_length"]))
            row, _tech = classify.enrich_domain(
                html, headers, root_domain=item["root_domain"], url=item["url"],
                crawl_id=crawl_id, classifier=classifier, wappalyzer=wappalyzer,
                source_run_id=source_run_id, resolved_at=resolved_at)
            domain_rows.append(row)
        except Exception as exc:  # noqa: BLE001 - skip a bad record, keep the shard going
            errors += 1
            LOGGER.warning("enrich failed for %s: %s", item.get("root_domain"), exc)
    schema.write_domain_rows_parquet(domain_rows, out_path)
    return {"domains": len(domain_rows), "errors": errors, "out": str(out_path)}


def _make_ch_client():
    """Native ClickHouse client from CLICKHOUSE_* env (reachable over Tailscale)."""
    from clickhouse_driver import Client

    secure = os.environ.get("CLICKHOUSE_SECURE", "").lower() in {"1", "true", "yes", "on"}
    return Client(host=os.environ["CLICKHOUSE_HOST"],
                  port=int(os.environ.get("CLICKHOUSE_NATIVE_PORT", "9002")),
                  user=os.environ.get("CLICKHOUSE_USER", "default"),
                  password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
                  database=os.environ.get("CLICKHOUSE_DATABASE", "corpscout"),
                  secure=secure)


def _load_classifier():
    """Build the classifier with reference matrices loaded straight from ClickHouse (no npz)."""
    from commoncrawl_enrich import nace_embed
    from commoncrawl_enrich.classifier import PageClassifier

    return PageClassifier.from_clickhouse(_make_ch_client(), nace_embed.EmbeddingClient.from_env())


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description="Enrich a worklist shard of domains.")
    ap.add_argument("--worklist", required=True, help="Parquet shard of worklist rows")
    ap.add_argument("--out", required=True)
    ap.add_argument("--crawl-id", required=True)
    args = ap.parse_args(argv)

    import boto3
    import pyarrow.parquet as pq

    rows = pq.read_table(args.worklist).to_pylist()
    stats = run_shard(rows, s3=boto3.client("s3"), classifier=_load_classifier(),
                      crawl_id=args.crawl_id, out_path=args.out)
    print(json.dumps(stats, default=str))


if __name__ == "__main__":
    main()
