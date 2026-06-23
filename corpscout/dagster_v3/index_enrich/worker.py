"""Process a worklist shard: fetch each record, enrich, write one Parquet of domain rows."""
import argparse
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from commoncrawl_enrich import extract
from index_enrich import classify, fetch, schema

LOGGER = logging.getLogger(__name__)


def _fetch_record(s3, item: dict) -> tuple:
    """Byte-range fetch + parse one record -> (item, text, emails, subdomain). I/O-bound."""
    html, _headers = fetch.fetch_warc_record(
        s3, item["warc_filename"], int(item["warc_record_offset"]),
        int(item["warc_record_length"]))
    parsed = extract.parse_html(html)
    emails = [e.email for e in extract.extract_emails(html)]
    return item, (parsed.text or html), emails, classify._subdomain(item["url"])


def run_shard(worklist: list[dict], *, s3, classifier, crawl_id: str, out_path,
              source_run_id: str = "", resolved_at: datetime | None = None,
              fetch_workers: int = 32) -> dict:
    """Fetch the whole shard in parallel (I/O), then classify ALL texts in one batched call so
    the embedding GPU gets full batches instead of one text at a time."""
    resolved_at = resolved_at or datetime.now(timezone.utc)

    records: list[tuple] = []
    errors = 0
    with ThreadPoolExecutor(max_workers=fetch_workers) as pool:
        futures = [pool.submit(_fetch_record, s3, item) for item in worklist]
        for fut in as_completed(futures):
            try:
                records.append(fut.result())
            except Exception as exc:  # noqa: BLE001 - skip a bad record, keep the shard going
                errors += 1
                LOGGER.warning("fetch/parse failed: %s", exc)

    results = classifier.classify([r[1] for r in records]) if records else []
    domain_rows = [
        (crawl_id, item["url"], item["root_domain"], sub, emails, len(emails),
         res.page_type, float(res.page_type_score),
         res.nace_code, res.nace_label, res.nace_division,
         int(res.nace_confident), float(res.nace_margin), float(res.nace_score), res.method,
         res.nace_top3, res.nace_top3_labels, [float(s) for s in res.nace_top3_scores],
         item["url"], source_run_id, resolved_at)
        for (item, _text, emails, sub), res in zip(records, results)
    ]
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
