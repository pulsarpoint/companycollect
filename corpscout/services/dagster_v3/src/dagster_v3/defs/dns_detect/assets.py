"""dns_record_services_clickhouse: DNS records → dns-detect → ClickHouse.

Design: docs/superpowers/specs/2026-09-28-dns-detect-slice-4-storage-design.md.
128 static hash partitions. Each run streams its bucket's routable records that
lack a current resolution, sends them to the dns-detect service in chunks
(a few in flight to use the service's cores), and inserts the results, then
the resolutions, in acknowledged batches. Nothing is deleted: views keep each
record's latest resolution, so a failed run is simply re-run.

A sensor queues every partition when the service's knowledge versions change,
and a daily schedule picks up new scans. Both start STOPPED.
"""

import json
import os
from collections import deque
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import RESOLVED_DATABASE, assert_clickhouse_tables_exist
from dagster_v3.defs.dns_detect import sql
from dagster_v3.defs.dns_detect.resource import DEFAULT_API_URL, DnsDetectResource

GROUP_NAME = "dns_detect"
JOB_NAME = "dns_record_services_job"
PARTITIONS = dg.StaticPartitionsDefinition(sql.partition_keys())
CHUNK_SIZE = 50_000
WORKERS = 4  # matches the service's CPU quota
REQUIRED_TABLES = (sql.DNS_RECORDS_TABLE, sql.RESOLUTIONS_TABLE, sql.SERVICES_TABLE)
READ_SETTINGS = {"do_not_merge_across_partitions_select_final": 1, "max_block_size": 10_000}

RECORD_FIELDS = ("record_id", "root_domain", "name", "type", "value", "first_seen", "last_seen")


def record_dict(row) -> dict:
    return dict(zip(RECORD_FIELDS, row, strict=True))


def _date(value: str) -> date:
    return date.fromisoformat(value[:10])


def rows_for(records: list[dict], lines: list[dict], rules_version: str, ip_version: str, resolved_at: datetime):
    """Insert rows for one resolved chunk: (resolutions, results)."""
    if len(lines) != len(records):
        raise ValueError(f"dns-detect answered {len(lines)} lines for {len(records)} records")
    resolutions, results = [], []
    for record, out in zip(records, lines, strict=True):
        if out["record_id"] != record["record_id"]:
            raise ValueError(f"dns-detect answered out of order: {out['record_id']} for {record['record_id']}")
        if "analyzer" not in out:
            raise ValueError("dns-detect answered without an analyzer field: the service predates this asset, redeploy it")
        rid = bytes.fromhex(record["record_id"])
        # Both tables are keyed on the input record's own fields, so results
        # always join their resolution (the service normalises its copies).
        root, name, rtype = record["root_domain"], record["name"], record["type"]
        resolutions.append((
            rid, root, name, rtype, out["analyzer"], rules_version, ip_version,
            len(out["results"]), [(f["code"], f.get("detail", "")) for f in out["findings"]],
            _date(record["first_seen"]), _date(record["last_seen"]), resolved_at,
        ))
        for r in out["results"]:
            results.append((
                rid, root, name, rtype, r["analyzer"], r["subject"], r["service_type"],
                r["provider_key"], r["provider_slug"], r["service_key"], r["rule_id"], float(r["confidence"]),
                int(bool(r["fallback"])), _date(r["valid_from"]), _date(r["valid_to"]), resolved_at,
            ))
    return resolutions, results


def _chunks(rows: Iterable, size: int) -> Iterator[list[dict]]:
    chunk: list[dict] = []
    for row in rows:
        chunk.append(record_dict(row))
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def read_watermark(reader, bucket: int):
    """The bucket's newest load time. Params are passed (even empty) so the
    driver renders the SQL's %% as %."""
    return reader.execute(sql.watermark_sql(RESOLVED_DATABASE, bucket), {})[0][0]


def selection_since(previous: dict | None, versions: dict) -> str | None:
    """The watermark an incremental run may use, or None for a full selection:
    only when the last successful run recorded one under the same knowledge
    versions (a version change can make any record stale)."""
    if not previous or not previous.get("watermark"):
        return None
    if previous.get("rules_version") != versions["rules_version"] or previous.get("ip_version") != versions["ip_version"]:
        return None
    return previous["watermark"]


def resolve_partition(reader, writer, service, bucket: int, log, *, chunk_size: int = CHUNK_SIZE, workers: int = WORKERS,
                      versions: dict | None = None, since: str | None = None) -> dict:
    """Stream the bucket's candidates through the service and store the
    answers. Results are inserted before resolutions, so a crash never marks a
    record resolved without its results. With since, only domains loaded after
    that watermark are considered."""
    versions = versions or service.knowledge()
    params = {"rules_version": versions["rules_version"], "ip_version": versions["ip_version"]}
    if since:
        params["since"] = since
    query = sql.candidates_sql(RESOLVED_DATABASE, bucket, incremental=since is not None)
    rows = reader.execute_iter(query, params, settings=READ_SETTINGS)
    counts = {"records": 0, "results": 0, "findings": 0, "chunks": 0}
    insert_results = sql.insert_sql(RESOLVED_DATABASE, sql.SERVICES_TABLE, sql.SERVICE_COLUMNS)
    insert_resolutions = sql.insert_sql(RESOLVED_DATABASE, sql.RESOLUTIONS_TABLE, sql.RESOLUTION_COLUMNS)

    def store(chunk: list[dict], answer) -> None:
        lines, rules_version, ip_version = answer
        resolved_at = datetime.now(UTC).replace(tzinfo=None)
        resolutions, results = rows_for(chunk, lines, rules_version, ip_version, resolved_at)
        if results:
            writer.execute(insert_results, results)
        writer.execute(insert_resolutions, resolutions)
        counts["records"] += len(resolutions)
        counts["results"] += len(results)
        counts["findings"] += sum(len(r[8]) for r in resolutions)
        counts["chunks"] += 1
        log.info("bucket %d: %d records resolved (%d results)", bucket, counts["records"], counts["results"])

    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending: deque = deque()
        for chunk in _chunks(rows, chunk_size):
            pending.append((chunk, pool.submit(service.resolve, chunk)))
            while len(pending) >= workers:
                chunk_done, future = pending.popleft()
                store(chunk_done, future.result())
        while pending:
            chunk_done, future = pending.popleft()
            store(chunk_done, future.result())
    return counts


@dg.asset(
    name="dns_record_services_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "go"},
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="dns_detect",
    description=(
        "DNS records resolved by the dns-detect service into services and evidence: "
        "corpscout.dns_record_resolutions and corpscout.dns_record_services (migration 000468), "
        "read through domain_services_history / domain_services_now. 128 hash partitions; each run "
        "resolves only records without a current resolution (new, older rules, older IP ranges for "
        "A/AAAA/SPF, or a grown window)."
    ),
)
def dns_record_services_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
    dns_detect: DnsDetectResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=RESOLVED_DATABASE, tables=REQUIRED_TABLES)
    bucket = sql.partition_bucket(context.partition_key)
    started = datetime.now(UTC)
    versions = dns_detect.knowledge()
    since = selection_since(_previous_metadata(context), versions)
    with clickhouse.get_connection() as reader, clickhouse.get_connection() as writer:
        # Snapshot the watermark before selecting: anything loaded later is
        # newer than it and is picked up by the next run.
        latest = read_watermark(reader, bucket)
        context.log.info("bucket %d: %s selection%s", bucket, "incremental" if since else "full", f" since {since}" if since else "")
        counts = resolve_partition(reader, writer, dns_detect, bucket, context.log, versions=versions, since=since)
    seconds = (datetime.now(UTC) - started).total_seconds()
    return dg.MaterializeResult(metadata={
        **counts, "seconds": round(seconds, 1),
        "records_per_second": round(counts["records"] / seconds) if seconds else 0,
        "mode": "incremental" if since else "full",
        "watermark": latest.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] if latest else "",
        "rules_version": versions["rules_version"], "ip_version": versions["ip_version"],
    })


def _previous_metadata(context: AssetExecutionContext) -> dict | None:
    """Metadata of this partition's latest successful materialization."""
    records = context.instance.fetch_materializations(
        dg.AssetRecordsFilter(asset_key=context.asset_key, asset_partitions=[context.partition_key]), limit=1
    ).records
    if not records or records[0].asset_materialization is None:
        return None
    return {k: getattr(v, "value", v) for k, v in records[0].asset_materialization.metadata.items()}


dns_record_services_job = dg.define_asset_job(name=JOB_NAME, selection=dg.AssetSelection.assets(dns_record_services_clickhouse))

_ACTIVE = [dg.DagsterRunStatus.QUEUED, dg.DagsterRunStatus.NOT_STARTED, dg.DagsterRunStatus.STARTING, dg.DagsterRunStatus.STARTED]


def all_partitions(context, refresh_key: str) -> list[dg.RunRequest] | dg.SkipReason:
    """One run per partition, unless a previous refresh is still running."""
    if context.instance.get_run_records(filters=dg.RunsFilter(job_name=JOB_NAME, statuses=_ACTIVE), limit=1):
        return dg.SkipReason(f"{JOB_NAME} still has queued or running partitions")
    return [dg.RunRequest(run_key=f"{refresh_key}-{key}", partition_key=key, tags={"dns_detect/refresh": refresh_key})
            for key in PARTITIONS.get_partition_keys()]


@dg.schedule(job=dns_record_services_job, cron_schedule="20 5 * * *", execution_timezone="UTC",
             default_status=dg.DefaultScheduleStatus.STOPPED)
def dns_record_services_daily(context: dg.ScheduleEvaluationContext):
    """New DNS scans: every partition, which resolves only what is new."""
    return all_partitions(context, f"daily-{context.scheduled_execution_time:%Y%m%d}")


@dg.sensor(job=dns_record_services_job, minimum_interval_seconds=600, default_status=dg.DefaultSensorStatus.STOPPED)
def dns_detect_knowledge_sensor(context: dg.SensorEvaluationContext, dns_detect: DnsDetectResource):
    """A new rules or IP version: every partition (an IP-only change re-resolves only A/AAAA/SPF)."""
    k = dns_detect.knowledge()
    cursor = json.dumps({"rules_version": k["rules_version"], "ip_version": k["ip_version"]})
    if not context.cursor:
        return dg.SensorResult(skip_reason=dg.SkipReason("recorded the current knowledge versions as the baseline"), cursor=cursor)
    if context.cursor == cursor:
        return dg.SkipReason("knowledge versions unchanged")
    key = "knowledge-" + k["rules_version"][-12:] + "-" + k["ip_version"][-12:]
    result = all_partitions(context, key)
    if isinstance(result, dg.SkipReason):
        return result
    return dg.SensorResult(run_requests=result, cursor=cursor)


defs = dg.Definitions(
    assets=[dns_record_services_clickhouse],
    jobs=[dns_record_services_job],
    schedules=[dns_record_services_daily],
    sensors=[dns_detect_knowledge_sensor],
    resources={"dns_detect": DnsDetectResource(api_url=os.environ.get("DNS_DETECT_API_URL", DEFAULT_API_URL))},
)
