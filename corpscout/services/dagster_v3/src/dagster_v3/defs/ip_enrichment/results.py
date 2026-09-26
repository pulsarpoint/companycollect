"""Process a frozen IP enrichment draft: pages of remaining entries until nothing remains.

Remaining work is a live ClickHouse query per 256-way bucket (entries without a result
of this execution; each query anti-joins one primary-key range of the results table),
every page costs a fixed number of ClickHouse round trips whatever its size, outcomes
are stored in acknowledged micro-batches, and completion counts come from the results
table. Nothing about a page is persisted, so a resume is the same loop again. Entries
a registry budget, a registry pause or a failed bootstrap deferred stay remaining; when
a whole pass resolved nothing else the run waits for the earliest window, then walks
again.
"""

import json
from collections.abc import Callable, Iterator
from contextlib import closing
from datetime import UTC, datetime
from time import monotonic
from uuid import UUID, uuid5

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common import queue_execution
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.common.result_buffer import ResultBuffer
from dagster_v3.defs.commoncrawl_geoip.freshness import MAX_AGE, freshness
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_rdap.apnic_whois import ApnicWhoisClient
from dagster_v3.defs.commoncrawl_rdap.assets import (
    BEST_KNOWN_SEMANTICS_WARNING,
    RDAP_USER_AGENT,
)
from dagster_v3.defs.commoncrawl_rdap.ripe_rest import RipeRestClient
from dagster_v3.defs.ip_enrichment.enrichment import (
    IpEnrichmentResultsConfig,
    RdapClient,
    RdapEnricher,
    geoip_result,
    maxminddb,
)
from dagster_v3.defs.ip_enrichment.input import (
    ERROR_STATUSES,
    INPUT_RELATION,
    PROCESSOR_VERSION,
    RESULT_RELATION,
    bucket_prefix,
)

LOOKUP_STATUSES = ("city_lookup_status", "asn_lookup_status", "rdap_lookup_status")
# Page size, request budgets and pacing are transport: they may change between resumes.
TRANSPORT_SETTINGS = (
    "batch_size",
    "max_requests",
    "request_delay_seconds",
    "registry_daily_budgets",
)
NOT_FROZEN = {"task_id", "execution_id", *TRANSPORT_SETTINGS}
FAILED_SQL = " OR ".join(f"{column} IN %(errors)s" for column in LOOKUP_STATUSES)
RESULT_BATCH = 500
# Seconds between progress lines: a 48.6M-address run has ~194k pages.
PROGRESS_SECONDS = 60.0
# APNIC's NIRs answer under their own name, but the request was sent to (and budgeted
# as) APNIC.
APNIC_NIRS = ("jpnic", "krnic", "twnic", "idnic", "cnnic", "irinn", "vnnic")
# Requests any writer made in the last day, per registry, oldest first: the per-registry
# budget window survives a resume and counts the legacy bucket worker too. Every stored
# network (direct, fallback or parent) is one request. Requests that stored no network
# (errors, not_found, the redirected half of a reroute) are not counted: the lookup
# markers record no registry.
REGISTRY_USAGE_SQL = f"""SELECT if(rir IN {APNIC_NIRS!r}, 'apnic', rir),
        toFloat64(dateDiff('second', fetched_at, now64(6)))
    FROM corpscout.rdap_networks
    WHERE fetched_at >= now64(6) - INTERVAL 1 DAY
    ORDER BY fetched_at"""


def execution_parameters(task: dict) -> dict:
    execution = task["config"]["execution"]
    return {
        "task": str(task["task_id"]),
        "exec": execution["execution_id"],
        "errors": ERROR_STATUSES,
    }


def start_ip_execution(
    store,
    client,
    config: IpEnrichmentResultsConfig,
    run_id: str,
    *,
    default_execution_id=None,
) -> dict:
    """Freeze the draft into an execution, or return the saved one to resume."""

    def snapshot() -> tuple[dict, int]:
        task = store.task(config.task_id)
        [(total,)] = client.execute(
            f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
            {"task": config.task_id},
        )
        if total == 0 or total != task["total"]:
            raise ValueError("Queue is empty or its membership changed")
        return {"relation": INPUT_RELATION, "total": total}, total

    return queue_execution.start_execution(
        store,
        task_id=config.task_id,
        processor=PROCESSOR_VERSION,
        profile={
            **config.model_dump(exclude=NOT_FROZEN),
            "processor_version": PROCESSOR_VERSION,
        },
        execution_id=config.execution_id,
        freshness_days=config.rdap_cache_days,
        run_id=run_id,
        snapshot=snapshot,
        transport_keys=TRANSPORT_SETTINGS,
        default_execution_id=default_execution_id,
        label="IP enrichment",
    )


def task_buckets(client, task: dict) -> list[int]:
    return [
        bucket
        for (bucket,) in client.execute(
            f"SELECT DISTINCT bucket FROM {INPUT_RELATION} WHERE task_id=%(task)s ORDER BY bucket",
            {"task": str(task["task_id"])},
        )
    ]


def _bucket_parameters(task: dict, bucket: int) -> dict:
    return {
        **execution_parameters(task),
        "bucket": bucket,
        "after": bucket_prefix(bucket),
        "stop": bucket_prefix(bucket + 1),
    }


def remaining_entries(
    client, task: dict, *, bucket: int, after: str | None = None, limit: int
) -> list[dict]:
    """Frozen entries of one bucket after ``after`` without a result of this execution."""
    parameters = {**_bucket_parameters(task, bucket), "limit": limit}
    if after is not None:
        parameters["after"] = after
    rows = client.execute(
        f"""SELECT input_id, ip, ip_version, bucket FROM {INPUT_RELATION}
        WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s
          AND input_id NOT IN (
              SELECT input_id FROM {RESULT_RELATION}
              WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s)
        ORDER BY input_id LIMIT %(limit)s""",
        parameters,
    )
    return [
        dict(zip(("input_id", "ip", "ip_version", "bucket"), row, strict=True))
        for row in rows
    ]


def remaining_pages(
    client, task: dict, buckets: list[int], *, size: int
) -> Iterator[list[dict]]:
    """Pages of up to ``size`` remaining entries in (bucket, input_id) order.

    Every read is one bucket's range, but a page is filled from as many buckets as it
    takes, so a small task still resolves in full pages. The cursor only moves forward:
    a page is never read twice inside a pass, whether or not its results were flushed.
    """
    page: list[dict] = []
    for bucket in buckets:
        after = None
        while True:
            wanted = size - len(page)
            rows = remaining_entries(
                client, task, bucket=bucket, after=after, limit=wanted
            )
            page.extend(rows)
            if len(page) >= size:
                yield page
                page = []
            if len(rows) < wanted:
                break  # nothing further in this bucket
            after = rows[-1]["input_id"]
    if page:
        yield page


def count_outcomes(client, task: dict, buckets: list[int]) -> tuple[int, int, int]:
    """(remaining, succeeded, failed) over the task; every query is one primary-key range."""
    remaining = succeeded = failed = 0
    for bucket in buckets:
        parameters = _bucket_parameters(task, bucket)
        [(left,)] = client.execute(
            f"""SELECT count() FROM {INPUT_RELATION}
            WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s
              AND input_id NOT IN (
                  SELECT input_id FROM {RESULT_RELATION}
                  WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s)""",
            parameters,
        )
        [(ok, bad)] = client.execute(
            f"""SELECT countIf(NOT failed), countIf(failed) FROM (
                SELECT input_id, argMax({FAILED_SQL}, tuple(completed_at, result_id)) AS failed
                FROM {RESULT_RELATION}
                WHERE bucket = %(bucket)s AND task_id = %(task)s AND execution_id = %(exec)s
                  AND input_id IN (
                      SELECT input_id FROM {INPUT_RELATION}
                      WHERE task_id = %(task)s AND input_id > %(after)s AND input_id < %(stop)s)
                GROUP BY input_id)""",
            parameters,
        )
        remaining += left
        succeeded += ok
        failed += bad
    return remaining, succeeded, failed


def store_results(client, records: list[dict]) -> None:
    columns = list(records[0])
    client.execute(
        f"INSERT INTO {RESULT_RELATION} ({','.join(columns)}) VALUES",
        [tuple(record[column] for column in columns) for record in records],
        settings={"async_insert": 1, "wait_for_async_insert": 1},
    )


def run_ip_enrichment(
    context,
    client,
    task: dict,
    config: IpEnrichmentResultsConfig,
    *,
    enricher,
    city_reader,
    asn_reader,
    clock: Callable[[], float] | None = None,
) -> dict:
    """Walk the task bucket by bucket until a whole pass finds nothing remaining.

    A page's outcomes enter the buffer together and the cursor moves past the page, so
    a page is never read twice inside a pass; the buffer is flushed at the end of every
    pass and a further pass confirms that nothing remains. A reached max_requests
    budget flushes what was resolved and stops; the rest stays remaining for the
    resume. A pass that resolved nothing while entries were deferred (a registry at its
    budget or paused, or the bootstrap paused) waits for the earliest window and walks
    again. Whatever is buffered when the loop fails is still stored. Progress is logged
    at most once a minute (``clock`` is injectable) and at the end of every pass.
    """
    execution = task["config"]["execution"]
    execution_uuid = UUID(execution["execution_id"])
    counts = {
        "written": 0,
        "pages": 0,
        "request_limit_reached": False,
        "budget_waits": 0,
        "budget_wait_seconds": 0.0,
    }

    def flush(records: list[dict]) -> None:
        store_results(client, records)
        counts["written"] += len(records)

    buffer: ResultBuffer[dict] = ResultBuffer(
        flush, max_items=RESULT_BATCH, max_seconds=5.0
    )
    buckets = task_buckets(client, task)
    clock = clock or monotonic
    last_log = None

    def progress(rows: list[dict], *, force: bool = False) -> None:
        """At most one progress line a minute (plus one per pass), with running totals."""
        nonlocal last_log
        now = clock()
        if not force and last_log is not None and now - last_log < PROGRESS_SECONDS:
            return
        last_log = now
        context.log.info(
            "IP enrichment execution=%s pages=%s written=%s buffered=%s bucket=%s "
            "rdap_requests=%s cache_hits=%s deferred=%s",
            execution["execution_id"],
            counts["pages"],
            counts["written"],
            len(buffer),
            rows[-1]["bucket"] if rows else "-",
            enricher.requests,
            enricher.cache_hits,
            enricher.deferred,
        )

    try:
        while True:
            processed = 0
            enricher.reset_pass()
            rows: list[dict] = []
            for rows in remaining_pages(client, task, buckets, size=config.batch_size):
                rdap = enricher.resolve_page(rows)
                records = []
                for row in rows:
                    if row["ip"] not in rdap:
                        continue  # deferred, or the request budget ran out
                    checked_at = datetime.now(UTC)
                    records.append(
                        {
                            "ip": row["ip"],
                            "result_id": str(uuid5(execution_uuid, row["input_id"])),
                            "task_id": str(task["task_id"]),
                            "execution_id": execution["execution_id"],
                            "input_id": row["input_id"],
                            "source_run_id": context.run.run_id,
                            "processor_version": PROCESSOR_VERSION,
                            "attempt": 1,
                            "completed_at": checked_at,
                            **geoip_result(
                                row["ip"],
                                city_reader,
                                asn_reader,
                                checked_at=checked_at,
                                retry_seconds=config.transient_retry_seconds,
                            ),
                            **rdap[row["ip"]],
                        }
                    )
                buffer.add(records)
                processed += len(records)
                counts["pages"] += 1
                progress(rows)
                if enricher.budget_reached:
                    buffer.flush()
                    counts["request_limit_reached"] = True
                    progress(rows, force=True)
                    return counts
            buffer.flush()  # an unacknowledged batch fails the run; the resume re-reads its rows
            progress(rows, force=True)
            if processed == 0:
                if not enricher.deferred:
                    return counts
                context.log.warning(
                    "Nothing else remains; deferred %s. Waiting %.0f s before the next pass",
                    enricher.deferred,
                    enricher.seconds_until_budget_frees(),
                )
                counts["budget_waits"] += 1
                counts["budget_wait_seconds"] += enricher.wait_for_registry_budget()
    except BaseException:
        # Keep what was resolved before the failure; a flush error must not mask it.
        try:
            buffer.flush()
        except Exception as error:  # the original exception is re-raised below
            context.log.warning(
                "Could not store %s buffered results after a failure: %r",
                len(buffer),
                error,
            )
        raise


def finish_ip_execution(store, client, task: dict) -> dict:
    """Counts come from this execution's results; skipped is the rest of the total (expected 0)."""
    remaining, succeeded, failed = count_outcomes(
        client, task, task_buckets(client, task)
    )
    return queue_execution.record_completion(
        store,
        task_id=str(task["task_id"]),
        remaining=remaining,
        succeeded=succeeded,
        failed=failed,
    )


@dg.asset(
    deps=["ip_enrichment_input"],
    group_name="ip_enrichment",
    kinds={"python", "clickhouse", "maxmind", "rdap"},
    # Share the RDAP pool with the legacy crawler so both respect its concurrency limit.
    pool="commoncrawl_rdap",
    metadata={"dagster/table_name": RESULT_RELATION},
    description="Freeze an IP enrichment draft, then enrich the remaining addresses page by page "
    "with GeoIP, ASN and RDAP; outcomes are stored in acknowledged batches. Re-run the task to "
    "resume its saved execution. Completed tasks drop their input partition; retry failed "
    "addresses in a new draft (retry_failed_task_id).",
)
def ip_enrichment_results(
    context: dg.AssetExecutionContext,
    config: IpEnrichmentResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    maxmind_geoip: MaxMindDatabaseResource,
) -> dg.MaterializeResult:
    def complete(store, task) -> dict:
        return queue_execution.complete_task(
            context,
            store,
            clickhouse,
            task,
            processor=PROCESSOR_VERSION,
            relation=INPUT_RELATION,
            tag_prefix="ip_enrichment",
            label="IP enrichment",
        )

    with (
        processing.get_store() as store,
        store.selection_lock(config.task_id),
        clickhouse.get_connection() as client,
    ):
        task = store.task(config.task_id)
        if (
            task is None
            or task["processor"] != PROCESSOR_VERSION
            or task["queue_scope"] is None
        ):
            raise ValueError(
                "task_id must name an IP enrichment draft; add addresses with ip_enrichment_input first"
            )
        task = start_ip_execution(
            store,
            client,
            config,
            context.run.run_id,
            default_execution_id=context.run.root_run_id or context.run.run_id,
        )
        execution = task["config"]["execution"]
        context.instance.add_run_tags(
            context.run.run_id,
            {
                "processing/task_id": config.task_id,
                "ip_enrichment/execution_id": execution["execution_id"],
                "ip_enrichment/execution": json.dumps(execution, sort_keys=True),
            },
        )
        if task["status"] == "completed":
            return dg.MaterializeResult(
                metadata={
                    "task_id": config.task_id,
                    "execution_id": execution["execution_id"],
                    "already_completed": True,
                    **complete(store, task),
                }
            )
        assert_clickhouse_tables_exist(
            clickhouse,
            database="corpscout",
            tables=(
                "ip_enrichment_results",
                "ip_enrichment_current",
                "rdap_networks",
                "rdap_networks_current",
                "rdap_network_segments",
                "rdap_network_segments_current",
                "rdap_ip_lookup_results_current",
                "rdap_network_registry_class",
                "rdap_network_registry_class_current",
                "ip_registry_ready",
            ),
        )
        city_path, asn_path = maxmind_geoip.database_paths()
        with (
            maxminddb.open_database(city_path) as city_reader,
            maxminddb.open_database(asn_path) as asn_reader,
            closing(RdapClient(user_agent=RDAP_USER_AGENT)) as rdap_client,
            closing(RipeRestClient(user_agent=RDAP_USER_AGENT)) as ripe_client,
            closing(ApnicWhoisClient()) as apnic_client,
        ):
            builds = {}
            for kind, reader in (("City", city_reader), ("ASN", asn_reader)):
                if kind not in reader.metadata().database_type:
                    raise ValueError(f"expected a MaxMind {kind} database")
                builds[f"GeoLite2-{kind}"] = datetime.fromtimestamp(
                    reader.metadata().build_epoch, UTC
                )
            geolite2 = freshness(builds, datetime.now(UTC))
            if not geolite2.passed:
                context.log.warning(
                    "%s Older than %s days; replace the files by hand "
                    "(docs/operations/ip-enrichment-draft-queue.md).",
                    geolite2.description,
                    MAX_AGE.days,
                )
            client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
            enricher = RdapEnricher(
                client,
                rdap_client,
                ripe_client,
                apnic_client,
                config,
                context.log,
                started_at=datetime.fromisoformat(execution["started_at"]),
                cache_cutoff=datetime.fromisoformat(execution["freshness_cutoff"]),
            )
            enricher.seed_registry_usage(client.execute(REGISTRY_USAGE_SQL))
            counts = run_ip_enrichment(
                context,
                client,
                task,
                config,
                enricher=enricher,
                city_reader=city_reader,
                asn_reader=asn_reader,
            )
            if enricher.networks_written:
                client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
        metadata = {
            "task_id": config.task_id,
            "execution_id": execution["execution_id"],
            "results_table": RESULT_RELATION,
            "coverage_semantics": BEST_KNOWN_SEMANTICS_WARNING,
            "geolite2_city_build": builds["GeoLite2-City"].date().isoformat(),
            "geolite2_asn_build": builds["GeoLite2-ASN"].date().isoformat(),
            "rdap_requests": enricher.requests,
            "rdap_cache_hits": enricher.cache_hits,
            "parent_lookup_failures": enricher.parent_failures,
            "registry_level_responses": enricher.registry_level_responses,
            "rdap_requests_by_registry": enricher.requests_by_registry,
            "rdap_person_entities_by_registry": enricher.person_entities_by_registry,
            "rdap_deferrals_by_registry": enricher.deferrals_by_registry,
            "rdap_fallbacks_by_registry": enricher.rdap_fallbacks_by_registry,
            "reroutes_by_registry": enricher.reroutes_by_registry,
            "pauses_by_registry": enricher.pauses_by_registry,
            **counts,
        }
        if counts["request_limit_reached"]:
            raise dg.Failure(
                "RDAP request budget reached; results saved. Re-run this task to resume "
                "the saved execution.",
                metadata=metadata,
                allow_retries=False,
            )
        task = finish_ip_execution(store, client, task)
        return dg.MaterializeResult(metadata={**metadata, **complete(store, task)})


# dagster.yaml retries failed runs twice, and dg.Failure(allow_retries=False) only bypasses
# op retry policies: a run stopped by max_requests (or a crash) would be relaunched and
# resume the same execution, spending up to 3x max_requests. Resuming is the operator's
# re-run. `tags` (not `run_tags`) so the backoffice's GraphQL launch, which merges the
# job's definition tags, carries it too.
ip_enrichment_results_job = dg.define_asset_job(
    "ip_enrichment_results_job",
    selection=dg.AssetSelection.assets(ip_enrichment_results),
    tags={"dagster/max_retries": "0"},
)
defs = dg.Definitions(assets=[ip_enrichment_results], jobs=[ip_enrichment_results_job])
