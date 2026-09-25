"""Process a frozen input task into durable GeoIP and RDAP enrichment history."""

import json
from contextlib import closing
from datetime import UTC, datetime
from uuid import UUID, uuid5

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.commoncrawl_geoip.resources import MaxMindDatabaseResource
from dagster_v3.defs.commoncrawl_rdap.assets import (
    BEST_KNOWN_SEMANTICS_WARNING,
    RDAP_USER_AGENT,
)
from dagster_v3.defs.ip_enrichment.enrichment import (
    IpEnrichmentResultsConfig,
    RdapClient,
    RdapEnricher,
    RequestBudgetReached,
    geoip_result,
    maxminddb,
)
from dagster_v3.defs.ip_enrichment.input import INPUT_RELATION, PROCESSOR_VERSION

RESULT_TABLE = "corpscout.ip_enrichment_results"
EXECUTION_TAG = "ip_enrichment/execution"
LOOKUP_STATUSES = ("city_lookup_status", "asn_lookup_status", "rdap_lookup_status")
ERROR_STATUSES = {"retryable_error", "terminal_error"}


def prepare_execution(context, config, task):
    execution_id = config.execution_id or context.run.root_run_id or context.run.run_id
    original = context.instance.get_run_by_id(execution_id)
    if original is None:
        raise ValueError("execution_id must identify the original results Dagster run")
    work_config = {
        name: getattr(config, name)
        for name in (
            "task_id",
            "force_rdap",
            "rdap_cache_days",
            "parent_depth",
            "rate_limit_retry_seconds",
            "transient_retry_seconds",
        )
    }
    if EXECUTION_TAG in original.tags:
        execution = json.loads(original.tags[EXECUTION_TAG])
        if (
            execution["config"] != work_config
            or execution["processor_version"] != PROCESSOR_VERSION
        ):
            raise ValueError(
                "resume must keep the input task and lookup configuration unchanged"
            )
        if execution["source_info"] != task["source_info"]:
            raise ValueError("the input selection differs from the original execution")
    else:
        if config.execution_id is not None:
            raise ValueError("the original run has no saved IP enrichment execution")
        execution = {
            "execution_id": execution_id,
            "config": work_config,
            "source_info": task["source_info"],
            "processor_version": PROCESSOR_VERSION,
        }
    tags = {EXECUTION_TAG: json.dumps(execution), "processing/task_id": config.task_id}
    context.instance.add_run_tags(execution_id, tags)
    context.instance.add_run_tags(context.run.run_id, tags)
    return execution


def page_outcomes(client, rows, task_id, execution_id):
    history = client.execute(
        f"""SELECT input_id, attempt, toString(execution_id),
        city_lookup_status, asn_lookup_status, rdap_lookup_status
        FROM {RESULT_TABLE} FINAL
        PREWHERE (bucket, ip) IN %(keys)s
        WHERE task_id=%(task_id)s AND input_id IN %(ids)s""",
        {
            "keys": tuple({(row["bucket"], row["ip"]) for row in rows}),
            "task_id": task_id,
            "ids": tuple(row["input_id"] for row in rows),
        },
    )
    attempts, completed = {}, {}
    for input_id, attempt, stored_execution, *statuses in history:
        attempts[input_id] = max(attempts.get(input_id, 0), attempt)
        if stored_execution == execution_id:
            completed[input_id] = any(status in ERROR_STATUSES for status in statuses)
    return attempts, completed


def insert_result(client, record):
    client.execute(
        f"INSERT INTO {RESULT_TABLE} ({','.join(record)}) VALUES",
        [tuple(record.values())],
        settings={"async_insert": 1, "wait_for_async_insert": 1},
    )


@dg.asset(
    deps=["ip_enrichment_input"],
    group_name="ip_enrichment",
    kinds={"python", "clickhouse", "maxmind", "rdap"},
    # Share the RDAP pool with the legacy crawler so both respect its concurrency limit.
    pool="commoncrawl_rdap",
    metadata={"dagster/table_name": RESULT_TABLE},
    description="Enrich one prepared task with GeoIP, ASN and RDAP range/CIDR information. "
    "Writes outcomes to ip_enrichment_results and reusable RDAP network storage. "
    "Resume a partial run with execution_id. Completed outcomes are never repeated within that execution.",
)
def ip_enrichment_results(
    context: dg.AssetExecutionContext,
    config: IpEnrichmentResultsConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    maxmind_geoip: MaxMindDatabaseResource,
) -> dg.MaterializeResult:
    with processing.get_store() as store, store.selection_lock(config.task_id):
        task = store.task(config.task_id)
        if (
            task is None
            or task["processor"] != PROCESSOR_VERSION
            or task["status"] not in {"selected", "ready"}
        ):
            raise ValueError("materialize ip_enrichment_input first with this task_id")
        info = task["source_info"]
        if (
            info["relation"] != INPUT_RELATION
            or info.get("selection_task_id") != config.task_id
        ):
            raise ValueError(
                "task does not reference the IP enrichment input selection"
            )
        queue = ClickHouseInputQueue(
            clickhouse, INPUT_RELATION, selection_task_id=config.task_id
        )
        inspected = queue.inspect()
        if any(inspected[key] != info[key] for key in inspected):
            raise ValueError("input selection changed since it was prepared")
        execution = prepare_execution(context, config, task)
        execution_id = execution["execution_id"]
        context.add_output_metadata(
            {"task_id": config.task_id, "execution_id": execution_id}
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
        counts = {
            "total": info["total"],
            "written": 0,
            "resumed": 0,
            "failed": 0,
            "rdap_requests": 0,
            "rdap_cache_hits": 0,
            "request_limit_reached": False,
            "parent_lookup_failures": 0,
            "registry_level_responses": 0,
        }
        if info["total"]:
            city_path, asn_path = maxmind_geoip.database_paths()
            with (
                clickhouse.get_connection() as client,
                maxminddb.open_database(city_path) as city_reader,
                maxminddb.open_database(asn_path) as asn_reader,
                closing(RdapClient(user_agent=RDAP_USER_AGENT)) as rdap_client,
            ):
                for kind, reader in (("City", city_reader), ("ASN", asn_reader)):
                    if kind not in reader.metadata().database_type:
                        raise ValueError(f"expected a MaxMind {kind} database")
                client.execute("SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie")
                rdap = RdapEnricher(client, rdap_client, config, context.log)
                after = None
                while rows := queue.read(info, after=after, limit=config.batch_size):
                    # ClickHouse SELECT * omits MATERIALIZED columns in the shared queue reader.
                    buckets = dict(
                        client.execute(
                            "SELECT ip, toUInt16(modulo(cityHash64(ip), 256)) FROM "
                            "(SELECT arrayJoin(%(ips)s) AS ip)",
                            {"ips": list({row["ip"] for row in rows})},
                        )
                    )
                    for row in rows:
                        row["bucket"] = buckets[row["ip"]]
                    attempts, completed = page_outcomes(
                        client, rows, config.task_id, execution_id
                    )
                    for row in rows:
                        input_id = row["input_id"]
                        if input_id in completed:
                            counts["resumed"] += 1
                            counts["failed"] += int(completed[input_id])
                            continue
                        geoip = geoip_result(
                            row["ip"],
                            city_reader,
                            asn_reader,
                            checked_at=datetime.now(UTC),
                            retry_seconds=config.transient_retry_seconds,
                        )
                        try:
                            registration = rdap.lookup(row["ip"], row["bucket"])
                        except RequestBudgetReached:
                            counts["request_limit_reached"] = True
                            break
                        record = {
                            "ip": row["ip"],
                            "result_id": str(uuid5(UUID(execution_id), input_id)),
                            "task_id": config.task_id,
                            "execution_id": execution_id,
                            "input_id": input_id,
                            "source_run_id": context.run.run_id,
                            "processor_version": PROCESSOR_VERSION,
                            "attempt": attempts.get(input_id, 0) + 1,
                            "completed_at": datetime.now(UTC),
                            **geoip,
                            **registration,
                        }
                        insert_result(client, record)
                        counts["written"] += 1
                        counts["failed"] += int(
                            any(
                                record[key] in ERROR_STATUSES for key in LOOKUP_STATUSES
                            )
                        )
                    context.log.info(
                        "IP enrichment execution=%s processed=%s/%s failed=%s rdap_requests=%s",
                        execution_id,
                        counts["written"] + counts["resumed"],
                        info["total"],
                        counts["failed"],
                        rdap.requests,
                    )
                    if counts["request_limit_reached"]:
                        break
                    after = rows[-1]["input_id"]
                if rdap.networks_written:
                    client.execute(
                        "SYSTEM RELOAD DICTIONARY corpscout.rdap_network_trie"
                    )
                counts.update(
                    rdap_requests=rdap.requests,
                    rdap_cache_hits=rdap.cache_hits,
                    parent_lookup_failures=rdap.parent_failures,
                    registry_level_responses=rdap.registry_level_responses,
                )
        counts["remaining"] = info["total"] - counts["written"] - counts["resumed"]
        metadata = {
            "task_id": config.task_id,
            "execution_id": execution_id,
            "results_table": RESULT_TABLE,
            "coverage_semantics": BEST_KNOWN_SEMANTICS_WARNING,
            **counts,
        }
        if counts["remaining"] or counts["failed"]:
            raise dg.Failure(
                "Results saved. Resume unfinished inputs with this execution_id. "
                "For saved lookup errors, start a new execution after retry_after (or force_rdap=true).",
                metadata=metadata,
                allow_retries=False,
            )
        return dg.MaterializeResult(metadata=metadata)


ip_enrichment_results_job = dg.define_asset_job(
    "ip_enrichment_results_job",
    selection=dg.AssetSelection.assets(ip_enrichment_results),
)
ip_enrichment_workflow = dg.define_asset_job(
    "ip_enrichment_workflow",
    selection=dg.AssetSelection.assets(ip_enrichment_results).upstream(),
    description="Freeze an IP selection, then enrich it with GeoIP, ASN and RDAP.",
)
defs = dg.Definitions(
    assets=[ip_enrichment_results],
    jobs=[ip_enrichment_results_job, ip_enrichment_workflow],
)
