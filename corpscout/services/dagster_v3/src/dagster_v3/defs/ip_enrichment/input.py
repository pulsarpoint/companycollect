"""Append IP selections to the open IP enrichment draft (shared queue contract).

A submission selects addresses from an explicit list, a filtered ClickHouse relation
or the failed results of an earlier task, normalizes them in ClickHouse and appends
the ones the draft does not hold yet. Nothing is frozen here; the results asset
freezes the draft when it starts.
"""

import hashlib
import json
import re
from ipaddress import IPv6Address, ip_address
from typing import Self
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.clickhouse_queue import validate_relation
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore

INPUT_RELATION = "corpscout.ip_enrichment_input"
RESULT_RELATION = "corpscout.ip_enrichment_results"
PROCESSOR_VERSION = "ip-enrichment-v1"
ERROR_STATUSES = ("retryable_error", "terminal_error")
# The entry identity, computed where the entries live: the address's 256-way bucket first,
# so a task is walked bucket by bucket, then the JSON tuple of source, record and IP.
# Migration 000456 enforces the same expression in its valid_identity CHECK.
INPUT_ID_SQL = (
    "concat(leftPad(toString(toUInt16(cityHash64({ip}) % 256)), 3, '0'), ':', "
    "toJSONString(tuple({source}, {record}, {ip})))"
)


def bucket_prefix(bucket: int) -> str:
    """Every input_id of ``bucket`` starts with this; ``bucket_prefix(bucket + 1)`` ends the range."""
    return f"{bucket:03d}:"


class IpEnrichmentInputConfig(dg.Config):
    task_id: str | None = Field(
        default=None,
        description="Open draft to append to. Omit to use the scope's open draft.",
    )
    submission_id: str | None = Field(
        default=None,
        description="Stable receipt UUID; retry a failed import with the same value.",
    )
    queue_scope: str = Field(default="workspace", min_length=1)
    ips: list[str] = Field(
        default_factory=list,
        description="Explicit IPv4/IPv6 addresses. Use one of ips, source_relation, retry_failed_task_id.",
    )
    source_relation: str | None = Field(
        default=None,
        description="Source ClickHouse table/view, as corpscout.table.",
    )
    retry_failed_task_id: str | None = Field(
        default=None,
        description="Queue the addresses whose result in this task has a City, ASN or RDAP error.",
    )
    ip_column: str = "ip"
    source_record_id_column: str | None = Field(
        default=None,
        description="Optional source record identity. Defaults to the canonical IP.",
    )
    observed_at_column: str | None = Field(
        default=None,
        description="Optional source observation timestamp column.",
    )
    filters: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Exact matches: OR within each value list, AND between columns.",
    )
    ip_search: str = Field(
        default="", description="Exact IP or literal IP prefix to select."
    )
    excluded_ips: list[str] = Field(default_factory=list)
    source_final: bool = False
    select_all: bool = False
    max_rows: int | None = Field(
        default=None,
        ge=1,
        description="Limit distinct source-record/IP submissions, sorted by IP and record ID.",
    )
    source_name: str | None = Field(
        default=None,
        description="Provenance label. Defaults to source_relation, retry:<task> or manual.",
    )
    source_run_id: str | None = Field(
        default=None,
        description="Original source run ID. Defaults to this Dagster run ID.",
    )

    @field_validator("task_id", "submission_id", "retry_failed_task_id")
    @classmethod
    def valid_uuid(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("queue_scope")
    @classmethod
    def valid_scope(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("queue_scope must not be blank")
        return value

    @field_validator("ips", "excluded_ips")
    @classmethod
    def canonical_ips(cls, values: list[str]) -> list[str]:
        normalized = set()
        for value in values:
            if "%" in value:
                raise ValueError("scoped IPv6 addresses are not supported")
            address = ip_address(value.strip())
            # ClickHouse renders IPv4-mapped IPv6 with dotted IPv4 notation.
            if isinstance(address, IPv6Address) and address.ipv4_mapped is not None:
                normalized.add(f"::ffff:{address.ipv4_mapped}")
            else:
                normalized.add(str(address))
        return sorted(normalized)

    @field_validator("ip_search")
    @classmethod
    def valid_search(cls, value: str) -> str:
        value = value.strip().lower()
        if value and re.fullmatch(r"[0-9a-f:.]{1,45}", value) is None:
            raise ValueError("ip_search must be an IP address or literal IP prefix")
        return value

    @field_validator("source_relation")
    @classmethod
    def valid_source(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = validate_relation(value)
        if value.split(".")[0] != "corpscout":
            raise ValueError("source_relation must be in the corpscout database")
        if value == INPUT_RELATION:
            raise ValueError("the source must differ from the input queue")
        return value

    @field_validator("source_name")
    @classmethod
    def valid_source_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value or "\0" in value:
            raise ValueError("source_name must be nonempty and contain no NUL")
        return value

    @model_validator(mode="after")
    def valid_selection(self) -> Self:
        modes = [
            bool(self.ips),
            self.source_relation is not None,
            self.retry_failed_task_id is not None,
        ]
        if sum(modes) != 1:
            raise ValueError(
                "provide exactly one of nonempty ips, source_relation or retry_failed_task_id"
            )
        for column in (
            self.ip_column,
            self.source_record_id_column,
            self.observed_at_column,
            *self.filters,
        ):
            if (
                column is not None
                and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) is None
            ):
                raise ValueError("column names must be simple SQL identifiers")
        if any(not values for values in self.filters.values()):
            raise ValueError("each filter needs at least one value")
        if self.source_relation is None:
            if (
                self.filters
                or self.ip_search
                or self.excluded_ips
                or self.source_final
                or self.select_all
                or self.ip_column != "ip"
                or self.source_record_id_column is not None
                or self.observed_at_column is not None
            ):
                raise ValueError("table selection options require source_relation")
        elif not (
            self.filters
            or self.ip_search
            or self.max_rows is not None
            or self.select_all
        ):
            raise ValueError("provide filters, max_rows, or explicit select_all=true")
        return self


def selected_ips_sql(config: IpEnrichmentInputConfig) -> tuple[str, dict]:
    """Normalize in ClickHouse and group submissions without fetching the source into Python."""
    params = {}
    where = ""
    if config.retry_failed_task_id is not None:
        failed = " OR ".join(
            f"{column} IN %(errors)s"
            for column in (
                "city_lookup_status",
                "asn_lookup_status",
                "rdap_lookup_status",
            )
        )
        relation = (
            f"(SELECT ip FROM {RESULT_RELATION} FINAL "
            f"WHERE task_id = %(failed_task)s AND ({failed})) AS source"
        )
        ip_value = "source.ip"
        params.update(failed_task=config.retry_failed_task_id, errors=ERROR_STATUSES)
    elif config.source_relation is None:
        relation = "(SELECT arrayJoin(%(ips)s) AS explicit_ip) AS source"
        ip_value = "source.explicit_ip"
        params["ips"] = config.ips
    else:
        relation = (
            config.source_relation
            + " AS source"
            + (" FINAL" if config.source_final else "")
        )
        ip_value = f"trimBoth(ifNull(toString(source.`{config.ip_column}`), ''))"
        predicates = []
        for index, (column, values) in enumerate(sorted(config.filters.items())):
            predicates.append(f"source.`{column}` IN %(filter_{index})s")
            params[f"filter_{index}"] = tuple(sorted(set(values)))
        if config.ip_search:
            try:
                address = ip_address(config.ip_search)
            except ValueError:
                predicates.extend(
                    [
                        f"source.`{config.ip_column}` >= %(ip_prefix)s",
                        f"source.`{config.ip_column}` < %(ip_prefix_end)s",
                    ]
                )
                params["ip_prefix"] = config.ip_search
                params["ip_prefix_end"] = config.ip_search[:-1] + chr(
                    ord(config.ip_search[-1]) + 1
                )
            else:
                canonical = (
                    f"::ffff:{address.ipv4_mapped}"
                    if isinstance(address, IPv6Address)
                    and address.ipv4_mapped is not None
                    else str(address)
                )
                predicates.append(f"{ip_value} = %(exact_ip)s")
                params["exact_ip"] = canonical
        where = " WHERE " + " AND ".join(predicates) if predicates else ""
    record_id = (
        f"trimBoth(ifNull(toString(source.`{config.source_record_id_column}`), ''))"
        if config.source_record_id_column is not None
        else "ifNull(normalized_ip, '')"
    )
    observed_at = (
        f"toDateTime64(source.`{config.observed_at_column}`, 6, 'UTC')"
        if config.observed_at_column is not None
        else "CAST(NULL AS Nullable(DateTime64(6, 'UTC')))"
    )
    excluded = ""
    if config.excluded_ips:
        excluded = " AND normalized_ip NOT IN %(excluded_ips)s"
        params["excluded_ips"] = tuple(config.excluded_ips)
    limit = ""
    if config.max_rows is not None:
        limit = " LIMIT %(limit)s"
        params["limit"] = config.max_rows
    return (
        f"""SELECT assumeNotNull(normalized_ip) AS ip,
        record_ref AS source_record_id, max(source_observed_at) AS observed_at
    FROM (
        SELECT coalesce(toString(toIPv4OrNull({ip_value})),
                        toString(toIPv6OrNull({ip_value}))) AS normalized_ip,
            {record_id} AS record_ref, {observed_at} AS source_observed_at
        FROM {relation}{where}
    )
    WHERE normalized_ip IS NOT NULL{excluded}
    GROUP BY normalized_ip, record_ref
    ORDER BY ip, source_record_id{limit}""",
        params,
    )


def source_label(config: IpEnrichmentInputConfig) -> str:
    if config.source_name is not None:
        return config.source_name
    if config.source_relation is not None:
        return config.source_relation
    if config.retry_failed_task_id is not None:
        return "retry:" + config.retry_failed_task_id
    return "manual"


def load_ip_draft(
    config: IpEnrichmentInputConfig,
    submission_id: str,
    store: ProcessingStore,
    clickhouse: ClickhouseResource,
    *,
    run_id: str,
) -> dict:
    selection = config.model_dump(exclude={"task_id", "submission_id", "queue_scope"})
    selection["filters"] = {
        key: sorted(set(values)) for key, values in config.filters.items()
    }
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    # Keep bulk manual values out of PostgreSQL receipts.
    for key in ("ips", "excluded_ips"):
        selection[key] = {
            "count": len(selection[key]),
            "sha256": hashlib.sha256(json.dumps(selection[key]).encode()).hexdigest(),
        }
    source = source_label(config)
    receipt = draft_queue.submission(store, submission_id)
    task_id = str(receipt["task_id"]) if receipt else None
    if receipt:
        task = store.task(task_id)
        if (
            task["processor"] != PROCESSOR_VERSION
            or task["queue_scope"] != config.queue_scope
            or (config.task_id is not None and config.task_id != task_id)
            or receipt["selection_fingerprint"] != fingerprint
        ):
            raise ValueError(
                "submission_id belongs to a different selection, task or scope"
            )
        if receipt["status"] == "completed":
            return {
                "task_id": task_id,
                "submission_id": submission_id,
                "input_count": receipt["input_count"],
                "total": task["total"],
            }
    while True:
        if receipt is None:
            task_id = draft_queue.find_draft(
                store,
                scope=config.queue_scope,
                processor=PROCESSOR_VERSION,
                task_id=config.task_id,
            )
        with store.selection_lock(task_id):
            latest = draft_queue.submission(store, submission_id)
            if latest is not None:
                if (
                    str(latest["task_id"]) != task_id
                    or latest["selection_fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "submission_id belongs to another task or selection"
                    )
                if latest["status"] == "completed":
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": latest["input_count"],
                        "total": store.task(task_id)["total"],
                    }
            if (
                store.task(task_id)["status"] != "draft"
                and receipt is None
                and config.task_id is None
            ):
                continue  # Start won the race: re-resolve to the next draft.
            receipt = draft_queue.prepare_submission(
                store,
                task_id=task_id,
                submission_id=submission_id,
                source=source,
                selection=selection,
                fingerprint=fingerprint,
            )
            if receipt["status"] == "completed":
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": receipt["input_count"],
                    "total": store.task(task_id)["total"],
                }
            try:
                with clickhouse.get_connection() as client:
                    query_id = "ip-queue-import:" + submission_id
                    client.execute(
                        "KILL QUERY WHERE query_id=%(id)s SYNC", {"id": query_id}
                    )
                    # A retry replaces only this submission's rows, from the current source.
                    client.execute(
                        f"DELETE FROM {INPUT_RELATION} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                        settings={"lightweight_deletes_sync": 2},
                    )
                    if config.source_relation is not None:
                        columns = {
                            row[0]
                            for row in client.execute(
                                f"DESCRIBE TABLE {config.source_relation}"
                            )
                        }
                        required = {config.ip_column, *config.filters}
                        if config.source_record_id_column is not None:
                            required.add(config.source_record_id_column)
                        if config.observed_at_column is not None:
                            required.add(config.observed_at_column)
                        if required - columns:
                            raise ValueError(
                                "source is missing columns: "
                                + ", ".join(sorted(required - columns))
                            )
                    selected_sql, params = selected_ips_sql(config)
                    params.update(
                        task=task_id,
                        submission=submission_id,
                        source_name=source,
                        source_run_id=config.source_run_id or run_id,
                    )
                    # Escape the literal "%" in INPUT_ID_SQL's modulo (cityHash64(ip) % 256):
                    # clickhouse_driver's params-bound execute() runs `query % escaped`, so an
                    # un-doubled "%" that is not a %(name)s placeholder breaks substitution
                    # (see the same "%%" escaping in norway_brreg_financial/assets/*.py).
                    identity = INPUT_ID_SQL.replace("%", "%%").format(
                        ip="chosen.ip",
                        source="%(source_name)s",
                        record="chosen.source_record_id",
                    )
                    # Entries land straight in the task's partition; the draft keeps one row
                    # per identity, so overlapping submissions append only what is new.
                    client.execute(
                        f"""INSERT INTO {INPUT_RELATION}
                        (task_id, input_id, ip, source_name, source_record_id, source_run_id, observed_at, submission_id)
                        SELECT %(task)s, s.input_id, s.ip, %(source_name)s, s.source_record_id,
                            %(source_run_id)s, s.observed_at, %(submission)s
                        FROM (
                            SELECT {identity} AS input_id, chosen.ip AS ip,
                                chosen.source_record_id AS source_record_id, chosen.observed_at AS observed_at
                            FROM ({selected_sql}) AS chosen
                        ) AS s
                        LEFT ANTI JOIN (
                            SELECT input_id FROM {INPUT_RELATION} WHERE task_id=%(task)s
                        ) AS e USING (input_id)""",
                        params,
                        query_id=query_id,
                        settings={
                            "async_insert": 0,
                            "use_query_cache": 0,
                            "max_threads": 4,
                            "max_bytes_before_external_group_by": 536870912,
                            "max_bytes_before_external_sort": 536870912,
                            # The draft side of the anti-join can hold tens of millions of ids.
                            "join_algorithm": "grace_hash",
                        },
                    )
                    [(count,)] = client.execute(
                        f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        {"task": task_id, "submission": submission_id},
                    )
                    [(total,)] = client.execute(
                        f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
                        {"task": task_id},
                    )
                draft_queue.finish_submission(
                    store,
                    submission_id=submission_id,
                    task_id=task_id,
                    count=count,
                    total=total,
                )
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": count,
                    "total": total,
                }
            except BaseException:
                draft_queue.fail_submission(store, submission_id)
                raise


@dg.asset(
    group_name="ip_enrichment",
    kinds={"clickhouse", "postgres"},
    pool="ip_enrichment_input",
    metadata={"dagster/table_name": INPUT_RELATION},
    description="Append IPs from a list, a filtered ClickHouse relation or a task's failed "
    "results to the open IP enrichment draft. Does not start enrichment.",
)
def ip_enrichment_input(
    context: dg.AssetExecutionContext,
    config: IpEnrichmentInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    submission_id = str(
        UUID(
            config.submission_id
            or context.run.tags.get("processing/submission_id")
            or context.run.root_run_id
            or context.run.run_id
        )
    )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/submission_id": submission_id}
    )
    with processing.get_store() as store:
        metadata = load_ip_draft(
            config, submission_id, store, clickhouse, run_id=context.run.run_id
        )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/task_id": metadata["task_id"]}
    )
    context.log.info(
        "IP enrichment draft %s: submission %s added %s rows, total %s",
        metadata["task_id"],
        submission_id,
        metadata["input_count"],
        metadata["total"],
    )
    return dg.MaterializeResult(metadata={**metadata, "input_relation": INPUT_RELATION})


ip_enrichment_input_job = dg.define_asset_job(
    "ip_enrichment_input_job",
    selection=dg.AssetSelection.assets(ip_enrichment_input),
)
defs = dg.Definitions(assets=[ip_enrichment_input], jobs=[ip_enrichment_input_job])
