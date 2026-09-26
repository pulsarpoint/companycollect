"""Prepare fixed IP submissions from a list or a filtered ClickHouse relation."""

import hashlib
import json
import re
from ipaddress import IPv6Address, ip_address
from typing import Self
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common.clickhouse_queue import (
    ClickHouseInputQueue,
    validate_relation,
)
from dagster_v3.defs.common.processing import ProcessingResource

INPUT_RELATION = "corpscout.ip_enrichment_input"
PROCESSOR_VERSION = "ip-enrichment-v1"

# The entry identity, computed where the entries live: the address's 256-way bucket first,
# so a task is walked bucket by bucket, then the JSON tuple of source, record and IP.
# Migration 000453 enforces the same expression in its valid_identity CHECK.
INPUT_ID_SQL = (
    "concat(leftPad(toString(toUInt16(cityHash64({ip}) % 256)), 3, '0'), ':', "
    "toJSONString(tuple({source}, {record}, {ip})))"
)


class IpEnrichmentInputConfig(dg.Config):
    task_id: str | None = Field(
        default=None,
        description="Selection UUID. Reuse to resume the same frozen selection.",
    )
    ips: list[str] = Field(
        default_factory=list,
        description="Explicit IPv4/IPv6 addresses. Use either ips or source_relation.",
    )
    source_relation: str | None = Field(
        default=None,
        description="Source ClickHouse table/view, as corpscout.table.",
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
        description="Provenance label. Defaults to source_relation or manual.",
    )
    source_run_id: str | None = Field(
        default=None,
        description="Original source run ID. Defaults to this Dagster run ID.",
    )

    @field_validator("task_id")
    @classmethod
    def valid_task_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

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
        if bool(self.ips) == (self.source_relation is not None):
            raise ValueError("provide either nonempty ips or source_relation, not both")
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
    if config.source_relation is None:
        relation = "(SELECT arrayJoin(%(ips)s) AS explicit_ip) AS source"
        ip_value = "source.explicit_ip"
        params["ips"] = config.ips
        where = ""
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


@dg.asset(
    group_name="ip_enrichment",
    kinds={"clickhouse", "postgres"},
    pool="ip_enrichment_input",
    metadata={"dagster/table_name": INPUT_RELATION},
    description="Prepare an immutable IP enrichment input batch from explicit IPs or "
    "a filtered ClickHouse table. Returns task_id and selected counts for downstream processing.",
)
def ip_enrichment_input(
    context: dg.AssetExecutionContext,
    config: IpEnrichmentInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    task_id = str(
        UUID(
            config.task_id
            or context.run.tags.get("processing/task_id")
            or context.run.root_run_id
            or context.run.run_id
        )
    )
    context.instance.add_run_tags(context.run.run_id, {"processing/task_id": task_id})
    context.add_output_metadata({"task_id": task_id, "input_relation": INPUT_RELATION})
    selection = config.model_dump(exclude={"task_id"})
    # Preserve fingerprints of already prepared batches that predate these optional filters.
    if not selection["ip_search"]:
        del selection["ip_search"]
    if not selection["excluded_ips"]:
        del selection["excluded_ips"]
    selection["filters"] = {
        key: sorted(set(values)) for key, values in config.filters.items()
    }
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    queue = ClickHouseInputQueue(clickhouse, INPUT_RELATION, selection_task_id=task_id)
    with processing.get_store() as store, store.selection_lock(task_id):
        task, created = store.prepare_selection(
            task_id,
            processor=PROCESSOR_VERSION,
            fingerprint=fingerprint,
        )
        if task["status"] == "preparing":
            sql, params = selected_ips_sql(config)
            params.update(
                task_id=task_id,
                source_name=config.source_name or config.source_relation or "manual",
                source_run_id=config.source_run_id or context.run.run_id,
            )
            query_id = "ip-enrichment-input:" + task_id
            with clickhouse.get_connection() as client:
                queue.identity(client)
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
                if not created:
                    # Fence any server-side insert left running by the interrupted client.
                    client.execute(
                        "KILL QUERY WHERE query_id=%(query_id)s SYNC",
                        {"query_id": query_id},
                    )
                    client.execute(
                        f"ALTER TABLE {INPUT_RELATION} DELETE WHERE task_id=%(task_id)s",
                        {"task_id": task_id},
                        settings={"mutations_sync": 2},
                    )
                client.execute(
                    f"""INSERT INTO {INPUT_RELATION}
                    (task_id, input_id, ip, source_name, source_record_id, source_run_id, observed_at)
                    SELECT %(task_id)s,
                        toJSONString(tuple(%(source_name)s, selected.source_record_id, selected.ip)),
                        selected.ip, %(source_name)s, selected.source_record_id,
                        %(source_run_id)s, selected.observed_at
                    FROM ({sql}) AS selected""",
                    params,
                    query_id=query_id,
                    settings={
                        "async_insert": 0,
                        "max_threads": 4,
                        "max_bytes_before_external_group_by": 536870912,
                        "max_bytes_before_external_sort": 536870912,
                    },
                )
                [(unique_ips, invalid_ids)] = client.execute(
                    f"""SELECT uniqExact(ip), countIf(empty(source_record_id) OR position(source_record_id, char(0)) > 0)
                    FROM {INPUT_RELATION} WHERE task_id=%(task_id)s""",
                    {"task_id": task_id},
                )
                if invalid_ids:
                    raise ValueError(
                        "selected source records contain empty or invalid record IDs"
                    )
            info = {**queue.inspect(), "unique_ips": unique_ips}
            store.finish_selection(task_id, info)
            task = store.task(task_id)
        context.log.info(
            "Prepared IP enrichment input task=%s rows=%s", task_id, task["total"]
        )
        return dg.MaterializeResult(
            metadata={
                "task_id": task_id,
                "input_relation": INPUT_RELATION,
                "source_relation": config.source_relation or "explicit IP list",
                "selected_inputs": task["total"],
                "selected_ips": task["source_info"]["unique_ips"],
            }
        )


ip_enrichment_input_job = dg.define_asset_job(
    "ip_enrichment_input_job",
    selection=dg.AssetSelection.assets(ip_enrichment_input),
)
defs = dg.Definitions(assets=[ip_enrichment_input], jobs=[ip_enrichment_input_job])
