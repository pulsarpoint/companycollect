"""Append company selections to a retryable workspace draft."""

import hashlib
import json
import re
from typing import Self
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common import draft_queue
from dagster_v3.defs.common.clickhouse_queue import ClickHouseInputQueue, validate_relation
from dagster_v3.defs.common.processing import ProcessingResource, ProcessingStore

from dagster_v3.defs.company_domains.queue_tables import (
    INPUT_RELATION,
    INPUT_COLUMNS,
    PROCESSOR,
)


class BraveQueueInputConfig(dg.Config):
    task_id: str | None = None
    source_relation: str
    company_id_column: str = "company_id"
    company_name_column: str = "company_name"
    country_code: str
    company_ids: list[str] = Field(default_factory=list, description="Testing only. Use filters for production selections.")
    excluded_company_ids: list[str] = Field(default_factory=list)
    company_name_pattern: str | None = Field(default=None, min_length=1)
    company_id_length: int | None = Field(default=None, ge=1)
    filters: dict[str, list[str]] = Field(default_factory=dict)
    source_final: bool = False
    max_companies: int | None = Field(default=None, ge=1, le=1_000_000)
    select_all: bool = False
    submission_id: str
    queue_scope: str = Field(default="workspace:SE", min_length=1)
    source_name: str = Field(default="backoffice:se-companies", min_length=1)

    @field_validator("source_relation")
    @classmethod
    def named_source(cls, value: str) -> str:
        value = validate_relation(value)
        if value in {INPUT_RELATION, "corpscout.company_brave_search_input"}:
            raise ValueError("the source must differ from the input queue")
        return value

    @field_validator("task_id")
    @classmethod
    def stable_task_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("country_code")
    @classmethod
    def country(cls, value: str) -> str:
        value = value.strip().upper()
        if not re.fullmatch(r"[A-Z]{2}", value):
            raise ValueError("country_code must be a two-letter country code")
        return value

    @model_validator(mode="after")
    def selection(self) -> Self:
        for column in (self.company_id_column, self.company_name_column, *self.filters):
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column):
                raise ValueError("column names must be simple SQL identifiers")
        if any(not values for values in self.filters.values()):
            raise ValueError("each column filter needs at least one value")
        if not (
            self.company_ids or self.filters or self.company_name_pattern
            or self.company_id_length or self.max_companies or self.select_all
        ):
            raise ValueError(
                "provide company_ids, filters, max_companies or explicit select_all"
            )
        UUID(self.submission_id)
        if self.country_code != "SE":
            raise ValueError("Brave currently supports Swedish companies only")
        if not self.queue_scope.strip():
            raise ValueError("Choose a nonempty queue scope")
        return self


def selected_companies(config: BraveQueueInputConfig) -> tuple[str, dict, list[dict]]:
    """Selection stays in ClickHouse; potentially large filter values travel as data."""
    predicates = []
    params = {"country": config.country_code}
    external = []
    filters = list(sorted(config.filters.items()))
    if config.company_ids:
        filters.append((config.company_id_column, config.company_ids))
    for index, (column, values) in enumerate(filters):
        name = f"selection_filter_{index}"
        predicates.append(f"toString(source.`{column}`) IN (SELECT value FROM {name})")
        external.append(
            {
                "name": name,
                "structure": [("value", "String")],
                "data": [(value,) for value in sorted(set(values))],
            }
        )
    if config.excluded_company_ids:
        predicates.append(
            f"toString(source.`{config.company_id_column}`) NOT IN (SELECT value FROM exclusions)"
        )
        external.append(
            {
                "name": "exclusions",
                "structure": [("value", "String")],
                "data": [
                    (value,) for value in sorted(set(config.excluded_company_ids))
                ],
            }
        )
    if config.company_name_pattern is not None:
        predicates.append(f"source.`{config.company_name_column}` ILIKE %(pattern)s")
        params["pattern"] = config.company_name_pattern
    if config.company_id_length is not None:
        predicates.append(
            f"length(toString(source.`{config.company_id_column}`)) = %(id_length)s"
        )
        params["id_length"] = config.company_id_length
    where = " WHERE " + " AND ".join(predicates) if predicates else ""
    # Stable name choice if a source repeats an identity. Draft entries already present
    # retain their original name; later submissions only contribute missing companies.
    sql = (
        f"SELECT trimBoth(ifNull(toString(source.`{config.company_id_column}`),'')) AS company_id, "
        f"minIf(trimBoth(ifNull(toString(source.`{config.company_name_column}`),'')), "
        f"trimBoth(ifNull(toString(source.`{config.company_name_column}`),'')) != '') AS company_name "
        f"FROM {config.source_relation} AS source{' FINAL' if config.source_final else ''}{where} "
        "GROUP BY company_id ORDER BY company_id"
    )
    if config.max_companies is not None:
        sql += " LIMIT %(limit)s"
        params["limit"] = config.max_companies
    return sql, params, external


def load_draft(
    *,
    config: BraveQueueInputConfig,
    run_id: str,
    clickhouse: ClickhouseResource,
    store: ProcessingStore,
) -> dict:
    submission_id = str(UUID(config.submission_id))
    selection = config.model_dump(exclude={"task_id", "submission_id", "queue_scope"})
    for key in ("company_ids", "excluded_company_ids"):
        selection[key] = sorted(set(selection[key]))
    selection["filters"] = {
        key: sorted(set(values)) for key, values in config.filters.items()
    }
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    while True:
        receipt = draft_queue.submission(store, submission_id)
        task_id = (
            str(receipt["task_id"])
            if receipt
            else draft_queue.find_draft(
                store,
                scope=config.queue_scope,
                processor=PROCESSOR,
                task_id=config.task_id,
            )
        )
        with store.selection_lock(task_id):
            task = store.task(task_id)
            if (
                task["processor"] != PROCESSOR
                or task["queue_scope"] != config.queue_scope
            ):
                raise ValueError("Submission belongs to a different processor or scope")
            if config.task_id is not None and config.task_id != task_id:
                raise ValueError("Submission belongs to a different task")
            receipt = draft_queue.submission(store, submission_id)
            if receipt:
                if (
                    str(receipt["task_id"]) != task_id
                    or receipt["selection_fingerprint"] != fingerprint
                ):
                    raise ValueError(
                        "Submission ID already belongs to a different selection or task"
                    )
                if receipt["status"] == "completed":
                    return {
                        "task_id": task_id,
                        "submission_id": submission_id,
                        "input_count": receipt["input_count"],
                        "total": task["total"],
                    }
            elif task["status"] != "draft" and config.task_id is None:
                continue
            draft_queue.prepare_submission(
                store,
                task_id=task_id,
                submission_id=submission_id,
                source=config.source_name,
                selection=selection,
                fingerprint=fingerprint,
            )
            try:
                sql, params, external = selected_companies(config)
                params.update(
                    task=task_id,
                    submission=submission_id,
                    source=config.source_name,
                    run=run_id,
                )
                query_id = "brave-queue-import:" + submission_id
                with clickhouse.get_connection() as client:
                    client.execute(
                        "KILL QUERY WHERE query_id=%(query)s SYNC", {"query": query_id}
                    )
                    client.execute(
                        f"DELETE FROM {INPUT_RELATION} WHERE task_id=%(task)s AND submission_id=%(submission)s",
                        params,
                        settings={"lightweight_deletes_sync": 2},
                    )
                    [(valid, invalid)] = client.execute(
                        f"SELECT countIf(company_id != '' AND company_name != ''), "
                        f"countIf(company_id = '' OR company_name = '') FROM ({sql})",
                        params,
                        external_tables=external,
                    )
                    if invalid and not valid:
                        raise ValueError("Selected source contains no valid companies")
                    if valid > 1_000_000:
                        raise ValueError(
                            "A submission supports at most one million companies"
                        )
                    selected = f"SELECT * FROM ({sql}) WHERE company_id != '' AND company_name != ''"
                    missing = (
                        selected
                        + f" AND concat(%(country)s,':',company_id) NOT IN (SELECT input_id FROM {INPUT_RELATION} WHERE task_id=%(task)s)"
                    )
                    [(new_count,)] = client.execute(
                        f"SELECT count() FROM ({missing})",
                        params,
                        external_tables=external,
                    )
                    [(total,)] = client.execute(
                        f"SELECT count() FROM {INPUT_RELATION} WHERE task_id=%(task)s",
                        params,
                    )
                    if total + new_count > 1_000_000:
                        raise ValueError(
                            "A Brave draft supports at most one million companies"
                        )
                    client.execute(
                        f"INSERT INTO {INPUT_RELATION} ({','.join(INPUT_COLUMNS[:-1])}) "
                        "SELECT %(task)s,concat(%(country)s,':',company_id),%(country)s,company_id,company_name,"
                        f"%(source)s,company_id,%(run)s,%(submission)s FROM ({missing})",
                        params,
                        external_tables=external,
                        query_id=query_id,
                        settings={"async_insert": 0},
                    )
                # Re-read acknowledged totals; a changed source cannot silently exceed the cap.
                total = ClickHouseInputQueue(
                    clickhouse, INPUT_RELATION, selection_task_id=task_id
                ).inspect()["total"]
                if total > 1_000_000:
                    raise ValueError(
                        "Source changed during import; draft exceeds one million companies"
                    )
                draft_queue.finish_submission(
                    store,
                    task_id=task_id,
                    submission_id=submission_id,
                    count=valid,
                    total=total,
                )
                if invalid:
                    dg.get_dagster_logger().warning(
                        "Skipped %d source records with empty company IDs or names",
                        invalid,
                    )
                return {
                    "task_id": task_id,
                    "submission_id": submission_id,
                    "input_count": valid,
                    "total": total,
                    "invalid_source_rows": invalid,
                }
            except BaseException:
                draft_queue.fail_submission(store, submission_id)
                raise


@dg.asset(
    group_name="brave_domain_search",
    kinds={"clickhouse", "postgres"},
    pool="company_brave_input",
    metadata={"dagster/table_name": INPUT_RELATION},
)
def company_brave_queue_input(
    context: dg.AssetExecutionContext,
    config: BraveQueueInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    context.instance.add_run_tags(
        context.run.run_id, {"processing/submission_id": config.submission_id}
    )
    with processing.get_store() as store:
        metadata = load_draft(
            config=config, run_id=context.run.run_id, clickhouse=clickhouse, store=store
        )
    context.instance.add_run_tags(
        context.run.run_id, {"processing/task_id": metadata["task_id"]}
    )
    return dg.MaterializeResult(metadata=metadata)


company_brave_queue_input_job = dg.define_asset_job(
    "company_brave_queue_input_job",
    selection=dg.AssetSelection.assets(company_brave_queue_input),
)
defs = dg.Definitions(
    assets=[company_brave_queue_input], jobs=[company_brave_queue_input_job]
)
