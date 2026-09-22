"""Materialize a fixed company selection directly into the ClickHouse Brave queue."""

import hashlib
import json
import re
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
from dagster_v3.defs.company_domains.assets import PROCESSOR_VERSION

INPUT_RELATION = "corpscout.company_brave_search_input"


class BraveInputConfig(dg.Config):
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
    max_companies: int | None = Field(default=None, ge=1)
    select_all: bool = False

    @field_validator("source_relation")
    @classmethod
    def named_source(cls, value: str) -> str:
        value = validate_relation(value)
        if value == INPUT_RELATION:
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
        return self


@dg.asset(
    group_name="brave_domain_search",
    kinds={"clickhouse", "postgres"},
    pool="company_brave_input",
    tags={"source": "brave"},
    description="Prepare a fixed company selection inside ClickHouse from filter parameters. "
    "Materialize independently to inspect the selected count, then process its task_id with Brave.",
)
def company_brave_search_input(
    context: dg.AssetExecutionContext,
    config: BraveInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
) -> dg.MaterializeResult:
    task_id = (
        config.task_id
        or context.run.tags.get("processing/task_id")
        or context.run.run_id
    )
    context.instance.add_run_tags(context.run.run_id, {"processing/task_id": task_id})
    context.add_output_metadata({"task_id": task_id, "input_relation": INPUT_RELATION})
    selection = config.model_dump(exclude={"task_id"})
    selection["company_ids"] = sorted(set(selection["company_ids"]))
    selection["excluded_company_ids"] = sorted(set(selection["excluded_company_ids"]))
    # Retain fingerprints of selections prepared before these optional filters existed.
    for key in ("excluded_company_ids", "company_name_pattern", "company_id_length"):
        if not selection[key]:
            del selection[key]
    selection["filters"] = {
        key: sorted(set(values)) for key, values in selection["filters"].items()
    }
    fingerprint = hashlib.sha256(
        json.dumps(selection, sort_keys=True).encode()
    ).hexdigest()
    source = ClickHouseInputQueue(clickhouse, INPUT_RELATION, selection_task_id=task_id)
    with processing.get_store() as store, store.selection_lock(task_id):
        task, created = store.prepare_selection(
            task_id, processor=PROCESSOR_VERSION, fingerprint=fingerprint
        )
        if task["status"] == "preparing":
            predicates = []
            params = {"task_id": task_id, "country": config.country_code}
            if config.company_ids:
                predicates.append(
                    f"toString(`{config.company_id_column}`) IN %(company_ids)s"
                )
                params["company_ids"] = tuple(config.company_ids)
            if config.excluded_company_ids:
                predicates.append(
                    f"toString(`{config.company_id_column}`) NOT IN %(excluded_company_ids)s"
                )
                params["excluded_company_ids"] = tuple(config.excluded_company_ids)
            if config.company_name_pattern is not None:
                predicates.append(f"`{config.company_name_column}` ILIKE %(company_name_pattern)s")
                params["company_name_pattern"] = config.company_name_pattern
            if config.company_id_length is not None:
                predicates.append(f"length(toString(`{config.company_id_column}`)) = %(company_id_length)s")
                params["company_id_length"] = config.company_id_length
            for index, (column, values) in enumerate(sorted(config.filters.items())):
                predicates.append(f"`{column}` IN %(filter_{index})s")
                params[f"filter_{index}"] = tuple(values)
            where = " WHERE " + " AND ".join(predicates) if predicates else ""
            final = " FINAL" if config.source_final else ""
            limit = " LIMIT %(limit)s" if config.max_companies is not None else ""
            if config.max_companies is not None:
                params["limit"] = config.max_companies
            query_id = "brave-input:" + task_id
            with clickhouse.get_connection() as client:
                source.identity(client)
                columns = {
                    row[0]
                    for row in client.execute(
                        f"DESCRIBE TABLE {config.source_relation}"
                    )
                }
                required = {
                    config.company_id_column,
                    config.company_name_column,
                    *config.filters,
                }
                if required - columns:
                    raise ValueError(
                        "source is missing columns: "
                        + ", ".join(sorted(required - columns))
                    )
                if not created:
                    # A previous client may have died while its INSERT continued on the server.
                    # Stop only that task's insert before removing its unconfirmed partial rows.
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
                    (task_id,input_id,company_id,company_name,country_code)
                    SELECT %(task_id)s,concat(%(country)s,':',toString(`{config.company_id_column}`)),
                        toString(`{config.company_id_column}`),
                        trimBoth(ifNull(toString(`{config.company_name_column}`),'')),%(country)s
                    FROM {config.source_relation}{final}{where} ORDER BY toString(`{config.company_id_column}`){limit}""",
                    params,
                    query_id=query_id,
                    settings={"async_insert": 0},
                )
                [(invalid,)] = client.execute(
                    f"SELECT countIf(empty(trimBoth(company_id)) OR empty(company_name)) FROM {INPUT_RELATION} WHERE task_id=%(task_id)s",
                    {"task_id": task_id},
                )
                if invalid:
                    raise ValueError(
                        "selected companies contain empty company IDs or names"
                    )
            info = source.inspect()
            store.finish_selection(task_id, info)
            task = store.task(task_id)
        context.log.info(
            "Prepared Brave input task=%s total=%s", task_id, task["total"]
        )
        return dg.MaterializeResult(
            metadata={
                "task_id": task_id,
                "input_relation": INPUT_RELATION,
                "selected_companies": task["total"],
                "source_relation": config.source_relation,
            }
        )


company_brave_search_input_job = dg.define_asset_job(
    "company_brave_search_input_job",
    selection=dg.AssetSelection.assets(company_brave_search_input),
)
company_brave_search_workflow = dg.define_asset_job(
    "company_brave_search_workflow",
    selection=dg.AssetSelection.assets(
        "company_brave_search_input", "company_brave_search_results"
    ),
)
defs = dg.Definitions(
    assets=[company_brave_search_input],
    jobs=[company_brave_search_input_job, company_brave_search_workflow],
)
