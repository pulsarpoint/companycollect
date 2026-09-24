"""Select and seed recurring crawl inputs, freezing each selection under a task.

Like the Brave input, a selection is fixed per task_id: the request tables keep the
recurring per-domain configuration, and website_crawl_task_domains records exactly
which domains a task will process.
"""

import hashlib
import json
import re
from typing import Self
from uuid import UUID

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common.clickhouse_queue import validate_relation
from dagster_v3.defs.common.processing import ProcessingResource
from dagster_v3.defs.website_crawl.se_domains import SE_DOMAIN_TABLE, SeDomainFilters

INPUT_TABLES = (
    "corpscout.website_full_crawl_requests",
    "corpscout.website_jobs_crawl_requests",
    "corpscout.website_site_info_requests",
)
CRAWL_TYPES = dict(zip(INPUT_TABLES, ("full", "jobs", "site_info"), strict=True))
TASK_DOMAINS = "corpscout.website_crawl_task_domains"
TASK_TAG = "processing/task_id"


def task_processor(crawl_type: str) -> str:
    """processing.tasks processor name; the results asset checks it matches its type."""
    return f"website-crawl-{crawl_type}-v1"


class CrawlInputConfig(dg.Config):
    task_id: str | None = Field(
        default=None,
        description="Selection task UUID. Defaults to the run's processing/task_id tag, then the run ID.",
    )
    source_relation: str | None = Field(
        default=None, description="Source ClickHouse database.table or view."
    )
    targets: list[str] = Field(
        default_factory=list,
        description="Explicit hostnames or HTTP(S) URLs, instead of a source table.",
    )
    id_column: str = Field(default="domain", description="Column matched by ids.")
    website_column: str = Field(
        default="domain", description="Column containing a hostname or HTTP(S) URL."
    )
    ids: list[str] = Field(default_factory=list)
    excluded_ids: list[str] = Field(default_factory=list)
    se_domain_filters: SeDomainFilters | None = None
    filters: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Column-to-values filters: OR within each list, AND between columns and ids.",
    )
    source_final: bool = Field(
        default=False, description="Read a replacing source table using FINAL."
    )
    select_all: bool = False
    max_domains: int | None = Field(
        default=None,
        ge=1,
        description="Cap the selected distinct domains, sorted alphabetically.",
    )
    priority: int | None = Field(
        default=None,
        ge=0,
        le=100,
        description="Priority for newly added domains. Omit to use the table default.",
    )

    @field_validator("task_id")
    @classmethod
    def stable_task_id(cls, value: str | None) -> str | None:
        return str(UUID(value)) if value is not None else None

    @field_validator("source_relation")
    @classmethod
    def source_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = validate_relation(value)
        if value.split(".")[0] != "corpscout":
            raise ValueError("source_relation must be in the corpscout database")
        if value in INPUT_TABLES or value.removesuffix("_current") in INPUT_TABLES:
            raise ValueError("select from a source inventory, not a crawl input table")
        return value

    @model_validator(mode="after")
    def selection(self) -> Self:
        if bool(self.targets) == (self.source_relation is not None):
            raise ValueError("provide targets or source_relation, not both")
        if self.targets and (
            self.ids
            or self.excluded_ids
            or self.filters
            or self.se_domain_filters is not None
            or self.source_final
            or self.select_all
        ):
            raise ValueError("table selection options require source_relation")
        for column in (self.id_column, self.website_column, *self.filters):
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", column) is None:
                raise ValueError("column names must be simple SQL identifiers")
        if any(
            not value.strip() or "\0" in value
            for value in [*self.ids, *self.excluded_ids]
        ):
            raise ValueError("ids must be nonempty strings without NUL characters")
        if any(not values for values in self.filters.values()):
            raise ValueError("each filter needs at least one value")
        if self.se_domain_filters is not None and (
            self.source_relation != SE_DOMAIN_TABLE
            or not self.source_final
            or self.id_column != "root_domain"
            or self.website_column != "root_domain"
        ):
            raise ValueError(
                "SE domain filters require the current se_company_domain table with root_domain identity and website columns"
            )
        if not (
            self.targets
            or self.ids
            or self.filters
            or self.select_all
            or (
                self.se_domain_filters is not None
                and self.se_domain_filters.model_dump(exclude_defaults=True)
            )
        ):
            raise ValueError("provide ids, filters, or explicit select_all=true")
        return self


def selected_domains_sql(config: CrawlInputConfig) -> tuple[str, dict]:
    predicates = []
    parameters = {}
    if config.ids:
        predicates.append(f"toString(`{config.id_column}`) IN %(ids)s")
        parameters["ids"] = tuple(sorted(set(config.ids)))
    if config.excluded_ids:
        predicates.append(f"toString(`{config.id_column}`) NOT IN %(excluded_ids)s")
        parameters["excluded_ids"] = tuple(sorted(set(config.excluded_ids)))
    if config.se_domain_filters is not None:
        se_predicates, se_parameters = config.se_domain_filters.predicates()
        predicates.extend(se_predicates)
        parameters.update(se_parameters)
    for index, (column, values) in enumerate(sorted(config.filters.items())):
        predicates.append(f"toString(`{column}`) IN %(filter_{index})s")
        parameters[f"filter_{index}"] = tuple(sorted(set(values)))
    where = " WHERE " + " AND ".join(predicates) if predicates else ""
    final = " FINAL" if config.source_final else ""
    limit = " LIMIT %(limit)s" if config.max_domains is not None else ""
    if config.max_domains is not None:
        parameters["limit"] = config.max_domains
    source_rows = f"SELECT trimBoth(ifNull(toString(`{config.website_column}`), '')) AS raw_website FROM {config.source_relation}{final}{where}"
    if config.targets:
        parameters["targets"] = sorted(set(config.targets))
        source_rows = "SELECT trimBoth(arrayJoin(%(targets)s)) AS raw_website"
    # Keep normalization and deduplication inside ClickHouse for large inventories.
    # Parse the authority explicitly: domain() drops some valid IDN/port combinations.
    return (
        f"""
        WITH source_rows AS (
            {source_rows}
        ), urls AS (
            SELECT raw_website,
                multiIf(
                    match(raw_website, '(?i)^[a-z][a-z0-9+.-]*://'),
                        concat(lower(protocol(raw_website)), substring(raw_website, length(protocol(raw_website)) + 1)),
                    startsWith(raw_website, '//'), concat('https:', raw_website),
                    concat('https://', raw_website)
                ) AS seed_url
            FROM source_rows
        ), hosts AS (
            SELECT *, extract(seed_url, '^https?://([^/?#]*)') AS authority,
                extract(authority, '^([^:@]+)(:[0-9]+)?$') AS raw_host,
                extract(authority, ':([0-9]+)$') AS port,
                tryIdnaEncode(lowerUTF8(replaceRegexpOne(raw_host, '[.]$', ''))) AS ascii_host
            FROM urls
        ), normalized AS (
            SELECT replaceRegexpOne(ascii_host, '^www[.]', '') AS crawl_domain,
                replaceOne(cutFragment(seed_url), concat('://', authority),
                    concat('://', ascii_host, if(empty(port), '', concat(':', port)))) AS website_url
            FROM hosts
            WHERE notEmpty(raw_website)
                AND protocol(seed_url) IN ('http', 'https')
                AND NOT match(raw_website, '[[:space:][:cntrl:]]')
                AND length(ascii_host) <= 253
                AND match(ascii_host, '^[a-z0-9]([a-z0-9-]{{0,61}}[a-z0-9])?([.][a-z0-9]([a-z0-9-]{{0,61}}[a-z0-9])?)+$')
                AND (empty(port) OR toUInt32OrZero(port) BETWEEN 1 AND 65535)
        )
        SELECT crawl_domain AS domain,
            argMin(website_url, tuple(protocol(website_url) != 'https', length(website_url), website_url)) AS website_url
        FROM normalized
        GROUP BY crawl_domain
        ORDER BY crawl_domain{limit}
    """,
        parameters,
    )


def selection_fingerprint(config: CrawlInputConfig, target: str) -> str:
    selection = config.model_dump(exclude={"task_id", "targets"}, mode="json")
    for key in ("ids", "excluded_ids"):
        selection[key] = sorted(set(selection[key]))
    selection["filters"] = {
        key: sorted(set(values)) for key, values in selection["filters"].items()
    }
    selection["target"] = target
    return hashlib.sha256(json.dumps(selection, sort_keys=True).encode()).hexdigest()


def seed_crawl_inputs(
    context: dg.AssetExecutionContext,
    config: CrawlInputConfig,
    clickhouse: ClickhouseResource,
    processing: ProcessingResource,
    target: str,
) -> dg.MaterializeResult:
    if config.source_relation is None:
        raise ValueError("Use website_crawl_input for manual targets")
    if target not in INPUT_TABLES:
        raise ValueError("unknown crawl input table")
    crawl_type = CRAWL_TYPES[target]
    task_id = config.task_id or context.run.tags.get(TASK_TAG) or context.run.run_id
    context.instance.add_run_tags(context.run.run_id, {TASK_TAG: task_id})
    selection, parameters = selected_domains_sql(config)
    parameters["source"] = config.source_relation
    parameters["task_id"] = task_id
    parameters["crawl_type"] = crawl_type
    priority_column = ", priority" if config.priority is not None else ""
    priority_value = ", %(priority)s" if config.priority is not None else ""
    if config.priority is not None:
        parameters["priority"] = config.priority
    inserted = 0
    with (
        processing.get_store() as store,
        store.selection_lock(task_id),
        clickhouse.get_connection() as client,
    ):
        task, created = store.prepare_selection(
            task_id,
            processor=task_processor(crawl_type),
            fingerprint=selection_fingerprint(config, target),
        )
        if task["status"] == "preparing":
            inserted = insert_selection(
                client,
                config,
                target,
                selection,
                parameters,
                priority_column,
                priority_value,
                recover=not created,
            )
            [(total,)] = client.execute(
                f"SELECT count() FROM {TASK_DOMAINS} FINAL WHERE task_id=%(task_id)s",
                {"task_id": task_id},
            )
            store.finish_selection(
                task_id,
                {
                    "relation": TASK_DOMAINS,
                    "crawl_type": crawl_type,
                    "input_relation": target,
                    "total": total,
                },
            )
            task = store.task(task_id)
    context.log.info(
        "Prepared crawl task=%s type=%s selected=%s added=%s from %s",
        task_id,
        crawl_type,
        task["total"],
        inserted,
        config.source_relation,
    )
    return dg.MaterializeResult(
        metadata={
            "task_id": task_id,
            "source_relation": config.source_relation,
            "input_relation": target,
            "task_relation": TASK_DOMAINS,
            "selected_domains": task["total"],
            "inserted_domains": inserted,
            "existing_inputs": "preserved",
        }
    )


def insert_selection(
    client,
    config: CrawlInputConfig,
    target: str,
    selection: str,
    parameters: dict,
    priority_column: str,
    priority_value: str,
    *,
    recover: bool,
) -> int:
    """Add missing request rows, then freeze this task's membership."""
    task_id = parameters["task_id"]
    for relation in (target, target + "_current"):
        if client.execute(f"EXISTS TABLE {relation}") != [(1,)]:
            raise ValueError(
                f"apply ClickHouse migration 000429 before selecting inputs: {relation}"
            )
    if client.execute(f"EXISTS TABLE {TASK_DOMAINS}") != [(1,)]:
        raise ValueError(
            f"apply ClickHouse migration 000431 before selecting inputs: {TASK_DOMAINS}"
        )
    columns = {
        row[0] for row in client.execute(f"DESCRIBE TABLE {config.source_relation}")
    }
    required = {config.website_column, *config.filters}
    if config.ids or config.excluded_ids:
        required.add(config.id_column)
    if config.se_domain_filters is not None:
        required.update(
            {
                "root_domain",
                "company_id",
                "sources",
                "association",
                "active",
                "confidence",
            }
        )
    if required - columns:
        raise ValueError(
            "source is missing columns: " + ", ".join(sorted(required - columns))
        )
    client.execute(
        f"""INSERT INTO {target}
            (domain, website_url, source, created_at, updated_at, revision{priority_column})
        SELECT selected.domain, selected.website_url, %(source)s, now64(6), now64(6), 1{priority_value}
        FROM ({selection}) AS selected
        LEFT ANTI JOIN {target}_current AS existing ON selected.domain = existing.domain""",
        parameters,
        # Reject an overlapping insert, including a server query left by a lost client.
        query_id="website-crawl-input:" + target,
        settings={
            "async_insert": 0,
            "replace_running_query": 0,
            "use_query_cache": 0,
        },
    )
    inserted = client.last_query.progress.written_rows
    query_id = "website-crawl-task:" + task_id
    if recover:
        # A previous client may have died while its INSERT continued on the server.
        # Stop only that task's insert before removing its unconfirmed partial rows.
        client.execute(
            "KILL QUERY WHERE query_id=%(query_id)s SYNC", {"query_id": query_id}
        )
        client.execute(
            f"ALTER TABLE {TASK_DOMAINS} DELETE WHERE task_id=%(task_id)s",
            {"task_id": task_id},
            settings={"mutations_sync": 2},
        )
    client.execute(
        f"""INSERT INTO {TASK_DOMAINS} (task_id, crawl_type, domain)
        SELECT %(task_id)s, %(crawl_type)s, selected.domain FROM ({selection}) AS selected""",
        parameters,
        query_id=query_id,
        settings={"async_insert": 0, "use_query_cache": 0},
    )
    return inserted
