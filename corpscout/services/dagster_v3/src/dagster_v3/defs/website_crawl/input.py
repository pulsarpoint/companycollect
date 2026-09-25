"""Selection configuration and the ClickHouse query that normalizes website targets.

Drafts (``queue_input``) append the selected domains to ``website_crawl_task_domains``
and seed missing recurring presets; executions read the task's frozen membership.
"""

import re
from typing import Self
from uuid import UUID

import dagster as dg
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common.clickhouse_queue import validate_relation
from dagster_v3.defs.website_crawl.se_domains import SE_DOMAIN_TABLE, SeDomainFilters

INPUT_TABLES = (
    "corpscout.website_full_crawl_requests",
    "corpscout.website_jobs_crawl_requests",
    "corpscout.website_site_info_requests",
)
TASK_DOMAINS = "corpscout.website_crawl_task_domains"


def task_processor(crawl_type: str) -> str:
    """processing.tasks processor name; the draft execution checks it matches its type."""
    return f"website-crawl-{crawl_type}-v1"


class CrawlInputConfig(dg.Config):
    task_id: str | None = Field(
        default=None,
        description="Open crawl draft to add to. Omit to use or create the open draft for this queue scope and crawl type.",
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
