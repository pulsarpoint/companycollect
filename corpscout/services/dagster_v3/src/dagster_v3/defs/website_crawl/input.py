"""Select and seed recurring crawl inputs without replacing operator settings."""

import re
from typing import Self

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator, model_validator

from dagster_v3.defs.common.clickhouse_queue import validate_relation
from dagster_v3.defs.website_crawl.se_domains import SE_DOMAIN_TABLE, SeDomainFilters

INPUT_TABLES = (
    "corpscout.website_full_crawl_requests",
    "corpscout.website_jobs_crawl_requests",
    "corpscout.website_site_info_requests",
)


class CrawlInputConfig(dg.Config):
    source_relation: str = Field(
        description="Source ClickHouse database.table or view."
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

    @field_validator("source_relation")
    @classmethod
    def source_name(cls, value: str) -> str:
        value = validate_relation(value)
        if value.split(".")[0] != "corpscout":
            raise ValueError("source_relation must be in the corpscout database")
        if value in INPUT_TABLES or value.removesuffix("_current") in INPUT_TABLES:
            raise ValueError("select from a source inventory, not a crawl input table")
        return value

    @model_validator(mode="after")
    def selection(self) -> Self:
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
            self.ids
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
    # Keep normalization and deduplication inside ClickHouse for large inventories.
    # Parse the authority explicitly: domain() drops some valid IDN/port combinations.
    return (
        f"""
        WITH source_rows AS (
            SELECT trimBoth(ifNull(toString(`{config.website_column}`), '')) AS raw_website
            FROM {config.source_relation}{final}{where}
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


def seed_crawl_inputs(
    context: dg.AssetExecutionContext,
    config: CrawlInputConfig,
    clickhouse: ClickhouseResource,
    target: str,
) -> dg.MaterializeResult:
    if target not in INPUT_TABLES:
        raise ValueError("unknown crawl input table")
    selection, parameters = selected_domains_sql(config)
    parameters["source"] = config.source_relation
    priority_column = ", priority" if config.priority is not None else ""
    priority_value = ", %(priority)s" if config.priority is not None else ""
    if config.priority is not None:
        parameters["priority"] = config.priority
    with clickhouse.get_connection() as client:
        for relation in (target, target + "_current"):
            if client.execute(f"EXISTS TABLE {relation}") != [(1,)]:
                raise ValueError(
                    f"apply ClickHouse migration 000429 before selecting inputs: {relation}"
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
    context.log.info(
        "Added %s crawl inputs from %s to %s", inserted, config.source_relation, target
    )
    return dg.MaterializeResult(
        metadata={
            "source_relation": config.source_relation,
            "input_relation": target,
            "inserted_domains": inserted,
            "existing_inputs": "preserved",
        }
    )
