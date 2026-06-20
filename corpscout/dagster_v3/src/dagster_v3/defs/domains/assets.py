import uuid
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
)
from dagster_v3.defs.domains import tables

GROUP_NAME = "domains"


@dg.asset(
    deps=[
        dg.AssetKey("finland_ytj_resolved_clickhouse"),
        dg.AssetKey("norway_resolved_clickhouse"),
        dg.AssetKey("wikidata_company_seed_clickhouse"),
    ],
    group_name=GROUP_NAME,
    kinds={"clickhouse"},
    description=(
        "Builds the website domain dimension and company-to-domain links from "
        "resolved source website tables."
    ),
)
def domains_clickhouse(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse,
        database=RESOLVED_DATABASE,
        tables=tables.DOMAIN_TABLES,
    )
    with clickhouse.get_connection() as client:
        row_counts = replace_domain_clickhouse_tables(client)

    context.log.info("Completed domain ClickHouse rebuild: row_counts=%s", row_counts)
    return dg.MaterializeResult(metadata=row_counts)


def replace_domain_clickhouse_tables(clickhouse_client: Any) -> dict[str, int]:
    domains_stage = _stage_table_name(tables.DOMAINS_TABLE)
    company_links_stage = _stage_table_name(tables.COMPANY_WEBSITE_DOMAINS_TABLE)
    created_stage_tables = [
        _qualified_table(company_links_stage),
        _qualified_table(domains_stage),
    ]
    primary_error: Exception | None = None

    try:
        clickhouse_client.execute(
            f"CREATE TABLE {_qualified_table(domains_stage)} AS "
            f"{_qualified_table(tables.DOMAINS_TABLE)}"
        )
        clickhouse_client.execute(
            f"CREATE TABLE {_qualified_table(company_links_stage)} AS "
            f"{_qualified_table(tables.COMPANY_WEBSITE_DOMAINS_TABLE)}"
        )

        clickhouse_client.execute(_company_website_domains_insert_sql(company_links_stage))
        clickhouse_client.execute(_domains_insert_sql(domains_stage, company_links_stage))

        clickhouse_client.execute(
            f"EXCHANGE TABLES {_qualified_table(domains_stage)} "
            f"AND {_qualified_table(tables.DOMAINS_TABLE)}"
        )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {_qualified_table(company_links_stage)} "
            f"AND {_qualified_table(tables.COMPANY_WEBSITE_DOMAINS_TABLE)}"
        )

        return {
            tables.DOMAINS_TABLE: _table_count(
                clickhouse_client,
                tables.DOMAINS_TABLE,
            ),
            tables.COMPANY_WEBSITE_DOMAINS_TABLE: _table_count(
                clickhouse_client,
                tables.COMPANY_WEBSITE_DOMAINS_TABLE,
            ),
        }
    except Exception as exc:
        primary_error = exc
        raise
    finally:
        _drop_stage_tables(
            clickhouse_client,
            created_stage_tables,
            suppress_errors=primary_error is not None,
        )


def _company_website_domains_insert_sql(stage_table: str) -> str:
    columns = _column_list(tables.COMPANY_WEBSITE_DOMAINS_COLUMNS)
    return f"""
    INSERT INTO {_qualified_table(stage_table)} ({columns})
    SELECT
        source_website_table,
        source_website_id,
        country_iso2,
        source_slug,
        company_id_type,
        company_id,
        website_url,
        website_normalized_url,
        website_host,
        root_domain,
        is_current,
        is_primary,
        now64(3) AS resolved_at
    FROM
    (
        SELECT
            'fi_websites' AS source_website_table,
            ifNull(
                nullIf(trim(websites.source_record_id), ''),
                concat('fi_websites:', websites.business_id, ':', websites.website_normalized_url)
            ) AS source_website_id,
            'FI' AS country_iso2,
            'finland_ytj' AS source_slug,
            'business_id' AS company_id_type,
            websites.business_id AS company_id,
            websites.website_url AS website_url,
            websites.website_normalized_url AS website_normalized_url,
            websites.website_host AS website_host,
            websites.root_domain AS root_domain,
            websites.is_current AS is_current,
            websites.is_primary AS is_primary
        FROM {_qualified_table("fi_websites")} AS websites
        WHERE nullIf(trim(websites.root_domain), '') IS NOT NULL

        UNION ALL

        SELECT
            'no_websites' AS source_website_table,
            ifNull(
                nullIf(trim(websites.source_record_id), ''),
                concat('no_websites:', websites.org_number, ':', websites.website_normalized_url)
            ) AS source_website_id,
            'NO' AS country_iso2,
            'norway_brreg' AS source_slug,
            'org_number' AS company_id_type,
            websites.org_number AS company_id,
            websites.website_url AS website_url,
            websites.website_normalized_url AS website_normalized_url,
            websites.website_host AS website_host,
            websites.root_domain AS root_domain,
            websites.is_current AS is_current,
            websites.is_primary AS is_primary
        FROM {_qualified_table("no_websites")} AS websites
        WHERE nullIf(trim(websites.root_domain), '') IS NOT NULL

        UNION ALL

        SELECT
            'wikidata_company_websites' AS source_website_table,
            ifNull(
                nullIf(trim(websites.source_record_id), ''),
                concat(
                    'wikidata_company_websites:',
                    websites.wikidata_id,
                    ':',
                    websites.website_normalized_url
                )
            ) AS source_website_id,
            nullIf(trim(ifNull(companies.headquarters_country_iso2, '')), '') AS country_iso2,
            'wikidata' AS source_slug,
            'wikidata_id' AS company_id_type,
            websites.wikidata_id AS company_id,
            websites.website_url AS website_url,
            websites.website_normalized_url AS website_normalized_url,
            websites.website_host AS website_host,
            websites.root_domain AS root_domain,
            1 AS is_current,
            websites.is_primary_candidate AS is_primary
        FROM {_qualified_table("wikidata_company_websites")} AS websites
        LEFT JOIN {_qualified_table("wikidata_companies")} AS companies
            ON companies.wikidata_id = websites.wikidata_id
        WHERE nullIf(trim(websites.root_domain), '') IS NOT NULL
    )
    """


def _domains_insert_sql(stage_table: str, company_links_stage: str) -> str:
    columns = _column_list(tables.DOMAINS_COLUMNS)
    return f"""
    INSERT INTO {_qualified_table(stage_table)} ({columns})
    SELECT
        root_domain,
        countDistinct(company_id) AS company_count,
        countDistinct(website_normalized_url) AS website_count,
        countDistinct(source_slug) AS source_slug_count,
        countDistinctIf(country_iso2, country_iso2 IS NOT NULL) AS country_count,
        now64(3) AS resolved_at
    FROM {_qualified_table(company_links_stage)}
    GROUP BY root_domain
    """


def _table_count(clickhouse_client: Any, table: str) -> int:
    rows = clickhouse_client.execute(f"SELECT count() FROM {_qualified_table(table)}")
    return int(rows[0][0]) if rows else 0


def _drop_stage_tables(
    clickhouse_client: Any,
    stage_tables: list[str],
    *,
    suppress_errors: bool,
) -> None:
    first_error: Exception | None = None
    failed_tables: list[str] = []
    for table in stage_tables:
        try:
            clickhouse_client.execute(f"DROP TABLE IF EXISTS {table}")
        except Exception as exc:
            if suppress_errors:
                continue
            if first_error is None:
                first_error = exc
            failed_tables.append(table)

    if first_error is not None:
        raise RuntimeError(
            "Failed to drop domain stage table(s): " + ", ".join(failed_tables)
        ) from first_error


def _stage_table_name(table: str) -> str:
    return f"_tmp_{table}_{uuid.uuid4().hex}"


def _column_list(columns: tuple[str, ...]) -> str:
    return ", ".join(_quote_identifier(column) for column in columns)


def _qualified_table(table: str) -> str:
    return f"{_quote_identifier(RESOLVED_DATABASE)}.{_quote_identifier(table)}"


def _quote_identifier(identifier: str) -> str:
    escaped = identifier.replace("`", "``")
    return f"`{escaped}`"


@dg.definitions
def defs() -> dg.Definitions:
    return dg.Definitions(assets=[domains_clickhouse])
