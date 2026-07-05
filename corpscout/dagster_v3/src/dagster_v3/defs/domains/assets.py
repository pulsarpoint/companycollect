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
        dg.AssetKey("czech_ares_clickhouse_company_contacts"),
        dg.AssetKey("latvia_ur_clickhouse_company_contacts"),
        dg.AssetKey("estonia_ar_clickhouse_company_domains"),
        dg.AssetKey("brazil_comp_rfb_clickhouse_company_domains"),
        dg.AssetKey("norway_brreg_clickhouse_canonical_contacts"),
        dg.AssetKey("finland_ytj_clickhouse_canonical_contacts"),
        dg.AssetKey("wikidata_clickhouse_canonical_contacts"),
    ],
    group_name=GROUP_NAME,
    kinds={"clickhouse"},
    description=(
        "Builds the website domain dimension and company-to-domain links from "
        "the seven canonical <src>_company_domains tables."
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

        clickhouse_client.execute(
            _company_website_domains_insert_sql(company_links_stage)
        )
        clickhouse_client.execute(
            _domains_insert_sql(domains_stage, company_links_stage)
        )

        # links swap first: the aggregate is computed FROM the links and must
        # never be newer than what links readers see
        clickhouse_client.execute(
            f"EXCHANGE TABLES {_qualified_table(company_links_stage)} "
            f"AND {_qualified_table(tables.COMPANY_WEBSITE_DOMAINS_TABLE)}"
        )
        clickhouse_client.execute(
            f"EXCHANGE TABLES {_qualified_table(domains_stage)} "
            f"AND {_qualified_table(tables.DOMAINS_TABLE)}"
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


def _canonical_domain_arm(source: dict[str, str]) -> str:
    table = source["table"]
    return f"""
        SELECT
            '{table}' AS source_website_table,
            ifNull(
                nullIf(trim(websites.source_record_id), ''),
                concat('{table}:', websites.registry_id, ':', websites.domain)
            ) AS source_website_id,
            nullIf(trim(websites.country_iso2), '') AS country_iso2,
            '{source["source_slug"]}' AS source_slug,
            '{source["registry_id_type"]}' AS company_id_type,
            websites.registry_id AS company_id,
            websites.website_url AS website_url,
            websites.website_normalized_url AS website_normalized_url,
            websites.website_host AS website_host,
            websites.domain AS root_domain,
            websites.domain_source AS domain_source,
            websites.is_current AS is_current,
            websites.is_primary AS is_primary
        FROM {_qualified_table(table)} AS websites
        WHERE nullIf(trim(websites.domain), '') IS NOT NULL"""


def _company_website_domains_insert_sql(stage_table: str) -> str:
    columns = _column_list(tables.COMPANY_WEBSITE_DOMAINS_COLUMNS)
    arms = "\n\n        UNION ALL\n".join(
        _canonical_domain_arm(source) for source in tables.CANONICAL_DOMAIN_SOURCES
    )
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
        domain_source,
        is_current,
        is_primary,
        now64(3) AS resolved_at
    FROM
    (
{arms}
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
