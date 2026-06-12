"""Asset checks for Finland PRH YTJ ClickHouse tables."""

import dagster as dg

from dagster_corpscout.resources.clickhouse import ClickHouseResource
from dagster_corpscout.sources.finland.prh_ytj.assets import (
    company_explorer_cache,
    industry_nace_mappings,
)
from dagster_corpscout.sources.finland.prh_ytj.explorer_cache import (
    COMPANY_EXPLORER_CACHE_TABLE,
    COMPANY_EXPLORER_VIEW_TABLE,
)
from dagster_corpscout.sources.finland.prh_ytj.industry_mapping import INDUSTRY_NACE_MAPPING_TABLE


def check_industry_nace_mappings_have_rows(clickhouse) -> dg.AssetCheckResult:
    client = clickhouse.client()
    rows, mapped_rows, unmapped_rows = client.query(
        "SELECT count(), "
        "countIf(`mapping_status` = 'mapped'), "
        "countIf(`mapping_status` = 'unmapped') "
        f"FROM {_qualified_table(clickhouse.database, INDUSTRY_NACE_MAPPING_TABLE)}"
    ).result_rows[0]
    return dg.AssetCheckResult(
        passed=rows > 0 and mapped_rows > 0,
        metadata={
            "rows": int(rows),
            "mapped_rows": int(mapped_rows),
            "unmapped_rows": int(unmapped_rows),
        },
    )


def check_company_explorer_cache_matches_view(clickhouse) -> dg.AssetCheckResult:
    client = clickhouse.client()
    explorer_rows = int(
        client.query(
            f"SELECT count() FROM {_qualified_table(clickhouse.database, COMPANY_EXPLORER_VIEW_TABLE)}"
        ).result_rows[0][0]
    )
    cache_rows = int(
        client.query(
            f"SELECT count() FROM {_qualified_table(clickhouse.database, COMPANY_EXPLORER_CACHE_TABLE)}"
        ).result_rows[0][0]
    )
    return dg.AssetCheckResult(
        passed=explorer_rows > 0 and cache_rows == explorer_rows,
        metadata={
            "explorer_rows": explorer_rows,
            "cache_rows": cache_rows,
        },
    )


@dg.asset_check(asset=industry_nace_mappings, name="mapping_rows_present")
def industry_nace_mappings_rows_present(
    clickhouse: ClickHouseResource,
) -> dg.AssetCheckResult:
    return check_industry_nace_mappings_have_rows(clickhouse)


@dg.asset_check(asset=company_explorer_cache, name="cache_matches_explorer_view")
def company_explorer_cache_matches_view(
    clickhouse: ClickHouseResource,
) -> dg.AssetCheckResult:
    return check_company_explorer_cache_matches_view(clickhouse)


def _qualified_table(database: str, table: str) -> str:
    return f"{_quote_ident(database)}.{_quote_ident(table)}"


def _quote_ident(identifier: str) -> str:
    return "`" + identifier.replace("`", "``") + "`"
