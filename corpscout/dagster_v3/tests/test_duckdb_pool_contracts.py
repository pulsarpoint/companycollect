"""Single-writer DuckDB pool contracts.

Every asset that opens a module's DuckDB file(s) must carry that module's
concurrency pool so the instance-level ``pools.default_limit: 1`` serializes
the writers (see CLAUDE.md "DuckDB (per-source file)").
"""

import dagster as dg

FINLAND_XBRL_DUCKDB_ASSETS = (
    "data_snapshot_duckdb",
    "data_snapshot_duckdb_ch",
    "data_daily_duckdb",
    "data_daily_duckdb_ch",
    "data_snapshot_xml_duckdb",
    "data_daily_xml_duckdb",
    "fi_financial_statements_ch",
    "fi_financial_metrics_parquet",
)

NACE_DUCKDB_ASSETS = (
    "nace_raw_duckdb",
    "nace_categories_duckdb",
    "nace_categories_clickhouse",
)


def _asset_graph():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def().asset_graph


def test_finland_xbrl_duckdb_assets_share_the_module_pool() -> None:
    asset_graph = _asset_graph()
    for asset_name in FINLAND_XBRL_DUCKDB_ASSETS:
        node = asset_graph.get(dg.AssetKey(asset_name))
        assert node.pools == {"finland_xbrl_duckdb"}, asset_name


def test_nace_duckdb_assets_share_the_module_pool() -> None:
    asset_graph = _asset_graph()
    for asset_name in NACE_DUCKDB_ASSETS:
        node = asset_graph.get(dg.AssetKey(asset_name))
        assert node.pools == {"nace_duckdb"}, asset_name
