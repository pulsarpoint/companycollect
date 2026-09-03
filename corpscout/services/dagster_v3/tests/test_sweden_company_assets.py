from pathlib import Path

import dagster as dg


def test_sweden_company_refresh_job_and_schedule_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    schedule = repo.get_schedule_def("sweden_company_refresh_weekly")
    assert schedule.cron_schedule == "15 6 * * 1"
    assert schedule.job.name == "sweden_company_refresh_job"

    asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_company_refresh_job"
        ).asset_layer.executable_asset_keys
    }
    assert asset_keys == {
        "sweden_company_raw_snapshot_s3",
        "sweden_company_raw_duckdb",
        "sweden_company_normalized_duckdb",
        "sweden_company_companies_clickhouse",
        "sweden_company_scb_companies_clickhouse",
        "sweden_company_profile_history_clickhouse",
        "sweden_company_addresses_clickhouse",
        "sweden_company_industries_clickhouse",
        "sweden_company_industry_history_clickhouse",
    }
    assert "sweden_company_clickhouse" not in asset_keys

    asset_graph = repo.asset_graph
    asset_node = asset_graph.get(dg.AssetKey("sweden_company_raw_snapshot_s3"))
    assert asset_node.group_name == "sweden_company"
    duckdb_node = asset_graph.get(dg.AssetKey("sweden_company_raw_duckdb"))
    assert duckdb_node.group_name == "sweden_company"
    normalized_node = asset_graph.get(dg.AssetKey("sweden_company_normalized_duckdb"))
    assert normalized_node.group_name == "sweden_company"
    for asset_key in (
        "sweden_company_companies_clickhouse",
        "sweden_company_scb_companies_clickhouse",
        "sweden_company_addresses_clickhouse",
        "sweden_company_industries_clickhouse",
    ):
        clickhouse_node = asset_graph.get(dg.AssetKey(asset_key))
        assert clickhouse_node.group_name == "sweden_company"
        assert clickhouse_node.parent_keys == {
            dg.AssetKey("sweden_company_normalized_duckdb")
        }
        # Read-only exporters share the source DuckDB pool: a DuckDB writer
        # excludes readers across processes, so unpooled reads collide with
        # a concurrent refresh (see data-source-guidelines).
        assert clickhouse_node.pools == {"sweden_company_duckdb"}


def test_sweden_company_docs_describe_registry_pipeline_scope() -> None:
    doc_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "dagster_v3"
        / "defs"
        / "sweden_company"
        / "docs"
        / "sweden_company-design.md"
    )

    text = doc_path.read_text(encoding="utf-8")
    assert "raw ZIP" in text
    assert "ClickHouse" in text
    assert "weekly" in text


def test_identities_normalized_check_registered() -> None:
    from dagster_v3.defs.sweden_company.assets import defs

    check_specs = [
        spec
        for checks_def in defs.asset_checks or []
        for spec in checks_def.check_specs
    ]
    names = {(spec.asset_key.to_user_string(), spec.name) for spec in check_specs}
    assert ("sweden_company_companies_clickhouse", "identities_normalized") in names
