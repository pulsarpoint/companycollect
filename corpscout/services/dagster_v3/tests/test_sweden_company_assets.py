from pathlib import Path

import dagster as dg


def test_sweden_company_refresh_job_and_schedule_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    schedule = repo.get_schedule_def("sweden_company_refresh_weekly")
    assert schedule.cron_schedule == "0 22 * * 1"
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
        "sweden_company_scb_companies_clickhouse",
        "sweden_company_bolagsverket_companies_clickhouse",
        "sweden_company_profile_history_clickhouse",
        "sweden_company_industries_clickhouse",
        "sweden_company_industry_history_clickhouse",
    }
    assert "sweden_company_clickhouse" not in asset_keys
    assert "sweden_company_addresses_clickhouse" not in asset_keys

    asset_graph = repo.asset_graph
    asset_node = asset_graph.get(dg.AssetKey("sweden_company_raw_snapshot_s3"))
    assert asset_node.group_name == "sweden_company"
    duckdb_node = asset_graph.get(dg.AssetKey("sweden_company_raw_duckdb"))
    assert duckdb_node.group_name == "sweden_company"
    normalized_node = asset_graph.get(dg.AssetKey("sweden_company_normalized_duckdb"))
    assert normalized_node.group_name == "sweden_company"
    for asset_key in (
        "sweden_company_scb_companies_clickhouse",
        "sweden_company_bolagsverket_companies_clickhouse",
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


def test_sweden_company_refresh_weekly_feeds_the_address_chain() -> None:
    """Re-enabled 2026-09-25 (spec 2026-09-24-address-weekly-chain-design.md, follow-up): the
    registers were last loaded by hand on 2026-09-03 while Bolagsverket republishes both bulk
    files every Monday. Monday 22:00 Stockholm loads them (about 40 min) before the Sweden
    address chain starts at 01:05 Tuesday, so the address extractors see fresh registers."""
    from dagster_v3.defs.sweden_company.assets import sweden_company_refresh_weekly

    assert sweden_company_refresh_weekly.cron_schedule == "0 22 * * 1"
    assert sweden_company_refresh_weekly.execution_timezone == "Europe/Stockholm"
    assert sweden_company_refresh_weekly.default_status == dg.DefaultScheduleStatus.RUNNING
    assert sweden_company_refresh_weekly.job.name == "sweden_company_refresh_job"


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
    assert ("sweden_company_scb_companies_clickhouse", "identities_normalized") in names
