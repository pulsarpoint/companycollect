import inspect
from pathlib import Path

import dagster as dg


def test_sweden_financial_backfill_and_current_assets_are_separate() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    schedule = repo.get_schedule_def("sweden_financial_current_year_weekly")
    assert schedule.cron_schedule == "45 6 * * 6"
    assert schedule.job.name == "sweden_financial_current_year_job"

    backfill_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_backfill_job"
        ).asset_layer.executable_asset_keys
    }
    assert backfill_asset_keys == {
        "sweden_financial_backfill_raw_archives_s3",
        "sweden_financial_backfill_report_xhtml_catalog_duckdb",
        "sweden_financial_backfill_parsed_reports_duckdb",
    }

    current_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_current_year_job"
        ).asset_layer.executable_asset_keys
    }
    assert current_job_asset_keys == {
        "sweden_financial_current_raw_archives_s3",
        "sweden_financial_current_report_xhtml_catalog_duckdb",
        "sweden_financial_current_parsed_reports_duckdb",
    }

    backfill_raw_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_backfill_raw_archives_s3")
    )
    assert backfill_raw_node.group_name == "sweden_financial"
    assert backfill_raw_node.pools == set()
    assert (
        type(backfill_raw_node.partitions_def).__name__ == "StaticPartitionsDefinition"
    )
    assert backfill_raw_node.partitions_def.get_partition_keys() == [
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
        "2026",
    ]

    backfill_catalog_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_backfill_report_xhtml_catalog_duckdb")
    )
    assert backfill_catalog_node.group_name == "sweden_financial"
    assert backfill_catalog_node.pools == set()
    assert backfill_catalog_node.parent_keys == {
        dg.AssetKey("sweden_financial_backfill_raw_archives_s3")
    }
    assert backfill_catalog_node.partitions_def is backfill_raw_node.partitions_def

    backfill_parsed_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_backfill_parsed_reports_duckdb")
    )
    assert backfill_parsed_node.group_name == "sweden_financial"
    assert backfill_parsed_node.pools == set()
    assert backfill_parsed_node.parent_keys == {
        dg.AssetKey("sweden_financial_backfill_report_xhtml_catalog_duckdb")
    }
    assert backfill_parsed_node.partitions_def is backfill_raw_node.partitions_def

    current_raw_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_current_raw_archives_s3")
    )
    assert current_raw_node.group_name == "sweden_financial"
    assert current_raw_node.pools == set()
    assert (
        type(current_raw_node.partitions_def).__name__ == "StaticPartitionsDefinition"
    )
    current_keys = current_raw_node.partitions_def.get_partition_keys()
    assert current_keys[0] == "2026-07-04"
    assert current_keys[1] == "2026-07-11"
    assert current_keys[-1] == "2026-12-26"
    assert "2026" not in current_keys

    current_catalog_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_current_report_xhtml_catalog_duckdb")
    )
    assert current_catalog_node.group_name == "sweden_financial"
    assert current_catalog_node.pools == {"sweden_financial_current_2026_duckdb"}
    assert current_catalog_node.parent_keys == {
        dg.AssetKey("sweden_financial_current_raw_archives_s3")
    }
    assert current_catalog_node.partitions_def is current_raw_node.partitions_def

    current_parsed_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_current_parsed_reports_duckdb")
    )
    assert current_parsed_node.group_name == "sweden_financial"
    assert current_parsed_node.pools == {"sweden_financial_current_2026_duckdb"}
    assert current_parsed_node.parent_keys == {
        dg.AssetKey("sweden_financial_current_report_xhtml_catalog_duckdb")
    }
    assert current_parsed_node.partitions_def is current_raw_node.partitions_def

    clickhouse_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_clickhouse_job"
        ).asset_layer.executable_asset_keys
    }
    assert clickhouse_job_asset_keys == {
        "sweden_financial_reports_clickhouse",
        "sweden_financial_facts_clickhouse",
    }

    for asset_key in (
        "sweden_financial_reports_clickhouse",
        "sweden_financial_facts_clickhouse",
    ):
        clickhouse_node = repo.asset_graph.get(dg.AssetKey(asset_key))
        assert clickhouse_node.group_name == "sweden_financial"
        assert clickhouse_node.pools == set()
        assert clickhouse_node.partitions_def is None
        assert clickhouse_node.parent_keys == {
            dg.AssetKey("sweden_financial_backfill_parsed_reports_duckdb"),
            dg.AssetKey("sweden_financial_current_parsed_reports_duckdb"),
        }


def test_sweden_financial_raw_assets_do_not_require_duckdb_resource() -> None:
    from dagster_v3.defs.sweden_financial.assets import (
        sweden_financial_backfill_raw_archives_s3,
        sweden_financial_backfill_report_xhtml_catalog_duckdb,
        sweden_financial_current_raw_archives_s3,
        sweden_financial_current_report_xhtml_catalog_duckdb,
    )

    backfill_parameters = inspect.signature(
        sweden_financial_backfill_raw_archives_s3
    ).parameters
    backfill_catalog_parameters = inspect.signature(
        sweden_financial_backfill_report_xhtml_catalog_duckdb
    ).parameters
    current_parameters = inspect.signature(
        sweden_financial_current_raw_archives_s3
    ).parameters
    current_catalog_parameters = inspect.signature(
        sweden_financial_current_report_xhtml_catalog_duckdb
    ).parameters

    assert "sweden_financial_duckdb" not in backfill_parameters
    assert "sweden_financial_duckdb" not in backfill_catalog_parameters
    assert "sweden_financial_duckdb" not in current_parameters
    assert "sweden_financial_duckdb" not in current_catalog_parameters


def test_sweden_financial_duckdb_path_is_partitioned_by_year() -> None:
    from dagster_v3.defs.sweden_financial.parsing import (
        sweden_financial_source_duckdb_path,
    )

    assert sweden_financial_source_duckdb_path("2026") == Path(
        "data/sweden_financial/sweden_financial_source_2026.duckdb"
    )


def test_sweden_financial_docs_describe_raw_archive_scope() -> None:
    doc_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "dagster_v3"
        / "defs"
        / "sweden_financial"
        / "docs"
        / "sweden_financial-design.md"
    )

    text = doc_path.read_text(encoding="utf-8")
    assert "outer ZIP" in text
    assert "archive sync manifest" in text
    assert "XHTML extraction" in text
