from pathlib import Path

import dagster as dg


def test_sweden_financial_year_partitioned_jobs_and_schedule_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    schedule = repo.get_schedule_def("sweden_financial_current_year_weekly")
    assert schedule.cron_schedule == "45 6 * * 1"
    assert schedule.job.name == "sweden_financial_current_year_job"

    asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_backfill_job"
        ).asset_layer.executable_asset_keys
    }
    assert asset_keys == {
        "sweden_financial_raw_archives_s3",
        "sweden_financial_report_xhtml_catalog_duckdb",
    }

    current_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_current_year_job"
        ).asset_layer.executable_asset_keys
    }
    assert current_job_asset_keys == asset_keys

    asset_node = repo.asset_graph.get(dg.AssetKey("sweden_financial_raw_archives_s3"))
    assert asset_node.group_name == "sweden_financial"
    assert type(asset_node.partitions_def).__name__ == "StaticPartitionsDefinition"
    assert asset_node.partitions_def.get_partition_keys()[:6] == [
        "2020",
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
    ]

    catalog_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_report_xhtml_catalog_duckdb")
    )
    assert catalog_node.group_name == "sweden_financial"
    assert catalog_node.parent_keys == {dg.AssetKey("sweden_financial_raw_archives_s3")}
    assert catalog_node.partitions_def is asset_node.partitions_def


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
    assert "no manifest" in text
    assert "XHTML extraction" in text
