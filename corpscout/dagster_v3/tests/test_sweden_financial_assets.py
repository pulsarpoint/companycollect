from pathlib import Path

import dagster as dg


def test_sweden_financial_raw_archive_job_and_schedule_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    schedule = repo.get_schedule_def("sweden_financial_raw_archives_weekly")
    assert schedule.cron_schedule == "45 6 * * 1"
    assert schedule.job.name == "sweden_financial_raw_archives_refresh_job"

    asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_raw_archives_refresh_job"
        ).asset_layer.executable_asset_keys
    }
    assert asset_keys == {"sweden_financial_raw_archives_s3"}

    asset_node = repo.asset_graph.get(dg.AssetKey("sweden_financial_raw_archives_s3"))
    assert asset_node.group_name == "sweden_financial"


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
