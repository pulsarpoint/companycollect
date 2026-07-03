from pathlib import Path

from dagster_v3.defs.sweden_company import assets


def test_sweden_company_raw_asset_job_and_schedule_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()
    schedule = repo.get_schedule_def("sweden_company_raw_snapshot_weekly")
    assert schedule.cron_schedule == "15 6 * * 1"
    assert schedule.job.name == "sweden_company_raw_snapshot_job"

    asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_company_raw_snapshot_job"
        ).asset_layer.executable_asset_keys
    }
    assert asset_keys == {
        "sweden_company_raw_snapshot_s3",
        "sweden_company_raw_duckdb",
    }

    asset_graph = repo.asset_graph
    asset_node = asset_graph.get(assets.SWEDEN_COMPANY_RAW_ASSET_KEY)
    assert asset_node.group_name == "sweden_company"
    duckdb_node = asset_graph.get(assets.SWEDEN_COMPANY_RAW_DUCKDB_ASSET_KEY)
    assert duckdb_node.group_name == "sweden_company"


def test_sweden_company_docs_describe_raw_download_only_scope() -> None:
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
    assert "No parsing" in text
    assert "weekly" in text
