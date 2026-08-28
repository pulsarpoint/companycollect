import tempfile
import zipfile
from contextlib import nullcontext
from pathlib import Path

import dagster as dg
import pytest

from dagster_v3.defs.sweden_platsbanken import assets


class _DuckDBResource:
    def get_connection(self) -> nullcontext[object]:
        return nullcontext(object())


class _HistoricalObjectStore:
    def __init__(self) -> None:
        self.downloaded_paths: list[Path] = []

    def download_file(
        self,
        key: str,
        target_path: Path,
        bucket: str | None = None,
    ) -> None:
        if self.downloaded_paths:
            assert not self.downloaded_paths[-1].parent.exists()
        with zipfile.ZipFile(target_path, "w") as archive:
            archive.writestr("jobs.jsonl", f'{{"object_key":"{key}"}}\n')
        self.downloaded_paths.append(target_path)


def test_platsbanken_assets_and_manual_jobs_are_registered() -> None:
    from dagster_v3.definitions import defs as load_defs

    repository = load_defs().get_repository_def()
    asset_graph = repository.asset_graph
    expected_assets = {
        "sweden_platsbanken_historical_archives_s3",
        "sweden_platsbanken_historical_raw_duckdb",
        "sweden_platsbanken_historical_normalized_duckdb",
        "sweden_platsbanken_historical_clickhouse",
        "sweden_platsbanken_jobstream_snapshot_s3",
        "sweden_platsbanken_jobstream_snapshot_raw_duckdb",
        "sweden_platsbanken_jobstream_snapshot_normalized_duckdb",
        "sweden_platsbanken_jobstream_snapshot_clickhouse",
        "sweden_platsbanken_jobstream_events_s3",
        "sweden_platsbanken_jobstream_events_raw_duckdb",
        "sweden_platsbanken_jobstream_events_normalized_duckdb",
        "sweden_platsbanken_jobstream_events_clickhouse",
        "sweden_platsbanken_company_jobs_clickhouse",
    }
    assert expected_assets.issubset(
        {key.path[-1] for key in asset_graph.get_all_asset_keys()}
    )

    for asset_name in expected_assets:
        node = asset_graph.get(dg.AssetKey(asset_name))
        assert node.group_name == "sweden_platsbanken"
        assert node.partitions_def is None
        if "duckdb" in asset_name or asset_name.endswith("historical_clickhouse"):
            assert node.pools == {"sweden_platsbanken_duckdb"}
        if asset_name.endswith("snapshot_clickhouse"):
            assert node.pools == {"sweden_platsbanken_duckdb"}
        if asset_name.endswith("events_clickhouse"):
            assert node.pools == {"sweden_platsbanken_duckdb"}

    assert _job_assets(repository, "sweden_platsbanken_historical_backfill_job") == {
        "sweden_platsbanken_historical_archives_s3",
        "sweden_platsbanken_historical_raw_duckdb",
        "sweden_platsbanken_historical_normalized_duckdb",
        "sweden_platsbanken_historical_clickhouse",
        "sweden_platsbanken_company_jobs_clickhouse",
    }
    assert _job_assets(repository, "sweden_platsbanken_jobstream_bootstrap_job") == {
        "sweden_platsbanken_jobstream_snapshot_s3",
        "sweden_platsbanken_jobstream_snapshot_raw_duckdb",
        "sweden_platsbanken_jobstream_snapshot_normalized_duckdb",
        "sweden_platsbanken_jobstream_snapshot_clickhouse",
        "sweden_platsbanken_company_jobs_clickhouse",
    }
    incremental_assets = _job_assets(
        repository,
        "sweden_platsbanken_jobstream_incremental_job",
    )
    assert incremental_assets == {
        "sweden_platsbanken_jobstream_events_s3",
        "sweden_platsbanken_jobstream_events_raw_duckdb",
        "sweden_platsbanken_jobstream_events_normalized_duckdb",
        "sweden_platsbanken_jobstream_events_clickhouse",
        "sweden_platsbanken_company_jobs_clickhouse",
    }
    assert "sweden_platsbanken_jobstream_snapshot_s3" not in incremental_assets


def test_company_projection_requires_all_history_sources_and_company_spine() -> None:
    from dagster_v3.definitions import defs as load_defs

    node = (
        load_defs()
        .resolve_asset_graph()
        .get(dg.AssetKey("sweden_platsbanken_company_jobs_clickhouse"))
    )
    assert node.parent_keys == {
        dg.AssetKey("sweden_platsbanken_historical_clickhouse"),
        dg.AssetKey("sweden_platsbanken_jobstream_snapshot_clickhouse"),
        dg.AssetKey("sweden_platsbanken_jobstream_events_clickhouse"),
        dg.AssetKey("sweden_company_companies_clickhouse"),
    }


def test_historical_raw_duckdb_releases_each_archive_before_downloading_next(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original_temporary_directory = tempfile.TemporaryDirectory
    object_store = _HistoricalObjectStore()
    loaded_paths: list[Path] = []

    monkeypatch.setattr(
        assets.tempfile,
        "TemporaryDirectory",
        lambda **kwargs: original_temporary_directory(dir=tmp_path, **kwargs),
    )
    monkeypatch.setattr(
        assets,
        "latest_historical_manifest",
        lambda _object_store: {
            "source_run_id": "historical-run",
            "retrieved_at": "2026-08-28T12:00:00+00:00",
            "archives": [
                {
                    "object_key": "historical/2016.zip",
                    "source_url": "https://example.test/2016.zip",
                },
                {
                    "object_key": "historical/2017.zip",
                    "source_url": "https://example.test/2017.zip",
                },
            ],
        },
    )

    def load_jsonl(**parameters: object) -> int:
        jsonl_path = parameters["jsonl_path"]
        assert isinstance(jsonl_path, Path)
        assert jsonl_path.exists()
        loaded_paths.append(jsonl_path)
        return 1

    monkeypatch.setattr(assets, "replace_raw_jsonl_table", load_jsonl)
    monkeypatch.setattr(assets, "append_raw_jsonl_table", load_jsonl)

    result = assets.sweden_platsbanken_historical_raw_duckdb.node_def.compute_fn.decorated_fn(
        sweden_platsbanken_duckdb=_DuckDBResource(),
        sweden_platsbanken_object_store=object_store,
    )

    assert result.metadata["archive_count"] == 2
    assert result.metadata["raw_rows"] == 2
    assert len(loaded_paths) == 2
    assert all(not path.parent.exists() for path in loaded_paths)


def _job_assets(repository: object, job_name: str) -> set[str]:
    job = repository.get_job(job_name)  # type: ignore[attr-defined]
    return {key.path[-1] for key in job.asset_layer.executable_asset_keys}
