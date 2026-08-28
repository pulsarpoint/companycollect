import tempfile
import zipfile
from contextlib import nullcontext
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest

from dagster_v3.defs.sweden_platsbanken import assets
from dagster_v3.defs.sweden_platsbanken import tables


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

    historical_assets = {
        "sweden_platsbanken_historical_archives_s3",
        "sweden_platsbanken_historical_raw_duckdb",
        "sweden_platsbanken_historical_normalized_duckdb",
        "sweden_platsbanken_historical_clickhouse",
    }
    for asset_name in expected_assets:
        node = asset_graph.get(dg.AssetKey(asset_name))
        assert node.group_name == "sweden_platsbanken"
        if asset_name in historical_assets:
            assert node.partitions_def == assets.HISTORICAL_PARTITIONS
            assert node.backfill_policy == assets.HISTORICAL_BACKFILL_POLICY
            assert node.pools == {"sweden_platsbanken_duckdb"}
        else:
            assert node.partitions_def is None
        if "duckdb" in asset_name:
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
    }
    assert _job_assets(repository, "sweden_platsbanken_company_jobs_job") == {
        "sweden_platsbanken_company_jobs_clickhouse"
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


def test_historical_partitions_cover_complete_and_current_archive_years() -> None:
    partition_keys = assets.HISTORICAL_PARTITIONS.get_partition_keys(
        current_time=datetime(2026, 8, 28, tzinfo=UTC)
    )

    assert partition_keys == [str(year) for year in range(2016, 2027)]


def test_historical_partition_uses_an_isolated_duckdb_file() -> None:
    first = tables.partition_duckdb_path("2016")
    second = tables.partition_duckdb_path("2017")

    assert first != second
    assert "partition_key=2016" in str(first)
    assert "partition_key=2017" in str(second)

    for invalid in ("", "../escape", "2016-1", "not-a-year"):
        with pytest.raises(ValueError):
            tables.partition_duckdb_path(invalid)


def test_historical_raw_duckdb_releases_each_archive_before_downloading_next(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    original_temporary_directory = tempfile.TemporaryDirectory
    object_store = _HistoricalObjectStore()
    loaded_paths: list[Path] = []
    opened_partitions: list[tuple[str, str]] = []

    monkeypatch.setattr(
        assets.tempfile,
        "TemporaryDirectory",
        lambda **kwargs: original_temporary_directory(dir=tmp_path, **kwargs),
    )
    monkeypatch.setattr(
        assets,
        "latest_historical_manifest",
        lambda _object_store, partition_year: {
            "source_run_id": "historical-run",
            "retrieved_at": "2026-08-28T12:00:00+00:00",
            "partition_year": partition_year,
            "archives": [
                {
                    "object_key": "historical/2016.zip",
                    "source_url": "https://example.test/2016.zip",
                },
                {
                    "object_key": "historical/2016-Q4.zip",
                    "source_url": "https://example.test/2016-Q4.zip",
                },
            ],
        },
    )

    def open_partition(*, source: str, partition: str) -> nullcontext[object]:
        opened_partitions.append((source, partition))
        return nullcontext(object())

    monkeypatch.setattr(assets, "open_partition_duckdb", open_partition)
    monkeypatch.setattr(
        assets, "apply_duckdb_runtime_settings", lambda *args, **kwargs: None
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
        context=dg.build_asset_context(partition_key="2016"),
        sweden_platsbanken_object_store=object_store,
    )

    assert result.metadata["archive_count"] == 2
    assert result.metadata["raw_rows"] == 2
    assert result.metadata["partition_year"] == "2016"
    assert opened_partitions == [("sweden_platsbanken", "2016")]
    assert len(loaded_paths) == 2
    assert all(not path.parent.exists() for path in loaded_paths)


def _job_assets(repository: object, job_name: str) -> set[str]:
    job = repository.get_job(job_name)  # type: ignore[attr-defined]
    return {key.path[-1] for key in job.asset_layer.executable_asset_keys}
