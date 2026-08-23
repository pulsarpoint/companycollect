import dagster as dg


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


def _job_assets(repository: object, job_name: str) -> set[str]:
    job = repository.get_job(job_name)  # type: ignore[attr-defined]
    return {key.path[-1] for key in job.asset_layer.executable_asset_keys}
