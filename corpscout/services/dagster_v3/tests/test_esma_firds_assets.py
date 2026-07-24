import dagster as dg


def _repository():
    from dagster_v3.definitions import defs as load_defs

    return load_defs().get_repository_def()


def test_esma_firds_assets_are_registered_with_explicit_dependencies_and_pool() -> None:
    repository = _repository()
    graph = repository.asset_graph
    expected = {
        "esma_firds_full_raw_files_s3",
        "esma_firds_delta_raw_files_s3",
        "esma_firds_cancellations_raw_files_s3",
        "esma_firds_instrument_events_duckdb",
        "esma_firds_instruments_current_duckdb",
        "esma_firds_clickhouse",
    }

    assert expected.issubset(
        {key.path[-1] for key in graph.get_all_asset_keys()}
    )
    duckdb_node = graph.get(
        dg.AssetKey("esma_firds_instrument_events_duckdb")
    )
    assert duckdb_node.parent_keys == {
        dg.AssetKey("esma_firds_full_raw_files_s3"),
        dg.AssetKey("esma_firds_delta_raw_files_s3"),
        dg.AssetKey("esma_firds_cancellations_raw_files_s3"),
    }
    assert duckdb_node.pools == {"esma_firds_duckdb"}

    current_node = graph.get(
        dg.AssetKey("esma_firds_instruments_current_duckdb")
    )
    assert current_node.parent_keys == {
        dg.AssetKey("esma_firds_instrument_events_duckdb")
    }
    assert current_node.pools == {"esma_firds_duckdb"}

    clickhouse_node = graph.get(dg.AssetKey("esma_firds_clickhouse"))
    assert clickhouse_node.parent_keys == {
        dg.AssetKey("esma_firds_instruments_current_duckdb")
    }
    assert clickhouse_node.pools == {"esma_firds_duckdb"}


def test_esma_firds_jobs_separate_daily_delta_from_weekly_full_refresh() -> None:
    repository = _repository()
    daily = repository.get_job("esma_firds_delta_refresh_job")
    weekly = repository.get_job("esma_firds_weekly_refresh_job")

    assert {
        key.path[-1] for key in daily.asset_layer.executable_asset_keys
    } == {
        "esma_firds_delta_raw_files_s3",
        "esma_firds_instrument_events_duckdb",
        "esma_firds_instruments_current_duckdb",
        "esma_firds_clickhouse",
    }
    assert {
        key.path[-1] for key in weekly.asset_layer.executable_asset_keys
    } == {
        "esma_firds_full_raw_files_s3",
        "esma_firds_delta_raw_files_s3",
        "esma_firds_cancellations_raw_files_s3",
        "esma_firds_instrument_events_duckdb",
        "esma_firds_instruments_current_duckdb",
        "esma_firds_clickhouse",
    }


def test_esma_firds_schedules_are_stopped_until_live_validation() -> None:
    repository = _repository()
    daily = repository.get_schedule_def("esma_firds_delta_daily")
    weekly = repository.get_schedule_def("esma_firds_full_weekly")

    assert daily.job_name == "esma_firds_delta_refresh_job"
    assert daily.cron_schedule == "40 10 * * *"
    assert daily.execution_timezone == "Europe/Paris"
    assert daily.default_status == dg.DefaultScheduleStatus.STOPPED

    assert weekly.job_name == "esma_firds_weekly_refresh_job"
    assert weekly.cron_schedule == "50 12 * * 0"
    assert weekly.execution_timezone == "Europe/Paris"
    assert weekly.default_status == dg.DefaultScheduleStatus.STOPPED
