from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.ats_clickhouse import (
    AtsClickhouseTables,
    _insert_lifecycle_events,
)


PROVIDERS = ("greenhouse", "lever", "ashby", "smartrecruiters")


def test_sweden_ats_assets_are_independent_and_schedules_start_stopped() -> None:
    from dagster_v3.defs.sweden_ashby.assets import defs as ashby_defs
    from dagster_v3.defs.sweden_greenhouse.assets import defs as greenhouse_defs
    from dagster_v3.defs.sweden_lever.assets import defs as lever_defs
    from dagster_v3.defs.sweden_smartrecruiters.assets import (
        defs as smartrecruiters_defs,
    )

    repository = dg.Definitions.merge(
        greenhouse_defs,
        lever_defs,
        ashby_defs,
        smartrecruiters_defs,
        dg.Definitions(
            resources={
                "clickhouse": ClickhouseResource(
                    host="localhost",
                    user="default",
                    password="",
                    database="corpscout",
                )
            }
        ),
    ).get_repository_def()
    graph = repository.asset_graph

    for provider in PROVIDERS:
        asset_names = {
            f"sweden_{provider}_snapshot_s3",
            f"sweden_{provider}_snapshot_duckdb",
            f"sweden_{provider}_snapshot_clickhouse",
        }
        for asset_name in asset_names:
            node = graph.get(dg.AssetKey(asset_name))
            assert node.group_name == f"sweden_{provider}"
            assert all("platsbanken" not in key.path[-1] for key in node.parent_keys)

        job = repository.get_job(f"sweden_{provider}_snapshot_job")
        assert {
            key.path[-1] for key in job.asset_layer.executable_asset_keys
        } == asset_names

        schedule = repository.get_schedule_def(f"sweden_{provider}_daily_schedule")
        assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED


def test_sweden_ats_sources_have_separate_storage_contracts() -> None:
    from dagster_v3.defs.sweden_ashby import tables as ashby
    from dagster_v3.defs.sweden_greenhouse import tables as greenhouse
    from dagster_v3.defs.sweden_lever import tables as lever
    from dagster_v3.defs.sweden_smartrecruiters import tables as smartrecruiters

    contracts = (greenhouse, lever, ashby, smartrecruiters)
    assert len({module.S3_BUCKET for module in contracts}) == len(contracts)
    assert len({module.DUCKDB_FILE_NAME for module in contracts}) == len(contracts)
    assert len({module.BOARDS_TABLE for module in contracts}) == len(contracts)
    assert len({module.CURRENT_TABLE for module in contracts}) == len(contracts)

    for module in contracts:
        assert module.CLICKHOUSE_TABLES == (
            module.BOARDS_TABLE,
            module.BOARD_COMPANY_LINKS_TABLE,
            module.BOARD_SNAPSHOTS_TABLE,
            module.VERSIONS_TABLE,
            module.EVENTS_TABLE,
            module.CURRENT_TABLE,
            module.LOCATIONS_TABLE,
            module.COMPENSATIONS_TABLE,
        )


def test_sweden_ats_migration_creates_only_source_owned_tables() -> None:
    migrations = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000359_corpscout_sweden_ats_job_sources.up.sql"
    ).read_text()

    for provider in PROVIDERS:
        for suffix in (
            "boards",
            "board_company_links",
            "board_snapshots",
            "job_ad_versions",
            "job_ad_events",
            "job_ad_current",
            "job_ad_location_versions",
            "job_ad_compensation_versions",
        ):
            assert f"corpscout.se_{provider}_{suffix}" in migrations

    assert "company_job_current" not in migrations
    assert "company_job_history" not in migrations
    assert "company_hiring_monthly" not in migrations
    assert "se_platsbanken" not in migrations


def test_reviewed_board_links_use_sweden_company_identity() -> None:
    from dagster_v3.defs.sweden_ashby.source import BOARDS as ashby
    from dagster_v3.defs.sweden_greenhouse.source import BOARDS as greenhouse
    from dagster_v3.defs.sweden_lever.source import BOARDS as lever
    from dagster_v3.defs.sweden_smartrecruiters.source import BOARDS as smartrecruiters

    boards = (*greenhouse, *lever, *ashby, *smartrecruiters)
    assert {board.board_token for board in boards} == {
        "mentimeter",
        "seb",
        "lovable",
        "HMGroup",
    }
    assert all(board.company_id.isdigit() for board in boards)
    assert all(len(board.company_id) == 10 for board in boards)
    assert all(board.country_code == "SE" for board in boards)


def test_lifecycle_sql_is_provider_local_and_distinguishes_every_transition() -> None:
    class RecordingClient:
        def __init__(self) -> None:
            self.sql = ""

        def execute(self, sql: str) -> None:
            self.sql = sql

    client = RecordingClient()
    tables = AtsClickhouseTables(
        database="corpscout",
        duckdb_schema="sweden_greenhouse",
        boards="se_greenhouse_boards",
        board_company_links="se_greenhouse_board_company_links",
        board_snapshots="se_greenhouse_board_snapshots",
        versions="se_greenhouse_job_ad_versions",
        events="se_greenhouse_job_ad_events",
        current="se_greenhouse_job_ad_current",
        locations="se_greenhouse_job_ad_location_versions",
        compensations="se_greenhouse_job_ad_compensation_versions",
        columns={},
    )

    _insert_lifecycle_events(
        client,
        tables=tables,
        stage_names={
            tables.current: "incoming_greenhouse_current",
            tables.board_snapshots: "incoming_greenhouse_snapshots",
        },
        event_stage="incoming_greenhouse_events",
    )

    for event_type in (
        "first_seen",
        "content_changed",
        "closed_by_absence",
        "reopened",
    ):
        assert event_type in client.sql
    assert "se_greenhouse_job_ad_current" in client.sql
    assert "se_greenhouse_job_ad_events" in client.sql
    assert "platsbanken" not in client.sql
    assert "se_lever" not in client.sql
    assert "se_ashby" not in client.sql
    assert "se_smartrecruiters" not in client.sql
