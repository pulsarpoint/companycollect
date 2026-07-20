import inspect
from pathlib import Path
from typing import Any

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
    # The weekly job runs the full chain end-to-end -- sync, catalog, parse,
    # and both ClickHouse exports as separate assets in ONE run -- so the
    # export scope is consumed in the same run that recorded it and cannot
    # be orphaned by a later year-file rebuild (the 2026-07-18 incident).
    assert current_job_asset_keys == {
        "sweden_financial_current_raw_archives_s3",
        "sweden_financial_current_report_xhtml_catalog_duckdb",
        "sweden_financial_current_parsed_reports_duckdb",
        "sweden_financial_current_reports_clickhouse",
        "sweden_financial_current_facts_clickhouse",
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

    # The current (weekly refresh) chain is deliberately UNPARTITIONED
    # (2026-07-20 order-independence design): weekly partition identities
    # existed only to give exports a bookkeeping scope, which a yearly
    # rebuild could wipe. The exports reconcile against ClickHouse instead.
    current_raw_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_current_raw_archives_s3")
    )
    assert current_raw_node.group_name == "sweden_financial"
    assert current_raw_node.pools == set()
    assert current_raw_node.partitions_def is None

    current_catalog_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_current_report_xhtml_catalog_duckdb")
    )
    assert current_catalog_node.group_name == "sweden_financial"
    assert current_catalog_node.pools == {"sweden_financial_current_2026_duckdb"}
    assert current_catalog_node.parent_keys == {
        dg.AssetKey("sweden_financial_current_raw_archives_s3")
    }
    assert current_catalog_node.partitions_def is None

    current_parsed_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_current_parsed_reports_duckdb")
    )
    assert current_parsed_node.group_name == "sweden_financial"
    assert current_parsed_node.pools == {"sweden_financial_current_2026_duckdb"}
    assert current_parsed_node.parent_keys == {
        dg.AssetKey("sweden_financial_current_report_xhtml_catalog_duckdb")
    }
    assert current_parsed_node.partitions_def is None

    clickhouse_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_clickhouse_job"
        ).asset_layer.executable_asset_keys
    }
    assert clickhouse_job_asset_keys == {
        "sweden_financial_metrics_clickhouse",
        "se_financial_history_clickhouse",
        "se_company_officers_clickhouse",
        "se_company_audits_clickhouse",
    }

    backfill_clickhouse_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_backfill_clickhouse_job"
        ).asset_layer.executable_asset_keys
    }
    assert backfill_clickhouse_job_asset_keys == {
        "sweden_financial_backfill_reports_clickhouse",
        "sweden_financial_backfill_facts_clickhouse",
    }

    current_clickhouse_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_current_clickhouse_job"
        ).asset_layer.executable_asset_keys
    }
    assert current_clickhouse_job_asset_keys == {
        "sweden_financial_current_reports_clickhouse",
        "sweden_financial_current_facts_clickhouse",
    }

    # The backfill exports are partition-scoped upserts mirroring their
    # parse counterparts' year partitions; the current exports are
    # unpartitioned reconcilers.
    for asset_key in (
        "sweden_financial_backfill_reports_clickhouse",
        "sweden_financial_backfill_facts_clickhouse",
    ):
        clickhouse_node = repo.asset_graph.get(dg.AssetKey(asset_key))
        assert clickhouse_node.group_name == "sweden_financial"
        assert clickhouse_node.pools == set()
        assert clickhouse_node.partitions_def is backfill_raw_node.partitions_def
        assert clickhouse_node.parent_keys == {
            dg.AssetKey("sweden_financial_backfill_parsed_reports_duckdb"),
        }

    for asset_key in (
        "sweden_financial_current_reports_clickhouse",
        "sweden_financial_current_facts_clickhouse",
    ):
        clickhouse_node = repo.asset_graph.get(dg.AssetKey(asset_key))
        assert clickhouse_node.group_name == "sweden_financial"
        assert clickhouse_node.pools == {"sweden_financial_current_2026_duckdb"}
        assert clickhouse_node.partitions_def is None
        assert clickhouse_node.parent_keys == {
            dg.AssetKey("sweden_financial_current_parsed_reports_duckdb"),
        }

    metrics_node = repo.asset_graph.get(
        dg.AssetKey("sweden_financial_metrics_clickhouse")
    )
    assert metrics_node.group_name == "sweden_financial"
    assert metrics_node.partitions_def is None
    assert metrics_node.parent_keys == {
        dg.AssetKey("exchange_rates_v2_clickhouse"),
        dg.AssetKey("sweden_financial_backfill_reports_clickhouse"),
        dg.AssetKey("sweden_financial_current_reports_clickhouse"),
        dg.AssetKey("sweden_financial_backfill_facts_clickhouse"),
        dg.AssetKey("sweden_financial_current_facts_clickhouse"),
    }


def test_se_financial_history_clickhouse_asset_is_wired_correctly() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()

    history_node = repo.asset_graph.get(dg.AssetKey("se_financial_history_clickhouse"))
    assert history_node.group_name == "sweden_financial"
    assert history_node.pools == set()
    assert history_node.partitions_def is None
    assert history_node.parent_keys == {
        dg.AssetKey("sweden_financial_backfill_reports_clickhouse"),
        dg.AssetKey("sweden_financial_current_reports_clickhouse"),
        dg.AssetKey("sweden_financial_backfill_facts_clickhouse"),
        dg.AssetKey("sweden_financial_current_facts_clickhouse"),
        dg.AssetKey("sweden_financial_metrics_clickhouse"),
    }

    # The history asset should refresh whenever metrics refreshes: it lives
    # in the same (currently unscheduled, manually/backfill-triggered)
    # clickhouse job selection as reports/facts/metrics.
    clickhouse_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_clickhouse_job"
        ).asset_layer.executable_asset_keys
    }
    assert "se_financial_history_clickhouse" in clickhouse_job_asset_keys


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
    assert "se_financial_facts_with_source" in text
    assert "se_financial_metrics" in text


def test_archive_ingest_complete_check_registered() -> None:
    from dagster_v3.defs.sweden_financial.assets import defs

    check_specs = [
        spec
        for checks_def in defs.asset_checks or []
        for spec in checks_def.check_specs
    ]
    names = {(spec.asset_key.to_user_string(), spec.name) for spec in check_specs}
    # Attached to the unpartitioned derived metrics rebuild: the check's
    # semantics are whole-table (all years) completeness, which must not
    # fail a single-partition export run.
    assert ("sweden_financial_metrics_clickhouse", "archive_ingest_complete") in names


def test_sweden_financial_archive_ingest_gap_result_passes_within_tolerance() -> None:
    from dagster_v3.defs.sweden_financial.assets import (
        sweden_financial_archive_ingest_gap_result,
    )

    result = sweden_financial_archive_ingest_gap_result(
        upstream_counts={"2025": 100, "2026": 241},
        processed_counts={"2025": 100, "2026": 235},
    )

    assert result.passed is True
    assert result.metadata["max_gap"].value == 6
    assert result.metadata["per_year"].data["2026"] == {
        "upstream": 241,
        "processed": 235,
        "gap": 6,
    }


def test_sweden_financial_archive_ingest_gap_result_fails_when_gap_exceeds_tolerance() -> (
    None
):
    from dagster_v3.defs.sweden_financial.assets import (
        sweden_financial_archive_ingest_gap_result,
    )

    # 2026-07-18: reproduces the discovered seam -- 216 of 241 upstream 2026
    # archives were silently never ingested (only the yearly backfill ran,
    # and the weekly "current" partitions only start 2026-07-04).
    result = sweden_financial_archive_ingest_gap_result(
        upstream_counts={"2026": 241},
        processed_counts={"2026": 25},
    )

    assert result.passed is False
    assert result.metadata["max_gap"].value == 216
    assert result.metadata["per_year"].data["2026"] == {
        "upstream": 241,
        "processed": 25,
        "gap": 216,
    }


def test_sweden_financial_archive_ingest_gap_result_treats_missing_years_as_zero() -> (
    None
):
    from dagster_v3.defs.sweden_financial.assets import (
        sweden_financial_archive_ingest_gap_result,
    )

    result = sweden_financial_archive_ingest_gap_result(
        upstream_counts={"2020": 50, "2027": 10},
        processed_counts={"2020": 50},
    )

    assert result.passed is False
    assert result.metadata["per_year"].data["2027"] == {
        "upstream": 10,
        "processed": 0,
        "gap": 10,
    }
    assert result.metadata["max_gap"].value == 10


class _FakeArchiveCheckClickHouseClient:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.statements: list[str] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[Any, ...]]:
        del params
        self.statements.append(sql)
        return self.rows


class _FakeArchiveCheckClickHouseResource:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.client = _FakeArchiveCheckClickHouseClient(rows)

    def get_connection(self) -> "_FakeArchiveCheckConnection":
        return _FakeArchiveCheckConnection(self.client)


class _FakeArchiveCheckConnection:
    def __init__(self, client: _FakeArchiveCheckClickHouseClient) -> None:
        self.client = client

    def __enter__(self) -> _FakeArchiveCheckClickHouseClient:
        return self.client

    def __exit__(self, *args: Any) -> None:
        return None


def test_sweden_financial_processed_archive_counts_by_year_groups_rows() -> None:
    from dagster_v3.defs.sweden_financial.assets import (
        sweden_financial_processed_archive_counts_by_year,
    )

    clickhouse = _FakeArchiveCheckClickHouseResource(
        [("2020", 12), ("2026", 25)]
    )

    counts = sweden_financial_processed_archive_counts_by_year(clickhouse)

    assert counts == {"2020": 12, "2026": 25}
    assert "GROUP BY archive_year" in clickhouse.client.statements[0]
    assert "corpscout.se_financial_reports" in clickhouse.client.statements[0]
