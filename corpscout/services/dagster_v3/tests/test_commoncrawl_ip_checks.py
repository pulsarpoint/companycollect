from contextlib import contextmanager
from datetime import UTC, datetime

import dagster as dg
import pytest

from dagster_v3.defs.commoncrawl_geoip.assets import GEOIP_MISSING_BY_BUCKET_SQL
from dagster_v3.defs.commoncrawl_ip import (
    commoncrawl_ip_partition_key,
)
from dagster_v3.defs.commoncrawl_ip_checks import (
    GEOIP_UPDATE_CHECK_KEY,
    RDAP_UPDATE_CHECK_KEY,
    _geoip_check_result,
    _rdap_check_result,
    commoncrawl_geoip_update_available,
    commoncrawl_ip_enrichment_backlog_checks,
    commoncrawl_rdap_update_available,
    defs as check_defs,
)
from dagster_v3.defs.commoncrawl_rdap.assets import RDAP_ACTIONABLE_BY_BUCKET_SQL
from dagster_v3.defs.commoncrawl_ip_observation import (
    commoncrawl_ip_addresses_observation,
)


CHECKED_AT = datetime(2026, 7, 13, 12, 30, tzinfo=UTC)


class FakeQueryClient:
    def __init__(
        self,
        *,
        geoip_rows: list[tuple[object, ...]] | None = None,
        rdap_rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.geoip_rows = geoip_rows or []
        self.rdap_rows = rdap_rows or []
        self.queries: list[str] = []

    def execute(self, query: str) -> list[tuple[object, ...]]:
        self.queries.append(query)
        if query == GEOIP_MISSING_BY_BUCKET_SQL:
            return self.geoip_rows
        if query == RDAP_ACTIONABLE_BY_BUCKET_SQL:
            return self.rdap_rows
        raise AssertionError(f"Unexpected query: {query}")


class FakeClickhouseResource:
    def __init__(self, client: FakeQueryClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


@pytest.mark.parametrize(
    ("bucket_index", "partition_key"),
    ((0, "bucket_000"), (7, "bucket_007"), (255, "bucket_255")),
)
def test_commoncrawl_ip_partition_key_formats_stable_bucket(
    bucket_index: int,
    partition_key: str,
) -> None:
    assert commoncrawl_ip_partition_key(bucket_index) == partition_key


@pytest.mark.parametrize("bucket_index", (-1, 256))
def test_commoncrawl_ip_partition_key_rejects_out_of_range(bucket_index: int) -> None:
    with pytest.raises(ValueError, match="out of range"):
        commoncrawl_ip_partition_key(bucket_index)


@pytest.mark.parametrize("bucket_index", (True, 7.0, "7"))
def test_commoncrawl_ip_partition_key_rejects_non_integer(bucket_index: object) -> None:
    with pytest.raises(TypeError, match="must be an integer"):
        commoncrawl_ip_partition_key(bucket_index)


def test_geoip_backlog_query_is_missing_only_across_all_buckets() -> None:
    assert (
        "FROM corpscout.commoncrawl_ip_addresses FINAL" in GEOIP_MISSING_BY_BUCKET_SQL
    )
    assert "FROM corpscout.commoncrawl_ip_geoip FINAL" in GEOIP_MISSING_BY_BUCKET_SQL
    assert "toUInt8(1) AS matched" in GEOIP_MISSING_BY_BUCKET_SQL
    assert "USING (bucket, ip)" in GEOIP_MISSING_BY_BUCKET_SQL
    assert "ifNull(current.matched, 0) = 0" in GEOIP_MISSING_BY_BUCKET_SQL
    assert "GROUP BY addresses.bucket" in GEOIP_MISSING_BY_BUCKET_SQL
    assert "ORDER BY addresses.bucket" in GEOIP_MISSING_BY_BUCKET_SQL
    assert "city_db_build_epoch" not in GEOIP_MISSING_BY_BUCKET_SQL
    assert "asn_db_build_epoch" not in GEOIP_MISSING_BY_BUCKET_SQL
    assert "%(bucket_index)s" not in GEOIP_MISSING_BY_BUCKET_SQL


def test_rdap_backlog_query_matches_executable_candidate_semantics() -> None:
    assert "FROM corpscout.commoncrawl_ip_addresses FINAL" in (
        RDAP_ACTIONABLE_BY_BUCKET_SQL
    )
    assert "toIPv4(addresses.ip)" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert "toIPv6(addresses.ip)" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert RDAP_ACTIONABLE_BY_BUCKET_SQL.count("WHERE ip_version = 4") == 2
    assert RDAP_ACTIONABLE_BY_BUCKET_SQL.count("WHERE ip_version = 6") == 2
    assert "UNION ALL" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert "corpscout.rdap_network_trie" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert "corpscout.rdap_ip_lookup_results_current" in (RDAP_ACTIONABLE_BY_BUCKET_SQL)
    assert "toUInt8(1) AS matched" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert "ifNull(current.matched, 0) = 0" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert "current.lookup_status = 'retryable_error'" in (
        RDAP_ACTIONABLE_BY_BUCKET_SQL
    )
    assert "current.retry_after <= now64(3)" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert "countIf(actionable_reason = 'new_uncovered')" in (
        RDAP_ACTIONABLE_BY_BUCKET_SQL
    )
    assert "countIf(actionable_reason = 'due_retry')" in (RDAP_ACTIONABLE_BY_BUCKET_SQL)
    assert "GROUP BY bucket" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert "ORDER BY bucket" in RDAP_ACTIONABLE_BY_BUCKET_SQL
    assert "%(bucket_index)s" not in RDAP_ACTIONABLE_BY_BUCKET_SQL


def test_geoip_check_passes_with_no_missing_source_ips() -> None:
    result = _geoip_check_result([], checked_at=CHECKED_AT)

    assert result.passed is True
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert result.description == (
        "GeoIP enrichment covers all current CommonCrawl IP addresses."
    )
    assert result.metadata["actionable_ip_count"].value == 0
    assert result.metadata["actionable_bucket_count"].value == 0
    assert result.metadata["actionable_partitions"].value == []
    assert result.metadata["oldest_uncovered_first_seen"].value is None
    assert result.metadata["checked_at_utc"].value == CHECKED_AT.isoformat()


def test_geoip_check_warns_with_sorted_actionable_partitions() -> None:
    oldest = datetime(2026, 7, 10, 8, 0, tzinfo=UTC)
    result = _geoip_check_result(
        [
            (193, 3, datetime(2026, 7, 11, 8, 0, tzinfo=UTC)),
            (7, 42, oldest),
        ],
        checked_at=CHECKED_AT,
    )

    assert result.passed is False
    assert result.severity == dg.AssetCheckSeverity.WARN
    assert result.description == (
        "45 IP addresses in 2 partitions need manual GeoIP enrichment."
    )
    assert result.metadata["actionable_ip_count"].value == 45
    assert result.metadata["actionable_bucket_count"].value == 2
    assert result.metadata["actionable_partitions"].value == [
        {"partition_key": "bucket_007", "actionable_ip_count": 42},
        {"partition_key": "bucket_193", "actionable_ip_count": 3},
    ]
    assert result.metadata["oldest_uncovered_first_seen"].value == (oldest.isoformat())


def test_rdap_check_warns_with_new_and_retry_reason_counts() -> None:
    oldest = datetime(2026, 7, 9, 8, 0, tzinfo=UTC)
    result = _rdap_check_result(
        [
            (205, 5, 4, 1, datetime(2026, 7, 12, 8, 0, tzinfo=UTC)),
            (7, 12, 10, 2, oldest),
        ],
        checked_at=CHECKED_AT,
    )

    assert result.passed is False
    assert result.description == (
        "17 IP addresses in 2 partitions need manual RDAP enrichment."
    )
    assert result.metadata["actionable_ip_count"].value == 17
    assert result.metadata["new_uncovered_ip_count"].value == 14
    assert result.metadata["due_retry_ip_count"].value == 3
    assert result.metadata["actionable_bucket_count"].value == 2
    assert result.metadata["actionable_partitions"].value == [
        {
            "partition_key": "bucket_007",
            "actionable_ip_count": 12,
            "new_uncovered_ip_count": 10,
            "due_retry_ip_count": 2,
        },
        {
            "partition_key": "bucket_205",
            "actionable_ip_count": 5,
            "new_uncovered_ip_count": 4,
            "due_retry_ip_count": 1,
        },
    ]
    assert result.metadata["oldest_uncovered_first_seen"].value == (oldest.isoformat())


def test_backlog_results_reject_duplicate_or_inconsistent_rows() -> None:
    first_seen = datetime(2026, 7, 10, tzinfo=UTC)
    with pytest.raises(ValueError, match="Duplicate GeoIP backlog partition"):
        _geoip_check_result(
            [(7, 2, first_seen), (7, 3, first_seen)],
            checked_at=CHECKED_AT,
        )

    with pytest.raises(ValueError, match="reason counts do not equal"):
        _rdap_check_result(
            [(7, 5, 2, 2, first_seen)],
            checked_at=CHECKED_AT,
        )


def test_checks_execute_their_read_only_queries() -> None:
    client = FakeQueryClient()
    clickhouse = FakeClickhouseResource(client)

    geoip_result = commoncrawl_geoip_update_available(clickhouse)
    rdap_result = commoncrawl_rdap_update_available(clickhouse)

    assert geoip_result.passed is True
    assert rdap_result.passed is True
    assert client.queries == [
        GEOIP_MISSING_BY_BUCKET_SQL,
        RDAP_ACTIONABLE_BY_BUCKET_SQL,
    ]


def test_check_query_errors_propagate() -> None:
    class FailingClient(FakeQueryClient):
        def execute(self, query: str) -> list[tuple[object, ...]]:
            raise RuntimeError("ClickHouse unavailable")

    with pytest.raises(RuntimeError, match="ClickHouse unavailable"):
        commoncrawl_geoip_update_available(FakeClickhouseResource(FailingClient()))


def test_manual_check_job_executes_checks_without_materializing_assets() -> None:
    client = FakeQueryClient(
        geoip_rows=[(7, 2, datetime(2026, 7, 10, tzinfo=UTC))],
        rdap_rows=[(7, 3, 2, 1, datetime(2026, 7, 10, tzinfo=UTC))],
    )
    repository = dg.Definitions.merge(
        dg.Definitions(
            assets=[commoncrawl_ip_addresses_observation],
            resources={"clickhouse": FakeClickhouseResource(client)},
        ),
        check_defs,
    ).get_repository_def()
    job = repository.get_job(commoncrawl_ip_enrichment_backlog_checks.name)
    selection = commoncrawl_ip_enrichment_backlog_checks.selection

    assert job.partitions_def is None
    assert selection.resolve(repository.asset_graph) == set()
    assert selection.resolve_checks(repository.asset_graph) == {
        GEOIP_UPDATE_CHECK_KEY,
        RDAP_UPDATE_CHECK_KEY,
    }

    result = job.execute_in_process()

    assert result.success is True
    assert result.get_asset_materialization_events() == []
    assert {
        evaluation.check_name for evaluation in result.get_asset_check_evaluations()
    } == {
        GEOIP_UPDATE_CHECK_KEY.name,
        RDAP_UPDATE_CHECK_KEY.name,
    }
    assert all(
        evaluation.passed is False and evaluation.severity == dg.AssetCheckSeverity.WARN
        for evaluation in result.get_asset_check_evaluations()
    )


def test_project_registers_manual_checks_without_commoncrawl_automation() -> None:
    from dagster_v3.definitions import defs as load_project_defs

    repository = load_project_defs().get_repository_def()

    assert {
        GEOIP_UPDATE_CHECK_KEY,
        RDAP_UPDATE_CHECK_KEY,
    } <= repository.asset_graph.asset_check_keys
    assert repository.get_job(commoncrawl_ip_enrichment_backlog_checks.name)
    assert check_defs.sensors is None
    assert check_defs.schedules is None
    for check_definition in check_defs.asset_checks:
        for check_spec in check_definition.check_specs:
            assert check_spec.automation_condition is None
            assert check_spec.blocking is False
    assert all(
        "commoncrawl_ip_enrichment" not in sensor.name
        for sensor in repository.sensor_defs
    )
    assert all(
        "commoncrawl_ip_enrichment" not in schedule.name
        for schedule in repository.schedule_defs
    )
