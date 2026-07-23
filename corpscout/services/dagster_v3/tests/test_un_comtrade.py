import csv
import io
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import dagster as dg
from dagster_clickhouse import ClickhouseResource
import duckdb
import pytest

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.common import clickhouse_checks
from dagster_v3.defs.un_comtrade import assets, source, tables, transform


TOTALS_COLUMNS = (
    "typeCode",
    "freqCode",
    "refYear",
    "period",
    "reporterCode",
    "reporterISO",
    "reporterDesc",
    "flowCode",
    "flowDesc",
    "partnerCode",
    "partner2Code",
    "classificationCode",
    "classificationSearchCode",
    "isOriginalClassification",
    "cmdCode",
    "aggrLevel",
    "customsCode",
    "motCode",
    "cifvalue",
    "fobvalue",
    "primaryValue",
    "legacyEstimationFlag",
    "isReported",
    "isAggregate",
)

AVAILABILITY_COLUMNS = (
    "DatasetCode",
    "TypeCode",
    "FreqCode",
    "Period",
    "ReporterCode",
    "ReporterISO",
    "ReporterDesc",
    "ClassificationCode",
    "ClassificationSearchCode",
    "IsOriginalClassification",
    "IsExtendedFlowCode",
    "IsExtendedPartnerCode",
    "IsExtendedPartner2Code",
    "IsExtendedCmdCode",
    "IsExtendedCustomsCode",
    "IsExtendedMotCode",
    "TotalRecords",
    "DatasetChecksum",
    "FirstReleased",
    "LastReleased",
)


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.created_buckets: list[str] = []
        self.uploaded_keys: list[str] = []
        self.downloaded_keys: list[str] = []

    def ensure_bucket(self, bucket: str | None = None) -> None:
        assert bucket is not None
        self.created_buckets.append(bucket)

    def exists(self, key: str, bucket: str | None = None) -> bool:
        assert bucket is not None
        return (bucket, key) in self.objects

    def upload_file(
        self,
        key: str,
        source_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.uploaded_keys.append(key)
        self.objects[(bucket, key)] = Path(source_path).read_bytes()

    def download_file(
        self,
        key: str,
        target_path: str | Path,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.downloaded_keys.append(key)
        Path(target_path).write_bytes(self.objects[(bucket, key)])

    def write_json(
        self,
        key: str,
        body: str,
        bucket: str | None = None,
    ) -> None:
        assert bucket is not None
        self.objects[(bucket, key)] = body.encode("utf-8")

    def read_bytes(self, key: str, bucket: str | None = None) -> bytes:
        assert bucket is not None
        return self.objects[(bucket, key)]


class FakeResponse:
    def __init__(self, body: bytes, content_type: str = "text/csv") -> None:
        self.body = body
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
        }

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int) -> list[bytes]:
        midpoint = max(1, len(self.body) // 2)
        return [self.body[:midpoint], self.body[midpoint:]]


class FakeSession:
    def __init__(self, responses: dict[str, bytes]) -> None:
        self.responses = responses
        self.calls: list[str] = []
        self.headers: dict[str, str] = {}
        self.closed = False

    def get(self, url: str, *, timeout: int, stream: bool) -> FakeResponse:
        self.calls.append(url)
        return FakeResponse(self.responses[url])

    def close(self) -> None:
        self.closed = True


def _csv_bytes(columns: tuple[str, ...], rows: list[dict[str, object]]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=columns, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode("utf-8")


def _totals_csv(
    year: int,
    *,
    duplicate: bool = False,
    malformed_primary_value: bool = False,
) -> bytes:
    rows: list[dict[str, object]] = []
    for reporter_code, reporter_iso, reporter_name in (
        (8, "ALB", "Albania"),
        (12, "DZA", "Algeria"),
    ):
        for flow_code, flow_name in (("M", "Import"), ("X", "Export")):
            value = reporter_code * 1_000_000 + year
            rows.append(
                {
                    "typeCode": "C",
                    "freqCode": "A",
                    "refYear": year,
                    "period": year,
                    "reporterCode": reporter_code,
                    "reporterISO": reporter_iso,
                    "reporterDesc": reporter_name,
                    "flowCode": flow_code,
                    "flowDesc": flow_name,
                    "partnerCode": 0,
                    "partner2Code": 0,
                    "classificationCode": "H6",
                    "classificationSearchCode": "HS",
                    "isOriginalClassification": "True",
                    "cmdCode": "TOTAL",
                    "aggrLevel": 0,
                    "customsCode": "C00",
                    "motCode": 0,
                    "cifvalue": f"{value}.125" if flow_code == "M" else "",
                    "fobvalue": f"{value}.250" if flow_code == "X" else "",
                    "primaryValue": (
                        "not-a-number"
                        if malformed_primary_value
                        and reporter_code == 8
                        and flow_code == "M"
                        else f"{value}.125"
                    ),
                    "legacyEstimationFlag": 0,
                    "isReported": str(reporter_code == 12),
                    "isAggregate": str(reporter_code != 12),
                }
            )
    if duplicate:
        rows.append(dict(rows[0]))
    return _csv_bytes(TOTALS_COLUMNS, rows)


def _availability_csv(years: tuple[int, ...]) -> bytes:
    rows: list[dict[str, object]] = []
    for year in years:
        for reporter_code, reporter_iso, reporter_name in (
            (8, "ALB", "Albania"),
            (12, "DZA", "Algeria"),
        ):
            rows.append(
                {
                    "DatasetCode": f"20{reporter_code:03d}{year}010120100",
                    "TypeCode": "C",
                    "FreqCode": "A",
                    "Period": year,
                    "ReporterCode": reporter_code,
                    "ReporterISO": reporter_iso,
                    "ReporterDesc": reporter_name,
                    "ClassificationCode": "H6",
                    "ClassificationSearchCode": "HS",
                    "IsOriginalClassification": "True",
                    "IsExtendedFlowCode": "False",
                    "IsExtendedPartnerCode": "True",
                    "IsExtendedPartner2Code": "True",
                    "IsExtendedCmdCode": "True",
                    "IsExtendedCustomsCode": "True",
                    "IsExtendedMotCode": "True",
                    "TotalRecords": 100_000 + reporter_code,
                    "DatasetChecksum": f"-{year}{reporter_code}",
                    "FirstReleased": "5/7/2025 3:38:06 PM",
                    "LastReleased": "7/1/2026 2:59:39 AM",
                }
            )
    return _csv_bytes(AVAILABILITY_COLUMNS, rows)


def _fixture_responses(
    *,
    start_year: int,
    end_year: int,
    duplicate_year: int | None = None,
    malformed_year: int | None = None,
) -> dict[str, bytes]:
    years = tuple(range(start_year, end_year + 1))
    responses = {
        source.annual_totals_url(year): _totals_csv(
            year,
            duplicate=year == duplicate_year,
            malformed_primary_value=year == malformed_year,
        )
        for year in years
    }
    for period_batch in source.availability_period_batches(years):
        responses[source.data_availability_url(period_batch)] = _availability_csv(
            period_batch
        )
    return responses


def _sync_fixture(
    *,
    object_store: FakeObjectStore,
    run_id: str,
    start_year: int = 2023,
    end_year: int = 2024,
    duplicate_year: int | None = None,
    malformed_year: int | None = None,
) -> dict:
    source.sync_un_comtrade_snapshot(
        object_store=object_store,
        run_id=run_id,
        retrieved_at=datetime(2026, 7, 23, 12, 0, tzinfo=UTC),
        start_year=start_year,
        end_year=end_year,
        subscription_key="test-secret",
        session=FakeSession(
            _fixture_responses(
                start_year=start_year,
                end_year=end_year,
                duplicate_year=duplicate_year,
                malformed_year=malformed_year,
            )
        ),
        timeout_seconds=30,
        request_interval_seconds=0,
    )
    return source.read_snapshot_manifest(object_store=object_store, run_id=run_id)


def test_annual_totals_request_is_all_reporters_and_total_world_trade() -> None:
    url = source.annual_totals_url(2024)
    query = parse_qs(urlparse(url).query)

    assert urlparse(url).path == "/data/v1/get/C/A/HS"
    assert "reporterCode" not in query
    assert "subscription-key" not in query
    assert query == {
        "period": ["2024"],
        "cmdCode": ["TOTAL"],
        "flowCode": ["M,X"],
        "partnerCode": ["0"],
        "partner2Code": ["0"],
        "customsCode": ["C00"],
        "motCode": ["0"],
        "maxRecords": ["100000"],
        "includeDesc": ["true"],
        "format": ["csv"],
    }


def test_availability_periods_are_batched_at_the_source_limit() -> None:
    batches = source.availability_period_batches(tuple(range(2010, 2026)))

    assert tuple(map(len, batches)) == (4, 12)
    assert batches[0] == tuple(range(2010, 2014))
    assert batches[1] == tuple(range(2014, 2026))


def test_subscription_key_is_required_but_not_stored_in_manifest(
    monkeypatch,
) -> None:
    monkeypatch.delenv(source.UN_COMTRADE_SUBSCRIPTION_KEY_ENV, raising=False)
    with pytest.raises(ValueError, match=source.UN_COMTRADE_SUBSCRIPTION_KEY_ENV):
        source.subscription_key_from_environment()

    object_store = FakeObjectStore()
    session = FakeSession(_fixture_responses(start_year=2023, end_year=2024))
    result = source.sync_un_comtrade_snapshot(
        object_store=object_store,
        run_id="run-1",
        retrieved_at=datetime(2026, 7, 23, 12, 0, tzinfo=UTC),
        start_year=2023,
        end_year=2024,
        subscription_key="test-secret",
        session=session,
        timeout_seconds=30,
        request_interval_seconds=0,
    )

    manifest_bytes = object_store.objects[
        (source.UN_COMTRADE_RAW_BUCKET, source.snapshot_manifest_key("run-1"))
    ]
    manifest = json.loads(manifest_bytes)
    assert session.headers[source.SUBSCRIPTION_KEY_HEADER] == "test-secret"
    assert b"test-secret" not in manifest_bytes
    assert object_store.created_buckets == [source.UN_COMTRADE_RAW_BUCKET]
    assert result.metadata["year_count"] == 2
    assert result.metadata["object_count"] == 3
    assert result.metadata["source_record_count"] == 12
    assert manifest["start_year"] == 2023
    assert manifest["end_year"] == 2024
    assert [item["year"] for item in manifest["annual_totals"]] == [2023, 2024]
    assert len(manifest["availability"]) == 1
    assert all("sha256=" in item["object_key"] for item in manifest["annual_totals"])


def test_snapshot_reuses_content_addressed_objects() -> None:
    object_store = FakeObjectStore()
    _sync_fixture(object_store=object_store, run_id="run-1")

    second = source.sync_un_comtrade_snapshot(
        object_store=object_store,
        run_id="run-2",
        retrieved_at=datetime(2026, 7, 23, 13, 0, tzinfo=UTC),
        start_year=2023,
        end_year=2024,
        subscription_key="test-secret",
        session=FakeSession(_fixture_responses(start_year=2023, end_year=2024)),
        timeout_seconds=30,
        request_interval_seconds=0,
    )

    assert second.metadata["downloaded_object_count"] == 0
    assert second.metadata["reused_object_count"] == 3
    assert len(object_store.uploaded_keys) == 3


def test_snapshot_uses_availability_to_skip_an_unreleased_latest_year() -> None:
    object_store = FakeObjectStore()
    period_batch = (2023, 2024)
    responses = {
        source.data_availability_url(period_batch): _availability_csv((2023,)),
        source.annual_totals_url(2023): _totals_csv(2023),
    }
    session = FakeSession(responses)

    source.sync_un_comtrade_snapshot(
        object_store=object_store,
        run_id="early-year",
        retrieved_at=datetime(2026, 1, 10, 12, 0, tzinfo=UTC),
        start_year=2023,
        end_year=2024,
        subscription_key="test-secret",
        session=session,
        timeout_seconds=30,
        request_interval_seconds=0,
    )

    manifest = source.read_snapshot_manifest(
        object_store=object_store,
        run_id="early-year",
    )
    assert source.annual_totals_url(2024) not in session.calls
    assert manifest["requested_end_year"] == 2024
    assert manifest["end_year"] == 2023
    assert [item["year"] for item in manifest["annual_totals"]] == [2023]
    assert manifest["availability"][0]["periods"] == [2023]


def test_snapshot_does_not_write_manifest_for_invalid_csv(monkeypatch) -> None:
    object_store = FakeObjectStore()
    responses = _fixture_responses(start_year=2024, end_year=2024)
    responses[source.annual_totals_url(2024)] = b'{"error":"rate limited"}'
    monkeypatch.setattr(source.time, "sleep", lambda _: None)

    with pytest.raises(RuntimeError, match="UN Comtrade download failed"):
        source.sync_un_comtrade_snapshot(
            object_store=object_store,
            run_id="failed-run",
            retrieved_at=datetime(2026, 7, 23, 12, 0, tzinfo=UTC),
            start_year=2024,
            end_year=2024,
            subscription_key="test-secret",
            session=FakeSession(responses),
            timeout_seconds=30,
            request_interval_seconds=0,
        )

    assert (
        source.UN_COMTRADE_RAW_BUCKET,
        source.snapshot_manifest_key("failed-run"),
    ) not in object_store.objects


def test_duckdb_normalizes_totals_and_availability_from_verified_s3_files(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    manifest = _sync_fixture(object_store=object_store, run_id="run-1")

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=manifest,
    ) as local_snapshot:
        assert len(object_store.downloaded_keys) == 3
        with duckdb.connect(str(tmp_path / "un_comtrade.duckdb")) as connection:
            counts = transform.replace_un_comtrade_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_historical_reporters=2,
            )
            totals = connection.execute(
                f"""
                select year, reporter_iso, flow_code, primary_value_usd,
                       cif_value_usd, fob_value_usd, source_run_id
                from {tables.UN_COMTRADE_DUCKDB_SCHEMA}.
                     {tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE}
                where year = 2024 and reporter_iso = 'ALB'
                order by flow_code
                """
            ).fetchall()
            availability = connection.execute(
                f"""
                select year, reporter_iso, source_total_records,
                       first_released_at, last_released_at
                from {tables.UN_COMTRADE_DUCKDB_SCHEMA}.
                     {tables.UN_COMTRADE_AVAILABILITY_TABLE}
                where year = 2024 and reporter_iso = 'ALB'
                """
            ).fetchone()

    assert counts == {
        "availability_rows": 4,
        "annual_total_rows": 8,
        "reporters": 2,
        "years": 2,
        "import_rows": 4,
        "export_rows": 4,
        "min_year": 2023,
        "max_year": 2024,
        "latest_year_reporters": 2,
    }
    assert totals == [
        (
            2024,
            "ALB",
            "M",
            Decimal("8002024.125"),
            Decimal("8002024.125"),
            None,
            "run-1",
        ),
        (
            2024,
            "ALB",
            "X",
            Decimal("8002024.125"),
            None,
            Decimal("8002024.250"),
            "run-1",
        ),
    ]
    assert availability[:3] == (2024, "ALB", 100008)
    assert availability[3].isoformat() == "2025-05-07T15:38:06"
    assert availability[4].isoformat() == "2026-07-01T02:59:39"


def test_duckdb_rejects_duplicates_without_replacing_previous_snapshot(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    valid_manifest = _sync_fixture(object_store=object_store, run_id="valid")
    duplicate_manifest = _sync_fixture(
        object_store=object_store,
        run_id="duplicate",
        duplicate_year=2024,
    )
    database_path = tmp_path / "un_comtrade.duckdb"

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=valid_manifest,
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            transform.replace_un_comtrade_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_historical_reporters=2,
            )

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=duplicate_manifest,
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            with pytest.raises(ValueError, match="duplicate annual totals"):
                transform.replace_un_comtrade_snapshot(
                    connection=connection,
                    local_snapshot=local_snapshot,
                    minimum_historical_reporters=2,
                )

            assert connection.execute(
                f"""
                select distinct source_run_id
                from {tables.UN_COMTRADE_DUCKDB_SCHEMA}.
                     {tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE}
                """
            ).fetchall() == [("valid",)]


def test_duckdb_rejects_malformed_values_without_replacing_previous_snapshot(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    valid_manifest = _sync_fixture(object_store=object_store, run_id="valid")
    malformed_manifest = _sync_fixture(
        object_store=object_store,
        run_id="malformed",
        malformed_year=2024,
    )
    database_path = tmp_path / "un_comtrade.duckdb"

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=valid_manifest,
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            transform.replace_un_comtrade_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_historical_reporters=2,
            )

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=malformed_manifest,
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            with pytest.raises(duckdb.ConversionException):
                transform.replace_un_comtrade_snapshot(
                    connection=connection,
                    local_snapshot=local_snapshot,
                    minimum_historical_reporters=2,
                )
            assert connection.execute(
                f"""
                select distinct source_run_id
                from {tables.UN_COMTRADE_DUCKDB_SCHEMA}.
                     {tables.UN_COMTRADE_ANNUAL_TOTALS_TABLE}
                """
            ).fetchall() == [("valid",)]


def test_duckdb_enforces_reporter_floor_outside_two_year_release_lag(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    manifest = _sync_fixture(
        object_store=object_store,
        run_id="sparse",
        start_year=2022,
        end_year=2024,
    )

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=manifest,
    ) as local_snapshot:
        with duckdb.connect(str(tmp_path / "un_comtrade.duckdb")) as connection:
            with pytest.raises(
                ValueError,
                match="historical years have too few reporters: 2022=2",
            ):
                transform.replace_un_comtrade_snapshot(
                    connection=connection,
                    local_snapshot=local_snapshot,
                    minimum_historical_reporters=3,
                )


def test_schema_contracts_match_the_clickhouse_migration() -> None:
    for columns, contract in tables.UN_COMTRADE_TABLE_CONTRACTS.values():
        assert columns == contract.column_names

    migration_path = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000160_corpscout_un_comtrade.up.sql"
    )
    migration_sql = migration_path.read_text(encoding="utf-8")
    for table_name in tables.UN_COMTRADE_TABLE_CONTRACTS:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in migration_sql
    assert "primary_value_usd Decimal(38, 3)" in migration_sql
    assert "ORDER BY (reporter_code, year, flow_code)" in migration_sql


def test_assets_job_and_monthly_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    raw = asset_graph.get(dg.AssetKey("un_comtrade_snapshot_s3"))
    normalized = asset_graph.get(dg.AssetKey("un_comtrade_annual_totals_duckdb"))
    published = asset_graph.get(dg.AssetKey("un_comtrade_annual_totals_clickhouse"))

    assert raw.parent_keys == set()
    assert raw.pools == set()
    assert normalized.parent_keys == {dg.AssetKey("un_comtrade_snapshot_s3")}
    assert normalized.pools == {assets.UN_COMTRADE_DUCKDB_POOL}
    assert published.parent_keys == {dg.AssetKey("un_comtrade_annual_totals_duckdb")}
    assert published.pools == {assets.UN_COMTRADE_DUCKDB_POOL}

    job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "un_comtrade_refresh_job"
        ).asset_layer.executable_asset_keys
    }
    assert job_keys == {
        "un_comtrade_snapshot_s3",
        "un_comtrade_annual_totals_duckdb",
        "un_comtrade_annual_totals_clickhouse",
    }
    schedule = repository.get_schedule_def("un_comtrade_monthly_schedule")
    assert schedule.job.name == "un_comtrade_refresh_job"
    assert schedule.cron_schedule == "10 6 10 * *"
    assert schedule.execution_timezone == "Europe/Belgrade"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED
    leaf = next(
        item
        for item in clickhouse_checks.CLICKHOUSE_LEAVES
        if item.asset_key == "un_comtrade_annual_totals_clickhouse"
    )
    assert leaf.tables == (
        "un_comtrade_annual_availability",
        "un_comtrade_annual_totals",
    )
    assert leaf.max_age == clickhouse_checks.MONTHLY


def test_clickhouse_publish_replaces_both_comtrade_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    object_store = FakeObjectStore()
    manifest = _sync_fixture(object_store=object_store, run_id="run-1")
    database_path = tmp_path / "un_comtrade.duckdb"
    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=manifest,
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            transform.replace_un_comtrade_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_historical_reporters=2,
            )

    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    with duckdb.connect(str(database_path), read_only=True) as connection:
        result = assets.export_un_comtrade_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert result == {
        "un_comtrade_annual_availability_rows": 4,
        "un_comtrade_annual_totals_rows": 8,
    }
    assert (
        sum(statement.startswith("CREATE TABLE") for statement in client.statements)
        == 2
    )
    assert (
        sum(statement.startswith("EXCHANGE TABLES") for statement in client.statements)
        == 2
    )


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserted_rows: list[list[tuple]] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple]:
        self.statements.append(sql)
        if params is not None:
            self.inserted_rows.append(list(params))
        return []
