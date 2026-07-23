import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from zipfile import ZipFile

import dagster as dg
from dagster_clickhouse import ClickhouseResource
import duckdb
import pyarrow as pa

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.imf_weo import assets, source, tables, transform


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
    def __init__(self, body: bytes, content_type: str) -> None:
        self.body = body
        self.headers = {
            "Content-Length": str(len(body)),
            "Content-Type": content_type,
        }

    @property
    def text(self) -> str:
        return self.body.decode("utf-8")

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int) -> list[bytes]:
        midpoint = max(1, len(self.body) // 2)
        return [self.body[:midpoint], self.body[midpoint:]]


class FakeSession:
    def __init__(self, responses: dict[str, tuple[bytes, str]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, bool]] = []

    def get(self, url: str, *, timeout: int, stream: bool) -> FakeResponse:
        self.calls.append((url, stream))
        body, content_type = self.responses[url]
        return FakeResponse(body, content_type)


def _raw_workbook_bytes() -> bytes:
    output = BytesIO()
    workbook = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
    <workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
      <sheets>
        <sheet name="April 2026 WEO" sheetId="1"/>
        <sheet name="Countries" sheetId="2"/>
        <sheet name="Country Groups" sheetId="3"/>
        <sheet name="Commodity Prices" sheetId="4"/>
        <sheet name="Country Group Composition" sheetId="5"/>
      </sheets>
    </workbook>
    """
    with ZipFile(output, "w") as archive:
        archive.writestr("xl/workbook.xml", workbook)
    return output.getvalue()


def _series_row(
    *,
    country_iso3: str,
    country_name: str,
    indicator_code: str,
    indicator_name: str,
    scale: str,
    unit: str,
    latest_actual_year: int,
    publication_at: str,
    values: dict[int, str | None],
) -> dict[str, str | None]:
    row: dict[str, str | None] = {
        "DATASET": "IMF.RES:WEO(9.0.0)",
        "SERIES_CODE": f"{country_iso3}.{indicator_code}.A",
        "COUNTRY.ID": country_iso3,
        "COUNTRY": country_name,
        "INDICATOR.ID": indicator_code,
        "INDICATOR": indicator_name,
        "INDICATOR.Description": f"Description for {indicator_name}",
        "FREQUENCY": "Annual",
        "SCALE": scale,
        "UNIT": unit,
        "COUNTRY_UPDATE_DATE": "2025-09-10",
        "PUBLICATION_DATE": publication_at,
        "UPDATE_DATE": publication_at,
        "METHODOLOGY.ID": "Other",
        "METHODOLOGY_NOTES": "Fixture methodology",
        "LATEST_ACTUAL_ANNUAL_DATA": str(latest_actual_year),
        "HISTORICAL_DATA_SOURCE": "National Statistics Office",
        "BASE_YEAR": "2020",
        "START_END_MONTHS_OF_REPORTING_YEAR": "January/December",
        "CHAIN_WEIGHTED": "No",
        "BASIS_OF_PROJECTIONS": "IMF staff estimate",
        "VALUATION": "Market prices",
        "PRICES_SECTOR_HARMONIZED_PRICES": None,
        "LABOR_SECTOR_EMPLOYMENT_TYPE": None,
        "FISCAL_SECTOR_GENERAL_GOVERNMENT_COMPOSITION": None,
        "FISCAL_SECTOR_VALUATION_OF_DEBT": None,
        "FISCAL_SECTOR_INSTRUMENTS_INCLUDED_IN_GROSS_AND_NET_DEBT": None,
        "TRADE_SECTOR_OIL_COVERAGE": None,
        "PRIMARY_DOMESTIC_CURRENCY": "Fixture currency",
    }
    for year in range(1999, 2027):
        row[str(year)] = values.get(year)
    return row


def _write_countries_workbook(
    path: Path,
    *,
    publication_at: str,
    corrected_gdp: str = "560",
) -> None:
    rows = [
        _series_row(
            country_iso3="NOR",
            country_name="Norway",
            indicator_code="NGDPD",
            indicator_name="GDP, current prices, US dollar",
            scale="Billions",
            unit="US dollar",
            latest_actual_year=2024,
            publication_at=publication_at,
            values={
                1999: "450",
                2000: "500",
                2024: "550",
                2025: corrected_gdp,
                2026: "570",
            },
        ),
        _series_row(
            country_iso3="NOR",
            country_name="Norway",
            indicator_code="LP",
            indicator_name="Population",
            scale="Millions",
            unit="Persons",
            latest_actual_year=2025,
            publication_at=publication_at,
            values={2000: "4.5", 2025: "5.5", 2026: "5.6"},
        ),
        _series_row(
            country_iso3="BRA",
            country_name="Brazil",
            indicator_code="PCPIPCH",
            indicator_name="Inflation",
            scale="Units",
            unit="Percent",
            latest_actual_year=2023,
            publication_at=publication_at,
            values={2000: "7", 2025: "4.5", 2026: "4"},
        ),
    ]
    with duckdb.connect(":memory:") as connection:
        connection.execute("install excel")
        connection.execute("load excel")
        connection.register("fixture_rows", pa.Table.from_pylist(rows))
        escaped_path = str(path).replace("'", "''")
        connection.execute(
            f"copy fixture_rows to '{escaped_path}' "
            "(format xlsx, header true, sheet 'Countries')"
        )


def _store_workbook(
    *,
    object_store: FakeObjectStore,
    workbook_path: Path,
    run_id: str,
) -> dict[str, object]:
    payload = workbook_path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    object_key = f"imf/weo/raw/sha256={digest}/WEO.xlsx"
    object_store.objects[(source.IMF_WEO_RAW_BUCKET, object_key)] = payload
    return {
        "source": "imf_weo",
        "run_id": run_id,
        "retrieved_at": "2026-07-23T08:30:00+00:00",
        "file": {
            "source_url": "https://example.test/WEO.xlsx",
            "object_key": object_key,
            "sha256": digest,
            "size_bytes": len(payload),
        },
    }


def test_discovers_current_entire_dataset_workbook() -> None:
    html = """
    <html><body>
      <a href="/other.xlsx">Other workbook</a>
      <a href="/-/media/WEOApr2026all.xlsx">April 2026 WEO Entire Dataset in Excel</a>
    </body></html>
    """

    assert source.discover_weo_workbook_url(html) == (
        "https://data.imf.org/-/media/WEOApr2026all.xlsx"
    )


def test_snapshot_downloads_and_reuses_content_addressed_workbook() -> None:
    workbook_url = "https://data.imf.org/-/media/WEOApr2026all.xlsx"
    landing_html = (
        '<a href="/-/media/WEOApr2026all.xlsx">'
        "April 2026 WEO Entire Dataset in Excel</a>"
    ).encode()
    responses = {
        source.IMF_WEO_DATASET_URL: (landing_html, "text/html"),
        workbook_url: (
            _raw_workbook_bytes(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ),
    }
    object_store = FakeObjectStore()

    first = source.sync_imf_weo_snapshot(
        object_store=object_store,
        run_id="run-1",
        retrieved_at=datetime(2026, 7, 23, 8, 30, tzinfo=UTC),
        session=FakeSession(responses),
        timeout_seconds=30,
    )
    second = source.sync_imf_weo_snapshot(
        object_store=object_store,
        run_id="run-2",
        retrieved_at=datetime(2026, 7, 23, 9, 30, tzinfo=UTC),
        session=FakeSession(responses),
        timeout_seconds=30,
    )

    assert first.metadata["downloaded_object_count"] == 1
    assert second.metadata["downloaded_object_count"] == 0
    assert second.metadata["reused_object_count"] == 1
    assert len(object_store.uploaded_keys) == 1
    manifest = source.read_snapshot_manifest(object_store=object_store, run_id="run-2")
    assert manifest["file"]["source_url"] == workbook_url
    assert "sha256=" in manifest["file"]["object_key"]
    assert manifest["file"]["workbook_sheets"] == [
        "April 2026 WEO",
        "Countries",
        "Country Groups",
        "Commodity Prices",
        "Country Group Composition",
    ]


def test_stream_download_uses_decoded_size_for_encoded_response(tmp_path: Path) -> None:
    response = FakeResponse(
        _raw_workbook_bytes(),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
    response.headers["Content-Encoding"] = "gzip"
    response.headers["Content-Length"] = str(len(response.body) - 10)

    class EncodedResponseSession:
        def get(self, url: str, *, timeout: int, stream: bool) -> FakeResponse:
            assert url == "https://example.test/WEO.xlsx"
            assert timeout == 30
            assert stream is True
            return response

    workbook_path = tmp_path / "WEO.xlsx"
    size_bytes, digest, _ = source._stream_download(
        source_url="https://example.test/WEO.xlsx",
        target_path=workbook_path,
        timeout_seconds=30,
        session=EncodedResponseSession(),
    )

    assert size_bytes == len(response.body)
    assert digest == hashlib.sha256(response.body).hexdigest()
    assert workbook_path.read_bytes() == response.body


def test_duckdb_normalization_unpivots_scales_and_classifies_estimates(
    tmp_path: Path,
) -> None:
    workbook_path = tmp_path / "WEOApr2026all.xlsx"
    _write_countries_workbook(
        workbook_path,
        publication_at="2026-04-14T13:00:00Z",
    )
    object_store = FakeObjectStore()
    manifest = _store_workbook(
        object_store=object_store,
        workbook_path=workbook_path,
        run_id="run-april",
    )

    transform.ensure_excel_extension_installed()
    with transform.local_snapshot_file(
        object_store=object_store,
        manifest=manifest,
    ) as local_snapshot:
        assert object_store.downloaded_keys == [manifest["file"]["object_key"]]
        with duckdb.connect(str(tmp_path / "imf.duckdb")) as connection:
            counts = transform.replace_imf_weo_vintage(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_country_count=2,
                minimum_indicator_count=3,
            )
            observations = connection.execute(
                f"""
                select country_iso3, indicator_code, year, value, value_base, is_estimate
                from {tables.IMF_WEO_DUCKDB_SCHEMA}.{tables.IMF_WEO_OBSERVATIONS_TABLE}
                order by country_iso3, indicator_code, year
                """
            ).fetchall()

    assert counts == {
        "vintage": "2026-04",
        "countries": 2,
        "indicators": 3,
        "series": 3,
        "observations": 10,
        "estimates": 5,
        "min_year": 2000,
        "max_year": 2026,
    }
    assert ("NOR", "NGDPD", 2000, 500.0, 500_000_000_000.0, False) in observations
    assert ("NOR", "NGDPD", 2025, 560.0, 560_000_000_000.0, True) in observations
    assert ("NOR", "LP", 2025, 5.5, 5_500_000.0, False) in observations
    assert ("BRA", "PCPIPCH", 2025, 4.5, 4.5, True) in observations
    assert all(row[2] >= 2000 for row in observations)


def test_duckdb_retains_new_vintages_and_replaces_same_vintage(tmp_path: Path) -> None:
    object_store = FakeObjectStore()
    transform.ensure_excel_extension_installed()
    database_path = tmp_path / "imf.duckdb"

    april_path = tmp_path / "april.xlsx"
    _write_countries_workbook(april_path, publication_at="2026-04-14T13:00:00Z")
    october_path = tmp_path / "october.xlsx"
    _write_countries_workbook(october_path, publication_at="2026-10-13T13:00:00Z")
    corrected_path = tmp_path / "october-corrected.xlsx"
    _write_countries_workbook(
        corrected_path,
        publication_at="2026-10-13T13:00:00Z",
        corrected_gdp="561",
    )

    for run_id, workbook_path in (
        ("run-april", april_path),
        ("run-october", october_path),
        ("run-october-corrected", corrected_path),
    ):
        manifest = _store_workbook(
            object_store=object_store,
            workbook_path=workbook_path,
            run_id=run_id,
        )
        with transform.local_snapshot_file(
            object_store=object_store,
            manifest=manifest,
        ) as local_snapshot:
            with duckdb.connect(str(database_path)) as connection:
                transform.replace_imf_weo_vintage(
                    connection=connection,
                    local_snapshot=local_snapshot,
                    minimum_country_count=2,
                    minimum_indicator_count=3,
                )

    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert (
            connection.execute(
                f"select count(*) from {tables.IMF_WEO_DUCKDB_SCHEMA}.{tables.IMF_WEO_VINTAGES_TABLE}"
            ).fetchone()[0]
            == 2
        )
        assert (
            connection.execute(
                f"select count(*) from {tables.IMF_WEO_DUCKDB_SCHEMA}.{tables.IMF_WEO_OBSERVATIONS_TABLE}"
            ).fetchone()[0]
            == 20
        )
        corrected_value = connection.execute(
            f"""
            select value
            from {tables.IMF_WEO_DUCKDB_SCHEMA}.{tables.IMF_WEO_OBSERVATIONS_TABLE}
            where vintage_date = date '2026-10-13'
              and country_iso3 = 'NOR'
              and indicator_code = 'NGDPD'
              and year = 2025
            """
        ).fetchone()[0]
    assert corrected_value == 561.0


def test_imf_weo_schema_contracts_match_clickhouse_migration() -> None:
    assert (
        tables.IMF_WEO_VINTAGES_COLUMNS == tables.IMF_WEO_VINTAGES_CONTRACT.column_names
    )
    assert tables.IMF_WEO_SERIES_COLUMNS == tables.IMF_WEO_SERIES_CONTRACT.column_names
    assert tables.IMF_WEO_OBSERVATIONS_COLUMNS == (
        tables.IMF_WEO_OBSERVATIONS_CONTRACT.column_names
    )
    assert tables.IMF_WEO_OBSERVATIONS_CONTRACT.column_types["value"] == "DOUBLE"
    assert tables.IMF_WEO_OBSERVATIONS_CONTRACT.column_types["is_estimate"] == "BOOLEAN"
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000158_corpscout_imf_weo.up.sql"
    ).read_text()

    for table_name, columns in (
        (tables.IMF_WEO_VINTAGES_TABLE, tables.IMF_WEO_VINTAGES_COLUMNS),
        (tables.IMF_WEO_SERIES_TABLE, tables.IMF_WEO_SERIES_COLUMNS),
        (tables.IMF_WEO_OBSERVATIONS_TABLE, tables.IMF_WEO_OBSERVATIONS_COLUMNS),
    ):
        table_start = migration.index(
            f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}"
        )
        body_start = migration.index("(\n", table_start) + 2
        body_end = migration.index("\n)\nENGINE", body_start)
        migration_columns = tuple(
            line.strip().split(maxsplit=1)[0]
            for line in migration[body_start:body_end].splitlines()
            if line.strip()
        )
        assert migration_columns == columns


def test_imf_weo_assets_job_and_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    raw = asset_graph.get(dg.AssetKey("imf_weo_snapshot_s3"))
    normalized = asset_graph.get(dg.AssetKey("imf_weo_observations_duckdb"))
    published = asset_graph.get(dg.AssetKey("imf_weo_observations_clickhouse"))

    assert raw.parent_keys == set()
    assert raw.pools == set()
    assert normalized.parent_keys == {dg.AssetKey("imf_weo_snapshot_s3")}
    assert normalized.pools == {assets.IMF_WEO_DUCKDB_POOL}
    assert published.parent_keys == {dg.AssetKey("imf_weo_observations_duckdb")}
    assert published.pools == {assets.IMF_WEO_DUCKDB_POOL}

    job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "imf_weo_refresh_job"
        ).asset_layer.executable_asset_keys
    }
    assert job_keys == {
        "imf_weo_snapshot_s3",
        "imf_weo_observations_duckdb",
        "imf_weo_observations_clickhouse",
    }
    schedule = repository.get_schedule_def("imf_weo_weekly_schedule")
    assert schedule.job.name == "imf_weo_refresh_job"
    assert schedule.cron_schedule == "0 5 * * 0"
    assert schedule.execution_timezone == "Europe/Belgrade"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING


def test_clickhouse_publish_replaces_all_imf_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "imf.duckdb"
    workbook_path = tmp_path / "april.xlsx"
    _write_countries_workbook(workbook_path, publication_at="2026-04-14T13:00:00Z")
    object_store = FakeObjectStore()
    manifest = _store_workbook(
        object_store=object_store,
        workbook_path=workbook_path,
        run_id="run-april",
    )
    transform.ensure_excel_extension_installed()
    with transform.local_snapshot_file(
        object_store=object_store,
        manifest=manifest,
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            transform.replace_imf_weo_vintage(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_country_count=2,
                minimum_indicator_count=3,
            )

    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    with duckdb.connect(str(database_path), read_only=True) as connection:
        result = assets.export_imf_weo_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert result == {
        "imf_weo_vintages_rows": 1,
        "imf_weo_series_rows": 3,
        "imf_weo_observations_rows": 10,
    }
    assert (
        sum(statement.startswith("CREATE TABLE") for statement in client.statements)
        == 3
    )
    assert (
        sum(statement.startswith("EXCHANGE TABLES") for statement in client.statements)
        == 3
    )
    assert (
        sum(
            statement.startswith("DROP TABLE IF EXISTS")
            for statement in client.statements
        )
        == 3
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
