import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
import zipfile

import dagster as dg
from dagster_clickhouse import ClickhouseResource
import duckdb

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.world_bank_macro import assets, source, tables, transform


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

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int) -> list[bytes]:
        midpoint = max(1, len(self.body) // 2)
        return [self.body[:midpoint], self.body[midpoint:]]


class FakeSession:
    def __init__(self, responses: dict[str, tuple[bytes, str]]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, *, timeout: int, stream: bool) -> FakeResponse:
        self.calls.append(url)
        body, content_type = self.responses[url]
        return FakeResponse(body, content_type)


def _country_catalog_bytes() -> bytes:
    return json.dumps(
        [
            {"page": 1, "pages": 1, "total": 2},
            [
                {
                    "id": "BRA",
                    "iso2Code": "BR",
                    "name": "Brazil",
                    "region": {"id": "LCN", "value": "Latin America & Caribbean"},
                    "incomeLevel": {"id": "UMC", "value": "Upper middle income"},
                },
                {
                    "id": "NOR",
                    "iso2Code": "NO",
                    "name": "Norway",
                    "region": {"id": "ECS", "value": "Europe & Central Asia"},
                    "incomeLevel": {"id": "HIC", "value": "High income"},
                },
            ],
        ]
    ).encode("utf-8")


def _observation_archive(rows: list[tuple[str, str, str, str, str, str]]) -> bytes:
    data_lines = [
        '\ufeff"Data Source","World Development Indicators",',
        "",
        '"Last Updated Date","2026-07-13",',
        "",
        '"Country Name","Country Code","Indicator Name","Indicator Code","Year","Value",',
        *[",".join(f'"{value}"' for value in row) + "," for row in rows],
    ]
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr(
            "API_Download_DS2_EN_csv_v2_TEST_LIST.csv",
            "\n".join(data_lines) + "\n",
        )
        archive.writestr(
            "Metadata_Indicator_API_Download_DS2_EN_csv_v2_TEST_LIST.csv",
            '"INDICATOR_CODE","INDICATOR_NAME"\n',
        )
        archive.writestr(
            "Metadata_Country_API_Download_DS2_EN_csv_v2_TEST_LIST.csv",
            '"Country Code","Region","IncomeGroup","TableName"\n',
        )
    return output.getvalue()


def _fixture_downloads() -> tuple[dict[str, tuple[bytes, str]], list[bytes]]:
    archive_one = _observation_archive(
        [
            ("Brazil", "BRA", "GDP (current US$)", "NY.GDP.MKTP.CD", "2023", "2100.5"),
            ("Norway", "NOR", "GDP (current US$)", "NY.GDP.MKTP.CD", "2023", "500.25"),
            (
                "Africa Eastern and Southern",
                "AFE",
                "GDP (current US$)",
                "NY.GDP.MKTP.CD",
                "2023",
                "9999",
            ),
            ("Brazil", "BRA", "GDP (current US$)", "NY.GDP.MKTP.CD", "2024", ""),
        ]
    )
    archive_two = _observation_archive(
        [
            (
                "Brazil",
                "BRA",
                "Exports of goods and services (current US$)",
                "NE.EXP.GNFS.CD",
                "2022",
                "300.75",
            ),
            (
                "Norway",
                "NOR",
                "Exports of goods and services (current US$)",
                "NE.EXP.GNFS.CD",
                "2022",
                "200.5",
            ),
        ]
    )
    bodies = [archive_one, archive_two]
    responses = {
        source.observation_download_url(bundle.indicators, end_year=2026): (
            bodies[index],
            "application/zip",
        )
        for index, bundle in enumerate(source.INDICATOR_BUNDLES)
    }
    responses[source.COUNTRY_CATALOG_URL] = (
        _country_catalog_bytes(),
        "application/json",
    )
    return responses, bodies


def test_indicator_bundles_cover_twenty_unique_indicators_for_all_countries() -> None:
    indicator_codes = [
        indicator
        for bundle in source.INDICATOR_BUNDLES
        for indicator in bundle.indicators
    ]

    assert len(source.INDICATOR_BUNDLES) == 2
    assert [len(bundle.indicators) for bundle in source.INDICATOR_BUNDLES] == [10, 10]
    assert len(indicator_codes) == len(set(indicator_codes)) == 20
    for bundle in source.INDICATOR_BUNDLES:
        url = source.observation_download_url(bundle.indicators, end_year=2026)
        assert "/country/all/indicator/" in url
        assert "date=2000%3A2026" in url
        assert "downloadformat=csv" in url
        assert "dataformat=list" in url


def test_snapshot_downloads_bulk_archives_and_country_catalog_to_s3() -> None:
    responses, bodies = _fixture_downloads()
    object_store = FakeObjectStore()
    retrieved_at = datetime(2026, 7, 23, 8, 30, tzinfo=UTC)

    result = source.sync_world_bank_snapshot(
        object_store=object_store,
        run_id="run-1",
        retrieved_at=retrieved_at,
        end_year=2026,
        session=FakeSession(responses),
        timeout_seconds=30,
    )

    manifest_key = source.snapshot_manifest_key("run-1")
    manifest = json.loads(
        object_store.objects[(source.WORLD_BANK_RAW_BUCKET, manifest_key)]
    )
    assert object_store.created_buckets == [source.WORLD_BANK_RAW_BUCKET]
    assert result.metadata["archive_count"] == 2
    assert result.metadata["object_count"] == 3
    assert result.metadata["downloaded_object_count"] == 3
    assert manifest["run_id"] == "run-1"
    assert manifest["start_year"] == 2000
    assert manifest["end_year"] == 2026
    assert {item["kind"] for item in manifest["files"]} == {
        "country_catalog",
        "observations",
    }
    observation_files = [
        item for item in manifest["files"] if item["kind"] == "observations"
    ]
    assert len(observation_files) == 2
    assert {item["bundle"] for item in observation_files} == {
        bundle.name for bundle in source.INDICATOR_BUNDLES
    }
    assert {item["sha256"] for item in observation_files} == {
        hashlib.sha256(body).hexdigest() for body in bodies
    }
    assert all("sha256=" in item["object_key"] for item in manifest["files"])


def test_snapshot_reuses_content_addressed_objects() -> None:
    responses, _ = _fixture_downloads()
    object_store = FakeObjectStore()

    source.sync_world_bank_snapshot(
        object_store=object_store,
        run_id="run-1",
        retrieved_at=datetime(2026, 7, 23, 8, 30, tzinfo=UTC),
        end_year=2026,
        session=FakeSession(responses),
        timeout_seconds=30,
    )
    second = source.sync_world_bank_snapshot(
        object_store=object_store,
        run_id="run-2",
        retrieved_at=datetime(2026, 7, 23, 9, 30, tzinfo=UTC),
        end_year=2026,
        session=FakeSession(responses),
        timeout_seconds=30,
    )

    assert second.metadata["downloaded_object_count"] == 0
    assert second.metadata["reused_object_count"] == 3
    assert len(object_store.uploaded_keys) == 3


def test_duckdb_normalization_discovers_countries_and_excludes_aggregates(
    tmp_path: Path,
) -> None:
    responses, _ = _fixture_downloads()
    object_store = FakeObjectStore()
    source.sync_world_bank_snapshot(
        object_store=object_store,
        run_id="run-1",
        retrieved_at=datetime(2026, 7, 23, 8, 30, tzinfo=UTC),
        end_year=2026,
        session=FakeSession(responses),
        timeout_seconds=30,
    )
    manifest = source.read_snapshot_manifest(object_store=object_store, run_id="run-1")

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=manifest,
    ) as local_snapshot:
        assert len(object_store.downloaded_keys) == 3
        with duckdb.connect(str(tmp_path / "world_bank.duckdb")) as connection:
            counts = transform.replace_world_bank_macro_observations(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_country_count=2,
            )
            rerun_counts = transform.replace_world_bank_macro_observations(
                connection=connection,
                local_snapshot=local_snapshot,
                minimum_country_count=2,
            )
            rows = connection.execute(
                f"""
                select country_code, country_iso3, indicator_code, year, value,
                       source_updated_date, source_run_id
                from {tables.WORLD_BANK_DUCKDB_SCHEMA}.{tables.WORLD_BANK_MACRO_TABLE}
                order by country_code, indicator_code
                """
            ).fetchall()

    assert counts == {
        "discovered_countries": 2,
        "observed_countries": 2,
        "indicators": 2,
        "rows": 4,
        "min_year": 2022,
        "max_year": 2023,
        "source_updated_date": "2026-07-13",
    }
    assert rerun_counts == counts
    assert rows == [
        (
            "br",
            "BRA",
            "NE.EXP.GNFS.CD",
            2022,
            300.75,
            datetime(2026, 7, 13).date(),
            "run-1",
        ),
        (
            "br",
            "BRA",
            "NY.GDP.MKTP.CD",
            2023,
            2100.5,
            datetime(2026, 7, 13).date(),
            "run-1",
        ),
        (
            "no",
            "NOR",
            "NE.EXP.GNFS.CD",
            2022,
            200.5,
            datetime(2026, 7, 13).date(),
            "run-1",
        ),
        (
            "no",
            "NOR",
            "NY.GDP.MKTP.CD",
            2023,
            500.25,
            datetime(2026, 7, 13).date(),
            "run-1",
        ),
    ]


def test_world_bank_schema_contract_and_clickhouse_ddl() -> None:
    assert (
        tables.WORLD_BANK_MACRO_COLUMNS
        == tables.WORLD_BANK_DUCKDB_CONTRACT.column_names
    )
    assert tables.WORLD_BANK_DUCKDB_CONTRACT.column_types["year"] == "USMALLINT"
    assert tables.WORLD_BANK_DUCKDB_CONTRACT.column_types["value"] == "DOUBLE"
    assert "CREATE TABLE IF NOT EXISTS corpscout.world_bank_macro_observations" in (
        tables.WORLD_BANK_MACRO_DDL
    )
    assert "ENGINE = MergeTree" in tables.WORLD_BANK_MACRO_DDL
    assert (
        "ORDER BY (country_code, indicator_code, year)" in tables.WORLD_BANK_MACRO_DDL
    )


def test_world_bank_assets_job_and_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    raw = asset_graph.get(dg.AssetKey("world_bank_snapshot_s3"))
    normalized = asset_graph.get(dg.AssetKey("world_bank_macro_observations_duckdb"))
    published = asset_graph.get(dg.AssetKey("world_bank_macro_observations_clickhouse"))

    assert raw.parent_keys == set()
    assert raw.pools == set()
    assert normalized.parent_keys == {dg.AssetKey("world_bank_snapshot_s3")}
    assert normalized.pools == {assets.WORLD_BANK_DUCKDB_POOL}
    assert published.parent_keys == {
        dg.AssetKey("world_bank_macro_observations_duckdb")
    }
    assert published.pools == {assets.WORLD_BANK_DUCKDB_POOL}

    job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "world_bank_macro_refresh_job"
        ).asset_layer.executable_asset_keys
    }
    assert job_keys == {
        "world_bank_snapshot_s3",
        "world_bank_macro_observations_duckdb",
        "world_bank_macro_observations_clickhouse",
    }
    schedule = repository.get_schedule_def("world_bank_macro_weekly_schedule")
    assert schedule.job.name == "world_bank_macro_refresh_job"
    assert schedule.cron_schedule == "20 4 * * 0"
    assert schedule.execution_timezone == "Europe/Belgrade"
    assert schedule.default_status == dg.DefaultScheduleStatus.RUNNING


def test_clickhouse_publish_replaces_from_duckdb(tmp_path: Path, monkeypatch) -> None:
    duckdb_path = tmp_path / "world_bank.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        transform.ensure_world_bank_duckdb_schema(connection)
        connection.execute(
            f"""
            insert into {tables.WORLD_BANK_DUCKDB_SCHEMA}.{tables.WORLD_BANK_MACRO_TABLE}
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "br",
                "BRA",
                "Brazil",
                "Latin America & Caribbean",
                "Upper middle income",
                "NY.GDP.MKTP.CD",
                "GDP (current US$)",
                2023,
                2100.5,
                "world_bank",
                "WDI",
                "2026-07-13",
                "https://example.test/source.zip",
                "world-bank/source.zip",
                "a" * 64,
                "run-1",
                "2026-07-23T08:30:00.000",
            ],
        )

    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    with duckdb.connect(str(duckdb_path)) as connection:
        result = assets.export_world_bank_macro_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert result == {
        "rows": 1,
        "table": "corpscout.world_bank_macro_observations",
    }
    assert client.statements[0].startswith(
        "CREATE TABLE `corpscout`.`_tmp_world_bank_macro_observations_"
    )
    assert client.statements[1].startswith(
        "INSERT INTO `corpscout`.`_tmp_world_bank_macro_observations_"
    )
    assert client.statements[2].startswith("EXCHANGE TABLES")
    assert client.statements[3].startswith("DROP TABLE IF EXISTS")


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserted_rows: list[list[tuple]] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple]:
        self.statements.append(sql)
        if params is not None:
            self.inserted_rows.append(list(params))
        return []
