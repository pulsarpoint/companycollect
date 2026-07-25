import gzip
import hashlib
import json
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import replace as dataclass_replace
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
from dagster_clickhouse import ClickhouseResource
import duckdb
import pytest

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.eurostat import assets, source, tables, transform


FIXTURE_DATASET = source.EurostatDataset(
    code="fixture_a",
    expected_dimensions=("freq", "unit", "na_item", "geo"),
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
        self.closed = False

    def get(self, url: str, *, timeout: int, stream: bool) -> FakeResponse:
        self.calls.append(url)
        body, content_type = self.responses[url]
        return FakeResponse(body, content_type)

    def close(self) -> None:
        self.closed = True


class SequencedSession(FakeSession):
    def __init__(self, responses: dict[str, list[tuple[bytes, str]]]) -> None:
        self.response_sequences = responses
        self.calls: list[str] = []
        self.closed = False

    def get(self, url: str, *, timeout: int, stream: bool) -> FakeResponse:
        self.calls.append(url)
        responses = self.response_sequences[url]
        body, content_type = responses.pop(0)
        return FakeResponse(body, content_type)


def _fixture_tsv_bytes(*, malformed: bool = False) -> bytes:
    malformed_value = "not-a-number" if malformed else "105.25 b"
    text = (
        "freq,unit,na_item,geo\\TIME_PERIOD\t2009 \t2010 \t2011 \t2012 \r\n"
        "A,CP_MEUR,B1GQ,NO\t99.0 \t100.5 p\t: @C\t102.0 \r\n"
        f"A,CP_MEUR,D21X31,NO\t: \t{malformed_value}\t: \t106.0 \r\n"
        "A,CP_MEUR,B1GQ,EU27_2020\t900.0 \t999.0 \t: \t1001.0 \r\n"
    )
    return gzip.compress(text.encode("utf-8"))


def _fixture_structure_bytes() -> bytes:
    dimension_values = {
        "freq": (("A", "Annual"),),
        "unit": (("CP_MEUR", "Current prices, million euro"),),
        "na_item": (
            ("B1GQ", "Gross domestic product"),
            ("D21X31", "Taxes less subsidies on products"),
        ),
        "geo": (
            ("NO", "Norway"),
            ("DE", "Germany"),
            ("EU27_2020", "European Union - 27 countries"),
        ),
    }
    codelists = []
    concepts = []
    dimensions = []
    for position, dimension_code in enumerate(
        FIXTURE_DATASET.expected_dimensions,
        start=1,
    ):
        codelist_code = dimension_code.upper()
        codes = "".join(
            (
                f'<s:Code id="{value_code}">'
                f'<c:Name xml:lang="en">{value_label}</c:Name>'
                "</s:Code>"
            )
            for value_code, value_label in dimension_values[dimension_code]
        )
        codelists.append(
            f'<s:Codelist agencyID="ESTAT" id="{codelist_code}" version="1.0">'
            f'<c:Name xml:lang="en">{dimension_code} values</c:Name>'
            f"{codes}</s:Codelist>"
        )
        concepts.append(
            f'<s:Concept id="{dimension_code}">'
            f'<c:Name xml:lang="en">{dimension_code} label</c:Name>'
            "<s:CoreRepresentation><s:Enumeration>"
            f'<Ref id="{codelist_code}" />'
            "</s:Enumeration></s:CoreRepresentation>"
            "</s:Concept>"
        )
        dimensions.append(
            f'<s:Dimension id="{dimension_code}" position="{position}">'
            "<s:ConceptIdentity>"
            f'<Ref id="{dimension_code}" />'
            "</s:ConceptIdentity>"
            "<s:LocalRepresentation><s:Enumeration>"
            f'<Ref id="{codelist_code}" />'
            "</s:Enumeration></s:LocalRepresentation>"
            "</s:Dimension>"
        )

    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<m:Structure xmlns:m="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message" '
        'xmlns:s="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure" '
        'xmlns:c="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">'
        "<m:Structures><s:Dataflows>"
        '<s:Dataflow id="FIXTURE_A" version="1.0">'
        '<c:Name xml:lang="en">Fixture annual national accounts</c:Name>'
        "<c:Annotations>"
        "<c:Annotation><c:AnnotationTitle>7</c:AnnotationTitle>"
        "<c:AnnotationType>OBS_COUNT</c:AnnotationType></c:Annotation>"
        "<c:Annotation><c:AnnotationTitle>2009</c:AnnotationTitle>"
        "<c:AnnotationType>OBS_PERIOD_OVERALL_OLDEST</c:AnnotationType></c:Annotation>"
        "<c:Annotation><c:AnnotationTitle>2012</c:AnnotationTitle>"
        "<c:AnnotationType>OBS_PERIOD_OVERALL_LATEST</c:AnnotationType></c:Annotation>"
        "<c:Annotation><c:AnnotationTitle>2026-07-20T23:00:00+0200</c:AnnotationTitle>"
        "<c:AnnotationType>UPDATE_DATA</c:AnnotationType></c:Annotation>"
        "<c:Annotation><c:AnnotationTitle>2026-06-01T23:00:00+0200</c:AnnotationTitle>"
        "<c:AnnotationType>UPDATE_STRUCTURE</c:AnnotationType></c:Annotation>"
        "</c:Annotations>"
        '<s:Structure><Ref id="FIXTURE_A" version="4.2" /></s:Structure>'
        "</s:Dataflow></s:Dataflows>"
        f"<s:Codelists>{''.join(codelists)}</s:Codelists>"
        '<s:Concepts><s:ConceptScheme id="FIXTURE_A">'
        f"{''.join(concepts)}"
        "</s:ConceptScheme></s:Concepts>"
        "<s:DataStructures>"
        '<s:DataStructure agencyID="ESTAT" id="FIXTURE_A" version="4.2">'
        "<s:DataStructureComponents><s:DimensionList>"
        f"{''.join(dimensions)}"
        '<s:TimeDimension id="TIME_PERIOD" position="5" />'
        "</s:DimensionList></s:DataStructureComponents>"
        "</s:DataStructure></s:DataStructures>"
        "</m:Structures></m:Structure>"
    ).encode("utf-8")


def _fixture_responses(
    *,
    malformed: bool = False,
) -> dict[str, tuple[bytes, str]]:
    return {
        source.dataset_tsv_url(FIXTURE_DATASET): (
            _fixture_tsv_bytes(malformed=malformed),
            "application/gzip",
        ),
        source.dataset_structure_url(FIXTURE_DATASET): (
            _fixture_structure_bytes(),
            "application/xml",
        ),
    }


def _sync_fixture(
    *,
    object_store: FakeObjectStore,
    run_id: str,
    malformed: bool = False,
) -> dict:
    source.sync_eurostat_snapshot(
        object_store=object_store,
        run_id=run_id,
        retrieved_at=datetime(2026, 7, 23, 8, 30, tzinfo=UTC),
        datasets=(FIXTURE_DATASET,),
        session=FakeSession(_fixture_responses(malformed=malformed)),
        timeout_seconds=30,
    )
    return source.read_snapshot_manifest(object_store=object_store, run_id=run_id)


def test_dataset_registry_covers_the_confirmed_annual_scope() -> None:
    assert [dataset.code for dataset in source.EUROSTAT_DATASETS] == [
        "nama_10_gdp",
        "nama_10_pc",
        "nama_10_a10",
        "gov_10dd_edpt1",
        "gov_10a_main",
        "prc_hicp_aind",
        "une_rt_a",
        "demo_gind",
        "bd_size",
        "bd_hg",
        "sbs_ovw_act",
        "sbs_sc_ovw",
    ]
    assert len({dataset.code for dataset in source.EUROSTAT_DATASETS}) == 12
    assert all(
        "geo" in dataset.expected_dimensions for dataset in source.EUROSTAT_DATASETS
    )
    assert all(
        "freq" in dataset.expected_dimensions for dataset in source.EUROSTAT_DATASETS
    )


def test_structure_parser_returns_dimensions_labels_and_source_annotations() -> None:
    metadata = source.parse_structure_metadata(
        _fixture_structure_bytes(),
        dataset=FIXTURE_DATASET,
    )

    assert metadata.dataset_code == "fixture_a"
    assert metadata.title == "Fixture annual national accounts"
    assert metadata.dsd_version == "4.2"
    assert metadata.source_observation_count == 7
    assert metadata.source_oldest_period == "2009"
    assert metadata.source_latest_period == "2012"
    assert metadata.data_updated_at.isoformat() == "2026-07-20T21:00:00+00:00"
    assert metadata.structure_updated_at.isoformat() == "2026-06-01T21:00:00+00:00"
    assert tuple(dimension.code for dimension in metadata.dimensions) == (
        "freq",
        "unit",
        "na_item",
        "geo",
    )
    assert metadata.dimensions[2].label == "na_item label"
    assert metadata.dimensions[2].values[0].label == "Gross domestic product"


def test_snapshot_downloads_content_addressed_tsv_and_structure_files() -> None:
    object_store = FakeObjectStore()

    result = source.sync_eurostat_snapshot(
        object_store=object_store,
        run_id="run-1",
        retrieved_at=datetime(2026, 7, 23, 8, 30, tzinfo=UTC),
        datasets=(FIXTURE_DATASET,),
        session=FakeSession(_fixture_responses()),
        timeout_seconds=30,
    )

    manifest_key = source.snapshot_manifest_key("run-1")
    manifest = json.loads(
        object_store.objects[(source.EUROSTAT_RAW_BUCKET, manifest_key)]
    )
    assert object_store.created_buckets == [source.EUROSTAT_RAW_BUCKET]
    assert result.metadata["dataset_count"] == 1
    assert result.metadata["object_count"] == 2
    assert result.metadata["downloaded_object_count"] == 2
    assert manifest["run_id"] == "run-1"
    assert manifest["start_year"] == 2010
    assert len(manifest["datasets"]) == 1
    dataset_entry = manifest["datasets"][0]
    assert dataset_entry["dataset_code"] == "fixture_a"
    assert dataset_entry["dimensions"] == ["freq", "unit", "na_item", "geo"]
    assert (
        dataset_entry["data"]["sha256"]
        == hashlib.sha256(_fixture_tsv_bytes()).hexdigest()
    )
    assert (
        dataset_entry["structure"]["sha256"]
        == hashlib.sha256(_fixture_structure_bytes()).hexdigest()
    )
    assert "sha256=" in dataset_entry["data"]["object_key"]
    assert "sha256=" in dataset_entry["structure"]["object_key"]


def test_snapshot_reuses_content_addressed_objects() -> None:
    object_store = FakeObjectStore()
    _sync_fixture(object_store=object_store, run_id="run-1")

    second = source.sync_eurostat_snapshot(
        object_store=object_store,
        run_id="run-2",
        retrieved_at=datetime(2026, 7, 23, 9, 30, tzinfo=UTC),
        datasets=(FIXTURE_DATASET,),
        session=FakeSession(_fixture_responses()),
        timeout_seconds=30,
    )

    assert second.metadata["downloaded_object_count"] == 0
    assert second.metadata["reused_object_count"] == 2
    assert len(object_store.uploaded_keys) == 2


def test_snapshot_retries_a_corrupt_gzip_before_writing_the_manifest(
    monkeypatch,
) -> None:
    object_store = FakeObjectStore()
    data_url = source.dataset_tsv_url(FIXTURE_DATASET)
    structure_url = source.dataset_structure_url(FIXTURE_DATASET)
    session = SequencedSession(
        {
            data_url: [
                (b"not-a-gzip-file", "application/octet-stream"),
                (_fixture_tsv_bytes(), "application/gzip"),
            ],
            structure_url: [(_fixture_structure_bytes(), "application/xml")],
        }
    )
    monkeypatch.setattr(source.time, "sleep", lambda _: None)

    source.sync_eurostat_snapshot(
        object_store=object_store,
        run_id="retry-run",
        retrieved_at=datetime(2026, 7, 23, 8, 30, tzinfo=UTC),
        datasets=(FIXTURE_DATASET,),
        session=session,
        timeout_seconds=30,
    )

    assert session.calls.count(data_url) == 2
    assert source.snapshot_manifest_key("retry-run") in {
        key
        for bucket, key in object_store.objects
        if bucket == source.EUROSTAT_RAW_BUCKET
    }


def test_snapshot_does_not_write_manifest_when_structure_validation_fails(
    monkeypatch,
) -> None:
    object_store = FakeObjectStore()
    monkeypatch.setattr(source.time, "sleep", lambda _: None)
    responses = _fixture_responses()
    responses[source.dataset_structure_url(FIXTURE_DATASET)] = (
        b"<invalid>",
        "application/xml",
    )

    with pytest.raises(RuntimeError, match="Eurostat download failed"):
        source.sync_eurostat_snapshot(
            object_store=object_store,
            run_id="failed-run",
            retrieved_at=datetime(2026, 7, 23, 8, 30, tzinfo=UTC),
            datasets=(FIXTURE_DATASET,),
            session=FakeSession(responses),
            timeout_seconds=30,
        )

    assert (
        source.EUROSTAT_RAW_BUCKET,
        source.snapshot_manifest_key("failed-run"),
    ) not in object_store.objects


def test_duckdb_normalization_unpivots_tsv_and_preserves_statuses(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    manifest = _sync_fixture(object_store=object_store, run_id="run-1")

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=manifest,
        datasets=(FIXTURE_DATASET,),
    ) as local_snapshot:
        assert len(object_store.downloaded_keys) == 2
        with duckdb.connect(str(tmp_path / "eurostat.duckdb")) as connection:
            counts = transform.replace_eurostat_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
            )
            dataset_row = connection.execute(
                f"""
                select dataset_code, title, dsd_version, source_observation_count,
                       source_oldest_period, source_latest_period, source_run_id
                from {tables.EUROSTAT_DUCKDB_SCHEMA}.{tables.EUROSTAT_DATASETS_TABLE}
                """
            ).fetchone()
            series_rows = connection.execute(
                f"""
                select series_key, geo_code, frequency, unit_code, source_line_number
                from {tables.EUROSTAT_DUCKDB_SCHEMA}.{tables.EUROSTAT_SERIES_TABLE}
                order by series_key
                """
            ).fetchall()
            observations = connection.execute(
                f"""
                select geo_code, series_key, year, value, status
                from {tables.EUROSTAT_DUCKDB_SCHEMA}.{tables.EUROSTAT_OBSERVATIONS_TABLE}
                order by geo_code, series_key, year
                """
            ).fetchall()
            gross_domestic_product = connection.execute(
                f"""
                select value_label
                from {tables.EUROSTAT_DUCKDB_SCHEMA}.
                     {tables.EUROSTAT_DIMENSION_VALUES_TABLE}
                where dataset_code = 'fixture_a'
                  and dimension_code = 'na_item'
                  and value_code = 'B1GQ'
                """
            ).fetchone()[0]
            dimension_rows = connection.execute(
                f"""
                select dimension_code, dimension_label, dimension_position,
                       value_code, value_label, value_position
                from {tables.EUROSTAT_DUCKDB_SCHEMA}.
                     {tables.EUROSTAT_DIMENSION_VALUES_TABLE}
                order by dimension_position, value_position
                """
            ).fetchall()

    assert counts == {
        "datasets": 1,
        "dimension_values": 7,
        "series": 3,
        "series_dimensions": 12,
        "observations": 7,
        "geographies": 2,
        "flagged_observations": 3,
        "flagged_missing_observations": 1,
        "min_year": 2010,
        "max_year": 2012,
    }
    assert dataset_row == (
        "fixture_a",
        "Fixture annual national accounts",
        "4.2",
        7,
        "2009",
        "2012",
        "run-1",
    )
    assert series_rows == [
        ("A,CP_MEUR,B1GQ,EU27_2020", "EU27_2020", "A", "CP_MEUR", 4),
        ("A,CP_MEUR,B1GQ,NO", "NO", "A", "CP_MEUR", 2),
        ("A,CP_MEUR,D21X31,NO", "NO", "A", "CP_MEUR", 3),
    ]
    assert observations == [
        ("EU27_2020", "A,CP_MEUR,B1GQ,EU27_2020", 2010, 999.0, ""),
        ("EU27_2020", "A,CP_MEUR,B1GQ,EU27_2020", 2012, 1001.0, ""),
        ("NO", "A,CP_MEUR,B1GQ,NO", 2010, 100.5, "p"),
        ("NO", "A,CP_MEUR,B1GQ,NO", 2011, None, "@C"),
        ("NO", "A,CP_MEUR,B1GQ,NO", 2012, 102.0, ""),
        ("NO", "A,CP_MEUR,D21X31,NO", 2010, 105.25, "b"),
        ("NO", "A,CP_MEUR,D21X31,NO", 2012, 106.0, ""),
    ]
    assert gross_domestic_product == "Gross domestic product"
    assert dimension_rows == [
        ("freq", "freq label", 1, "A", "Annual", 1),
        ("unit", "unit label", 2, "CP_MEUR", "Current prices, million euro", 1),
        ("na_item", "na_item label", 3, "B1GQ", "Gross domestic product", 1),
        (
            "na_item",
            "na_item label",
            3,
            "D21X31",
            "Taxes less subsidies on products",
            2,
        ),
        ("geo", "geo label", 4, "NO", "Norway", 1),
        ("geo", "geo label", 4, "DE", "Germany", 2),
        (
            "geo",
            "geo label",
            4,
            "EU27_2020",
            "European Union - 27 countries",
            3,
        ),
    ]


def test_dimension_value_insert_refuses_empty_metadata(tmp_path: Path) -> None:
    object_store = FakeObjectStore()
    manifest = _sync_fixture(object_store=object_store, run_id="run-1")

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=manifest,
        datasets=(FIXTURE_DATASET,),
    ) as local_snapshot:
        dataset_snapshot = local_snapshot.datasets[0]
        empty_snapshot = dataclass_replace(
            dataset_snapshot,
            metadata=dataclass_replace(
                dataset_snapshot.metadata,
                dimensions=(),
            ),
        )
        with duckdb.connect(str(tmp_path / "eurostat.duckdb")) as connection:
            transform.ensure_eurostat_duckdb_schema(connection)
            with pytest.raises(ValueError, match="has no dimension values"):
                transform._insert_dimension_values(
                    connection=connection,
                    dataset_snapshot=empty_snapshot,
                )


def test_duckdb_rejects_malformed_observations_without_replacing_existing_data(
    tmp_path: Path,
) -> None:
    object_store = FakeObjectStore()
    valid_manifest = _sync_fixture(object_store=object_store, run_id="valid")
    malformed_manifest = _sync_fixture(
        object_store=object_store,
        run_id="malformed",
        malformed=True,
    )
    database_path = tmp_path / "eurostat.duckdb"

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=valid_manifest,
        datasets=(FIXTURE_DATASET,),
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            transform.replace_eurostat_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
            )

    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=malformed_manifest,
        datasets=(FIXTURE_DATASET,),
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            try:
                transform.replace_eurostat_snapshot(
                    connection=connection,
                    local_snapshot=local_snapshot,
                )
            except ValueError as exc:
                assert "malformed observation" in str(exc)
            else:
                raise AssertionError(
                    "expected malformed observation validation failure"
                )

            assert connection.execute(
                f"""
                select distinct source_run_id
                from {tables.EUROSTAT_DUCKDB_SCHEMA}.{tables.EUROSTAT_DATASETS_TABLE}
                """
            ).fetchall() == [("valid",)]


def test_eurostat_schema_contracts_match_clickhouse_migration() -> None:
    for columns, contract in tables.EUROSTAT_TABLE_CONTRACTS.values():
        assert columns == contract.column_names

    migration_path = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000159_corpscout_eurostat.up.sql"
    )
    migration_sql = migration_path.read_text()
    for table_name in tables.EUROSTAT_TABLE_CONTRACTS:
        assert f"CREATE TABLE IF NOT EXISTS corpscout.{table_name}" in migration_sql
    assert "ENGINE = MergeTree" in migration_sql
    assert (
        "ORDER BY (dataset_code, geo_code, series_key, period_start)" in migration_sql
    )


def test_eurostat_assets_job_and_schedule_are_registered() -> None:
    repository = load_project_defs().get_repository_def()
    asset_graph = repository.asset_graph
    raw = asset_graph.get(dg.AssetKey("eurostat_snapshot_s3"))
    normalized = asset_graph.get(dg.AssetKey("eurostat_observations_duckdb"))
    published = asset_graph.get(dg.AssetKey("eurostat_observations_clickhouse"))

    assert raw.parent_keys == set()
    assert raw.pools == set()
    assert normalized.parent_keys == {dg.AssetKey("eurostat_snapshot_s3")}
    assert normalized.pools == {assets.EUROSTAT_DUCKDB_POOL}
    assert published.parent_keys == {dg.AssetKey("eurostat_observations_duckdb")}
    assert published.pools == {assets.EUROSTAT_DUCKDB_POOL}

    job_keys = {
        key.path[-1]
        for key in repository.get_job(
            "eurostat_refresh_job"
        ).asset_layer.executable_asset_keys
    }
    assert job_keys == {
        "eurostat_snapshot_s3",
        "eurostat_observations_duckdb",
        "eurostat_observations_clickhouse",
    }
    schedule = repository.get_schedule_def("eurostat_weekly_schedule")
    assert schedule.job.name == "eurostat_refresh_job"
    assert schedule.cron_schedule == "55 5 * * 0"
    assert schedule.execution_timezone == "Europe/Belgrade"
    assert schedule.default_status == dg.DefaultScheduleStatus.STOPPED


def test_clickhouse_publish_replaces_all_eurostat_tables(
    tmp_path: Path,
    monkeypatch,
) -> None:
    object_store = FakeObjectStore()
    manifest = _sync_fixture(object_store=object_store, run_id="run-1")
    database_path = tmp_path / "eurostat.duckdb"
    with transform.local_snapshot_files(
        object_store=object_store,
        manifest=manifest,
        datasets=(FIXTURE_DATASET,),
    ) as local_snapshot:
        with duckdb.connect(str(database_path)) as connection:
            transform.replace_eurostat_snapshot(
                connection=connection,
                local_snapshot=local_snapshot,
            )

    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    with duckdb.connect(str(database_path), read_only=True) as connection:
        result = assets.export_eurostat_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert result == {
        "eurostat_datasets_rows": 1,
        "eurostat_dimension_values_rows": 7,
        "eurostat_series_rows": 3,
        "eurostat_series_dimensions_rows": 12,
        "eurostat_observations_rows": 7,
    }
    assert (
        sum(statement.startswith("CREATE TABLE") for statement in client.statements)
        == 5
    )
    assert (
        sum(statement.startswith("EXCHANGE TABLES") for statement in client.statements)
        == 5
    )
    assert (
        sum(
            statement.startswith("DROP TABLE IF EXISTS")
            for statement in client.statements
        )
        == 5
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
