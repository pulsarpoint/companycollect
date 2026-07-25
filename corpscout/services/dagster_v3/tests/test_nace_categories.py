import hashlib
from collections.abc import Iterator
from contextlib import contextmanager

import dagster as dg
import dlt as dlt_lib
import duckdb
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.definitions import defs as load_project_defs
from dagster_v3.defs.nace import assets as nace_assets
from dagster_v3.defs.nace import source as nace_source
from dagster_v3.defs.nace import tables
from dagster_v3.defs.nace.source import NaceScheme, build_nace_rows, normalize_nace_code


def test_dlt_duckdb_destination_dependencies_are_available() -> None:
    import dlt

    assert hasattr(dlt.destinations, "duckdb")


def test_dagster_clickhouse_resource_dependency_is_available() -> None:
    assert ClickhouseResource


def test_nace_source_constants_define_official_versions() -> None:
    assert nace_source.SPARQL_ENDPOINT == "https://publications.europa.eu/webapi/rdf/sparql"
    assert nace_source.NACE_DUCKDB_PIPELINE_NAME == "nace_raw"
    assert nace_source.NACE_DUCKDB_DATASET_NAME == "nace_stage"
    assert nace_source.NACE_RAW_DLT_TABLE == "raw_sparql_payloads"
    assert nace_source.NACE_SCHEMES == (
        NaceScheme(
            "NACE_REV_2",
            "http://data.europa.eu/ux2/nace2/nace2",
            "2008-01-01",
            "2024-12-31",
            0,
        ),
        NaceScheme(
            "NACE_REV_2_1",
            "http://data.europa.eu/ux2/nace2.1/nace2.1",
            "2025-01-01",
            None,
            1,
        ),
    )


def test_nace_categories_schema_contracts() -> None:
    assert tables.NACE_DATABASE == "corpscout"
    assert tables.NACE_CATEGORIES_TABLE == "nace_categories"
    assert tables.QUALIFIED_NACE_CATEGORIES_TABLE == "corpscout.nace_categories"
    assert tables.NACE_CATEGORIES_COLUMNS == (
        "classification_version",
        "code",
        "normalized_code",
        "parent_code",
        "level",
        "section_code",
        "description_en",
        "concept_uri",
        "parent_concept_uri",
        "source_scheme_uri",
        "source_url",
        "source_payload_hash",
        "valid_from",
        "valid_to",
        "is_current",
        "source_run_id",
        "pulled_at",
        "_dlt_load_id",
        "_dlt_id",
    )
    assert tables.NACE_CATEGORIES_DUCKDB_CONTRACT.column_names == (
        tables.NACE_CATEGORIES_COLUMNS
    )
    assert tables.NACE_CATEGORIES_DUCKDB_COLUMN_TYPES["valid_from"] == "DATE"
    assert tables.NACE_CATEGORIES_DUCKDB_COLUMN_TYPES["pulled_at"] == "TIMESTAMP"
    assert "CREATE TABLE IF NOT EXISTS corpscout.nace_categories" in tables.NACE_CATEGORIES_DDL
    assert "ENGINE = ReplacingMergeTree(pulled_at)" in tables.NACE_CATEGORIES_DDL
    assert "ORDER BY (classification_version, normalized_code)" in tables.NACE_CATEGORIES_DDL


def test_parse_sparql_csv_reads_csv_bindings() -> None:
    rows = nace_source.parse_sparql_csv(
        "concept,notation,label,broader\n"
        "http://example.test/A,A,\"A Agriculture\",\n"
        "http://example.test/01,01,\"01 Crops\",http://example.test/A\n"
    )

    assert rows == [
        {
            "concept": "http://example.test/A",
            "notation": "A",
            "label": "A Agriculture",
            "broader": "",
        },
        {
            "concept": "http://example.test/01",
            "notation": "01",
            "label": "01 Crops",
            "broader": "http://example.test/A",
        },
    ]


def test_fetch_nace_scheme_payload_sends_csv_query_and_hashes_payload() -> None:
    session = FakeSparqlSession()

    payload = nace_source.fetch_nace_scheme_payload(
        scheme=nace_source.NACE_SCHEMES[0],
        session=session,
        user_agent="test-agent",
    )

    assert payload["classification_version"] == "NACE_REV_2"
    assert payload["source_url"] == nace_source.SPARQL_ENDPOINT
    assert payload["response_csv"] == session.bodies[0]
    assert payload["source_payload_hash"] == hashlib.sha256(
        session.bodies[0].encode("utf-8")
    ).hexdigest()
    assert payload["valid_from"] == "2008-01-01"
    assert payload["valid_to"] == "2024-12-31"
    assert payload["is_current"] == 0
    assert session.headers["User-Agent"] == "test-agent"
    assert session.calls[0][0] == nace_source.SPARQL_ENDPOINT
    assert session.calls[0][1]["format"] == "text/csv"
    assert "<http://data.europa.eu/ux2/nace2/nace2>" in session.calls[0][1]["query"]


def test_fetch_nace_scheme_rows_parses_raw_payload() -> None:
    session = FakeSparqlSession()

    rows, source_url, payload_hash = nace_source.fetch_nace_scheme_rows(
        scheme=nace_source.NACE_SCHEMES[0],
        session=session,
        user_agent="test-agent",
    )

    assert [row["notation"] for row in rows] == ["A", "01"]
    assert source_url == nace_source.SPARQL_ENDPOINT
    assert payload_hash == hashlib.sha256(session.bodies[0].encode("utf-8")).hexdigest()


def test_nace_raw_dlt_source_yields_one_payload_per_version() -> None:
    session = FakeSparqlSession()

    source = nace_source.nace_raw_source(
        source_run_id="test-run",
        pulled_at="2026-06-16T00:00:00.000Z",
        session=session,
    )
    rows = list(source.resources[nace_source.NACE_RAW_DLT_TABLE])

    assert [row["classification_version"] for row in rows] == [
        "NACE_REV_2",
        "NACE_REV_2_1",
    ]
    assert rows[0]["response_csv"] == session.bodies[0]
    assert rows[0]["valid_from"] == "2008-01-01"
    assert rows[0]["valid_to"] == "2024-12-31"
    assert rows[0]["is_current"] == 0
    assert rows[1]["response_csv"] == session.bodies[1]
    assert rows[1]["valid_from"] == "2025-01-01"
    assert rows[1]["valid_to"] is None
    assert rows[1]["is_current"] == 1
    assert {row["source_run_id"] for row in rows} == {"test-run"}
    assert all(len(row["source_payload_hash"]) == 64 for row in rows)
    assert [call[1]["format"] for call in session.calls] == ["text/csv", "text/csv"]


def test_nace_http_client_uses_dlt_retry_client() -> None:
    client = nace_source._nace_http_client(
        timeout_seconds=120,
        user_agent="test-agent",
        max_retries=5,
        retry_initial_delay_seconds=10.0,
        retry_max_delay_seconds=120.0,
    )

    assert client.__class__.__module__ == "dlt.sources.helpers.requests.retry"
    assert client._retry_kwargs["status_codes"] == (429, *range(500, 600))
    assert client._retry_kwargs["max_attempts"] == 5
    assert client._retry_kwargs["backoff_factor"] == 10.0
    assert client._retry_kwargs["respect_retry_after_header"] is True
    assert client._retry_kwargs["max_delay"] == 120.0


def test_nace_dlt_translator_maps_raw_duckdb_asset_contract() -> None:
    source = nace_source.nace_raw_source(schemes=())
    resource = source.resources[nace_source.NACE_RAW_DLT_TABLE]
    pipeline = dlt_lib.pipeline(
        pipeline_name="test_nace_raw",
        destination=dlt_lib.destinations.duckdb(":memory:"),
        dataset_name=nace_source.NACE_DUCKDB_DATASET_NAME,
        dev_mode=False,
    )
    data = type(
        "TranslatorData",
        (),
        {
            "resource": resource,
            "destination": pipeline.destination,
        },
    )()

    spec = nace_assets.NaceDltTranslator().get_asset_spec(data)

    assert spec.key == dg.AssetKey("nace_raw_duckdb")
    assert spec.group_name == "nace"
    assert spec.deps == []
    assert {"dlt", "duckdb", "reference"}.issubset(spec.kinds)


def test_nace_assets_are_registered_as_staged_flow() -> None:
    repository = load_project_defs().get_repository_def()
    asset_keys = {key.path[-1] for key in repository.asset_graph.get_all_asset_keys()}
    resource_keys = repository.get_top_level_resources().keys()

    assert "nace_raw_duckdb" in asset_keys
    assert "nace_categories_duckdb" in asset_keys
    assert "nace_categories_clickhouse" in asset_keys
    assert "nace_categories" not in asset_keys
    assert "dlt" in resource_keys
    assert "clickhouse" in resource_keys
    assert (
        repository.get_top_level_resources()["clickhouse"].configurable_resource_cls
        is ClickhouseResource
    )


def test_nace_registered_clickhouse_resource_reads_env_at_definition_time(
    monkeypatch,
) -> None:
    monkeypatch.setenv("CLICKHOUSE_NATIVE_PORT", "9440")
    monkeypatch.setenv("CLICKHOUSE_SECURE", "on")

    repository = load_project_defs().get_repository_def()
    resource = repository.get_top_level_resources()["clickhouse"].resource_fn.__self__

    assert isinstance(resource, ClickhouseResource)
    assert resource.port == 9440
    assert resource.secure is True


def test_normalize_nace_categories_duckdb_reads_raw_payloads(tmp_path) -> None:
    duckdb_path = tmp_path / "nace.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        _create_raw_nace_payloads_table(connection)
        for scheme, body in zip(nace_source.NACE_SCHEMES, FakeSparqlSession().bodies):
            connection.execute(
                f"""
                insert into {nace_source.NACE_DUCKDB_DATASET_NAME}.{nace_source.NACE_RAW_DLT_TABLE}
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    scheme.classification_version,
                    scheme.scheme_uri,
                    nace_source.SPARQL_ENDPOINT,
                    nace_source.nace_sparql_query(scheme.scheme_uri),
                    body,
                    hashlib.sha256(body.encode("utf-8")).hexdigest(),
                    scheme.valid_from,
                    scheme.valid_to,
                    scheme.is_current,
                    "run-1",
                    "2026-06-16T00:00:00.000",
                ],
            )

    with duckdb.connect(str(duckdb_path)) as connection:
        counts = nace_assets.normalize_nace_categories_duckdb(connection=connection)
        rerun_counts = nace_assets.normalize_nace_categories_duckdb(
            connection=connection
        )

    assert counts == {"raw_payloads": 2, "rows": 4}
    assert rerun_counts == counts
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        rows = connection.execute(
            f"""
            select classification_version, code, normalized_code, parent_code, section_code, description_en
            from {nace_source.NACE_DUCKDB_DATASET_NAME}.{tables.NACE_CATEGORIES_TABLE}
            order by classification_version, normalized_code
            """
        ).fetchall()

    assert rows == [
        ("NACE_REV_2", "01", "01", "A", "A", "01 Crops"),
        ("NACE_REV_2", "A", "A", None, "A", "A Agriculture"),
        ("NACE_REV_2_1", "05", "05", "B", "B", "05 Coal"),
        ("NACE_REV_2_1", "B", "B", None, "B", "B Mining"),
    ]


def test_normalize_nace_categories_clears_snapshot_for_empty_input(
    tmp_path,
) -> None:
    duckdb_path = tmp_path / "nace.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        _create_raw_nace_payloads_table(connection)
        nace_assets._ensure_nace_duckdb_schema(connection)
        connection.execute(
            f"""
            insert into {nace_source.NACE_DUCKDB_DATASET_NAME}.
                        {tables.NACE_CATEGORIES_TABLE}
            (classification_version)
            values ('STALE')
            """
        )

        counts = nace_assets.normalize_nace_categories_duckdb(connection=connection)
        remaining_rows = connection.execute(
            f"""
            select count(*)
            from {nace_source.NACE_DUCKDB_DATASET_NAME}.
                 {tables.NACE_CATEGORIES_TABLE}
            """
        ).fetchone()[0]

    assert counts == {"raw_payloads": 0, "rows": 0}
    assert remaining_rows == 0


def test_normalize_nace_categories_preserves_snapshot_for_invalid_input(
    tmp_path,
) -> None:
    duckdb_path = tmp_path / "nace.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        _create_raw_nace_payloads_table(connection)
        nace_assets._ensure_nace_duckdb_schema(connection)
        connection.execute(
            f"""
            insert into {nace_source.NACE_DUCKDB_DATASET_NAME}.
                        {tables.NACE_CATEGORIES_TABLE}
            (classification_version)
            values ('PRESERVED')
            """
        )
        scheme = nace_source.NACE_SCHEMES[0]
        connection.execute(
            f"""
            insert into {nace_source.NACE_DUCKDB_DATASET_NAME}.
                        {nace_source.NACE_RAW_DLT_TABLE}
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                scheme.classification_version,
                scheme.scheme_uri,
                nace_source.SPARQL_ENDPOINT,
                nace_source.nace_sparql_query(scheme.scheme_uri),
                "concept,notation,label,broader\n",
                "a" * 64,
                scheme.valid_from,
                scheme.valid_to,
                scheme.is_current,
                "run-1",
                "2026-06-16T00:00:00.000",
            ],
        )

        with pytest.raises(ValueError, match="returned no NACE rows"):
            nace_assets.normalize_nace_categories_duckdb(connection=connection)
        preserved_versions = connection.execute(
            f"""
            select classification_version
            from {nace_source.NACE_DUCKDB_DATASET_NAME}.
                 {tables.NACE_CATEGORIES_TABLE}
            """
        ).fetchall()

    assert preserved_versions == [("PRESERVED",)]


def test_export_nace_categories_clickhouse_replaces_from_duckdb(tmp_path, monkeypatch) -> None:
    duckdb_path = tmp_path / "nace.duckdb"
    with duckdb.connect(str(duckdb_path)) as connection:
        connection.execute(f"create schema {nace_source.NACE_DUCKDB_DATASET_NAME}")
        nace_assets._ensure_nace_duckdb_schema(connection)
        connection.execute(
            f"""
            insert into {nace_source.NACE_DUCKDB_DATASET_NAME}.{tables.NACE_CATEGORIES_TABLE}
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                "NACE_REV_2_1",
                "A",
                "A",
                None,
                "section",
                "A",
                "A Agriculture",
                "http://example.test/A",
                None,
                "http://example.test/scheme",
                nace_source.SPARQL_ENDPOINT,
                "a" * 64,
                "2025-01-01",
                None,
                1,
                "run-1",
                "2026-06-16T00:00:00.000",
                "",
                "row-id",
            ],
        )
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with duckdb.connect(str(duckdb_path)) as connection:
        counts = nace_assets.export_nace_categories_clickhouse(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert counts == {"rows": 1, "table": "corpscout.nace_categories"}
    assert client.statements[0].startswith("CREATE TABLE `corpscout`.`_tmp_nace_categories_")
    assert client.statements[0].endswith(" AS `corpscout`.`nace_categories`")
    assert client.statements[1].startswith("INSERT INTO `corpscout`.`_tmp_nace_categories_")
    assert client.statements[2].startswith("EXCHANGE TABLES `corpscout`.`_tmp_nace_categories_")
    assert client.statements[2].endswith(" AND `corpscout`.`nace_categories`")
    assert client.statements[3].startswith("DROP TABLE IF EXISTS `corpscout`.`_tmp_nace_categories_")
    assert client.inserted_rows[0][0][:3] == ("NACE_REV_2_1", "A", "A")


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserted_rows: list[list[tuple]] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple]:
        self.statements.append(sql)
        if params is not None:
            self.inserted_rows.append(list(params))
        return []


def _create_raw_nace_payloads_table(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        f"create schema if not exists {nace_source.NACE_DUCKDB_DATASET_NAME}"
    )
    connection.execute(
        f"""
        create table {nace_source.NACE_DUCKDB_DATASET_NAME}.
                     {nace_source.NACE_RAW_DLT_TABLE} (
          classification_version varchar,
          source_scheme_uri varchar,
          source_url varchar,
          request_query varchar,
          response_csv varchar,
          source_payload_hash varchar,
          valid_from date,
          valid_to date,
          is_current utinyint,
          source_run_id varchar,
          pulled_at timestamp
        )
        """
    )


class FakeSparqlResponse:
    def __init__(self, body: str) -> None:
        self.text = body

    def raise_for_status(self) -> None:
        return None


class FakeSparqlSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        self.calls: list[tuple[str, dict[str, str], int]] = []
        self.bodies = [
            (
                "concept,notation,label,broader\n"
                "http://data.europa.eu/ux2/nace2/A,A,\"A Agriculture\",\n"
                "http://data.europa.eu/ux2/nace2/01,01,\"01 Crops\",http://data.europa.eu/ux2/nace2/A\n"
            ),
            (
                "concept,notation,label,broader\n"
                "http://data.europa.eu/ux2/nace2.1/B,B,\"B Mining\",\n"
                "http://data.europa.eu/ux2/nace2.1/05,05,\"05 Coal\",http://data.europa.eu/ux2/nace2.1/B\n"
            ),
        ]
        self.responses = [FakeSparqlResponse(body) for body in self.bodies]

    def get(
        self,
        url: str,
        params: dict[str, str] | None = None,
        timeout: int = 120,
    ) -> FakeSparqlResponse:
        assert params is not None
        self.calls.append((url, params, timeout))
        return self.responses.pop(0)


def test_normalize_nace_code_preserves_sections_and_strips_numeric_punctuation() -> None:
    assert normalize_nace_code("A") == "A"
    assert normalize_nace_code("01") == "01"
    assert normalize_nace_code("01.1") == "011"
    assert normalize_nace_code("01.11") == "0111"
    assert normalize_nace_code("01-11") == "0111"
    assert normalize_nace_code("01/11") == "0111"


def test_build_nace_rows_preserves_hierarchy_and_versions() -> None:
    scheme = NaceScheme(
        classification_version="NACE_REV_2_1",
        scheme_uri="http://data.europa.eu/ux2/nace2.1/nace2.1",
        valid_from="2025-01-01",
        valid_to=None,
        is_current=1,
    )
    source_rows = [
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/A",
            "notation": "A",
            "label": "A AGRICULTURE, FORESTRY AND FISHING",
            "broader": "",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/01",
            "notation": "01",
            "label": "01 Crop and animal production, hunting and related service activities",
            "broader": "http://data.europa.eu/ux2/nace2.1/A",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/011",
            "notation": "01.1",
            "label": "01.1 Growing of non-perennial crops",
            "broader": "http://data.europa.eu/ux2/nace2.1/01",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/0111",
            "notation": "01.11",
            "label": "01.11 Growing of cereals, other than rice, leguminous crops and oil seeds",
            "broader": "http://data.europa.eu/ux2/nace2.1/011",
        },
    ]

    rows = build_nace_rows(
        scheme=scheme,
        source_rows=source_rows,
        source_url="https://publications.europa.eu/webapi/rdf/sparql",
        source_payload_hash="a" * 64,
        source_run_id="run-1",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert [row["level"] for row in rows] == ["section", "division", "group", "class"]
    assert rows[0]["section_code"] == "A"
    assert rows[1]["parent_code"] == "A"
    assert rows[2]["parent_code"] == "01"
    assert rows[3]["parent_code"] == "01.1"
    assert rows[3]["normalized_code"] == "0111"
    assert rows[3]["classification_version"] == "NACE_REV_2_1"
    assert rows[3]["valid_from"] == "2025-01-01"
    assert rows[3]["valid_to"] is None
    assert rows[3]["is_current"] == 1


def test_build_nace_rows_uses_normalized_keys_for_punctuation_variant_hierarchy() -> None:
    scheme = NaceScheme(
        classification_version="NACE_REV_2_1",
        scheme_uri="http://data.europa.eu/ux2/nace2.1/nace2.1",
        valid_from="2025-01-01",
        valid_to=None,
        is_current=1,
    )
    source_rows = [
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/A",
            "notation": "A",
            "label": "A AGRICULTURE, FORESTRY AND FISHING",
            "broader": "",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/01",
            "notation": "01",
            "label": "01 Crop and animal production, hunting and related service activities",
            "broader": "http://data.europa.eu/ux2/nace2.1/A",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/011",
            "notation": "01/1",
            "label": "01/1 Growing of non-perennial crops",
            "broader": "http://data.europa.eu/ux2/nace2.1/01",
        },
        {
            "concept": "http://data.europa.eu/ux2/nace2.1/0111",
            "notation": "01-11",
            "label": "01-11 Growing of cereals, other than rice",
            "broader": "http://data.europa.eu/ux2/nace2.1/011",
        },
    ]

    rows = build_nace_rows(
        scheme=scheme,
        source_rows=source_rows,
        source_url="https://publications.europa.eu/webapi/rdf/sparql",
        source_payload_hash="b" * 64,
        source_run_id="run-2",
        pulled_at="2026-06-16T00:00:00.000Z",
    )

    assert rows[2]["code"] == "01/1"
    assert rows[2]["normalized_code"] == "011"
    assert rows[2]["parent_code"] == "01"
    assert rows[2]["section_code"] == "A"
    assert rows[3]["code"] == "01-11"
    assert rows[3]["normalized_code"] == "0111"
    assert rows[3]["parent_code"] == "01.1"
    assert rows[3]["section_code"] == "A"
