import hashlib
from collections.abc import Iterator
from contextlib import contextmanager
from typing import get_type_hints

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.nace import source as nace_source
from dagster_v3.defs.nace import tables
from dagster_v3.defs.nace.clickhouse import prepare_nace_categories_table
from dagster_v3.defs.nace.source import NaceScheme, build_nace_rows, normalize_nace_code


def test_dlt_clickhouse_destination_dependencies_are_available() -> None:
    import clickhouse_connect
    import dlt

    assert clickhouse_connect
    assert hasattr(dlt.destinations, "clickhouse")


def test_dagster_clickhouse_resource_dependency_is_available() -> None:
    assert ClickhouseResource


def test_nace_source_constants_define_official_versions() -> None:
    assert nace_source.SPARQL_ENDPOINT == "https://publications.europa.eu/webapi/rdf/sparql"
    assert nace_source.NACE_CATEGORIES_DLT_TABLE == "nace_categories"
    assert nace_source.NACE_DLT_DATASET_NAME == "reference"
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


def test_nace_categories_clickhouse_schema_contract() -> None:
    assert tables.NACE_DATABASE == "reference"
    assert tables.NACE_CATEGORIES_TABLE == "nace_categories"
    assert tables.QUALIFIED_NACE_CATEGORIES_TABLE == "reference.nace_categories"
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
    )
    assert "CREATE TABLE IF NOT EXISTS reference.nace_categories" in tables.NACE_CATEGORIES_DDL
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


def test_fetch_nace_scheme_rows_sends_csv_query_and_hashes_payload() -> None:
    session = FakeSparqlSession()

    rows, source_url, payload_hash = nace_source.fetch_nace_scheme_rows(
        scheme=nace_source.NACE_SCHEMES[0],
        session=session,
        user_agent="test-agent",
    )

    assert [row["notation"] for row in rows] == ["A", "01"]
    assert source_url == nace_source.SPARQL_ENDPOINT
    assert payload_hash == hashlib.sha256(session.bodies[0].encode("utf-8")).hexdigest()
    assert session.headers["User-Agent"] == "test-agent"
    assert session.calls[0][0] == nace_source.SPARQL_ENDPOINT
    assert session.calls[0][1]["format"] == "text/csv"
    assert "<http://data.europa.eu/ux2/nace2/nace2>" in session.calls[0][1]["query"]


def test_nace_dlt_source_yields_both_versions_with_version_metadata() -> None:
    session = FakeSparqlSession()

    source = nace_source.nace_categories_source(
        source_run_id="test-run",
        pulled_at="2026-06-16T00:00:00.000Z",
        session=session,
    )
    rows = list(source.resources[nace_source.NACE_CATEGORIES_DLT_TABLE])

    assert [row["classification_version"] for row in rows] == [
        "NACE_REV_2",
        "NACE_REV_2",
        "NACE_REV_2_1",
        "NACE_REV_2_1",
    ]
    assert rows[0]["valid_from"] == "2008-01-01"
    assert rows[0]["valid_to"] == "2024-12-31"
    assert rows[0]["is_current"] == 0
    assert rows[2]["valid_from"] == "2025-01-01"
    assert rows[2]["valid_to"] is None
    assert rows[2]["is_current"] == 1
    assert {row["source_run_id"] for row in rows} == {"test-run"}
    assert all(len(row["source_payload_hash"]) == 64 for row in rows)
    assert [call[1]["format"] for call in session.calls] == ["text/csv", "text/csv"]
    assert "<http://data.europa.eu/ux2/nace2/nace2>" in session.calls[0][1]["query"]
    assert "<http://data.europa.eu/ux2/nace2.1/nace2.1>" in session.calls[1][1]["query"]


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


def test_nace_clickhouse_pipeline_targets_reference_dataset() -> None:
    pipeline = nace_source.nace_clickhouse_pipeline()

    assert pipeline.pipeline_name == "nace_categories"
    assert pipeline.dataset_name == "reference"
    assert pipeline.dev_mode is False
    assert pipeline.destination.destination_name == "clickhouse"


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)


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


def test_prepare_nace_categories_table_is_typed_for_official_resource() -> None:
    annotations = get_type_hints(prepare_nace_categories_table)

    assert annotations["clickhouse"] is ClickhouseResource


def test_prepare_nace_categories_table_uses_official_resource_connection(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()
    connection_calls: list[ClickhouseResource] = []

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        connection_calls.append(self)
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    prepare_nace_categories_table(resource)

    assert connection_calls == [resource]
    assert client.statements[0] == "CREATE DATABASE IF NOT EXISTS reference"
    assert client.statements[1].startswith("CREATE TABLE IF NOT EXISTS reference.nace_categories")
    assert client.statements[2] == "TRUNCATE TABLE reference.nace_categories"


def test_prepare_nace_categories_table_strips_ddl_whitespace(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient()

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    prepare_nace_categories_table(resource)

    assert client.statements == [
        "CREATE DATABASE IF NOT EXISTS reference",
        tables.NACE_CATEGORIES_DDL.strip(),
        "TRUNCATE TABLE reference.nace_categories",
    ]
    ddl = client.statements[1]
    assert ddl == ddl.strip()


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
