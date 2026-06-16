from collections.abc import Iterator
from contextlib import contextmanager
from typing import get_type_hints

from dagster_clickhouse import ClickhouseResource

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


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql: str) -> None:
        self.statements.append(sql)


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
