from dagster_v3.defs.nace import tables
from dagster_v3.defs.nace.resources import ClickHouseResource, prepare_nace_categories_table


def test_dlt_clickhouse_destination_dependencies_are_available() -> None:
    import clickhouse_connect
    import dlt

    assert clickhouse_connect
    assert hasattr(dlt.destinations, "clickhouse")


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
        self.commands: list[str] = []
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def command(self, sql: str) -> None:
        self.commands.append(sql)

    def insert(self, table: str, data: list[list[object]], column_names: list[str]) -> None:
        self.inserts.append((table, data, column_names))


def test_clickhouse_resource_reuses_supplied_client_for_table_setup() -> None:
    client = FakeClickHouseClient()
    resource = ClickHouseResource(
        host="localhost",
        password="secret",
        clickhouse_client=client,
    )

    prepare_nace_categories_table(resource)

    assert client.commands[0] == "CREATE DATABASE IF NOT EXISTS reference"
    assert client.commands[1].startswith("CREATE TABLE IF NOT EXISTS reference.nace_categories")
    assert client.commands[2] == "TRUNCATE TABLE reference.nace_categories"
