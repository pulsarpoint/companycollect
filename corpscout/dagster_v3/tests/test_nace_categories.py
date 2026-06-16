from dagster_v3.defs.nace import tables
from dagster_v3.defs.nace.clickhouse import prepare_nace_categories_table


def test_dlt_clickhouse_destination_dependencies_are_available() -> None:
    import clickhouse_connect
    import dlt

    assert clickhouse_connect
    assert hasattr(dlt.destinations, "clickhouse")


def test_dagster_clickhouse_resource_dependency_is_available() -> None:
    from dagster_clickhouse import ClickhouseResource

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


class FakeConnection:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    def __enter__(self) -> FakeClickHouseClient:
        return self.client

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        return None


class FakeClickhouseResource:
    def __init__(self) -> None:
        self.client = FakeClickHouseClient()

    def get_connection(self) -> FakeConnection:
        return FakeConnection(self.client)


def test_prepare_nace_categories_table_uses_official_resource_connection() -> None:
    resource = FakeClickhouseResource()

    prepare_nace_categories_table(resource)

    assert resource.client.statements == [
        "CREATE DATABASE IF NOT EXISTS reference",
        tables.NACE_CATEGORIES_DDL.strip(),
        "TRUNCATE TABLE reference.nace_categories",
    ]


def test_prepare_nace_categories_table_strips_ddl_whitespace() -> None:
    client = FakeClickHouseClient()
    resource = FakeClickhouseResource()
    resource.client = client

    prepare_nace_categories_table(resource)

    ddl = client.statements[1]
    assert ddl == ddl.strip()
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS reference.nace_categories")
