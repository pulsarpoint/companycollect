from contextlib import contextmanager
from pathlib import Path

import duckdb

from dagster_v3.defs.brazil_rfb import clickhouse, tables, transforms
from tests.test_brazil_rfb_transforms import _create_raw_tables


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: object | None = None):
        if "system.tables" in sql:
            return [
                (tables.BR_COMPANIES_TABLE_CH,),
                (tables.BR_ESTABLISHMENTS_TABLE_CH,),
            ]
        if isinstance(params, list):
            self.inserts.append((sql, params))
            return None
        self.statements.append(sql)
        return None


class FakeClickHouseResource:
    def __init__(self, client: FakeClickHouseClient) -> None:
        self.client = client

    @contextmanager
    def get_connection(self):
        yield self.client


def test_clickhouse_exports_replace_companies_and_establishments(tmp_path: Path) -> None:
    database_path = tmp_path / "br.duckdb"
    _create_raw_tables(database_path)
    transforms.build_brazil_rfb_companies_and_establishments(
        database_path=database_path,
        source_run_id="run-1",
    )
    fake_client = FakeClickHouseClient()
    fake_resource = FakeClickHouseResource(fake_client)

    company_rows = clickhouse.export_brazil_rfb_clickhouse_companies(
        database_path=database_path,
        clickhouse=fake_resource,
    )
    establishment_rows = clickhouse.export_brazil_rfb_clickhouse_establishments(
        database_path=database_path,
        clickhouse=fake_resource,
    )

    assert company_rows == 2
    assert establishment_rows == 2
    assert sum("EXCHANGE TABLES" in statement for statement in fake_client.statements) == 2
    assert len(fake_client.inserts) == 2
    company_insert_sql, company_insert_rows = fake_client.inserts[0]
    establishment_insert_sql, establishment_insert_rows = fake_client.inserts[1]
    assert tables.BR_COMPANIES_TABLE_CH in company_insert_sql
    assert tables.BR_ESTABLISHMENTS_TABLE_CH in establishment_insert_sql
    assert len(company_insert_rows[0]) == len(tables.BR_COMPANIES_EXPORT_COLUMNS)
    assert len(establishment_insert_rows[0]) == len(tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS)

    with duckdb.connect(str(database_path), read_only=True) as connection:
        assert connection.execute(
            f"select count(*) from {tables.DLT_DATASET_NAME}.{tables.COMPANIES_TABLE}"
        ).fetchone()[0] == company_rows
