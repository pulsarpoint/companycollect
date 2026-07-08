from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date

import duckdb
import pyarrow as pa
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse import resolved as clickhouse_resolved
from dagster_v3.defs.clickhouse.resolved import (
    REQUIRED_FINLAND_RESOLVED_TABLES,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
    replace_duckdb_connection_tables_in_clickhouse,
)


def _export_table(*, duckdb_path, **kwargs):
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        return export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=connection,
            **kwargs,
        )


def _replace_tables(*, duckdb_path, **kwargs):
    with duckdb.connect(str(duckdb_path), read_only=True) as connection:
        return replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=connection,
            **kwargs,
        )


class FakeClickHouseClient:
    def __init__(self, existing_tables: set[str]) -> None:
        self.existing_tables = existing_tables
        self.statements: list[str] = []

    def execute(
        self, sql: str, params: dict[str, object] | None = None
    ) -> list[tuple[str]]:
        self.statements.append(sql)
        if "system.tables" not in sql:
            raise AssertionError(sql)
        database = str(params["database"]) if params else ""
        requested = set(params["tables"]) if params else set()
        return [
            (table.removeprefix(f"{database}."),)
            for table in sorted(self.existing_tables)
            if table.startswith(f"{database}.")
            and table.removeprefix(f"{database}.") in requested
        ]


def test_required_finland_resolved_tables_are_explicit() -> None:
    assert REQUIRED_FINLAND_RESOLVED_TABLES == (
        "fi_companies",
        "fi_websites",
        "fi_industries",
        "fi_addresses",
        "fi_registered_entries",
        "fi_legal_forms",
        "fi_financial_statements",
    )


def test_assert_clickhouse_tables_exist_uses_official_resource(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient({"corpscout.fi_companies", "corpscout.fi_websites"})

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    assert_clickhouse_tables_exist(
        resource,
        database="corpscout",
        tables=("fi_companies", "fi_websites"),
    )

    assert "system.tables" in client.statements[0]


def test_assert_clickhouse_tables_exist_reports_missing_tables(monkeypatch) -> None:
    resource = ClickhouseResource(host="localhost")
    client = FakeClickHouseClient({"corpscout.fi_companies"})

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    try:
        assert_clickhouse_tables_exist(
            resource,
            database="corpscout",
            tables=("fi_companies", "fi_websites"),
        )
    except ValueError as exc:
        assert str(exc) == "Missing ClickHouse tables in corpscout: fi_websites"
    else:
        raise AssertionError("expected missing table error")


def test__export_table_inserts_rows_in_column_order(tmp_path) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar,
                country_iso2 varchar,
                source_system varchar
            )
            """
        )
        connection.execute(
            "insert into finland_resolved.fi_companies values ('1234567-8', 'FI', 'finland_prhytj')"
        )

    client = FakeInsertClickHouseClient()

    row_count = _export_table(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema="finland_resolved",
        duckdb_table="fi_companies",
        clickhouse_database="corpscout",
        clickhouse_table="fi_companies",
        columns=("business_id", "country_iso2", "source_system"),
        truncate=False,
    )

    assert row_count == 1
    assert client.statements == []
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`fi_companies` (`business_id`, `country_iso2`, `source_system`) VALUES",
            [("1234567-8", "FI", "finland_prhytj")],
        )
    ]


def test_export_duckdb_connection_table_to_clickhouse_inserts_rows_in_batches(
    tmp_path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_companies values
              ('1000001-1'),
              ('1000002-2'),
              ('1000003-3')
            """
        )

        client = FakeInsertClickHouseClient()

        row_count = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            duckdb_table="fi_companies",
            clickhouse_database="corpscout",
            clickhouse_table="fi_companies",
            columns=("business_id",),
            truncate=False,
            batch_size=2,
        )

    assert row_count == 3
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`fi_companies` (`business_id`) VALUES",
            [("1000001-1",), ("1000002-2",)],
        ),
        (
            "INSERT INTO `corpscout`.`fi_companies` (`business_id`) VALUES",
            [("1000003-3",)],
        ),
    ]


def test_export_duckdb_connection_table_to_clickhouse_logs_row_batch_progress(
    tmp_path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            "create table finland_resolved.fi_companies (business_id varchar)"
        )
        connection.execute(
            """
            insert into finland_resolved.fi_companies values
              ('1000001-1'),
              ('1000002-2'),
              ('1000003-3')
            """
        )

        client = FakeInsertClickHouseClient()
        messages: list[str] = []

        row_count = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            duckdb_table="fi_companies",
            clickhouse_database="corpscout",
            clickhouse_table="fi_companies",
            columns=("business_id",),
            truncate=False,
            batch_size=2,
            log=lambda message, *args: messages.append(message % args),
        )

    assert row_count == 3
    assert messages == [
        "Inserted ClickHouse row batch: table=corpscout.fi_companies "
        "batch_number=1 batch_rows=2 total_rows=2",
        "Inserted ClickHouse row batch: table=corpscout.fi_companies "
        "batch_number=2 batch_rows=1 total_rows=3",
    ]


def test_export_duckdb_connection_table_to_clickhouse_prefers_arrow_batches(
    tmp_path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_companies values
              ('1000001-1'),
              ('1000002-2'),
              ('1000003-3')
            """
        )

        client = FakeArrowClickHouseClient()

        row_count = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            duckdb_table="fi_companies",
            clickhouse_database="corpscout",
            clickhouse_table="fi_companies",
            columns=("business_id",),
            truncate=False,
            batch_size=2,
        )

    assert row_count == 3
    assert client.insert_calls == []
    assert [
        (database, table, arrow_table.column_names, arrow_table.to_pylist())
        for database, table, arrow_table in client.arrow_insert_calls
    ] == [
        (
            "corpscout",
            "fi_companies",
            ["business_id"],
            [{"business_id": "1000001-1"}, {"business_id": "1000002-2"}],
        ),
        (
            "corpscout",
            "fi_companies",
            ["business_id"],
            [{"business_id": "1000003-3"}],
        ),
    ]


def test_export_duckdb_connection_table_to_clickhouse_applies_column_expressions(
    tmp_path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar,
                activity_start_date date
            )
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_companies values
              ('old', date '1893-07-05'),
              ('valid', date '2020-01-01')
            """
        )

        client = FakeArrowClickHouseClient()

        row_count = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            duckdb_table="fi_companies",
            clickhouse_database="corpscout",
            clickhouse_table="fi_companies",
            columns=("business_id", "activity_start_date"),
            truncate=False,
            column_expressions={
                "activity_start_date": (
                    "case when activity_start_date >= date '1900-01-01' "
                    "then activity_start_date else null end"
                ),
            },
        )

    assert row_count == 2
    assert client.arrow_insert_calls == []
    assert client.insert_calls == []
    assert client.row_insert_calls == [
        (
            "corpscout",
            "fi_companies",
            [("old", None), ("valid", date(2020, 1, 1))],
            ("business_id", "activity_start_date"),
        )
    ]


def test_export_duckdb_connection_table_to_clickhouse_uses_rows_for_nullable_dates(
    tmp_path,
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema norway_resolved")
        connection.execute(
            """
            create table norway_resolved.no_companies (
                org_number varchar,
                incorporation_date date
            )
            """
        )
        connection.execute(
            """
            insert into norway_resolved.no_companies values
              ('old', null),
              ('valid', date '2020-01-01')
            """
        )

        client = FakeArrowClickHouseClient()

        row_count = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=client,
            duckdb_schema="norway_resolved",
            duckdb_table="no_companies",
            clickhouse_database="corpscout",
            clickhouse_table="no_companies",
            columns=("org_number", "incorporation_date"),
            truncate=False,
        )

    assert row_count == 2
    assert client.arrow_insert_calls == []
    assert client.insert_calls == []
    assert client.row_insert_calls == [
        (
            "corpscout",
            "no_companies",
            [("old", None), ("valid", date(2020, 1, 1))],
            ("org_number", "incorporation_date"),
        )
    ]


def test__export_table_inserts_rows_in_batches(tmp_path) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_companies values
              ('1000001-1'),
              ('1000002-2'),
              ('1000003-3')
            """
        )

    client = FakeInsertClickHouseClient()

    row_count = _export_table(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema="finland_resolved",
        duckdb_table="fi_companies",
        clickhouse_database="corpscout",
        clickhouse_table="fi_companies",
        columns=("business_id",),
        truncate=False,
        batch_size=2,
    )

    assert row_count == 3
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`fi_companies` (`business_id`) VALUES",
            [("1000001-1",), ("1000002-2",)],
        ),
        (
            "INSERT INTO `corpscout`.`fi_companies` (`business_id`) VALUES",
            [("1000003-3",)],
        ),
    ]


def test__export_table_uses_stage_then_exchange_for_truncate(
    tmp_path, monkeypatch
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar,
                source_system varchar
            )
            """
        )
        connection.execute(
            "insert into finland_resolved.fi_companies values ('1234567-8', 'finland_prhytj')"
        )

    monkeypatch.setattr(
        clickhouse_resolved.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": "deadbeef"})(),
    )
    client = FakeInsertClickHouseClient()

    row_count = _export_table(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema="finland_resolved",
        duckdb_table="fi_companies",
        clickhouse_database="corpscout",
        clickhouse_table="fi_companies",
        columns=("business_id", "source_system"),
        truncate=True,
    )

    assert row_count == 1
    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_deadbeef` AS `corpscout`.`fi_companies`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_deadbeef` AND `corpscout`.`fi_companies`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_deadbeef`",
    ]
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`_tmp_fi_companies_deadbeef` (`business_id`, `source_system`) VALUES",
            [("1234567-8", "finland_prhytj")],
        )
    ]


def test__export_table_returns_zero_for_empty_table(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar,
                source_system varchar
            )
            """
        )

    monkeypatch.setattr(
        clickhouse_resolved.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": "deadbeef"})(),
    )
    client = FakeInsertClickHouseClient()

    row_count = _export_table(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema="finland_resolved",
        duckdb_table="fi_companies",
        clickhouse_database="corpscout",
        clickhouse_table="fi_companies",
        columns=("business_id", "source_system"),
        truncate=True,
    )

    assert row_count == 0
    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_deadbeef` AS `corpscout`.`fi_companies`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_deadbeef` AND `corpscout`.`fi_companies`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_deadbeef`",
    ]
    assert client.insert_calls == []


def test__export_table_escapes_identifiers(tmp_path) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute('create schema "schema""name"')
        connection.execute(
            """
            create table "schema""name"."table""name" (
                "column""name" varchar
            )
            """
        )
        connection.execute(
            """
            insert into "schema""name"."table""name" values ('value')
            """
        )

    client = FakeInsertClickHouseClient()

    row_count = _export_table(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema='schema"name',
        duckdb_table='table"name',
        clickhouse_database="corp`scout",
        clickhouse_table="fi`companies",
        columns=('column"name',),
        truncate=False,
    )

    assert row_count == 1
    assert client.insert_calls == [
        (
            'INSERT INTO `corp``scout`.`fi``companies` (`column"name`) VALUES',
            [("value",)],
        )
    ]


def test__export_table_supports_dotted_schema_qualifier(
    tmp_path,
) -> None:
    database_path = tmp_path / "wikidata.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema wikidata.wikidata")
        connection.execute(
            """
            create table wikidata.wikidata.wikidata_companies (
                wikidata_id varchar
            )
            """
        )
        connection.execute(
            "insert into wikidata.wikidata.wikidata_companies values ('Q1')"
        )

    client = FakeInsertClickHouseClient()

    row_count = _export_table(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema="wikidata.wikidata",
        duckdb_table="wikidata_companies",
        clickhouse_database="corpscout",
        clickhouse_table="wikidata_companies",
        columns=("wikidata_id",),
        truncate=False,
    )

    assert row_count == 1
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`wikidata_companies` (`wikidata_id`) VALUES",
            [("Q1",)],
        )
    ]


def test__export_table_cleanup_attempts_drop_on_insert_failure(
    tmp_path, monkeypatch
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            "insert into finland_resolved.fi_companies values ('1234567-8')"
        )

    monkeypatch.setattr(
        clickhouse_resolved.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": "deadbeef"})(),
    )
    client = FailingInsertClickHouseClient()

    try:
        _export_table(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            duckdb_table="fi_companies",
            clickhouse_database="corpscout",
            clickhouse_table="fi_companies",
            columns=("business_id",),
            truncate=True,
        )
    except RuntimeError as exc:
        assert str(exc) == "insert failed"
    else:
        raise AssertionError("expected insert failure")

    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_deadbeef` AS `corpscout`.`fi_companies`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_deadbeef`",
    ]
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`_tmp_fi_companies_deadbeef` (`business_id`) VALUES",
            [("1234567-8",)],
        )
    ]


def test__export_table_raises_cleanup_error_after_successful_exchange(
    tmp_path, monkeypatch
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            "insert into finland_resolved.fi_companies values ('1234567-8')"
        )

    monkeypatch.setattr(
        clickhouse_resolved.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": "deadbeef"})(),
    )
    client = FailingDropClickHouseClient(
        failing_drops=(
            "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_deadbeef`",
        ),
    )

    try:
        _export_table(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            duckdb_table="fi_companies",
            clickhouse_database="corpscout",
            clickhouse_table="fi_companies",
            columns=("business_id",),
            truncate=True,
        )
    except RuntimeError as exc:
        assert (
            str(exc)
            == "Failed to drop ClickHouse stage table(s): `corpscout`.`_tmp_fi_companies_deadbeef`"
        )
        assert isinstance(exc.__cause__, RuntimeError)
        assert str(exc.__cause__) == (
            "drop failed for "
            "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_deadbeef`"
        )
    else:
        raise AssertionError("expected cleanup failure")

    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_deadbeef` AS `corpscout`.`fi_companies`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_deadbeef` AND `corpscout`.`fi_companies`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_deadbeef`",
    ]


def test__replace_tables_rolls_back_on_exchange_failure(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            create table finland_resolved.fi_websites (
                business_id varchar
            )
            """
        )
        connection.execute(
            "insert into finland_resolved.fi_companies values ('1234567-8')"
        )
        connection.execute(
            "insert into finland_resolved.fi_websites values ('1234567-8')"
        )

    stage_names = iter(["first", "second"])
    monkeypatch.setattr(
        clickhouse_resolved.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": next(stage_names)})(),
    )
    client = FailingSecondExchangeClickHouseClient()

    try:
        _replace_tables(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            clickhouse_database="corpscout",
            tables=(
                ("fi_companies", ("business_id",)),
                ("fi_websites", ("business_id",)),
            ),
        )
    except RuntimeError as exc:
        assert str(exc) == "exchange failed"
    else:
        raise AssertionError("expected exchange failure")

    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_first` AS `corpscout`.`fi_companies`",
        "CREATE TABLE `corpscout`.`_tmp_fi_websites_second` AS `corpscout`.`fi_websites`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_first` AND `corpscout`.`fi_companies`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_websites_second` AND `corpscout`.`fi_websites`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_first` AND `corpscout`.`fi_companies`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_websites_second`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_first`",
    ]
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`_tmp_fi_companies_first` (`business_id`) VALUES",
            [("1234567-8",)],
        ),
        (
            "INSERT INTO `corpscout`.`_tmp_fi_websites_second` (`business_id`) VALUES",
            [("1234567-8",)],
        ),
    ]


def test__replace_tables_loads_stages_in_batches(tmp_path, monkeypatch) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            create table finland_resolved.fi_websites (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_companies values
              ('1000001-1'),
              ('1000002-2'),
              ('1000003-3')
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_websites values
              ('2000001-1'),
              ('2000002-2')
            """
        )

    stage_names = iter(["first", "second"])
    monkeypatch.setattr(
        clickhouse_resolved.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": next(stage_names)})(),
    )
    client = FakeInsertClickHouseClient()

    row_counts = _replace_tables(
        duckdb_path=database_path,
        clickhouse_client=client,
        duckdb_schema="finland_resolved",
        clickhouse_database="corpscout",
        tables=(
            ("fi_companies", ("business_id",)),
            ("fi_websites", ("business_id",)),
        ),
        batch_size=2,
    )

    assert row_counts == {
        "fi_companies": 3,
        "fi_websites": 2,
    }
    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_first` AS `corpscout`.`fi_companies`",
        "CREATE TABLE `corpscout`.`_tmp_fi_websites_second` AS `corpscout`.`fi_websites`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_first` AND `corpscout`.`fi_companies`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_websites_second` AND `corpscout`.`fi_websites`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_websites_second`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_first`",
    ]
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`_tmp_fi_companies_first` (`business_id`) VALUES",
            [("1000001-1",), ("1000002-2",)],
        ),
        (
            "INSERT INTO `corpscout`.`_tmp_fi_companies_first` (`business_id`) VALUES",
            [("1000003-3",)],
        ),
        (
            "INSERT INTO `corpscout`.`_tmp_fi_websites_second` (`business_id`) VALUES",
            [("2000001-1",), ("2000002-2",)],
        ),
    ]


def test_replace_duckdb_connection_tables_in_clickhouse_loads_stages_in_batches(
    tmp_path, monkeypatch
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            create table finland_resolved.fi_websites (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_companies values
              ('1000001-1'),
              ('1000002-2'),
              ('1000003-3')
            """
        )
        connection.execute(
            """
            insert into finland_resolved.fi_websites values
              ('2000001-1'),
              ('2000002-2')
            """
        )

        stage_names = iter(["first", "second"])
        monkeypatch.setattr(
            clickhouse_resolved.uuid,
            "uuid4",
            lambda: type("U", (), {"hex": next(stage_names)})(),
        )
        client = FakeInsertClickHouseClient()

        row_counts = replace_duckdb_connection_tables_in_clickhouse(
            duckdb_connection=connection,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            clickhouse_database="corpscout",
            tables=(
                ("fi_companies", ("business_id",)),
                ("fi_websites", ("business_id",)),
            ),
            batch_size=2,
        )

    assert row_counts == {
        "fi_companies": 3,
        "fi_websites": 2,
    }
    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_first` AS `corpscout`.`fi_companies`",
        "CREATE TABLE `corpscout`.`_tmp_fi_websites_second` AS `corpscout`.`fi_websites`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_first` AND `corpscout`.`fi_companies`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_websites_second` AND `corpscout`.`fi_websites`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_websites_second`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_first`",
    ]
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`_tmp_fi_companies_first` (`business_id`) VALUES",
            [("1000001-1",), ("1000002-2",)],
        ),
        (
            "INSERT INTO `corpscout`.`_tmp_fi_companies_first` (`business_id`) VALUES",
            [("1000003-3",)],
        ),
        (
            "INSERT INTO `corpscout`.`_tmp_fi_websites_second` (`business_id`) VALUES",
            [("2000001-1",), ("2000002-2",)],
        ),
    ]


def test__replace_tables_surfaces_rollback_exchange_failures(
    tmp_path, monkeypatch
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            create table finland_resolved.fi_websites (
                business_id varchar
            )
            """
        )
        connection.execute(
            "insert into finland_resolved.fi_companies values ('1234567-8')"
        )
        connection.execute(
            "insert into finland_resolved.fi_websites values ('1234567-8')"
        )

    stage_names = iter(["first", "second"])
    monkeypatch.setattr(
        clickhouse_resolved.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": next(stage_names)})(),
    )
    client = FailingSecondExchangeAndRollbackClickHouseClient()

    try:
        _replace_tables(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            clickhouse_database="corpscout",
            tables=(
                ("fi_companies", ("business_id",)),
                ("fi_websites", ("business_id",)),
            ),
        )
    except RuntimeError as exc:
        assert (
            str(exc)
            == "Rollback failed after ClickHouse publish error; ClickHouse may be inconsistent. "
            "Failed rollback exchange(s): fi_companies "
            "(`corpscout`.`_tmp_fi_companies_first` <-> `corpscout`.`fi_companies`)"
        )
        assert isinstance(exc.__cause__, RuntimeError)
        assert str(exc.__cause__) == "exchange failed"
        assert exc.__context__ is exc.__cause__
    else:
        raise AssertionError("expected rollback failure")

    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_first` AS `corpscout`.`fi_companies`",
        "CREATE TABLE `corpscout`.`_tmp_fi_websites_second` AS `corpscout`.`fi_websites`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_first` AND `corpscout`.`fi_companies`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_websites_second` AND `corpscout`.`fi_websites`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_first` AND `corpscout`.`fi_companies`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_websites_second`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_first`",
    ]
    assert client.insert_calls == [
        (
            "INSERT INTO `corpscout`.`_tmp_fi_companies_first` (`business_id`) VALUES",
            [("1234567-8",)],
        ),
        (
            "INSERT INTO `corpscout`.`_tmp_fi_websites_second` (`business_id`) VALUES",
            [("1234567-8",)],
        ),
    ]


def test__replace_tables_raises_cleanup_error_after_successful_exchange(
    tmp_path, monkeypatch
) -> None:
    database_path = tmp_path / "source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema finland_resolved")
        connection.execute(
            """
            create table finland_resolved.fi_companies (
                business_id varchar
            )
            """
        )
        connection.execute(
            """
            create table finland_resolved.fi_websites (
                business_id varchar
            )
            """
        )
        connection.execute(
            "insert into finland_resolved.fi_companies values ('1234567-8')"
        )
        connection.execute(
            "insert into finland_resolved.fi_websites values ('1234567-8')"
        )

    stage_names = iter(["first", "second"])
    monkeypatch.setattr(
        clickhouse_resolved.uuid,
        "uuid4",
        lambda: type("U", (), {"hex": next(stage_names)})(),
    )
    client = FailingDropClickHouseClient(
        failing_drops=(
            "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_websites_second`",
            "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_first`",
        ),
    )

    try:
        _replace_tables(
            duckdb_path=database_path,
            clickhouse_client=client,
            duckdb_schema="finland_resolved",
            clickhouse_database="corpscout",
            tables=(
                ("fi_companies", ("business_id",)),
                ("fi_websites", ("business_id",)),
            ),
        )
    except RuntimeError as exc:
        assert (
            str(exc) == "Failed to drop ClickHouse stage table(s): "
            "`corpscout`.`_tmp_fi_websites_second`, "
            "`corpscout`.`_tmp_fi_companies_first`"
        )
        assert isinstance(exc.__cause__, RuntimeError)
        assert str(exc.__cause__) == (
            "drop failed for DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_websites_second`"
        )
    else:
        raise AssertionError("expected cleanup failure")

    assert client.statements == [
        "CREATE TABLE `corpscout`.`_tmp_fi_companies_first` AS `corpscout`.`fi_companies`",
        "CREATE TABLE `corpscout`.`_tmp_fi_websites_second` AS `corpscout`.`fi_websites`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_companies_first` AND `corpscout`.`fi_companies`",
        "EXCHANGE TABLES `corpscout`.`_tmp_fi_websites_second` AND `corpscout`.`fi_websites`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_websites_second`",
        "DROP TABLE IF EXISTS `corpscout`.`_tmp_fi_companies_first`",
    ]


class FakeInsertClickHouseClient(FakeClickHouseClient):
    def __init__(self) -> None:
        super().__init__(set())
        self.insert_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        if sql.startswith("INSERT INTO"):
            if not isinstance(params, list):
                raise AssertionError("insert params must be a list of row tuples")
            self.insert_calls.append((sql, params))
            return []
        self.statements.append(sql)
        return []


class FakeArrowClickHouseClient(FakeInsertClickHouseClient):
    def __init__(self) -> None:
        super().__init__()
        self.arrow_insert_calls: list[tuple[str | None, str, pa.Table]] = []
        self.row_insert_calls: list[
            tuple[str | None, str, list[tuple[object, ...]], tuple[str, ...]]
        ] = []

    def insert_arrow(
        self,
        table: str,
        arrow_table: pa.Table,
        database: str | None = None,
    ) -> None:
        self.arrow_insert_calls.append((database, table, arrow_table))

    def insert_rows(
        self,
        table: str,
        rows: list[tuple[object, ...]],
        columns: tuple[str, ...] | list[str],
        database: str | None = None,
    ) -> None:
        self.row_insert_calls.append((database, table, rows, tuple(columns)))


class FailingInsertClickHouseClient(FakeInsertClickHouseClient):
    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        if sql.startswith("INSERT INTO"):
            super().execute(sql, params)
            raise RuntimeError("insert failed")
        return super().execute(sql, params)


class FailingSecondExchangeClickHouseClient(FakeInsertClickHouseClient):
    def __init__(self) -> None:
        super().__init__()
        self._exchange_count = 0

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        if sql.startswith("EXCHANGE TABLES"):
            self._exchange_count += 1
            if self._exchange_count == 2:
                super().execute(sql, params)
                raise RuntimeError("exchange failed")
        return super().execute(sql, params)


class FailingSecondExchangeAndRollbackClickHouseClient(FakeInsertClickHouseClient):
    def __init__(self) -> None:
        super().__init__()
        self._exchange_count = 0

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        if sql.startswith("EXCHANGE TABLES"):
            self._exchange_count += 1
            if self._exchange_count == 2:
                super().execute(sql, params)
                raise RuntimeError("exchange failed")
            if self._exchange_count == 3:
                super().execute(sql, params)
                raise RuntimeError("rollback exchange failed")
        return super().execute(sql, params)


class FailingDropClickHouseClient(FakeInsertClickHouseClient):
    def __init__(self, *, failing_drops: tuple[str, ...]) -> None:
        super().__init__()
        self._failing_drops = set(failing_drops)

    def execute(self, sql: str, params: object | None = None) -> list[tuple[str]]:
        if sql.startswith("DROP TABLE IF EXISTS"):
            super().execute(sql, params)
            if sql in self._failing_drops:
                raise RuntimeError(f"drop failed for {sql}")
            return []
        return super().execute(sql, params)
