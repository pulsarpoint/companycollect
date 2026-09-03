from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.clickhouse import (
    export_sweden_company_clickhouse_companies,
    export_sweden_company_clickhouse_industries,
    publish_sweden_company_industry_history,
    publish_sweden_company_profile_history,
    publish_sweden_company_source_table,
    sweden_company_source_anti_join_sql,
)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.insert_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[str]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params else ()
            self.table_checks.append(requested)
            return [(table,) for table in requested]
        if "AS first_observations" in sql:
            return [(1, 0)]
        if "AS snapshot_removals" in sql:
            return [(1,)]
        if "AS new_versions" in sql:
            return [(1,)]
        stripped_sql = sql.lstrip()
        if stripped_sql.startswith("CREATE TABLE") or stripped_sql.startswith(
            "EXCHANGE TABLES"
        ):
            return []
        if stripped_sql.startswith("RENAME TABLE"):
            return []
        if stripped_sql.startswith("DROP TABLE"):
            return []
        if stripped_sql.startswith("INSERT INTO"):
            rows = params if isinstance(params, list) else []
            self.insert_calls.append((sql, rows))
            return []
        raise AssertionError(sql)

    def insert_rows(
        self,
        table: str,
        rows: list[tuple[object, ...]],
        column_names: list[str],
    ) -> None:
        columns = ", ".join(f"`{column}`" for column in column_names)
        self.insert_calls.append((f"INSERT INTO {table} ({columns}) VALUES", rows))


def test_export_sweden_company_clickhouse_companies_replaces_companies(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb(tmp_path) as connection:
        rows = export_sweden_company_clickhouse_companies(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert rows == 1
    assert client.table_checks == [(tables.COMPANIES_TABLE_CH,)]
    assert (
        f"CREATE TABLE `corpscout`.`_tmp_{tables.COMPANIES_TABLE_CH}_"
        in client.statements[1]
    )
    assert len(client.insert_calls) == 1
    assert client.insert_calls[0][0].startswith(
        "INSERT INTO `corpscout`.`_tmp_se_companies_"
    )
    assert client.insert_calls[0][1][0][0:4] == (
        "5560000000",
        "5560000000",
        "5560000000$ORGNR-IDORG",
        "5560000000",
    )


def test_export_sweden_company_clickhouse_industries_replaces_industries(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb(tmp_path) as connection:
        rows = export_sweden_company_clickhouse_industries(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert rows == 1
    assert client.table_checks == [(tables.INDUSTRIES_TABLE_CH,)]
    assert (
        f"CREATE TABLE `corpscout`.`_tmp_{tables.INDUSTRIES_TABLE_CH}_"
        in client.statements[1]
    )
    assert len(client.insert_calls) == 1
    assert client.insert_calls[0][0].startswith(
        "INSERT INTO `corpscout`.`_tmp_se_industries_"
    )
    assert client.insert_calls[0][1][0][0:4] == (
        "5560000000",
        1,
        True,
        "62010",
    )


def test_publish_sweden_company_profile_history_tracks_changes_and_removals(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb(tmp_path) as connection:
        result = publish_sweden_company_profile_history(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert result.registry.candidates == 1
    assert result.registry.observations_inserted == 2
    assert result.registry.first_observations == 1
    assert result.registry.removals == 1
    assert result.proceedings.candidates == 1
    assert result.proceedings.observations_inserted == 2
    assert client.table_checks == [
        (
            tables.COMPANY_REGISTRY_OBSERVATIONS_TABLE_CH,
            tables.COMPANY_REGISTRY_CURRENT_TABLE_CH,
            tables.COMPANY_PROCEEDING_OBSERVATIONS_TABLE_CH,
            tables.COMPANY_PROCEEDINGS_CURRENT_TABLE_CH,
        )
    ]
    removal_inserts = [
        statement
        for statement in client.statements
        if statement.lstrip().startswith("INSERT INTO") and "toUInt8(0)" in statement
    ]
    assert len(removal_inserts) == 2
    assert all("candidate.has_observation = 0" in sql for sql in removal_inserts)


def test_publish_sweden_company_industry_history_tracks_complete_sni_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb(tmp_path) as connection:
        result = publish_sweden_company_industry_history(
            duckdb_connection=connection,
            clickhouse=resource,
        )

    assert result.candidates == 1
    assert result.observations_inserted == 2
    assert result.first_observations == 1
    assert result.changes == 0
    assert result.removals == 1
    assert client.table_checks == [
        (
            tables.COMPANY_INDUSTRY_OBSERVATIONS_TABLE_CH,
            tables.COMPANY_INDUSTRY_CURRENT_TABLE_CH,
        )
    ]


def test_publish_sweden_company_source_table_inserts_only_changed_payloads(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The SCB source table is ReplacingMergeTree(observed_at) ORDER BY company_id, so the
    anti-join's right side must be the CURRENT row per company (argMax over observed_at),
    not the whole table: a whole-table anti-join would suppress an A -> B -> A revert and
    leave the stale B row current."""
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb(tmp_path) as connection:
        result = publish_sweden_company_source_table(
            duckdb_connection=connection,
            clickhouse=resource,
            duckdb_table="scb_companies",
            clickhouse_table=tables.SCB_COMPANIES_TABLE_CH,
            columns=tables.SE_SCB_COMPANIES_EXPORT_COLUMNS,
        )

    assert result.candidates == 1
    assert result.inserted == 1
    assert client.table_checks == [(tables.SCB_COMPANIES_TABLE_CH,)]
    assert (
        f"CREATE TABLE corpscout._tmp_{tables.SCB_COMPANIES_TABLE_CH}_"
        in client.statements[1]
    )
    # The fake records every INSERT statement; the target copy names the stage table in
    # its FROM clause too, so match the staged data insert by its prefix. The batch loader
    # (resolved.py's _insert_duckdb_rows_in_batches) backtick-quotes the qualified table.
    staged_inserts = [
        sql
        for sql, _ in client.insert_calls
        if sql.lstrip().startswith(
            f"INSERT INTO `{tables.SWEDEN_DATABASE}`.`_tmp_{tables.SCB_COMPANIES_TABLE_CH}_"
        )
    ]
    assert len(staged_inserts) == 1
    target_inserts = [
        statement
        for statement in client.statements
        if statement.lstrip().startswith(
            f"INSERT INTO {tables.QUALIFIED_SCB_COMPANIES_TABLE} ("
        )
    ]
    assert len(target_inserts) == 1
    assert "LEFT ANTI JOIN" in target_inserts[0]
    assert "argMax(source_payload_hash, observed_at)" in target_inserts[0]
    assert ", ".join(tables.SE_SCB_COMPANIES_EXPORT_COLUMNS) in target_inserts[0]
    # The stage table is always dropped.
    assert any(
        statement.lstrip().startswith("DROP TABLE IF EXISTS corpscout._tmp_")
        for statement in client.statements
    )


def test_sweden_company_source_anti_join_reads_the_current_row_per_company() -> None:
    sql = sweden_company_source_anti_join_sql(
        qualified_stage="corpscout.stage",
        qualified_target="corpscout.se_scb_companies",
    )

    assert "FROM corpscout.stage AS candidate" in sql
    assert "LEFT ANTI JOIN (" in sql
    assert (
        "SELECT company_id, argMax(source_payload_hash, observed_at) "
        "AS source_payload_hash" in sql
    )
    assert "FROM corpscout.se_scb_companies" in sql
    assert "GROUP BY company_id" in sql
    assert "ON current.company_id = candidate.company_id" in sql
    assert "AND current.source_payload_hash = candidate.source_payload_hash" in sql
    # FINAL is not used: it would read the whole table through the merge logic, and argMax
    # states the intent (one current row per company) without depending on merge state.
    assert "FINAL" not in sql


@contextmanager
def _sweden_company_duckdb(tmp_path: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    database_path = tmp_path / "sweden_company_source.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.companies (
                company_id varchar,
                registration_number varchar,
                bolagsverket_company_id_raw varchar,
                scb_company_id_raw varchar,
                legal_name varchar,
                legal_name_raw varchar,
                legal_name_registration_date date,
                legal_form_code varchar,
                status varchar,
                status_source varchar,
                status_observed_at timestamp,
                status_conflict integer,
                status_reason varchar,
                incorporation_date date,
                dissolution_date date,
                activity_description varchar,
                source_run_id varchar,
                bolagsverket_source_record_id varchar,
                scb_source_record_id varchar,
                bolagsverket_source_payload_hash varchar,
                scb_source_payload_hash varchar,
                updated_from_raw_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.company_addresses (
                company_id varchar,
                address_type varchar,
                source varchar,
                raw_address varchar,
                street_address varchar,
                care_of varchar,
                postal_code varchar,
                post_town varchar,
                country_code varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                updated_from_raw_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.company_industry_codes (
                company_id varchar,
                sequence integer,
                is_primary boolean,
                sni_code varchar,
                nace_rev2_class_code varchar,
                source_field varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                updated_from_raw_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.company_registry_states (
                company_id varchar,
                source varchar,
                company_id_raw varchar,
                legal_name varchar,
                legal_name_raw varchar,
                alternate_name varchar,
                legal_form_code varchar,
                source_status_code varchar,
                source_secondary_status_code varchar,
                derived_status varchar,
                status_reason varchar,
                incorporation_date date,
                dissolution_date date,
                activity_description varchar,
                name_protection_sequence varchar,
                registration_country_code varchar,
                marketing_block_code varchar,
                proceedings_raw varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                updated_from_raw_at timestamp,
                has_company integer,
                state_fingerprint varchar,
                observation_fingerprint varchar,
                observed_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.company_proceedings (
                company_id varchar,
                source varchar,
                proceeding_code varchar,
                effective_date date,
                raw_proceeding varchar,
                proceeding_identity varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                updated_from_raw_at timestamp,
                has_proceeding integer,
                proceeding_fingerprint varchar,
                observation_fingerprint varchar,
                observed_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.company_industry_states (
                company_id varchar,
                source varchar,
                ng1_code varchar,
                ng2_code varchar,
                ng3_code varchar,
                ng4_code varchar,
                ng5_code varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                updated_from_raw_at timestamp,
                has_industry integer,
                state_fingerprint varchar,
                observation_fingerprint varchar,
                observed_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.companies
            values (
                '5560000000',
                '5560000000',
                '5560000000$ORGNR-IDORG',
                '5560000000',
                'Acme AB',
                'Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01',
                '2020-01-01',
                'AB-ORGFO',
                'active',
                'bolagsverket',
                '2026-07-03 12:00:00',
                0,
                null,
                '2020-01-01',
                null,
                'Runs acme.se',
                'run-1',
                '5560000000$ORGNR-IDORG',
                '5560000000',
                'bolag-hash-1',
                'scb-hash-1',
                '2026-07-03 12:00:00'
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.company_addresses
            values (
                '5560000000',
                'postal',
                'bolagsverket',
                'Box 1$c/o CFO$STOCKHOLM$11122$SE-LAND',
                'Box 1',
                'c/o CFO',
                '11122',
                'STOCKHOLM',
                'SE',
                'run-1',
                '5560000000$ORGNR-IDORG',
                'bolag-hash-1',
                '2026-07-03 12:00:00'
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.company_industry_codes
            values (
                '5560000000',
                1,
                true,
                '62010',
                '6201',
                'Ng1',
                'run-1',
                '5560000000',
                'scb-hash-1',
                '2026-07-03 12:00:00'
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.company_registry_states
            values (
                '5560000000', 'bolagsverket', '5560000000$ORGNR-IDORG',
                'Acme AB', 'Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01', null,
                'AB-ORGFO', null, null, 'active', null, '2020-01-01', null,
                'Runs acme.se', '1', 'SE-LAND', null,
                '|LI-AVOMFO$2024-05-21', 'run-1',
                '5560000000$ORGNR-IDORG', 'bolag-hash-1',
                '2026-07-03 12:00:00', 1, repeat('a', 64), repeat('a', 64),
                '2026-07-03 12:00:00'
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.company_proceedings
            values (
                '5560000000', 'bolagsverket', 'LI-AVOMFO', '2024-05-21',
                'LI-AVOMFO$2024-05-21', repeat('b', 64), 'run-1',
                '5560000000$ORGNR-IDORG', 'bolag-hash-1',
                '2026-07-03 12:00:00', 1, repeat('b', 64), repeat('b', 64),
                '2026-07-03 12:00:00'
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.company_industry_states
            values (
                '5560000000', 'scb', '62010', '70220', null, null, null,
                'run-1', '5560000000', 'scb-hash-1',
                '2026-07-03 12:00:00', 1, repeat('c', 64), repeat('c', 64),
                '2026-07-03 12:00:00'
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.scb_companies (
                company_id varchar,
                company_id_raw varchar,
                legal_name varchar,
                alternate_name varchar,
                legal_form_code varchar,
                source_status_code varchar,
                source_secondary_status_code varchar,
                registration_date date,
                ng1_code varchar,
                ng2_code varchar,
                ng3_code varchar,
                ng4_code varchar,
                ng5_code varchar,
                care_of varchar,
                street_address varchar,
                postal_code varchar,
                post_town varchar,
                marketing_block_code varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                observed_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.scb_companies
            values (
                '5560000000', '5560000000', 'ACME SCB', null, '49', '0', '1',
                '2020-01-01', '62010', '70220', null, null, null,
                'c/o ACME', 'Main Street 1', '11122', 'STOCKHOLM', '1',
                'run-1', '5560000000', 'scb-hash-1', '2026-09-03 06:00:00'
            )
            """
        )
        yield connection
