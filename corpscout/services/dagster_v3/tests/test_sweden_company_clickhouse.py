from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.clickhouse import (
    export_sweden_company_clickhouse_companies,
    export_sweden_company_clickhouse_industries,
    publish_sweden_company_industry_history,
    publish_sweden_company_profile_history,
    publish_sweden_company_source_table,
    sweden_company_source_anti_join_sql,
    sweden_company_source_removal_sql,
    sweden_company_source_tombstone_insert_sql,
)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.insert_calls: list[tuple[str, list[tuple[object, ...]]]] = []
        self.removals = 0

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
        if "AS removals" in sql:
            return [(self.removals,)]
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


def test_publish_sweden_company_profile_history_publishes_proceedings_only(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The registry half retired with the 2026-09-03 basic-info design: the two register
    sources each have their own source table now. Proceedings keep the observation +
    _current shape until the proceedings entity's own slice."""
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

    assert result.candidates == 1
    assert result.observations_inserted == 2
    assert result.first_observations == 1
    assert result.changes == 0
    assert result.removals == 1
    assert client.table_checks == [
        (
            tables.COMPANY_PROCEEDING_OBSERVATIONS_TABLE_CH,
            tables.COMPANY_PROCEEDINGS_CURRENT_TABLE_CH,
        )
    ]
    removal_inserts = [
        statement
        for statement in client.statements
        if statement.lstrip().startswith("INSERT INTO") and "toUInt8(0)" in statement
    ]
    assert len(removal_inserts) == 1
    assert "candidate.has_observation = 0" in removal_inserts[0]
    assert not [
        statement for statement in client.statements if "registry" in statement
    ]


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
    assert result.removed == 0
    # Nothing was dropped from the delivery, so no tombstone row is written.
    assert all(
        not statement.startswith(
            f"INSERT INTO {tables.QUALIFIED_SCB_COMPANIES_TABLE} "
            "(company_id, company_id_raw, has_company"
        )
        for statement in client.statements
    )
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


def test_publish_sweden_company_source_table_publishes_the_bolagsverket_record(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """The same helper drives both register sources -- only the DuckDB table, the ClickHouse
    table and the column tuple differ, so a second wrapper would be pure forwarding."""
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
            duckdb_table="bolagsverket_companies",
            clickhouse_table=tables.BOLAGSVERKET_COMPANIES_TABLE_CH,
            columns=tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS,
        )

    assert result.candidates == 1
    assert result.inserted == 1
    assert client.table_checks == [(tables.BOLAGSVERKET_COMPANIES_TABLE_CH,)]
    target_inserts = [
        statement
        for statement in client.statements
        if statement.lstrip().startswith(
            f"INSERT INTO {tables.QUALIFIED_BOLAGSVERKET_COMPANIES_TABLE} ("
        )
    ]
    assert len(target_inserts) == 1
    assert "LEFT ANTI JOIN" in target_inserts[0]
    assert (
        ", ".join(tables.SE_BOLAGSVERKET_COMPANIES_EXPORT_COLUMNS) in target_inserts[0]
    )


def test_publish_sweden_company_source_table_tombstones_companies_the_delivery_dropped(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Owner decision 2026-09-03: a company the register stops delivering must not stay
    current. The publisher counts the companies whose CURRENT row is delivered and that the
    stage no longer holds, and appends one tombstone row per company -- after the changed
    rows went in and before the stage is dropped."""
    client = FakeClickHouseClient()
    client.removals = 1
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

    assert result.removed == 1
    removal_counts = [s for s in client.statements if "AS removals" in s]
    assert len(removal_counts) == 1
    assert "argMax(has_company, observed_at) AS has_company" in removal_counts[0]
    assert "WHERE current.has_company = 1" in removal_counts[0]
    tombstones = [
        s
        for s in client.statements
        if s.startswith(
            "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, "
            "has_company, source_run_id, source_record_id, source_payload_hash, "
            "observed_at)"
        )
    ]
    assert len(tombstones) == 1
    assert "SELECT current.company_id, '', 0, " in tombstones[0]
    assert "(SELECT any(source_run_id) FROM corpscout._tmp_se_scb_companies_" in (
        tombstones[0]
    )
    assert "(SELECT any(observed_at) FROM corpscout._tmp_se_scb_companies_" in (
        tombstones[0]
    )
    # The removal is computed after the changed rows went in, and before the stage drops.
    order = [
        next(i for i, s in enumerate(client.statements) if "AS new_versions" in s),
        next(i for i, s in enumerate(client.statements) if "AS removals" in s),
        next(
            i
            for i, s in enumerate(client.statements)
            if s.startswith("DROP TABLE IF EXISTS corpscout._tmp_se_scb_companies_")
        ),
    ]
    assert order == sorted(order)


def test_publish_sweden_company_source_table_refuses_an_empty_stage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Minor-3 of the final review: the empty-input refusal is the one guard that stops a
    bad load, and had no test. export_duckdb_connection_table_to_clickhouse enforces its
    own non-empty rule only on the truncate=True path (defs/clickhouse/resolved.py); this
    helper passes truncate=False, so the local `if candidates == 0: raise ValueError` in
    publish_sweden_company_source_table is the only guard, and it is live, not dead."""
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(self: ClickhouseResource) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with _sweden_company_duckdb_with_empty_scb_companies(tmp_path) as connection:
        with pytest.raises(ValueError, match="has 0 rows"):
            publish_sweden_company_source_table(
                duckdb_connection=connection,
                clickhouse=resource,
                duckdb_table="scb_companies",
                clickhouse_table=tables.SCB_COMPANIES_TABLE_CH,
                columns=tables.SE_SCB_COMPANIES_EXPORT_COLUMNS,
            )

    assert client.insert_calls == []
    assert not any(
        statement.lstrip().startswith(
            f"INSERT INTO {tables.QUALIFIED_SCB_COMPANIES_TABLE} ("
        )
        for statement in client.statements
    )
    # The refusal comes before any removal is computed: this is what stops a failed load
    # from tombstoning the whole register.
    assert not any("AS removals" in statement for statement in client.statements)
    # The stage table is created and then dropped in a `finally` that runs unconditionally
    # -- i.e. the DROP still runs even though it is the empty-stage refusal, not a
    # successful publish, that triggers it.
    assert any(
        statement.lstrip().startswith(
            f"CREATE TABLE corpscout._tmp_{tables.SCB_COMPANIES_TABLE_CH}_"
        )
        for statement in client.statements
    )
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


def test_sweden_company_source_removal_reads_the_current_row_per_company() -> None:
    """The left side is the current row per company, exactly like the anti-join's right
    side -- so a company already tombstoned is not tombstoned again, and the tombstone
    carries the delivery's own run id and observed_at."""
    removal = sweden_company_source_removal_sql(
        qualified_stage="corpscout.stage",
        qualified_target="corpscout.se_scb_companies",
    )

    assert "SELECT company_id, argMax(has_company, observed_at) AS has_company" in removal
    assert "FROM corpscout.se_scb_companies" in removal
    assert "GROUP BY company_id" in removal
    assert "LEFT ANTI JOIN corpscout.stage AS candidate" in removal
    assert "ON candidate.company_id = current.company_id" in removal
    assert removal.endswith("WHERE current.has_company = 1")
    assert "FINAL" not in removal

    insert = sweden_company_source_tombstone_insert_sql(
        qualified_stage="corpscout.stage",
        qualified_target="corpscout.se_scb_companies",
    )

    assert insert.startswith(
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, "
        "has_company, source_run_id, source_record_id, source_payload_hash, observed_at)"
    )
    # An empty record id and hash: a returning company always differs from its tombstone
    # and is inserted again by the anti-join.
    assert "SELECT current.company_id, '', 0, " in insert
    assert "(SELECT any(source_run_id) FROM corpscout.stage), '', '', " in insert
    assert "(SELECT any(observed_at) FROM corpscout.stage) " in insert
    assert insert.endswith(removal)


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
                registration_date_raw varchar,
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
                '2020-01-01', '20200101', '62010', '70220', null, null, null,
                'c/o ACME', 'Main Street 1', '11122', 'STOCKHOLM', '1',
                'run-1', '5560000000', 'scb-hash-1', '2026-09-03 06:00:00'
            )
            """
        )
        connection.execute(
            f"""
            create table {tables.DLT_DATASET_NAME}.bolagsverket_companies (
                company_id varchar,
                company_id_raw varchar,
                name_protection_sequence varchar,
                registration_country_code varchar,
                legal_name varchar,
                legal_name_raw varchar,
                legal_form_code varchar,
                registration_date date,
                registration_date_raw varchar,
                deregistration_date date,
                deregistration_date_raw varchar,
                deregistration_reason varchar,
                proceedings_raw varchar,
                activity_description varchar,
                postal_address varchar,
                source_run_id varchar,
                source_record_id varchar,
                source_payload_hash varchar,
                observed_at timestamp
            )
            """
        )
        connection.execute(
            f"""
            insert into {tables.DLT_DATASET_NAME}.bolagsverket_companies
            values (
                '5560000000', '5560000000$ORGNR-IDORG', '1', 'SE-LAND',
                'Acme AB', 'Acme AB$FORETAGSNAMN-ORGNAM$2020-01-01', 'AB-ORGFO',
                '2020-01-01', '2020-01-01', null, null, null, null, 'Runs acme.se',
                'Box 1$c/o CFO$STOCKHOLM$11122$SE-LAND',
                'run-1', '5560000000$ORGNR-IDORG', 'bolag-hash-1',
                '2026-09-03 06:00:00'
            )
            """
        )
        yield connection


@contextmanager
def _sweden_company_duckdb_with_empty_scb_companies(
    tmp_path: Path,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Mirrors _sweden_company_duckdb's scb_companies table shape, but leaves it empty --
    for Minor-3: a bad load that produces zero rows must not silently publish nothing."""
    database_path = tmp_path / "sweden_company_source_empty.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute(f"create schema {tables.DLT_DATASET_NAME}")
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
                registration_date_raw varchar,
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
        yield connection
