from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_company import tables
from dagster_v3.defs.sweden_company.clickhouse import (
    publish_sweden_company_clickhouse_addresses,
)


class _FakeClickHouse:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.address_history: list[tuple[object, ...]] = []
        self.address_candidates: list[tuple[object, ...]] = []

    def execute(
        self,
        sql: str,
        params: dict[str, object] | list[tuple[object, ...]] | None = None,
    ) -> list[tuple[object, ...]] | None:
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if isinstance(params, dict) else ()
            return [(table,) for table in requested]
        if "address_observations_inserted" in sql:
            changed = self._changed_rows()
            current = self._current_rows()
            first = sum(self._key(row) not in current for row in changed)
            removals = sum(
                self._key(row) in current
                and row[self._has_address_index] == 0
                and current[self._key(row)][self._has_address_index] == 1
                for row in changed
            )
            return [(len(changed), first, len(changed) - first, removals)]
        if isinstance(params, list):
            if "_tmp_se_company_addresses_" in sql:
                self.address_candidates = list(params)
            return None
        if f"INSERT INTO corpscout.{tables.COMPANY_ADDRESSES_TABLE_CH}" in sql:
            self.address_history.extend(self._changed_rows())
        self.statements.append(sql)
        return None

    def insert_rows(
        self,
        table: str,
        rows: list[tuple[object, ...]],
        column_names: list[str],
    ) -> None:
        assert tuple(column_names) == tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS
        if "_tmp_se_company_addresses_" in table:
            self.address_candidates = list(rows)

    @property
    def _fingerprint_index(self) -> int:
        return tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS.index(
            "observation_fingerprint"
        )

    @property
    def _has_address_index(self) -> int:
        return tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS.index("has_address")

    def _key(self, row: tuple[object, ...]) -> tuple[object, object, object]:
        columns = tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS
        return tuple(
            row[columns.index(column)]
            for column in ("company_id", "address_type", "source")
        )

    def _current_rows(
        self,
    ) -> dict[tuple[object, object, object], tuple[object, ...]]:
        return {self._key(row): row for row in self.address_history}

    def _changed_rows(self) -> list[tuple[object, ...]]:
        current = self._current_rows()
        return [
            row
            for row in self.address_candidates
            if self._key(row) not in current
            or row[self._fingerprint_index]
            != current[self._key(row)][self._fingerprint_index]
        ]


def _replace_candidate(
    connection: duckdb.DuckDBPyConnection,
    *,
    address: str,
    fingerprint: str,
    run_id: str,
    observed_at: datetime,
    has_address: int = 1,
) -> None:
    connection.execute("delete from sweden_company.company_addresses")
    connection.execute(
        """
        insert into sweden_company.company_addresses
        values (?, 'visiting_or_postal', 'scb', ?, ?, null, '16437', 'KISTA',
                'SE', ?, '8024123872', ?, ?, ?, ?, ?, ?)
        """,
        [
            "8024123872",
            address,
            address,
            run_id,
            f"payload-{run_id}",
            observed_at,
            has_address,
            fingerprint,
            fingerprint,
            observed_at,
        ],
    )


def test_changed_and_a_b_a_snapshots_preserve_address_history(
    tmp_path: Path,
    monkeypatch,
) -> None:
    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    database_path = tmp_path / "sweden.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        connection.execute("create schema sweden_company")
        connection.execute(
            """
            create table sweden_company.company_addresses (
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
                updated_from_raw_at timestamptz,
                has_address utinyint,
                address_fingerprint varchar,
                observation_fingerprint varchar,
                observed_at timestamptz
            )
            """
        )

        _replace_candidate(
            connection,
            address="BERGENGATAN 13  3 TR",
            fingerprint="a" * 64,
            run_id="run-a-1",
            observed_at=datetime(2026, 8, 8, 8, tzinfo=UTC),
        )
        initial = publish_sweden_company_clickhouse_addresses(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
        )
        unchanged = publish_sweden_company_clickhouse_addresses(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
        )

        _replace_candidate(
            connection,
            address="BERGENGATAN 15",
            fingerprint="b" * 64,
            run_id="run-b",
            observed_at=datetime(2026, 8, 15, 8, tzinfo=UTC),
        )
        changed = publish_sweden_company_clickhouse_addresses(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
        )

        _replace_candidate(
            connection,
            address="BERGENGATAN 13  3 TR",
            fingerprint="a" * 64,
            run_id="run-a-2",
            observed_at=datetime(2026, 8, 22, 8, tzinfo=UTC),
        )
        returned = publish_sweden_company_clickhouse_addresses(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
        )

        _replace_candidate(
            connection,
            address="",
            fingerprint="c" * 64,
            run_id="run-removed",
            observed_at=datetime(2026, 8, 29, 8, tzinfo=UTC),
            has_address=0,
        )
        removed = publish_sweden_company_clickhouse_addresses(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
        )

    raw_address_index = tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS.index(
        "raw_address"
    )
    assert initial.address_observations_inserted == 1
    assert initial.first_address_observations == 1
    assert unchanged.address_observations_inserted == 0
    assert changed.address_changes == 1
    assert returned.address_changes == 1
    assert removed.address_removals == 1
    assert [row[raw_address_index] for row in fake.address_history] == [
        "BERGENGATAN 13  3 TR",
        "BERGENGATAN 15",
        "BERGENGATAN 13  3 TR",
        "",
    ]
    assert any(
        "CREATE TABLE corpscout._tmp_se_company_addresses_" in statement
        and "AS corpscout.se_company_addresses_current" in statement
        for statement in fake.statements
    )
    assert any(
        "RENAME TABLE" in statement
        and "TO corpscout.se_company_addresses_current" in statement
        for statement in fake.statements
    )


def test_sweden_address_history_migrations_own_history_and_current_snapshot() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000255_corpscout_se_company_address_history.up.sql"
    ).read_text(encoding="utf-8")

    assert "RENAME TABLE corpscout.se_company_addresses" in migration
    assert "CREATE TABLE corpscout.se_company_addresses" in migration
    assert "CREATE OR REPLACE VIEW corpscout.se_company_addresses_current" in migration
    assert "PARTITION BY toYear(observed_at)" in migration
    assert "argMax(" in migration
    assert "tuple(observed_at, source_run_id)" in migration
    assert "DROP TABLE corpscout.se_company_addresses_legacy_000255" in migration
    table_sql = migration[migration.index("CREATE TABLE corpscout.se_company_addresses") :]
    table_sql = table_sql[: table_sql.index(";")]
    positions = [
        table_sql.index(f"\n    {column} ")
        for column in tables.SE_COMPANY_ADDRESS_OBSERVATION_COLUMNS
    ]
    assert positions == sorted(positions)

    snapshot_migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000256_corpscout_se_company_address_current_snapshot.up.sql"
    ).read_text(encoding="utf-8")

    assert "ENGINE = MergeTree" in snapshot_migration
    assert "ORDER BY (company_id, address_type, source)" in snapshot_migration
    assert "DROP VIEW corpscout.se_company_addresses_current" in snapshot_migration
    assert (
        "TO corpscout.se_company_addresses_current" in snapshot_migration
    )
