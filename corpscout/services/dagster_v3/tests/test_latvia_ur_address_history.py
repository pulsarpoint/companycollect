from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import duckdb
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.latvia_ur import assets, clickhouse, tables
from tests.test_latvia_ur_assets import (
    ACTIVITY_CSV,
    SAMPLE_CSV,
    _FakeSession,
    _load_latvia_address_fixtures,
)


class _FakeClickHouse:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.inserts: list[tuple[str, list[tuple[object, ...]]]] = []
        self.address_history: list[tuple[object, ...]] = []
        self.address_candidates: list[tuple[object, ...]] = []

    def execute(
        self,
        sql: str,
        params: list[tuple[object, ...]] | tuple[object, ...] | None = None,
    ) -> list[tuple[object, ...]] | None:
        if "system.tables" in sql:
            return [
                (tables.LV_COMPANIES_TABLE,),
                (tables.LV_COMPANY_ADDRESSES_TABLE,),
            ]
        if "address_observations_inserted" in sql:
            changed = self._changed_address_rows()
            current = self._current_addresses()
            first = sum(row[self._regcode_index] not in current for row in changed)
            source_changes = sum(
                row[self._regcode_index] in current
                and row[self._address_fingerprint_index]
                != current[row[self._regcode_index]][self._address_fingerprint_index]
                for row in changed
            )
            enrichment_changes = len(changed) - first - source_changes
            return [(len(changed), first, source_changes, enrichment_changes)]
        if isinstance(params, (list, tuple)):
            rows = list(params)
            self.inserts.append((sql, rows))
            if "_tmp_lv_company_addresses_" in sql:
                self.address_candidates = rows
            return None
        if f"INSERT INTO corpscout.{tables.LV_COMPANY_ADDRESSES_TABLE}" in sql:
            self.address_history.extend(self._changed_address_rows())
        self.statements.append(sql)
        return None

    @property
    def _regcode_index(self) -> int:
        return tables.LV_COMPANY_ADDRESSES_COLUMNS.index("regcode")

    @property
    def _address_fingerprint_index(self) -> int:
        return tables.LV_COMPANY_ADDRESSES_COLUMNS.index("address_fingerprint")

    @property
    def _observation_fingerprint_index(self) -> int:
        return tables.LV_COMPANY_ADDRESSES_COLUMNS.index("observation_fingerprint")

    def _current_addresses(self) -> dict[object, tuple[object, ...]]:
        return {row[self._regcode_index]: row for row in self.address_history}

    def _changed_address_rows(self) -> list[tuple[object, ...]]:
        current = self._current_addresses()
        return [
            row
            for row in self.address_candidates
            if row[self._regcode_index] not in current
            or row[self._observation_fingerprint_index]
            != current[row[self._regcode_index]][self._observation_fingerprint_index]
        ]


def _loaded_latvia_duckdb(tmp_path: Path) -> Path:
    database_path = tmp_path / "latvia_ur_source.duckdb"
    assets.run_latvia_ur_dlt_pipeline(
        database_path=database_path,
        run_id="source-run",
        session=_FakeSession(SAMPLE_CSV.encode("utf-8")),
        pipelines_dir=tmp_path / "dlt",
    )
    with duckdb.connect(str(database_path)) as connection:
        assets.load_latvia_company_activity_csv(
            duckdb_connection=connection,
            session=_FakeSession(ACTIVITY_CSV.encode("utf-8")),
        )
        _load_latvia_address_fixtures(connection)
    return database_path


def test_address_export_view_stamps_one_observation_and_stable_hashes(
    tmp_path: Path,
) -> None:
    database_path = _loaded_latvia_duckdb(tmp_path)
    observed_at = datetime(2026, 8, 7, 9, 30, tzinfo=UTC)

    with duckdb.connect(str(database_path)) as connection:
        clickhouse.create_latvia_ur_export_views(
            duckdb_connection=connection,
            source_run_id="dagster-run-1",
            observed_at=observed_at,
        )
        rows = connection.execute(
            f"""
            select
                regcode,
                source_run_id,
                observed_at,
                has_address,
                address_fingerprint,
                observation_fingerprint,
                address_city_name
            from {tables.DLT_DATASET_NAME}.{clickhouse.LV_COMPANY_ADDRESSES_EXPORT_VIEW}
            order by regcode
            """
        ).fetchall()

    assert len(rows) == 2
    assert rows[0][0:4] == (
        "40103550818",
        "dagster-run-1",
        observed_at,
        1,
    )
    assert len(rows[0][4]) == 64
    assert len(rows[0][5]) == 64
    assert rows[0][4] != rows[0][5]
    assert rows[0][6] == "Valmiera"


def test_publish_appends_changed_addresses_and_replaces_company_facts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = _loaded_latvia_duckdb(tmp_path)
    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    with duckdb.connect(str(database_path)) as connection:
        result = clickhouse.publish_latvia_ur_clickhouse(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
            source_run_id="dagster-run-1",
            observed_at=datetime(2026, 8, 7, 9, 30, tzinfo=UTC),
        )

    assert result.company_rows == 2
    assert result.address_candidates == 2
    assert result.address_observations_inserted == 2
    assert result.first_address_observations == 2
    assert result.source_address_changes == 0
    assert result.enrichment_only_changes == 0
    assert any(
        f"INSERT INTO corpscout.{tables.LV_COMPANY_ADDRESSES_TABLE}" in statement
        for statement in fake.statements
    )
    assert any("EXCHANGE TABLES" in statement for statement in fake.statements)
    assert any(
        len(rows[0]) == len(tables.LV_COMPANY_ADDRESSES_COLUMNS)
        for _, rows in fake.inserts
    )
    assert any(
        len(rows[0]) == len(tables.LV_COMPANIES_EXPORT_COLUMNS)
        for _, rows in fake.inserts
    )


def test_unchanged_and_a_b_a_publications_keep_only_observed_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = _loaded_latvia_duckdb(tmp_path)
    fake = _FakeClickHouse()

    @contextmanager
    def fake_get_connection(self):
        yield fake

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    def publish(connection, *, run_id: str, hour: int):
        return clickhouse.publish_latvia_ur_clickhouse(
            duckdb_connection=connection,
            clickhouse=ClickhouseResource(host="localhost"),
            source_run_id=run_id,
            observed_at=datetime(2026, 8, 7, hour, tzinfo=UTC),
        )

    qualified_entities = f"{tables.DLT_DATASET_NAME}.{tables.ENTITIES_TABLE}"
    with duckdb.connect(str(database_path)) as connection:
        initial = publish(connection, run_id="run-a-1", hour=9)
        unchanged = publish(connection, run_id="run-a-2", hour=10)
        connection.execute(
            f"update {qualified_entities} set address = 'Changed address' "
            "where regcode = '40103550818'"
        )
        changed = publish(connection, run_id="run-b", hour=11)
        connection.execute(
            f"update {qualified_entities} set address = 'Valmiera' "
            "where regcode = '40103550818'"
        )
        returned = publish(connection, run_id="run-a-3", hour=12)

    address_index = tables.LV_COMPANY_ADDRESSES_COLUMNS.index("address")
    regcode_index = tables.LV_COMPANY_ADDRESSES_COLUMNS.index("regcode")
    company_history = [
        row[address_index]
        for row in fake.address_history
        if row[regcode_index] == "40103550818"
    ]
    assert initial.address_observations_inserted == 2
    assert unchanged.address_observations_inserted == 0
    assert changed.address_observations_inserted == 1
    assert returned.address_observations_inserted == 1
    assert company_history == ["Valmiera", "Changed address", "Valmiera"]


def test_latvia_address_history_migration_owns_table_and_current_views() -> None:
    migration = (
        Path(__file__).resolve().parents[3]
        / "clickhouse"
        / "migrations"
        / "000254_corpscout_lv_company_addresses.up.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS corpscout.lv_company_addresses" in migration
    assert "CREATE OR REPLACE VIEW corpscout.lv_company_addresses_current" in migration
    assert "CREATE OR REPLACE VIEW corpscout.lv_companies_current" in migration
    assert "FROM corpscout.lv_company_addresses" in migration
    assert "argMax(" in migration
    assert "c.* EXCEPT" in migration
    assert "DROP COLUMN" not in migration
    table_sql = migration[
        migration.index("CREATE TABLE IF NOT EXISTS corpscout.lv_company_addresses") :
    ]
    table_sql = table_sql[: table_sql.index(";")]
    positions = [
        table_sql.index(f"\n    {column} ")
        for column in tables.LV_COMPANY_ADDRESSES_COLUMNS
    ]
    assert positions == sorted(positions)
