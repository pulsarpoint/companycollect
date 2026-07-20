import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.clickhouse import (
    QUALIFIED_SE_FINANCIAL_FACTS_TABLE,
    QUALIFIED_SE_FINANCIAL_REPORTS_TABLE,
    SE_FINANCIAL_FACTS_EXPORT_COLUMNS,
    SE_FINANCIAL_REPORTS_EXPORT_COLUMNS,
    SHRINK_GUARD_MIN_RATIO,
    guard_against_clickhouse_table_shrink,
    resolve_sweden_financial_partition_archive_keys,
    upsert_sweden_financial_facts_partition,
    upsert_sweden_financial_reports_partition,
)
from dagster_v3.defs.sweden_financial.parsing import SWEDEN_FINANCIAL_DATASET_NAME
from dagster_v3.defs.sweden_financial.storage import (
    sweden_financial_year_duckdb_connection,
)

_ARCHIVE_KEY_2025_A = "sweden_financial/raw_archives/year=2025/archive=a.zip"
_ARCHIVE_KEY_2025_B = "sweden_financial/raw_archives/year=2025/archive=b.zip"
_ARCHIVE_KEY_2024_Z = "sweden_financial/raw_archives/year=2024/archive=z.zip"
_ARCHIVE_KEY_2026_W1 = "sweden_financial/raw_archives/year=2026/archive=w1.zip"
_ARCHIVE_KEY_2026_W2 = "sweden_financial/raw_archives/year=2026/archive=w2.zip"


# --- fake ClickHouse client -------------------------------------------------

_COUNT_SQL_RE = re.compile(r"^SELECT count\(\) FROM (\S+) WHERE (\w+) IN %\(keys\)s$")
_DELETE_SQL_RE = re.compile(
    r"^ALTER TABLE (\S+) DELETE WHERE (\w+) IN %\(keys\)s "
    r"SETTINGS mutations_sync = 1$"
)
_STAGE_CREATE_SQL_RE = re.compile(
    r"^CREATE TABLE (corpscout\._tmp_\S+) \((\w+) String\) ENGINE = Memory$"
)
_STAGE_INSERT_SQL_RE = re.compile(
    r"^INSERT INTO (corpscout\._tmp_\S+) \((\w+)\) VALUES$"
)
_STAGE_COUNT_SQL_RE = re.compile(
    r"^SELECT count\(\) FROM (\S+) WHERE (\w+) IN "
    r"\(SELECT (\w+) FROM (corpscout\._tmp_\S+)\)$"
)
_STAGE_DELETE_SQL_RE = re.compile(
    r"^ALTER TABLE (\S+) DELETE WHERE (\w+) IN "
    r"\(SELECT (\w+) FROM (corpscout\._tmp_\S+)\) "
    r"SETTINGS mutations_sync = 1$"
)
_DROP_TABLE_SQL_RE = re.compile(r"^DROP TABLE IF EXISTS (corpscout\._tmp_\S+)$")

_STAGE_TABLE_NAME_RE = re.compile(
    r"^corpscout\._tmp_se_facts_scope_[0-9a-f]{32}$"
)


class StatefulFakeClickHouseClient:
    """A fake clickhouse-driver client that keeps real per-table row state.

    Supports exactly the operations the scoped upserts perform: the
    ``system.tables`` existence probe; the reports path's ``SELECT count()
    ... WHERE col IN %(keys)s`` / ``ALTER TABLE ... DELETE WHERE col IN
    %(keys)s``; the facts path's stage-table flow (``CREATE TABLE ...
    ENGINE = Memory``, data-block ``INSERT`` into the stage, count/delete
    via ``IN (SELECT ... FROM <stage>)`` resolved against the stage's
    actual row contents, ``DROP TABLE IF EXISTS``); and explicit-column
    ``INSERT INTO ... VALUES`` -- so idempotency and scope-isolation tests
    assert on the resulting row state, not on echoed SQL strings. The
    stage-subquery count/delete branches assert ``params is None``: the
    scope must live in the stage table's rows, never in query params.
    """

    def __init__(self, tables: dict[str, tuple[str, ...]]) -> None:
        self.columns = {name: tuple(columns) for name, columns in tables.items()}
        self.rows: dict[str, list[tuple[Any, ...]]] = {name: [] for name in tables}
        self.operations: list[tuple[str, Any]] = []
        self.created_stage_tables: list[str] = []

    def execute(self, sql: str, params: Any = None) -> list[tuple[Any, ...]]:
        normalized = " ".join(sql.split())
        self.operations.append((normalized, params))
        if "system.tables" in normalized:
            return [(table,) for table in params["tables"]]
        stage_create_match = _STAGE_CREATE_SQL_RE.match(normalized)
        if stage_create_match is not None:
            table, column = stage_create_match.groups()
            assert table not in self.columns, f"stage table already exists: {table}"
            self.columns[table] = (column,)
            self.rows[table] = []
            self.created_stage_tables.append(table)
            return []
        stage_insert_match = _STAGE_INSERT_SQL_RE.match(normalized)
        if stage_insert_match is not None:
            table, column = stage_insert_match.groups()
            assert self.columns[table] == (column,)
            self.rows[table].extend(params)
            return []
        drop_match = _DROP_TABLE_SQL_RE.match(normalized)
        if drop_match is not None:
            table = drop_match.group(1)
            self.columns.pop(table, None)
            self.rows.pop(table, None)
            return []
        count_match = _COUNT_SQL_RE.match(normalized)
        if count_match is not None:
            table, column = count_match.groups()
            index = self.columns[table].index(column)
            keys = set(params["keys"])
            return [
                (sum(1 for row in self.rows[table] if row[index] in keys),)
            ]
        delete_match = _DELETE_SQL_RE.match(normalized)
        if delete_match is not None:
            table, column = delete_match.groups()
            index = self.columns[table].index(column)
            keys = set(params["keys"])
            self.rows[table] = [
                row for row in self.rows[table] if row[index] not in keys
            ]
            return []
        stage_count_match = _STAGE_COUNT_SQL_RE.match(normalized)
        if stage_count_match is not None:
            table, column, stage_column, stage_table = stage_count_match.groups()
            assert params is None, "stage-scoped count must not carry params"
            keys = self._stage_keys(stage_table, stage_column)
            index = self.columns[table].index(column)
            return [
                (sum(1 for row in self.rows[table] if row[index] in keys),)
            ]
        stage_delete_match = _STAGE_DELETE_SQL_RE.match(normalized)
        if stage_delete_match is not None:
            table, column, stage_column, stage_table = stage_delete_match.groups()
            assert params is None, "stage-scoped delete must not carry params"
            keys = self._stage_keys(stage_table, stage_column)
            index = self.columns[table].index(column)
            self.rows[table] = [
                row for row in self.rows[table] if row[index] not in keys
            ]
            return []
        if normalized.startswith("INSERT INTO `corpscout`.`"):
            table_name = normalized.split("`")[3]
            self.rows[f"corpscout.{table_name}"].extend(params)
            return []
        raise AssertionError(normalized)

    def _stage_keys(self, stage_table: str, stage_column: str) -> set[Any]:
        index = self.columns[stage_table].index(stage_column)
        return {row[index] for row in self.rows[stage_table]}

    def statements(self) -> list[str]:
        return [sql for sql, _ in self.operations]

    def remaining_stage_tables(self) -> list[str]:
        return [name for name in self.columns if "._tmp_" in name]


def _reports_facts_fake_client() -> StatefulFakeClickHouseClient:
    return StatefulFakeClickHouseClient(
        {
            QUALIFIED_SE_FINANCIAL_REPORTS_TABLE: SE_FINANCIAL_REPORTS_EXPORT_COLUMNS,
            QUALIFIED_SE_FINANCIAL_FACTS_TABLE: SE_FINANCIAL_FACTS_EXPORT_COLUMNS,
        }
    )


@contextmanager
def _patched_clickhouse(
    monkeypatch, client: StatefulFakeClickHouseClient
) -> Iterator[ClickhouseResource]:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[StatefulFakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    yield resource


# --- scope resolution -------------------------------------------------------


def test_resolve_backfill_partition_archive_keys_is_whole_file_distinct(
    tmp_path: Path,
) -> None:
    _seed_year_file(
        tmp_path,
        "2025",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2025_A),
            ("5560000002", _ARCHIVE_KEY_2025_A),
            ("5560000003", _ARCHIVE_KEY_2025_B),
        ),
    )

    with sweden_financial_year_duckdb_connection("2025", root=tmp_path) as connection:
        keys = resolve_sweden_financial_partition_archive_keys(connection, "2025")

    assert keys == sorted([_ARCHIVE_KEY_2025_A, _ARCHIVE_KEY_2025_B])


def test_resolve_current_partition_archive_keys_uses_archive_sync_catalog(
    tmp_path: Path,
) -> None:
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2026_W1),
            ("5560000002", _ARCHIVE_KEY_2026_W2),
        ),
        catalog_rows=(
            # This partition, actually downloaded -> in scope.
            ("2026-07-11", "current", _ARCHIVE_KEY_2026_W2, True),
            # This partition, listed-but-not-downloaded (already present
            # upstream re-listing) -> out of scope.
            ("2026-07-11", "current", _ARCHIVE_KEY_2026_W1, False),
            # A different weekly partition -> out of scope.
            ("2026-07-04", "current", _ARCHIVE_KEY_2026_W1, True),
        ),
    )

    with sweden_financial_year_duckdb_connection("2026", root=tmp_path) as connection:
        keys = resolve_sweden_financial_partition_archive_keys(
            connection, "2026-07-11"
        )

    assert keys == [_ARCHIVE_KEY_2026_W2]


def test_resolve_partition_archive_keys_raises_when_backfill_scope_empty(
    tmp_path: Path,
) -> None:
    _seed_year_file(tmp_path, "2024", reports=())

    with sweden_financial_year_duckdb_connection("2024", root=tmp_path) as connection:
        with pytest.raises(ValueError, match="No Sweden financial archive keys"):
            resolve_sweden_financial_partition_archive_keys(connection, "2024")


def test_resolve_current_partition_returns_empty_when_synced_but_nothing_new(
    tmp_path: Path,
) -> None:
    # The weekly sync ran (catalog rows exist for this partition) but zero
    # archives changed upstream -- a legitimate quiet week, not bookkeeping
    # loss, so the scope resolves to empty instead of raising.
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(("5560000001", _ARCHIVE_KEY_2026_W1),),
        catalog_rows=(
            ("2026-07-18", "current", _ARCHIVE_KEY_2026_W1, False),
            ("2026-07-18", "current", _ARCHIVE_KEY_2026_W2, False),
        ),
    )

    with sweden_financial_year_duckdb_connection("2026", root=tmp_path) as connection:
        keys = resolve_sweden_financial_partition_archive_keys(
            connection, "2026-07-18"
        )

    assert keys == []


def test_resolve_partition_archive_keys_raises_when_weekly_partition_not_synced(
    tmp_path: Path,
) -> None:
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(("5560000001", _ARCHIVE_KEY_2026_W1),),
        catalog_rows=(("2026-07-04", "current", _ARCHIVE_KEY_2026_W1, True),),
    )

    with sweden_financial_year_duckdb_connection("2026", root=tmp_path) as connection:
        with pytest.raises(ValueError, match="No Sweden financial archive keys"):
            resolve_sweden_financial_partition_archive_keys(
                connection, "2026-07-18"
            )


# --- reports upsert ---------------------------------------------------------


def test_upsert_reports_partition_deletes_own_scope_then_inserts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path,
        "2025",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2025_A),
            ("5560000002", _ARCHIVE_KEY_2025_B),
        ),
    )
    client = _reports_facts_fake_client()
    # A stale row from a previous export of this same partition -- the
    # pre-count is therefore 1, so the delete mutation must be issued.
    stale_row = _clickhouse_report_row(
        company_id="5560000001",
        year="2025",
        archive_key=_ARCHIVE_KEY_2025_A,
    )
    client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].append(stale_row)

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2025", root=tmp_path
        ) as connection:
            metadata = upsert_sweden_financial_reports_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )

    assert metadata == {
        "partition": "2025",
        "archives": 2,
        "deleted": 1,
        "inserted": 2,
    }
    assert len(client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE]) == 2
    assert stale_row not in client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE]

    statements = client.statements()
    delete_index = next(
        index
        for index, sql in enumerate(statements)
        if sql.startswith("ALTER TABLE")
    )
    insert_index = next(
        index
        for index, sql in enumerate(statements)
        if sql.startswith("INSERT INTO")
    )
    assert delete_index < insert_index

    # The delete is bound as an Array parameter, never interpolated SQL.
    delete_sql, delete_params = next(
        (sql, params)
        for sql, params in client.operations
        if sql.startswith("ALTER TABLE")
    )
    assert delete_sql == (
        f"ALTER TABLE {QUALIFIED_SE_FINANCIAL_REPORTS_TABLE} DELETE "
        "WHERE source_archive_key IN %(keys)s SETTINGS mutations_sync = 1"
    )
    assert isinstance(delete_params, dict)
    assert sorted(delete_params["keys"]) == sorted(
        [_ARCHIVE_KEY_2025_A, _ARCHIVE_KEY_2025_B]
    )
    for archive_key in (_ARCHIVE_KEY_2025_A, _ARCHIVE_KEY_2025_B):
        assert not any(archive_key in sql for sql in statements)


def test_upsert_reports_partition_skips_delete_mutation_when_precount_zero(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # The steady-state new-archive case: nothing in scope exists in
    # ClickHouse yet, so no ALTER DELETE mutation is issued at all -- the
    # run is a pure insert, still reporting deleted=0.
    _seed_year_file(
        tmp_path, "2025", reports=(("5560000001", _ARCHIVE_KEY_2025_A),)
    )
    client = _reports_facts_fake_client()

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2025", root=tmp_path
        ) as connection:
            metadata = upsert_sweden_financial_reports_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )

    assert metadata["deleted"] == 0
    assert metadata["inserted"] == 1
    assert not any(
        sql.startswith("ALTER TABLE") for sql in client.statements()
    )


def test_upsert_reports_partition_skips_cleanly_on_quiet_week(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # Weekly sync ran but zero archives changed upstream: the export must
    # succeed as a no-op (never raise, never touch ClickHouse rows).
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(("5560000001", _ARCHIVE_KEY_2026_W1),),
        catalog_rows=(("2026-07-18", "current", _ARCHIVE_KEY_2026_W1, False),),
    )
    client = _reports_facts_fake_client()
    existing_row = _clickhouse_report_row(
        company_id="5560000001",
        year="2026",
        archive_key=_ARCHIVE_KEY_2026_W1,
    )
    client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].append(existing_row)

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            metadata = upsert_sweden_financial_reports_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2026-07-18",
            )

    assert metadata == {
        "partition": "2026-07-18",
        "archives": 0,
        "deleted": 0,
        "inserted": 0,
        "skipped_reason": "weekly sync recorded zero changed archives",
    }
    assert client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE] == [existing_row]
    assert not any(
        sql.startswith(("ALTER TABLE", "INSERT INTO"))
        for sql in client.statements()
    )


def test_upsert_facts_partition_skips_cleanly_on_quiet_week(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(("5560000001", _ARCHIVE_KEY_2026_W1),),
        facts=("5560000001",),
        catalog_rows=(("2026-07-18", "current", _ARCHIVE_KEY_2026_W1, False),),
    )
    client = _reports_facts_fake_client()

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            metadata = upsert_sweden_financial_facts_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2026-07-18",
            )

    assert metadata == {
        "partition": "2026-07-18",
        "archives": 0,
        "deleted": 0,
        "inserted": 0,
        "skipped_reason": "weekly sync recorded zero changed archives",
    }
    assert client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE] == []
    assert not any(
        sql.startswith(("ALTER TABLE", "INSERT INTO", "CREATE TABLE"))
        for sql in client.statements()
    )


def test_upsert_reports_partition_fails_loudly_when_scope_has_zero_source_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # The archive scope itself is NON-empty (the catalog says this weekly
    # partition downloaded an archive) but the local reports table holds
    # zero rows for it -- parse never ran / is out of sync. Exporting
    # nothing silently is forbidden.
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(),
        catalog_rows=(("2026-07-11", "current", _ARCHIVE_KEY_2026_W1, True),),
    )
    client = _reports_facts_fake_client()

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            with pytest.raises(ValueError, match="ZERO local report rows"):
                upsert_sweden_financial_reports_partition(
                    duckdb_connection=connection,
                    clickhouse=resource,
                    partition_key="2026-07-11",
                )

    assert not any(
        sql.startswith(("ALTER TABLE", "INSERT INTO", "CREATE TABLE"))
        for sql in client.statements()
    )


def test_upsert_reports_partition_insert_uses_explicit_export_columns(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path, "2025", reports=(("5560000001", _ARCHIVE_KEY_2025_A),)
    )
    client = _reports_facts_fake_client()

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2025", root=tmp_path
        ) as connection:
            upsert_sweden_financial_reports_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )

    insert_sql = next(
        sql for sql in client.statements() if sql.startswith("INSERT INTO")
    )
    expected_columns = ", ".join(
        f"`{column}`" for column in SE_FINANCIAL_REPORTS_EXPORT_COLUMNS
    )
    assert insert_sql == (
        f"INSERT INTO `corpscout`.`se_financial_reports` "
        f"({expected_columns}) VALUES"
    )


def test_upsert_reports_partition_is_idempotent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path,
        "2025",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2025_A),
            ("5560000002", _ARCHIVE_KEY_2025_B),
        ),
    )
    client = _reports_facts_fake_client()

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2025", root=tmp_path
        ) as connection:
            first = upsert_sweden_financial_reports_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )
            rows_after_first = sorted(
                client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE], key=repr
            )
            second = upsert_sweden_financial_reports_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )

    assert first == {"partition": "2025", "archives": 2, "deleted": 0, "inserted": 2}
    # The re-run deletes exactly what the first run inserted, then inserts
    # the same rows again -- the final state is identical.
    assert second == {"partition": "2025", "archives": 2, "deleted": 2, "inserted": 2}
    assert (
        sorted(client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE], key=repr)
        == rows_after_first
    )


def test_upsert_reports_partition_never_touches_other_partitions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # ClickHouse already holds rows from ANOTHER partition (e.g. exported by
    # a different host) -- the 2026-07-19 incident scenario. This host's
    # local DuckDB only has 2025.
    _seed_year_file(
        tmp_path, "2025", reports=(("5560000001", _ARCHIVE_KEY_2025_A),)
    )
    client = _reports_facts_fake_client()
    foreign_row = _clickhouse_report_row(
        company_id="5569990000",
        year="2024",
        archive_key=_ARCHIVE_KEY_2024_Z,
    )
    client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE].append(foreign_row)

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2025", root=tmp_path
        ) as connection:
            metadata = upsert_sweden_financial_reports_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )

    assert metadata["deleted"] == 0
    assert metadata["inserted"] == 1
    remaining = client.rows[QUALIFIED_SE_FINANCIAL_REPORTS_TABLE]
    assert len(remaining) == 2
    assert foreign_row in remaining


def test_upsert_reports_partition_fails_loudly_before_any_clickhouse_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(tmp_path, "2025", reports=())
    client = _reports_facts_fake_client()

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2025", root=tmp_path
        ) as connection:
            with pytest.raises(
                ValueError, match="No Sweden financial archive keys"
            ):
                upsert_sweden_financial_reports_partition(
                    duckdb_connection=connection,
                    clickhouse=resource,
                    partition_key="2025",
                )

    assert not any(
        sql.startswith(("ALTER TABLE", "INSERT INTO"))
        for sql in client.statements()
    )


# --- facts upsert -----------------------------------------------------------


def test_upsert_facts_partition_scopes_by_statement_key_via_stage_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path,
        "2025",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2025_A),
            ("5560000002", _ARCHIVE_KEY_2025_B),
        ),
        facts=("5560000001", "5560000002"),
    )
    client = _reports_facts_fake_client()
    statement_keys = [
        _statement_key("5560000001", "2025"),
        _statement_key("5560000002", "2025"),
    ]

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2025", root=tmp_path
        ) as connection:
            metadata = upsert_sweden_financial_facts_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )

    assert metadata == {
        "partition": "2025",
        "archives": 2,
        "deleted": 0,
        "inserted": 2,
    }
    assert len(client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE]) == 2

    statements = client.statements()
    # se_financial_facts has no source_archive_key column (migration 000090):
    # the facts scope is the partition's statement_key set, staged in a
    # per-run Memory table (a backfill year holds up to ~500k keys, which
    # would blow max_query_size if bound as a client-substituted param).
    create_sql = next(
        sql for sql in statements if sql.startswith("CREATE TABLE")
    )
    create_match = _STAGE_CREATE_SQL_RE.match(create_sql)
    assert create_match is not None
    stage_table = create_match.group(1)
    assert _STAGE_TABLE_NAME_RE.match(stage_table)

    # The scope travels as INSERT data blocks into the stage, never as SQL
    # text or query params.
    stage_insert_ops = [
        (sql, params)
        for sql, params in client.operations
        if sql == f"INSERT INTO {stage_table} (statement_key) VALUES"
    ]
    assert len(stage_insert_ops) == 1
    assert sorted(stage_insert_ops[0][1]) == [(key,) for key in statement_keys]

    count_sql, count_params = next(
        (sql, params)
        for sql, params in client.operations
        if sql.startswith("SELECT count()")
        and QUALIFIED_SE_FINANCIAL_FACTS_TABLE in sql
    )
    assert count_sql == (
        f"SELECT count() FROM {QUALIFIED_SE_FINANCIAL_FACTS_TABLE} "
        f"WHERE statement_key IN (SELECT statement_key FROM {stage_table})"
    )
    assert count_params is None

    # Pre-count was 0 (nothing in scope existed yet): the delete mutation is
    # skipped entirely -- pure insert.
    assert not any(sql.startswith("ALTER TABLE") for sql in statements)

    # The stage table is dropped, and no statement_key literal ever appears
    # in any query text (the reports-scale archive keys are the only
    # scope values allowed inline anywhere).
    assert f"DROP TABLE IF EXISTS {stage_table}" in statements
    assert client.remaining_stage_tables() == []
    for key in statement_keys:
        assert not any(key in sql for sql in statements)

    insert_sql = next(
        sql
        for sql in statements
        if sql.startswith("INSERT INTO `corpscout`.`")
    )
    expected_columns = ", ".join(
        f"`{column}`" for column in SE_FINANCIAL_FACTS_EXPORT_COLUMNS
    )
    assert insert_sql == (
        f"INSERT INTO `corpscout`.`se_financial_facts` "
        f"({expected_columns}) VALUES"
    )


def test_upsert_facts_weekly_partition_filters_facts_through_reports_join(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # A weekly current partition exports only the facts belonging to ITS
    # downloaded archives, resolved through the local reports table inside
    # DuckDB -- the other archive's facts stay out of scope entirely.
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(
            ("5560000001", _ARCHIVE_KEY_2026_W1),
            ("5560000002", _ARCHIVE_KEY_2026_W2),
        ),
        facts=("5560000001", "5560000002"),
        catalog_rows=(
            ("2026-07-11", "current", _ARCHIVE_KEY_2026_W2, True),
            ("2026-07-11", "current", _ARCHIVE_KEY_2026_W1, False),
        ),
    )
    client = _reports_facts_fake_client()
    in_scope_key = _statement_key("5560000002", "2026")

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            metadata = upsert_sweden_financial_facts_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2026-07-11",
            )

    assert metadata == {
        "partition": "2026-07-11",
        "archives": 1,
        "deleted": 0,
        "inserted": 1,
    }
    inserted_rows = client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE]
    key_index = SE_FINANCIAL_FACTS_EXPORT_COLUMNS.index("statement_key")
    assert [row[key_index] for row in inserted_rows] == [in_scope_key]

    # The stage delete scope is exactly the in-scope statement key.
    stage_insert_params = next(
        params
        for sql, params in client.operations
        if _STAGE_INSERT_SQL_RE.match(sql)
    )
    assert stage_insert_params == [(in_scope_key,)]


def test_upsert_facts_partition_fails_loudly_when_scope_has_zero_statement_keys(
    tmp_path: Path,
    monkeypatch,
) -> None:
    # Archive scope non-empty (catalog says downloaded) but the local
    # reports table resolves it to zero statement keys -- fail loud, never
    # silently export nothing.
    _seed_year_file(
        tmp_path,
        "2026",
        reports=(),
        catalog_rows=(("2026-07-11", "current", _ARCHIVE_KEY_2026_W1, True),),
    )
    client = _reports_facts_fake_client()

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2026", root=tmp_path
        ) as connection:
            with pytest.raises(ValueError, match="ZERO statement keys"):
                upsert_sweden_financial_facts_partition(
                    duckdb_connection=connection,
                    clickhouse=resource,
                    partition_key="2026-07-11",
                )

    assert not any(
        sql.startswith(("ALTER TABLE", "INSERT INTO", "CREATE TABLE"))
        for sql in client.statements()
    )


def test_upsert_facts_partition_is_idempotent_and_leaves_other_partitions(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _seed_year_file(
        tmp_path,
        "2025",
        reports=(("5560000001", _ARCHIVE_KEY_2025_A),),
        facts=("5560000001",),
    )
    client = _reports_facts_fake_client()
    foreign_row = _clickhouse_fact_row(company_id="5569990000", year="2024")
    client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE].append(foreign_row)

    with _patched_clickhouse(monkeypatch, client) as resource:
        with sweden_financial_year_duckdb_connection(
            "2025", root=tmp_path
        ) as connection:
            first = upsert_sweden_financial_facts_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )
            rows_after_first = sorted(
                client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE], key=repr
            )
            second = upsert_sweden_financial_facts_partition(
                duckdb_connection=connection,
                clickhouse=resource,
                partition_key="2025",
            )

    assert first["deleted"] == 0
    assert first["inserted"] == 1
    assert second["deleted"] == 1
    assert second["inserted"] == 1
    assert (
        sorted(client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE], key=repr)
        == rows_after_first
    )
    assert foreign_row in client.rows[QUALIFIED_SE_FINANCIAL_FACTS_TABLE]

    # Exactly one delete mutation was issued (the re-run, whose pre-count
    # was 1), and it is the stage-subquery form with no params -- the
    # statement_key never appears in any query text.
    delete_ops = [
        (sql, params)
        for sql, params in client.operations
        if sql.startswith("ALTER TABLE")
    ]
    assert len(delete_ops) == 1
    delete_sql, delete_params = delete_ops[0]
    assert _STAGE_DELETE_SQL_RE.match(delete_sql)
    assert delete_params is None
    assert not any(
        _statement_key("5560000001", "2025") in sql
        for sql in client.statements()
    )
    assert client.remaining_stage_tables() == []


# --- shrink guard (metrics-only full replace keeps it) ----------------------


def test_guard_against_clickhouse_table_shrink_refuses_at_49_percent() -> None:
    with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
        guard_against_clickhouse_table_shrink(
            qualified_table="corpscout.se_financial_metrics",
            existing_row_count=100,
            staged_row_count=49,
            allow_shrink=False,
        )


def test_guard_against_clickhouse_table_shrink_message_names_table_existing_staged() -> (
    None
):
    with pytest.raises(ValueError) as exc_info:
        guard_against_clickhouse_table_shrink(
            qualified_table="corpscout.se_financial_metrics",
            existing_row_count=100,
            staged_row_count=49,
            allow_shrink=False,
        )

    message = str(exc_info.value)
    assert "corpscout.se_financial_metrics" in message
    assert "100" in message
    assert "49" in message
    assert "allow_shrink=True" in message


def test_guard_against_clickhouse_table_shrink_allows_at_51_percent() -> None:
    guard_against_clickhouse_table_shrink(
        qualified_table="corpscout.se_financial_metrics",
        existing_row_count=100,
        staged_row_count=51,
        allow_shrink=False,
    )


def test_guard_against_clickhouse_table_shrink_allows_exact_50_percent_boundary() -> None:
    # SHRINK_GUARD_MIN_RATIO is inclusive: staged == exactly half of existing
    # is allowed, only strictly-below-half refuses.
    assert SHRINK_GUARD_MIN_RATIO == 0.5
    guard_against_clickhouse_table_shrink(
        qualified_table="corpscout.se_financial_metrics",
        existing_row_count=100,
        staged_row_count=50,
        allow_shrink=False,
    )


def test_guard_against_clickhouse_table_shrink_never_trips_on_empty_existing_table() -> (
    None
):
    # A never-before-populated (or already-empty) target has nothing to
    # shrink from -- the guard is a no-op regardless of staged count.
    guard_against_clickhouse_table_shrink(
        qualified_table="corpscout.se_financial_metrics",
        existing_row_count=0,
        staged_row_count=0,
        allow_shrink=False,
    )


def test_guard_against_clickhouse_table_shrink_allow_shrink_overrides() -> None:
    # The explicit override path: even a would-be-refused shrink proceeds
    # when the caller has explicitly opted in.
    guard_against_clickhouse_table_shrink(
        qualified_table="corpscout.se_financial_metrics",
        existing_row_count=100,
        staged_row_count=1,
        allow_shrink=True,
    )


# --- storage helpers --------------------------------------------------------


def test_sweden_financial_year_duckdb_connection_raises_when_file_missing(
    tmp_path: Path,
) -> None:
    with pytest.raises(ValueError, match="DuckDB file not found for partition 2023"):
        with sweden_financial_year_duckdb_connection("2023", root=tmp_path):
            pass


def test_sweden_financial_year_duckdb_connection_is_read_only(
    tmp_path: Path,
) -> None:
    _seed_year_file(
        tmp_path, "2025", reports=(("5560000001", _ARCHIVE_KEY_2025_A),)
    )

    with sweden_financial_year_duckdb_connection("2025", root=tmp_path) as connection:
        rows = connection.execute(
            f"select count(*) from {SWEDEN_FINANCIAL_DATASET_NAME}.reports"
        ).fetchone()
        assert rows[0] == 1
        with pytest.raises(duckdb.Error):
            connection.execute(
                f"delete from {SWEDEN_FINANCIAL_DATASET_NAME}.reports"
            )


# --- fixtures ---------------------------------------------------------------


def _statement_key(company_id: str, year: str) -> str:
    return f"statement-{company_id}-{year}"


def _seed_year_file(
    root: Path,
    year: str,
    *,
    reports: tuple[tuple[str, str], ...],
    facts: tuple[str, ...] = (),
    catalog_rows: tuple[tuple[str, str, str, bool], ...] = (),
) -> Path:
    """Create a per-year Sweden financial DuckDB file like the parse assets do.

    ``reports`` is ``(company_id, source_archive_key)`` pairs; ``facts`` is
    company ids (one fact per report, linked via the report's statement
    key); ``catalog_rows`` is ``(load_partition_key, sync_kind, s3_key,
    downloaded)`` rows for ``archive_sync_catalog``.
    """
    database_path = root / f"sweden_financial_source_{year}.duckdb"
    with duckdb.connect(str(database_path)) as connection:
        _create_sweden_financial_tables(connection)
        for company_id, archive_key in reports:
            _insert_report(
                connection,
                year=year,
                company_id=company_id,
                archive_key=archive_key,
            )
        for company_id in facts:
            _insert_fact(connection, year=year, company_id=company_id)
        for load_partition_key, sync_kind, s3_key, downloaded in catalog_rows:
            connection.execute(
                f"""
                insert into {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog
                values ('run-1', ?, ?, ?, 'upstream.zip', '', '', 0, '', ?, ?, 0,
                        '', '')
                """,
                [sync_kind, load_partition_key, year, s3_key, downloaded],
            )
    return database_path


def _create_sweden_financial_tables(connection: duckdb.DuckDBPyConnection) -> None:
    connection.execute(f"create schema {SWEDEN_FINANCIAL_DATASET_NAME}")
    connection.execute(
        f"""
        create table {SWEDEN_FINANCIAL_DATASET_NAME}.reports (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            statement_key varchar,
            company_id varchar,
            report_period_start date,
            report_period_end date,
            fiscal_year integer,
            reported_company_name varchar,
            report_language varchar,
            source_archive_key varchar,
            source_archive_name varchar,
            nested_zip_name varchar,
            xhtml_object_key varchar,
            xhtml_sha256 varchar,
            xhtml_size_bytes bigint,
            taxonomy_entrypoint varchar,
            schema_refs varchar,
            contexts_count bigint,
            units_count bigint,
            facts_count bigint,
            parser_version varchar,
            source_payload_hash varchar,
            resolved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        create table {SWEDEN_FINANCIAL_DATASET_NAME}.facts (
            country_iso2 varchar,
            source_slug varchar,
            source_run_id varchar,
            source_record_id varchar,
            statement_key varchar,
            company_id varchar,
            report_period_end date,
            fact_ordinal bigint,
            concept_qname varchar,
            concept_namespace varchar,
            concept_local_name varchar,
            context_id varchar,
            unit_id varchar,
            decimals varchar,
            precision varchar,
            value_kind varchar,
            raw_value varchar,
            amount_original decimal(38, 10),
            amount_usd decimal(38, 10),
            date_value date,
            text_value varchar,
            currency varchar,
            dimensions varchar,
            fx_rate_to_usd decimal(38, 12),
            fx_rate_date date,
            fx_source varchar,
            parser_version varchar,
            source_payload_hash varchar,
            resolved_at timestamp
        )
        """
    )
    connection.execute(
        f"""
        create table {SWEDEN_FINANCIAL_DATASET_NAME}.archive_sync_catalog (
            source_run_id varchar,
            sync_kind varchar,
            load_partition_key varchar,
            archive_year varchar,
            upstream_key varchar,
            source_last_modified varchar,
            etag varchar,
            source_size_bytes bigint,
            source_url varchar,
            s3_key varchar,
            downloaded boolean,
            stored_size_bytes bigint,
            sha256 varchar,
            content_type varchar
        )
        """
    )


def _insert_report(
    connection: duckdb.DuckDBPyConnection,
    *,
    year: str,
    company_id: str,
    archive_key: str,
) -> None:
    period_end = date(int(year), 12, 31)
    source_record_id = f"{company_id}:{period_end.isoformat()}:report.xhtml"
    connection.execute(
        f"""
        insert into {SWEDEN_FINANCIAL_DATASET_NAME}.reports
        values (
            'SE',
            'bolagsverket_annual_reports',
            'run-1',
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            'Acme AB',
            'sv',
            ?,
            'source.zip',
            'nested.zip',
            'sweden_financial/report_xhtml/report.xhtml',
            'abc123',
            42,
            'entrypoint',
            '[]',
            1,
            1,
            1,
            'parser-v1',
            'abc123',
            ?
        )
        """,
        [
            source_record_id,
            _statement_key(company_id, year),
            company_id,
            date(int(year), 1, 1),
            period_end,
            int(year),
            archive_key,
            datetime(2026, 1, 2, 3, 4, 5),
        ],
    )


def _insert_fact(
    connection: duckdb.DuckDBPyConnection,
    *,
    year: str,
    company_id: str,
) -> None:
    period_end = date(int(year), 12, 31)
    source_record_id = f"{company_id}:{period_end.isoformat()}:report.xhtml"
    connection.execute(
        f"""
        insert into {SWEDEN_FINANCIAL_DATASET_NAME}.facts
        values (
            'SE',
            'bolagsverket_annual_reports',
            'run-1',
            ?,
            ?,
            ?,
            ?,
            1,
            'se:Revenue',
            'se',
            'Revenue',
            'ctx-1',
            'SEK',
            '0',
            null,
            'numeric',
            '1000',
            ?,
            null,
            null,
            null,
            'SEK',
            '{{}}',
            null,
            null,
            '',
            'parser-v1',
            'abc123',
            ?
        )
        """,
        [
            source_record_id,
            _statement_key(company_id, year),
            company_id,
            period_end,
            Decimal("1000.0000000000"),
            datetime(2026, 1, 2, 3, 4, 5),
        ],
    )


def _clickhouse_report_row(
    *,
    company_id: str,
    year: str,
    archive_key: str,
) -> tuple[Any, ...]:
    """A pre-existing ClickHouse se_financial_reports row from another
    partition, in export-column order."""
    period_end = date(int(year), 12, 31)
    values = {column: None for column in SE_FINANCIAL_REPORTS_EXPORT_COLUMNS}
    values.update(
        country_iso2="SE",
        source_slug="bolagsverket_annual_reports",
        source_run_id="run-0",
        source_record_id=f"{company_id}:{period_end.isoformat()}:report.xhtml",
        statement_key=_statement_key(company_id, year),
        company_id=company_id,
        report_period_end=period_end,
        source_archive_key=archive_key,
    )
    return tuple(values[column] for column in SE_FINANCIAL_REPORTS_EXPORT_COLUMNS)


def _clickhouse_fact_row(*, company_id: str, year: str) -> tuple[Any, ...]:
    period_end = date(int(year), 12, 31)
    values = {column: None for column in SE_FINANCIAL_FACTS_EXPORT_COLUMNS}
    values.update(
        country_iso2="SE",
        source_slug="bolagsverket_annual_reports",
        source_run_id="run-0",
        source_record_id=f"{company_id}:{period_end.isoformat()}:report.xhtml",
        statement_key=_statement_key(company_id, year),
        company_id=company_id,
        report_period_end=period_end,
        fact_ordinal=1,
    )
    return tuple(values[column] for column in SE_FINANCIAL_FACTS_EXPORT_COLUMNS)
