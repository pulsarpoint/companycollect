"""The precedence export (spec section 8): global rows only, idempotent, and registered
without dependencies."""

from datetime import UTC, datetime

import dagster as dg

from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.assets import GROUP_NAME, export_precedence
from dagster_v3.defs.se_company.financial.precedence import precedence_rows
from dagster_v3.definitions import defs as load_project_defs

_SELECT_STORED_SQL = (
    f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
    "FINAL WHERE company_id = '' AND period_key = '' AND removed = 0"
)
_INSERT_SQL = (
    f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
    f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES"
)


class FakeClient:
    """Dispatches on the whole statement text against the table's own qualified name."""

    def __init__(self, stored: list[tuple] = (), stale: int = 0) -> None:
        self.calls: list[tuple[str, object]] = []
        self.stored = list(stored)
        self.stale = stale

    def execute(self, sql, params=None, settings=None):
        self.calls.append((sql, params))
        if sql == _SELECT_STORED_SQL:
            return list(self.stored)
        if sql.startswith(f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE}"):
            return []
        if sql.startswith(f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE}"):
            return [(self.stale,)]
        raise AssertionError(f"unexpected statement: {sql!r}")


def test_export_inserts_the_82_global_rows_and_counts_stale_pairs() -> None:
    client = FakeClient(stale=3)
    exported_at = datetime(2026, 9, 12, 8, 0, 0, 123000, tzinfo=UTC)
    pairs, stale = export_precedence(client, exported_at)
    assert (pairs, stale) == (82, 3)
    select_sql, _ = client.calls[0]
    assert select_sql == _SELECT_STORED_SQL
    insert_sql, rows = client.calls[1]
    assert insert_sql == _INSERT_SQL
    assert rows[0] == ("", "", "period_start", "reviewer", 20000, 0, "code", "", exported_at)
    assert all(row[0] == "" and row[1] == "" for row in rows)
    assert [row[2:5] for row in rows] == precedence_rows()
    count_sql, params = client.calls[2]
    assert "company_id = ''" in count_sql and "period_key = ''" in count_sql
    assert "removed = 0" in count_sql and "FINAL" in count_sql
    assert params == {"exported_at": "2026-09-12 08:00:00.123"}


def test_export_against_a_changed_dictionary_inserts_every_row() -> None:
    stored = list(precedence_rows())
    stored[-1] = (stored[-1][0], stored[-1][1], 999)  # one number differs from the dictionary
    client = FakeClient(stored=stored, stale=1)
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 9, 0, tzinfo=UTC))
    assert (pairs, stale) == (82, 1)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "INSERT", "SELECT"]


def test_export_against_matching_stored_rows_inserts_nothing() -> None:
    client = FakeClient(stored=list(precedence_rows()), stale=0)
    pairs, stale = export_precedence(client, datetime(2026, 9, 12, 10, 0, tzinfo=UTC))
    assert (pairs, stale) == (0, 0)
    assert [call[0].split(" ", 1)[0] for call in client.calls] == ["SELECT", "SELECT"]


def test_the_export_asset_is_registered_without_dependencies() -> None:
    repository = load_project_defs().get_repository_def()
    node = repository.asset_graph.get(dg.AssetKey("se_company_financial_precedence_clickhouse"))
    assert node.parent_keys == set()
    assert node.partitions_def is None
    assert node.group_name == GROUP_NAME == "se_company_financial"
