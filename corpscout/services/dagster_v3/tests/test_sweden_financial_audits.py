import re
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import dagster as dg
import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.audits import (
    QUALIFIED_SE_COMPANY_AUDITS_TABLE,
    SE_COMPANY_AUDITS_COLUMNS,
    SE_COMPANY_AUDITS_TABLE,
    _audits_quality_sql,
    build_audits_insert_sql,
    replace_se_company_audits_clickhouse,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"

_STAGE_TABLE = "`corpscout`.`_tmp_se_company_audits_test`"


def _migration_sql(file_name: str) -> str:
    return (MIGRATIONS_DIR / file_name).read_text(encoding="utf-8")


def _built_sql() -> str:
    return build_audits_insert_sql(_STAGE_TABLE)


def test_audits_table_and_columns_constants() -> None:
    assert SE_COMPANY_AUDITS_TABLE == "se_company_audits"
    assert QUALIFIED_SE_COMPANY_AUDITS_TABLE == "corpscout.se_company_audits"
    assert SE_COMPANY_AUDITS_COLUMNS == (
        "company_id",
        "fiscal_year",
        "statement_key",
        "audit_firm",
        "opinion_kind",
        "opinion_date",
        "resolved_at",
    )


def test_migration_000146_covers_audits_columns_in_order() -> None:
    """Contract test (mirrors officers.py's migration-column contract): every
    column in ``SE_COMPANY_AUDITS_COLUMNS`` must appear, in order, in
    migration 000146's up SQL -- pins the Python columns tuple to the
    ClickHouse schema so the two can never silently drift apart.
    """
    up_sql = _migration_sql("000146_corpscout_se_company_audits.up.sql")
    down_sql = _migration_sql("000146_corpscout_se_company_audits.down.sql")

    assert "CREATE DATABASE IF NOT EXISTS corpscout" in up_sql
    assert "CREATE TABLE IF NOT EXISTS corpscout.se_company_audits" in up_sql

    last_index = -1
    for column_name in SE_COMPANY_AUDITS_COLUMNS:
        marker = f"    {column_name} "
        index = up_sql.index(marker)
        assert index > last_index, (
            f"column {column_name!r} is out of order relative to "
            "SE_COMPANY_AUDITS_COLUMNS in the migration file"
        )
        last_index = index

    assert "opinion_kind LowCardinality(String)" in up_sql
    assert "opinion_date Nullable(Date32)" in up_sql
    assert "resolved_at DateTime64(3, 'UTC')" in up_sql
    assert "ENGINE = MergeTree" in up_sql
    assert "ORDER BY (company_id, fiscal_year, statement_key)" in up_sql
    assert "DROP TABLE IF EXISTS corpscout.se_company_audits" in down_sql


def test_audits_insert_sql_targets_stage_table_with_explicit_columns() -> None:
    sql = _built_sql()

    assert sql.startswith(f"INSERT INTO {_STAGE_TABLE} (")
    columns_block = sql[: sql.index(")\nSELECT")]
    for column_name in SE_COMPANY_AUDITS_COLUMNS:
        assert column_name in columns_block


def test_audits_sql_contains_both_audit_firm_name_spellings() -> None:
    sql = _built_sql()

    assert "'ValtRevisionsbolagNamn'" in sql
    assert "'ValtRevisionsbolagsnamn'" in sql


def test_audits_sql_contains_both_opinion_concepts() -> None:
    sql = _built_sql()

    assert (
        "'RevisorspateckningRevisionsberattelseEnligtStandardutformning'" in sql
    )
    assert (
        "'RevisorspateckningRevisionsberattelseAvvikerStandardutformning'" in sql
    )


def test_audits_sql_avviker_wins_over_enligt_in_opinion_kind() -> None:
    """Locks in the module docstring's "Avviker-wins" rule: the multiIf
    classifying opinion_kind must test the Avviker (modified) concept
    BEFORE the Enligt (standard) concept, so a statement carrying both facts
    (a malformed/edge-case filing) resolves to 'modified', never 'standard'.
    """
    sql = _built_sql()

    modified_index = sql.index(
        "countIf(concept_local_name = "
        "'RevisorspateckningRevisionsberattelseAvvikerStandardutformning') > 0, 'modified'"
    )
    standard_index = sql.index(
        "countIf(concept_local_name = "
        "'RevisorspateckningRevisionsberattelseEnligtStandardutformning') > 0, 'standard'"
    )
    unknown_index = sql.index("'unknown'", standard_index)

    assert modified_index < standard_index < unknown_index


def test_audits_sql_fiscal_year_and_grouping() -> None:
    sql = _built_sql()

    assert (
        "toInt32(coalesce(toYear(report_period_end), 0)) AS fiscal_year" in sql
    )
    assert "GROUP BY company_id, fiscal_year, statement_key" in sql


def test_audits_sql_keeps_rows_with_known_firm_or_opinion() -> None:
    # A group with a known audit firm OR a resolved opinion is kept even if
    # the other field didn't resolve -- only a group with NEITHER is dropped.
    sql = _built_sql()

    assert "HAVING audit_firm != '' OR opinion_kind != 'unknown'" in sql


def test_audits_quality_sql_counts_expected_metrics() -> None:
    sql = _audits_quality_sql(_STAGE_TABLE)

    assert sql.startswith("SELECT")
    assert f"FROM {_STAGE_TABLE}" in sql
    assert "count() AS row_count" in sql
    assert "uniqExact(company_id) AS company_count" in sql
    assert "countIf(opinion_kind = 'modified') AS modified_opinion_count" in sql
    assert "countIf(opinion_kind = 'unknown') AS unknown_opinion_count" in sql
    assert "countIf(fiscal_year = 0) AS null_fiscal_year_count" in sql


def test_audits_sql_survives_percent_formatting_with_driver_params() -> None:
    """Regression test mirroring officers.py's %-format round-trip check:
    ``replace_se_company_audits_clickhouse`` calls ``client.execute(sql,
    {...})`` with a params dict (to carry ``resolved_at``), so
    clickhouse_driver Python-%-formats the whole query text against that
    dict. Unlike officers.py's LIKE/ILIKE patterns, this SQL has no ``%``
    literal to double -- the only ``%`` in the query is the single driver
    placeholder, and formatting must not raise or corrupt the SQL.
    """
    sql = _built_sql()
    params = {
        "resolved_at": datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        "source_run_id": "run-1",
    }

    formatted = sql % params

    assert "%" not in formatted


def test_audits_sql_has_no_bare_percent_placeholder_leaks() -> None:
    # Only resolved_at is a driver placeholder; no other %(name)s markers,
    # and no stray %% doubling (this SQL has no LIKE/ILIKE literals).
    sql = _built_sql()

    placeholders = set(re.findall(r"%\([a-zA-Z_]+\)s", sql))
    assert placeholders == {"%(resolved_at)s"}
    assert "%%" not in sql


class _FakeAuditsClickHouseClient:
    def __init__(
        self,
        *,
        quality_row: tuple[object, ...],
        existing_row_count: int = 0,
    ) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.insert_parameters: dict[str, object] = {}
        self.quality_row = quality_row
        self.existing_row_count = existing_row_count

    def execute(
        self,
        sql: str,
        params: dict[str, object] | None = None,
    ) -> list[tuple[object, ...]]:
        self.statements.append(sql)
        if "system.tables" in sql:
            requested = tuple(params["tables"]) if params is not None else ()
            self.table_checks.append(requested)
            return [(table,) for table in requested]
        if sql.startswith("CREATE TABLE"):
            return []
        if sql.startswith("INSERT INTO"):
            self.insert_parameters = params or {}
            return []
        if "AS row_count" in sql:
            return [self.quality_row]
        # The shrink guard's pre-EXCHANGE existing-row-count check has no
        # alias, so it must be checked AFTER "AS row_count" above.
        if sql.startswith("SELECT count() FROM"):
            return [(self.existing_row_count,)]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def _patch_clickhouse(
    monkeypatch, client: _FakeAuditsClickHouseClient
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeAuditsClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


_DEFAULT_QUALITY_ROW = (500, 300, 40, 60, 15)


def test_replace_se_company_audits_is_atomic_and_reports_counts(monkeypatch) -> None:
    client = _FakeAuditsClickHouseClient(quality_row=_DEFAULT_QUALITY_ROW)
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_company_audits_clickhouse(
        clickhouse=resource,
        source_run_id="audits-run",
        resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
    )

    assert client.table_checks == [
        ("se_company_audits", "se_financial_facts"),
    ]
    assert any(statement.startswith("CREATE TABLE") for statement in client.statements)
    assert any(statement.startswith("INSERT INTO") for statement in client.statements)
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")

    assert client.insert_parameters["source_run_id"] == "audits-run"
    assert client.insert_parameters["resolved_at"] == datetime(
        2026, 7, 19, 15, 0, tzinfo=UTC
    )

    assert metadata["row_count"] == 500
    assert metadata["company_count"] == 300
    assert metadata["modified_opinion_count"] == 40
    assert metadata["unknown_opinion_count"] == 60
    assert metadata["null_fiscal_year_count"] == 15
    assert metadata["table"] == QUALIFIED_SE_COMPANY_AUDITS_TABLE
    assert metadata["source_run_id"] == "audits-run"


def test_replace_se_company_audits_refuses_to_swap_an_empty_stage(
    monkeypatch,
) -> None:
    client = _FakeAuditsClickHouseClient(quality_row=(0, 0, 0, 0, 0))
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="produced no rows"):
        replace_se_company_audits_clickhouse(
            clickhouse=resource,
            source_run_id="audits-run",
            resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    # Cleanup still runs (finally block) even on a refused replace.
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_se_company_audits_refuses_shrink_below_half(monkeypatch) -> None:
    quality_row = (49, *_DEFAULT_QUALITY_ROW[1:])
    client = _FakeAuditsClickHouseClient(
        quality_row=quality_row, existing_row_count=100
    )
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="Refusing to replace ClickHouse table"):
        replace_se_company_audits_clickhouse(
            clickhouse=resource,
            source_run_id="audits-run",
            resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        )

    assert not any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")


def test_replace_se_company_audits_allow_shrink_overrides_guard(monkeypatch) -> None:
    quality_row = (1, *_DEFAULT_QUALITY_ROW[1:])
    client = _FakeAuditsClickHouseClient(
        quality_row=quality_row, existing_row_count=100
    )
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_company_audits_clickhouse(
        clickhouse=resource,
        source_run_id="audits-run",
        resolved_at=datetime(2026, 7, 19, 15, 0, tzinfo=UTC),
        allow_shrink=True,
    )

    assert metadata["row_count"] == 1
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )


def test_se_company_audits_clickhouse_asset_is_wired_correctly() -> None:
    from dagster_v3.definitions import defs as load_defs

    repo = load_defs().get_repository_def()

    node = repo.asset_graph.get(dg.AssetKey("se_company_audits_clickhouse"))
    assert node.group_name == "sweden_financial"
    assert node.pools == set()
    assert node.partitions_def is None
    assert node.parent_keys == {dg.AssetKey("sweden_financial_facts_clickhouse")}

    # The audits table should refresh whenever metrics/history/officers
    # refresh: it lives in the same (currently unscheduled,
    # manually/backfill-triggered) clickhouse job selection.
    clickhouse_job_asset_keys = {
        key.path[-1]
        for key in repo.get_job(
            "sweden_financial_clickhouse_job"
        ).asset_layer.executable_asset_keys
    }
    assert "se_company_audits_clickhouse" in clickhouse_job_asset_keys
