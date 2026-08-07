from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.observations import (
    BOLAGSVERKET_FINANCIAL_CONCEPTS,
    QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
    SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_COLUMNS,
    SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
    build_bolagsverket_financial_observations_insert_sql,
    build_bolagsverket_financial_observations_quality_sql,
    replace_se_bolagsverket_financial_observations_clickhouse,
)

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
_MIGRATION_NAME = "000253_corpscout_se_bolagsverket_financial_observations"
_STAGE_TABLE = "`corpscout`.`_tmp_se_bolagsverket_financial_observations_test`"


def test_observations_migration_preserves_context_periods_and_owns_table() -> None:
    sql = (MIGRATIONS_DIR / f"{_MIGRATION_NAME}.up.sql").read_text()
    down_sql = (MIGRATIONS_DIR / f"{_MIGRATION_NAME}.down.sql").read_text()

    assert (
        "ALTER TABLE corpscout.se_financial_facts\n"
        "    ADD COLUMN IF NOT EXISTS context_period_start Nullable(Date32)" in sql
    )
    assert "ADD COLUMN IF NOT EXISTS context_period_end Nullable(Date32)" in sql
    assert (
        "CREATE TABLE IF NOT EXISTS "
        f"corpscout.{SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE}" in sql
    )
    for column in SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_COLUMNS:
        assert f"    {column} " in sql

    assert "value_original Nullable(Decimal(38, 10))" in sql
    assert "value_usd Nullable(Decimal(38, 10))" in sql
    assert "quality_flags Array(String)" in sql
    assert "ENGINE = MergeTree" in sql
    assert "source_fact_ordinal" in sql
    assert (
        f"DROP TABLE IF EXISTS corpscout.{SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE}"
        in down_sql
    )


def test_observations_sql_keeps_every_mapped_source_fact_without_resolution() -> None:
    sql = build_bolagsverket_financial_observations_insert_sql(_STAGE_TABLE)

    assert sql.startswith(f"INSERT INTO {_STAGE_TABLE} (")
    assert "source_fact_ordinal" in sql
    assert "source_context_id" in sql
    assert "context_period_start" in sql
    assert "context_period_end" in sql
    assert "represented_period_start" in sql
    assert "represented_period_end" in sql
    assert "represented_fiscal_year" in sql
    assert "observation_kind" in sql
    assert "revenue_overlap_relative_diff" in sql
    assert "quality_flags" in sql
    assert "revenue_overlap_disagreement" in sql
    assert "ambiguous_solidity_scale" in sql
    assert "represented_period_approximated" in sql

    for concept_name in BOLAGSVERKET_FINANCIAL_CONCEPTS:
        assert f"concept_local_name = '{concept_name}'" in sql

    assert "LIMIT 1 BY" not in sql
    assert "source_statement_key NOT IN" not in sql
    assert "observation_kind = 'reported' OR" not in sql


def test_observations_quality_sql_reports_source_grain_and_flags() -> None:
    sql = build_bolagsverket_financial_observations_quality_sql(_STAGE_TABLE)

    assert "count() AS row_count" in sql
    assert "uniqExact(company_id) AS company_count" in sql
    assert "uniqExact(source_statement_key) AS statement_count" in sql
    assert "countIf(observation_kind = 'reported') AS reported_count" in sql
    assert "countIf(observation_kind = 'comparative') AS comparative_count" in sql
    assert "countIf(observation_kind = 'other') AS other_count" in sql
    assert "countIf(notEmpty(quality_flags)) AS flagged_count" in sql


class _FakeObservationClickHouseClient:
    def __init__(
        self,
        *,
        quality_row: tuple[object, ...],
        existing_row_count: int = 0,
    ) -> None:
        self.quality_row = quality_row
        self.existing_row_count = existing_row_count
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []

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
        if sql.startswith("CREATE TABLE") or sql.startswith("INSERT INTO"):
            return []
        if "AS row_count" in sql:
            return [self.quality_row]
        if sql.startswith("SELECT count() FROM"):
            return [(self.existing_row_count,)]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def _patch_clickhouse(
    monkeypatch,
    client: _FakeObservationClickHouseClient,
) -> ClickhouseResource:
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[_FakeObservationClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)
    return resource


def test_observations_replace_is_atomic_and_reports_metadata(monkeypatch) -> None:
    client = _FakeObservationClickHouseClient(
        quality_row=(10, 2, 3, 4, 5, 1, 2, 2020, 2024),
        existing_row_count=12,
    )
    resource = _patch_clickhouse(monkeypatch, client)

    metadata = replace_se_bolagsverket_financial_observations_clickhouse(
        clickhouse=resource
    )

    assert client.table_checks == [
        (
            SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
            "se_financial_reports",
            "se_financial_facts",
            "exchange_rates",
        )
    ]
    assert any(sql.startswith("CREATE TABLE") for sql in client.statements)
    assert any(sql.startswith("INSERT INTO") for sql in client.statements)
    assert any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
    assert client.statements[-1].startswith("DROP TABLE")
    assert metadata == {
        "row_count": 10,
        "company_count": 2,
        "statement_count": 3,
        "reported_count": 4,
        "comparative_count": 5,
        "other_count": 1,
        "flagged_count": 2,
        "min_fiscal_year": 2020,
        "max_fiscal_year": 2024,
        "table": QUALIFIED_SE_BOLAGSVERKET_FINANCIAL_OBSERVATIONS_TABLE,
    }


def test_observations_replace_refuses_empty_stage(monkeypatch) -> None:
    client = _FakeObservationClickHouseClient(
        quality_row=(0, 0, 0, 0, 0, 0, 0, None, None)
    )
    resource = _patch_clickhouse(monkeypatch, client)

    with pytest.raises(ValueError, match="produced no rows"):
        replace_se_bolagsverket_financial_observations_clickhouse(clickhouse=resource)

    assert not any(sql.startswith("EXCHANGE TABLES") for sql in client.statements)
    assert client.statements[-1].startswith("DROP TABLE")
