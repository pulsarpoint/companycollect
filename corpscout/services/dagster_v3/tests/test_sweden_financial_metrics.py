from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.sweden_financial.metrics import (
    SE_FINANCIAL_METRICS_TABLE,
    SWEDEN_FINANCIAL_MAPPING_VERSION,
    build_sweden_financial_metrics_insert_sql,
    replace_sweden_financial_metrics_clickhouse,
)


class FakeClickHouseClient:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.table_checks: list[tuple[str, ...]] = []
        self.insert_parameters: dict[str, object] = {}

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
            return [(10, 8, 2019, 2025, 0, 1, 9, 8, 7, 7, 9, 9, 9, 8, 8, 7, 7, 5, 1, 6)]
        if sql.startswith("EXCHANGE TABLES") or sql.startswith("DROP TABLE"):
            return []
        raise AssertionError(sql)


def test_sweden_financial_metrics_sql_uses_current_precise_undimensioned_facts() -> (
    None
):
    sql = build_sweden_financial_metrics_insert_sql(
        "`corpscout`.`_tmp_se_financial_metrics_test`"
    )

    assert "lowerUTF8(context_id) IN ('period0', 'balans0')" in sql
    assert "dimensions = '{}'" in sql
    assert "toInt32OrNull(decimals)" in sql
    assert "argMaxIf" in sql
    assert "Nettoomsattning" in sql
    assert "MedelantaletAnstallda" in sql
    assert "KortfristigaFordringar" in sql
    assert "current_receivables_amount_original" in sql
    assert "negative derived liabilities" in sql
    assert "xhtml_source_uri" in sql
    assert "%(xhtml_uri_prefix)s" in sql
    assert "corpscout.exchange_rates" in sql


def test_sweden_financial_metrics_sql_excludes_non_statement_documents() -> None:
    # 2026-07-18: rar/rarc registration envelopes and figure-less race
    # documents must never surface in se_financial_metrics; see
    # build_sweden_financial_metrics_insert_sql's inline comment for the
    # full contract (gaap/coa bank filings are deliberately kept).
    sql = build_sweden_financial_metrics_insert_sql(
        "`corpscout`.`_tmp_se_financial_metrics_test`"
    )

    assert "NOT LIKE '%/ar/rar%'" in sql
    assert "LIKE '%/misc/race/%'" in sql
    assert "ifNull(facts.mapped_fact_count, 0) = 0" in sql
    # gaap/coa is never mentioned in the exclusion clause: bank filings are
    # kept even when figure-less.
    where_clause_start = sql.index("WHERE ifNull(reports.taxonomy_entrypoint")
    where_clause = sql[where_clause_start : where_clause_start + 400]
    assert "gaap" not in where_clause


def test_replace_sweden_financial_metrics_is_atomic_and_reports_coverage(
    monkeypatch,
) -> None:
    client = FakeClickHouseClient()
    resource = ClickhouseResource(host="localhost")

    @contextmanager
    def fake_get_connection(
        self: ClickhouseResource,
    ) -> Iterator[FakeClickHouseClient]:
        yield client

    monkeypatch.setattr(ClickhouseResource, "get_connection", fake_get_connection)

    counts = replace_sweden_financial_metrics_clickhouse(
        clickhouse=resource,
        source_run_id="metrics-run",
        resolved_at=datetime(2026, 7, 16, 20, 0, tzinfo=UTC),
    )

    assert client.table_checks == [
        (
            "se_financial_reports",
            "se_financial_facts",
            "se_financial_metrics",
            "exchange_rates",
        )
    ]
    assert client.insert_parameters["source_run_id"] == "metrics-run"
    assert client.insert_parameters["xhtml_uri_prefix"] == (
        "s3://source-sweden-financial/"
    )
    assert (
        client.insert_parameters["mapping_version"] == SWEDEN_FINANCIAL_MAPPING_VERSION
    )
    assert any(
        statement.startswith("EXCHANGE TABLES") for statement in client.statements
    )
    assert client.statements[-1].startswith("DROP TABLE")
    assert counts["row_count"] == 10
    assert counts["company_count"] == 8
    assert counts["mapped_statement_count"] == 9
    assert counts["missing_fx_count"] == 0
    assert counts["invalid_liabilities_statement_count"] == 1
    assert counts["revenue_statement_count"] == 8
    assert counts["employees_statement_count"] == 6
    assert counts["table"] == f"corpscout.{SE_FINANCIAL_METRICS_TABLE}"
