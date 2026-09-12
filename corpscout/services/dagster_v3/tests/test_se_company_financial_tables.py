"""The five financial-entity tables (spec 2026-09-11 section 4), pinned against the migration
DDL through tests/se_company_ddl.py so tables.py and the deployed schema cannot drift."""

from datetime import date

from dagster_v3.defs.se_company.financial import tables
from tests.se_company_ddl import declared_columns, table_block

COMPANY_ID_CHECK = "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')"
SCOPE_CHECK = "CONSTRAINT valid_scope CHECK scope IN ('standalone', 'consolidated')"
PERIOD_KEY_CHECK = "CONSTRAINT valid_period_key CHECK period_key = concat(scope, ':', toString(period_end))"


def test_the_twenty_monetary_fields_and_the_folded_order() -> None:
    assert len(tables.MONETARY_FIELDS) == 20
    assert tables.MONETARY_FIELDS[0] == "revenue" and tables.MONETARY_FIELDS[-1] == "dividend"
    assert len(set(tables.MONETARY_FIELDS)) == 20
    assert tables.PERIOD_FIELDS == ("period_start", "fiscal_year", "period_months")
    assert tables.FOLDED_FIELDS == (
        *tables.PERIOD_FIELDS, "currency", *tables.MONETARY_FIELDS, "employees",
    )
    assert len(tables.FOLDED_FIELDS) == 25
    assert tables.original_column("revenue") == "revenue_amount_original"
    assert tables.usd_column("revenue") == "revenue_amount_usd"
    assert tables.source_column("employees") == "employees_source"
    assert tables.period_key("standalone", date(2023, 12, 31)) == "standalone:2023-12-31"
    assert tables.SOURCES == (
        "bolagsverket", "bolagsverket_comparative", "esef", "ratsit", "reviewer", "reviewer_draft",
    )
    assert tables.SCOPES == ("standalone", "consolidated")
    assert tables.AMOUNT_SCALES == (1, 1000, 1000000)
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_financial"
    assert tables.SCRATCH_SCOPE_PREFIX == "corpscout._tmp_financial_scope_"


def test_suggestion_table_is_one_current_row_per_company_source_and_period() -> None:
    block = table_block(tables.SUGGESTION_TABLE)
    assert declared_columns(tables.SUGGESTION_TABLE) == list(tables.SUGGESTION_COLUMNS)
    assert len(tables.SUGGESTION_COLUMNS) == 63
    assert "ENGINE = ReplacingMergeTree(suggested_at)" in block
    assert "ORDER BY (company_id, source, period_key)" in block
    for check in (COMPANY_ID_CHECK, SCOPE_CHECK, PERIOD_KEY_CHECK):
        assert check in block, check
    assert "CONSTRAINT valid_amount_scale CHECK amount_scale IN (1, 1000, 1000000)" in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    period_end Date32," in block
    assert "    period_end_derived UInt8 DEFAULT 0," in block
    assert "    filing_fiscal_year Nullable(UInt16)," in block
    assert "    currency LowCardinality(Nullable(String))," in block
    assert "    amount_scale UInt32 DEFAULT 1," in block
    assert "    employees Nullable(UInt64)," in block
    assert "    fx_rate_to_usd Nullable(Decimal(38, 12))," in block
    assert "    fx_source LowCardinality(String) DEFAULT ''," in block
    for field in tables.MONETARY_FIELDS:
        assert f"    {tables.original_column(field)} Nullable(Decimal(38, 6))," in block, field
        assert f"    {tables.usd_column(field)} Nullable(Decimal(38, 6))," in block, field
    assert tables.SUGGESTION_VALUE_COLUMNS == (*tables.MONETARY_SUGGESTION_COLUMNS, "employees")
    assert len(tables.SUGGESTION_VALUE_COLUMNS) == 41
    assert "MATERIALIZED" not in block


def test_main_table_is_one_row_per_company_scope_and_period_end() -> None:
    block = table_block(tables.MAIN_TABLE)
    assert declared_columns(tables.MAIN_TABLE) == list(tables.MAIN_COLUMNS)
    assert len(tables.MAIN_COLUMNS) == 80
    assert "ENGINE = ReplacingMergeTree(folded_at)" in block
    assert "ORDER BY (company_id, scope, period_end)" in block
    for check in (COMPANY_ID_CHECK, SCOPE_CHECK, PERIOD_KEY_CHECK):
        assert check in block, check
    # The row's currency is decided first and is never NULL ('' when no source names one).
    assert "    currency LowCardinality(String)," in block
    for field in tables.FOLDED_FIELDS:
        assert tables.source_column(field) in tables.MAIN_COLUMNS, field
        assert f"    {tables.source_column(field)} LowCardinality(String)," in block, field
    assert "    sources Array(LowCardinality(String))," in block
    assert "    active UInt8," in block
    assert "    inactive_reason LowCardinality(String)," in block
    # No row-level fx columns: each figure's USD twin is its own winner's conversion.
    assert "fx_rate_to_usd" not in block and "amount_scale" not in block


def test_history_is_the_main_row_plus_the_change_block() -> None:
    block = table_block(tables.HISTORY_TABLE)
    assert declared_columns(tables.HISTORY_TABLE) == list(tables.HISTORY_COLUMNS)
    assert tables.HISTORY_COLUMNS == (
        *tables.MAIN_COLUMNS, "changed_fields", "changed_at", "change_kind", "fold_run_id",
    )
    # Append-only, written only by the fold from rows the main table already validated.
    assert "CONSTRAINT" not in block
    assert "ENGINE = MergeTree" in block
    assert "ORDER BY (company_id, scope, period_end, changed_at)" in block
    assert "    changed_fields Array(String)," in block
    assert "    change_kind LowCardinality(String)," in block


def test_precedence_table_is_global_or_company_scoped_with_a_period() -> None:
    block = table_block(tables.PRECEDENCE_TABLE)
    assert declared_columns(tables.PRECEDENCE_TABLE) == list(tables.PRECEDENCE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, period_key, field, source)" in block
    assert (
        "CONSTRAINT valid_company_id CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')"
        in block
    )
    # A global row (company_id '') never carries a period: only a company decision may.
    assert "CONSTRAINT valid_global_scope CHECK company_id != '' OR period_key = ''" in block
    assert "    removed UInt8 DEFAULT 0," in block
    assert "    decided_by LowCardinality(String) DEFAULT ''," in block


def test_rule_table_hides_a_period() -> None:
    block = table_block(tables.RULE_TABLE)
    assert declared_columns(tables.RULE_TABLE) == list(tables.RULE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, period_key, action)" in block
    assert COMPANY_ID_CHECK in block
    assert "CONSTRAINT valid_action CHECK action IN ('hide')" in block
    assert tables.RULE_ACTIONS == ("hide",)
    assert tables.INACTIVE_REASONS == ("", "hidden", "withdrawn")
    assert tables.CHANGE_KINDS == ("created", "updated", "hidden", "withdrawn", "reactivated")
