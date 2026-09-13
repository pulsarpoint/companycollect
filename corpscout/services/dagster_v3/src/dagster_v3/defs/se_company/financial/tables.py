"""Table names and column tuples of the financial entity, pinned against migration 000401
through tests/test_se_company_financial_tables.py so this module and the deployed schema
cannot drift. Fourth entity on the basic-info shape (spec 2026-09-11 section 4): keyed by
company, accounting scope and period end; no normalized layer."""

from datetime import date

DATABASE = "corpscout"
SUGGESTION_TABLE = "se_company_financial_suggestion"
MAIN_TABLE = "se_company_financial"
HISTORY_TABLE = "se_company_financial_history"
PRECEDENCE_TABLE = "se_company_financial_precedence"
RULE_TABLE = "se_company_financial_rule"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"
QUALIFIED_RULE_TABLE = f"{DATABASE}.{RULE_TABLE}"

# This entity's own scratch-table prefix (basic_info/extract.py:scope_pages), so a financial
# scan's scratch table can never collide with another entity's.
SCRATCH_SCOPE_PREFIX = "corpscout._tmp_financial_scope_"

SOURCES: tuple[str, ...] = (
    "bolagsverket", "bolagsverket_comparative", "esef", "ratsit", "reviewer", "reviewer_draft",
)
SCOPES: tuple[str, ...] = ("standalone", "consolidated")
AMOUNT_SCALES: tuple[int, ...] = (1, 1000, 1000000)
RULE_ACTIONS: tuple[str, ...] = ("hide",)
INACTIVE_REASONS: tuple[str, ...] = ("", "hidden", "withdrawn")
CHANGE_KINDS: tuple[str, ...] = ("created", "updated", "hidden", "withdrawn", "reactivated")

# The twenty monetary fields (spec section 4), in DDL order. Each is a pair of columns on
# the suggestion row and a triple (original, usd, source) on the main row.
MONETARY_FIELDS: tuple[str, ...] = (
    "revenue",
    "operating_costs",
    "operating_result",
    "result_after_financial_items",
    "net_result",
    "ebitda",
    "total_assets",
    "fixed_assets",
    "current_assets",
    "cash_and_bank",
    "equity",
    "share_capital",
    "untaxed_reserves",
    "provisions",
    "liabilities",
    "long_term_liabilities",
    "current_liabilities",
    "personnel_expenses",
    "wages_and_salaries",
    "dividend",
)
# The period attributes the fold decides with the same precedence machinery as the money.
PERIOD_FIELDS: tuple[str, ...] = ("period_start", "fiscal_year", "period_months")
# Every field with a precedence map, in the order the main row lays them out: the period
# attributes, the currency (decided first, spec 6), the money, the employees.
FOLDED_FIELDS: tuple[str, ...] = (*PERIOD_FIELDS, "currency", *MONETARY_FIELDS, "employees")


def original_column(field: str) -> str:
    """The native-currency column of a monetary field."""
    return f"{field}_amount_original"


def usd_column(field: str) -> str:
    """The USD twin of a monetary field."""
    return f"{field}_amount_usd"


def source_column(field: str) -> str:
    """The main row's `_source` column of any folded field."""
    return f"{field}_source"


def period_key(scope: str, period_end: date) -> str:
    """The suggestion key of a period: '<scope>:<period_end>' (the DDL's CHECK)."""
    return f"{scope}:{period_end.isoformat()}"


MONETARY_SUGGESTION_COLUMNS: tuple[str, ...] = tuple(
    column for field in MONETARY_FIELDS for column in (original_column(field), usd_column(field))
)
MONETARY_MAIN_COLUMNS: tuple[str, ...] = tuple(
    column
    for field in MONETARY_FIELDS
    for column in (original_column(field), usd_column(field), source_column(field))
)

# What an extractor (or the backoffice) inserts: every column of the table, in DDL order.
SUGGESTION_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "period_key", "suggestion_id", "suggested_at", "source_record_uid",
    "scope", "period_end", "period_end_derived", "period_start", "fiscal_year", "period_months",
    "filing_fiscal_year", "currency", "amount_scale",
    *MONETARY_SUGGESTION_COLUMNS,
    "employees", "fx_rate_to_usd", "fx_rate_date", "fx_source",
    "decided_by", "note", "source_run_id", "extractor_version",
)
# The value columns a live suggestion row may carry; a tombstone has every one NULL.
SUGGESTION_VALUE_COLUMNS: tuple[str, ...] = (*MONETARY_SUGGESTION_COLUMNS, "employees")
# A live suggestion row carries at least one of them. Applied on both sides of the extractors'
# state hash (suggestions.py) and to the rows the fold reads (batch.py); defined here, below
# every other module of the package, so both can import it without a cycle.
LIVE_ROW_PREDICATE = "(" + " OR ".join(f"{column} IS NOT NULL" for column in SUGGESTION_VALUE_COLUMNS) + ")"

# The main row, in DDL order: each folded field followed by its _source (the money as
# original, usd, source).
MAIN_COLUMNS: tuple[str, ...] = (
    "company_id", "scope", "period_end", "period_key",
    "period_start", "period_start_source",
    "fiscal_year", "fiscal_year_source",
    "period_months", "period_months_source",
    "currency", "currency_source",
    *MONETARY_MAIN_COLUMNS,
    "employees", "employees_source",
    "sources", "active", "inactive_reason", "folded_at", "fold_version", "source_run_id",
)
HISTORY_COLUMNS: tuple[str, ...] = (
    *MAIN_COLUMNS, "changed_fields", "changed_at", "change_kind", "fold_run_id",
)
PRECEDENCE_COLUMNS: tuple[str, ...] = (
    "company_id", "period_key", "field", "source", "precedence", "removed", "decided_by",
    "note", "decided_at",
)
RULE_COLUMNS: tuple[str, ...] = (
    "company_id", "period_key", "action", "removed", "decided_by", "note", "decided_at",
)
