"""The financial entity's target for the shared suggestion extract helper and the shape of
its state-hash scan (spec 2026-09-11 section 7; state_scan.py holds the machinery).

A source delivers one row per company and period; the row's key is period_key
('<scope>:<period_end>', the DDL's CHECK derives it from the two columns beside it). The
change scan hashes everything a source delivers per company (every column but company_id
and source), so a changed figure, a changed date or a vanished period all select the
company once, and the page writes the live rows plus one tombstone per stored period the
source no longer delivers. A live row always carries at least one figure or an employee
count (every extractor filters empty rows out); a tombstone carries none, and copies scope
and period_end from the stored row so the CHECK on period_key still holds.

currency is NULL when a source names none, never '' (the DDL refuses ''); money on a row
without a currency can never win the fold (spec 6.2).
"""

from collections.abc import Mapping
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget
from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.assets import GROUP_NAME

# What a source supplies: every column of the table except the four the INSERT stamps.
STAMPED_COLUMNS: tuple[str, ...] = ("suggestion_id", "suggested_at", "source_run_id", "extractor_version")
FINANCIAL_SELECT_COLUMNS: tuple[str, ...] = tuple(
    column for column in tables.SUGGESTION_COLUMNS if column not in STAMPED_COLUMNS
)
# What a company's state hash covers: everything a source delivers except its own key.
FINANCIAL_STATE_COLUMNS: tuple[str, ...] = tuple(
    column for column in FINANCIAL_SELECT_COLUMNS if column not in ("company_id", "source")
)

# The nullable columns with the exact CAST each side of every UNION ALL uses, so a tombstone
# row and a source that does not deliver a column agree on the type.
NULLABLE_COLUMN_TYPES: dict[str, str] = {
    "period_start": "Date32", "fiscal_year": "UInt16", "period_months": "UInt16",
    "filing_fiscal_year": "UInt16", "currency": "String", "employees": "UInt64",
    "fx_rate_to_usd": "Decimal(38, 12)", "fx_rate_date": "Date32",
    **{column: "Decimal(38, 6)" for column in tables.MONETARY_SUGGESTION_COLUMNS},
}
NULL_SQL: Mapping[str, str] = {
    column: f"CAST(NULL AS Nullable({type_}))" for column, type_ in NULLABLE_COLUMN_TYPES.items()
}
MONEY_NULL_SQL: Mapping[str, str] = {
    column: NULL_SQL[column] for column in tables.MONETARY_SUGGESTION_COLUMNS
}

# A live row has at least one figure or an employee count; a tombstone has none.
LIVE_ROW_PREDICATE = "(" + " OR ".join(f"{column} IS NOT NULL" for column in tables.SUGGESTION_VALUE_COLUMNS) + ")"

# One clock read per statement (two now64() calls were measured to differ), hashed into the
# lineage id with the row's key.
FINANCIAL_WITH_SQL = "WITH (SELECT now64(3, 'UTC')) AS stamp\n"
FINANCIAL_TRAILING_SELECT_SQL = (
    "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', "
    "candidate.period_key, '\\n', toString(stamp))))) AS suggestion_id, stamp AS suggested_at, "
    "%(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version"
)

FINANCIAL_TARGET = SuggestionTarget(
    database=tables.DATABASE,
    table=tables.SUGGESTION_TABLE,
    insert_columns=(*FINANCIAL_SELECT_COLUMNS, *STAMPED_COLUMNS),
    select_columns=FINANCIAL_SELECT_COLUMNS,
    trailing_select_sql=FINANCIAL_TRAILING_SELECT_SQL,
    asset_prefix="se_company_financial_suggestions_",
    group_name=GROUP_NAME,
    scratch_prefix=tables.SCRATCH_SCOPE_PREFIX,
    with_sql=FINANCIAL_WITH_SQL,
)

# Every extractor joins the published universe; a company without a basic-info row is never
# suggested (the person extractors do the same).
UNIVERSE_JOIN_SQL = (
    "INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS universe\n"
    "    ON universe.company_id = {alias}.company_id"
)


def universe_join_sql(alias: str) -> str:
    return UNIVERSE_JOIN_SQL.format(alias=alias)


def period_months_sql(start: str, end: str) -> str:
    """The rounded month count between two Date32 expressions, NULL when either is NULL."""
    return (
        f"if({start} IS NULL OR {end} IS NULL, CAST(NULL AS Nullable(UInt16)), "
        f"toUInt16(round((dateDiff('day', {start}, {end}) + 1) / 30.4375)))"
    )


def tombstone_values(source: str) -> dict[str, str]:
    """A tombstone row for `source`: the key and the two columns its CHECK derives it from
    copied from the stored row, every value NULL, the defaults for the rest."""
    return {
        "company_id": "stored.company_id",
        "source": f"'{source}'",
        "period_key": "stored.period_key",
        "source_record_uid": "''",
        "scope": "stored.scope",
        "period_end": "stored.period_end",
        "period_end_derived": "toUInt8(0)",
        "amount_scale": "toUInt32(1)",
        "fx_source": "''",
        "decided_by": "''",
        "note": "''",
        **NULL_SQL,
    }


def scan_for(source: str) -> state_scan.StateScan:
    return state_scan.StateScan(
        target=FINANCIAL_TARGET,
        select_columns=FINANCIAL_SELECT_COLUMNS,
        state_columns=FINANCIAL_STATE_COLUMNS,
        key_column="period_key",
        live_row_predicate=LIVE_ROW_PREDICATE,
        tombstone_columns=("period_key", "scope", "period_end"),
        tombstone_values=tombstone_values(source),
    )


def live_select_sql(
    *, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = ""
) -> str:
    return state_scan.live_select_sql(
        scan_for("x"), columns=columns, from_sql=from_sql, where_sql=where_sql, with_sql=with_sql
    )


def financial_changed_scope_sql(*, source: str, live_sql: str) -> str:
    return state_scan.changed_scope_sql(scan_for(source), source=source, live_sql=live_sql)


def financial_select_sql(*, source: str, live_sql: str) -> str:
    return state_scan.select_sql(scan_for(source), source=source, live_sql=live_sql)


def define_financial_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return state_scan.define_scan_asset(scan_for("x"), **kwargs)
