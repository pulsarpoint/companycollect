"""The person entity's target for the shared suggestion extract helper (spec 2026-09-09
section 6), and the two SQL shapes every person source needs.

A person source delivers MANY rows per company and se_company_person_suggestion carries no
observed_at column, so the change scan is the per-company state hash and tombstones are per
slot. Since financial slice 2 (2026-09-12) that machinery lives in
dagster_v3.defs.se_company.state_scan; this module keeps its public names (the four person
extractors and their tests import them) and delegates. The rendered SQL is byte-identical to
what this module rendered before the lift.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.assets import GROUP_NAME

# The sixteen columns a source supplies: every suggestion column except the two the INSERT
# stamps. There is no source_run_id and no extractor_version on this table.
PERSON_SELECT_COLUMNS: tuple[str, ...] = tuple(
    column for column in tables.SUGGESTION_COLUMNS if column not in ("suggestion_id", "suggested_at")
)
# What a company's state hash covers: everything a source delivers except its own key.
PERSON_STATE_COLUMNS: tuple[str, ...] = tuple(
    column for column in PERSON_SELECT_COLUMNS if column not in ("company_id", "source")
)

# The eleven nullable person columns, with the exact CAST each side of every UNION ALL uses,
# so a tombstone row and a source that does not deliver a column agree on the type.
NULLABLE_PERSON_COLUMNS: tuple[tuple[str, str], ...] = (
    ("full_name", "String"), ("first_name", "String"), ("last_name", "String"),
    ("birth_year", "UInt16"), ("wikidata_id", "String"), ("role_original", "String"),
    ("role_key", "String"), ("fiscal_year", "UInt16"), ("role_from", "Date"),
    ("role_to", "Date"), ("document_ref", "String"),
)
NULL_SQL: Mapping[str, str] = {
    column: f"CAST(NULL AS Nullable({type_}))" for column, type_ in NULLABLE_PERSON_COLUMNS
}

# A live row always carries a name (every extractor filters nameless source rows out); a
# tombstone carries none. Testing the three name columns is the cheap form of `every person
# column is NULL`, and it is what keeps a tombstone out of both sides of the state hash.
LIVE_ROW_PREDICATE = "(full_name IS NOT NULL OR first_name IS NOT NULL OR last_name IS NOT NULL)"

# Two now64() occurrences in one statement are not guaranteed the same instant (measured at
# about 0.5% of executions on the address entity, and when they differ every row of the page
# is affected). A scalar subquery binds it once; toString(DateTime64(3, 'UTC')) prints
# YYYY-MM-DD HH:MM:SS.mmm, which is what the id hashes.
PERSON_WITH_SQL = "WITH (SELECT now64(3, 'UTC')) AS stamp\n"
PERSON_TRAILING_SELECT_SQL = (
    "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', "
    "candidate.slot, '\\n', toString(stamp))))) AS suggestion_id, stamp AS suggested_at"
)

PERSON_TARGET = SuggestionTarget(
    database=tables.DATABASE,
    table=tables.SUGGESTION_TABLE,
    insert_columns=(*PERSON_SELECT_COLUMNS, "suggestion_id", "suggested_at"),
    select_columns=PERSON_SELECT_COLUMNS,
    trailing_select_sql=PERSON_TRAILING_SELECT_SQL,
    asset_prefix="se_company_person_suggestions_",
    group_name=GROUP_NAME,
    scratch_prefix=tables.SCRATCH_SCOPE_PREFIX,
    with_sql=PERSON_WITH_SQL,
)

PERSON_SCAN = state_scan.StateScan(
    target=PERSON_TARGET,
    select_columns=PERSON_SELECT_COLUMNS,
    state_columns=PERSON_STATE_COLUMNS,
    key_column="slot",
    live_row_predicate=LIVE_ROW_PREDICATE,
    tombstone_columns=("slot",),
    tombstone_values={
        "company_id": "stored.company_id",
        "slot": "stored.slot",
        "source_record_id": "''",
        "data": "'{}'",
        **NULL_SQL,
        # `source` is filled per source below: the scan is shared by four extractors.
        "source": "__SOURCE__",
    },
)


def _scan_for(source: str) -> state_scan.StateScan:
    values = dict(PERSON_SCAN.tombstone_values)
    values["source"] = f"'{source}'"
    return state_scan.StateScan(
        target=PERSON_SCAN.target, select_columns=PERSON_SCAN.select_columns,
        state_columns=PERSON_SCAN.state_columns, key_column=PERSON_SCAN.key_column,
        live_row_predicate=PERSON_SCAN.live_row_predicate,
        tombstone_columns=PERSON_SCAN.tombstone_columns, tombstone_values=values,
    )


def live_select_sql(
    *, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = ""
) -> str:
    """One source's current rows, projected onto PERSON_SELECT_COLUMNS in order."""
    return state_scan.live_select_sql(
        PERSON_SCAN, columns=columns, from_sql=from_sql, where_sql=where_sql, with_sql=with_sql
    )


def person_state_sql(alias: str) -> str:
    """One company's whole delivered state for one source, as a single hash."""
    return state_scan.state_sql(PERSON_SCAN, alias)


def stored_live_sql(*, source: str, columns: Sequence[str], scoped: bool) -> str:
    """The company's live (non-tombstone) suggestion rows for one source."""
    return state_scan.stored_live_sql(PERSON_SCAN, source=source, columns=columns, scoped=scoped)


def person_changed_scope_sql(*, source: str, live_sql: str) -> str:
    """Companies whose delivered state differs from what is stored."""
    return state_scan.changed_scope_sql(PERSON_SCAN, source=source, live_sql=live_sql)


def person_select_sql(*, source: str, live_sql: str) -> str:
    """The page's rows: everything the source still delivers, plus one tombstone per stored
    live slot it no longer delivers (spec 3.1: every person column NULL, `data` '{}')."""
    return state_scan.select_sql(_scan_for(source), source=source, live_sql=live_sql)


def define_person_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return state_scan.define_scan_asset(PERSON_SCAN, **kwargs)
