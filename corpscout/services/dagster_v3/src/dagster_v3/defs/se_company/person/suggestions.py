"""The person entity's target for the shared suggestion extract helper (spec 2026-09-09
section 6), and the two SQL shapes every person source needs.

A person source delivers MANY rows per company and se_company_person_suggestion carries no
observed_at column, so neither of the address entity's two moves works here.

1. THE CHANGE SCAN IS A PER-COMPANY STATE HASH, not a timestamp. Each side -- what the
   source delivers now, and the company's live (non-tombstone) suggestion rows -- is
   reduced to one sha256 over its sorted, length-prefixed rows. A company is visited when
   only one side has it (never suggested, or gone from the source) or the two hashes
   differ; equal hashes mean there is nothing to write, so the scan converges after one
   pass. A timestamp would not: se_financial_report_signatories is rebuilt whole on every
   run and all 5.5M of its rows carry the same resolved_at, so `newer than last time` is
   either everything or nothing, and a signature line that disappeared has no row left to
   carry a stamp at all.
2. TOMBSTONES ARE PER SLOT, not per company. The page select is `live UNION ALL
   tombstones`, the tombstones being the stored live slots the source no longer delivers --
   a LEFT ANTI JOIN of the stored slots against the same `live` CTE. Writing the tombstone
   takes the row out of `live`, so the next scan sees equal hashes and stops.

The `live` CTE is referenced twice but written once, and that matters: `%(company_ids)s`
renders about 13 bytes per id and the helper executes every page under
ID_BOUND_QUERY_SETTINGS' max_query_size of 1 MiB. Each person page select binds the ids
exactly twice (the live CTE and the stored-slot read), which is why jobs.py pages at 10,000.
"""

from collections.abc import Mapping, Sequence
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget, define_suggestion_asset
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


def live_select_sql(
    *, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = ""
) -> str:
    """One source's current rows, projected onto PERSON_SELECT_COLUMNS in order.

    `columns` maps every one of the sixteen names to its SQL expression, so a source that
    forgets one fails at import instead of writing a short row into a UNION ALL whose other
    branch has all sixteen.
    """
    missing = sorted(set(PERSON_SELECT_COLUMNS) - set(columns))
    extra = sorted(set(columns) - set(PERSON_SELECT_COLUMNS))
    if missing or extra:
        raise ValueError(f"live_select_sql columns: missing={missing} extra={extra}")
    projection = ",\n".join(f"    {columns[column]} AS {column}" for column in PERSON_SELECT_COLUMNS)
    return f"{with_sql}SELECT\n{projection}\n{from_sql}\n{where_sql}"


def person_state_sql(alias: str) -> str:
    """One company's whole delivered state for one source, as a single hash.

    Length-prefixed fields (the shape corpscout's own person_profile_hash uses) so no value
    containing the separator can imitate another row; arraySort so the row order a scan
    happens to produce cannot change the hash; ifNull(toString(...), '') so a NULL and an
    empty string are told apart by the length prefix rather than by concat returning NULL.
    """
    fields: list[str] = []
    for column in PERSON_STATE_COLUMNS:
        value = f"ifNull(toString({alias}.{column}), '')"
        fields.append(f"toString(length({value})), ':', {value}")
    row = ", '\\n', ".join(fields)
    return f"lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(concat({row}))), '\\n'))))"


def stored_live_sql(*, source: str, columns: Sequence[str], scoped: bool) -> str:
    """The company's live (non-tombstone) suggestion rows for one source.

    The source is a literal rather than %(source)s because `run_extractor` binds `source`
    only into the scope's params, never into the page select's.
    """
    scope = "\n    AND company_id IN %(company_ids)s" if scoped else ""
    return (
        f"SELECT company_id, {', '.join(columns)}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n"
        f"WHERE source = '{source}' AND {LIVE_ROW_PREDICATE}{scope}"
    )


def person_changed_scope_sql(*, source: str, live_sql: str) -> str:
    """Companies whose delivered state differs from what is stored.

    Both sides are aliased `live` so the state expression is one identical text: any drift
    between them would re-extract every company on every run. Two aggregations and no join,
    so the result cannot depend on join_use_nulls. `count() < 2` catches a company only one
    side has -- new, or vanished from the source and due its tombstones.
    """
    state = person_state_sql("live")
    stored = stored_live_sql(source=source, columns=PERSON_STATE_COLUMNS, scoped=False)
    return (
        "SELECT company_id FROM (\n"
        f"    SELECT company_id, {state} AS state\n"
        f"    FROM ({live_sql}) AS live\n"
        "    GROUP BY company_id\n"
        "    UNION ALL\n"
        f"    SELECT company_id, {state} AS state\n"
        f"    FROM ({stored}) AS live\n"
        "    GROUP BY company_id\n"
        ") AS sides\n"
        "GROUP BY company_id\n"
        "HAVING count() < 2 OR uniqExact(state) > 1"
    )


def person_select_sql(*, source: str, live_sql: str) -> str:
    """The page's rows: everything the source still delivers, plus one tombstone per stored
    live slot it no longer delivers (spec 3.1: every person column NULL, `data` '{}')."""
    live_projection = ", ".join(f"live.{column} AS {column}" for column in PERSON_SELECT_COLUMNS)
    tombstone_values: dict[str, str] = {
        "company_id": "stored.company_id",
        "source": f"'{source}'",
        "slot": "stored.slot",
        "source_record_id": "''",
        "data": "'{}'",
        **NULL_SQL,
    }
    tombstone_projection = ",\n".join(
        f"    {tombstone_values[column]} AS {column}" for column in PERSON_SELECT_COLUMNS
    )
    stored_slots = stored_live_sql(source=source, columns=("slot",), scoped=True)
    return (
        f"WITH live AS (\n{live_sql}\n)\n"
        f"SELECT {live_projection}\n"
        "FROM live\n"
        "UNION ALL\n"
        f"SELECT\n{tombstone_projection}\n"
        f"FROM (\n{stored_slots}\n) AS stored\n"
        "LEFT ANTI JOIN (SELECT company_id, slot FROM live) AS live_slots\n"
        "    ON live_slots.company_id = stored.company_id AND live_slots.slot = stored.slot"
    )


def define_person_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return define_suggestion_asset(target=PERSON_TARGET, **kwargs)
