"""The address entity's target for the shared suggestion extract helper (spec section 7):
the raw table, its thirteen source-provided columns, and the INSERT expressions that stamp
suggestion_id and suggested_at from one bound stamp."""

from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.assets import GROUP_NAME
from dagster_v3.defs.se_company.address.normalize import SCRATCH_SCOPE_PREFIX
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget, define_suggestion_asset

ADDRESS_SELECT_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "source_record_uid", "observed_at", "kind", *tables.RAW_ADDRESS_COLUMNS,
)

# Two now64() occurrences in one statement are not guaranteed the same instant -- measured
# about 0.5% of executions differ, and when they do every row of the statement is affected,
# desynchronizing suggestion_id from suggested_at for a whole page. A scalar subquery binds
# the stamp once (0 mismatches measured); the stamp prints as YYYY-MM-DD HH:MM:SS.mmm, the
# format normalize.py hashes for normalized_id.
ADDRESS_WITH_SQL = "WITH (SELECT now64(3, 'UTC')) AS stamp\n"

ADDRESS_TRAILING_SELECT_SQL = (
    "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), '\\n', candidate.slot, '\\n', "
    "toString(stamp))))) AS suggestion_id, "
    "CAST(NULL AS Nullable(String)) AS decided_by, CAST(NULL AS Nullable(String)) AS note, "
    "CAST(NULL AS Nullable(FixedString(64))) AS replaces_key, "
    "stamp AS suggested_at, %(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version"
)

ADDRESS_TARGET = SuggestionTarget(
    database=tables.DATABASE,
    table=tables.SUGGESTION_TABLE,
    insert_columns=(
        *ADDRESS_SELECT_COLUMNS,
        "suggestion_id", "decided_by", "note", "replaces_key", "suggested_at", "source_run_id", "extractor_version",
    ),
    select_columns=ADDRESS_SELECT_COLUMNS,
    trailing_select_sql=ADDRESS_TRAILING_SELECT_SQL,
    asset_prefix="se_company_address_suggestions_",
    group_name=GROUP_NAME,
    scratch_prefix=SCRATCH_SCOPE_PREFIX,
    with_sql=ADDRESS_WITH_SQL,
)


# A live address row always carries a street; a tombstone carries none. This is the address
# entity's LIVE_ROW_PREDICATE (person/suggestions.py has the same idea over its three name
# columns), and it is what keeps an already-tombstoned slot out of the tombstone branch.
ADDRESS_LIVE_ROW_PREDICATE = "street_address IS NOT NULL"

# What a tombstone copies from the stored row: the slot it retires, and the kind it keeps
# (spec 2026-09-11 section 5.3).
ADDRESS_TOMBSTONE_COLUMNS: tuple[str, ...] = ("slot", "kind")


def address_select_sql(*, live_sql: str, source: str) -> str:
    """The page's rows: everything the source still delivers, plus one tombstone per stored
    live slot it no longer delivers (spec 2026-09-11 section 5.3).

    The shape is person/suggestions.py::person_select_sql's -- a `live` CTE written once and
    read twice, a LEFT ANTI JOIN of the stored live slots against it -- with one addition the
    person entity does not need. The address suggestion table HAS an observed_at column and
    the change scan is that watermark (basic_info/extract.py::changed_scope_sql compares the
    source's current stamp with argMax(observed_at, suggested_at) over the stored rows). Every
    row a page writes shares one suggested_at, so a tombstone stamped with the stored row's
    OLDER observed_at could win that argMax out of the tie and re-select the company on every
    run for ever. The tombstone therefore takes the page's own observed_at for its company,
    which `live` already carries on every row of that company.

    `source` is a literal rather than %(source)s because run_extractor binds `source` only
    into the scope query's params, never into the page select's.

    A source with one slot per company (SCB, Bolagsverket) needs none of this: it tombstones
    by nulling its single row in place, on `has_company = 0`.
    """
    live_projection = ", ".join(f"live.{column} AS {column}" for column in ADDRESS_SELECT_COLUMNS)
    tombstone: dict[str, str] = {
        "company_id": "stored.company_id",
        "source": f"'{source}'",
        "slot": "stored.slot",
        # A tombstone names no source record, exactly as the person entity's does.
        "source_record_uid": "''",
        "observed_at": "report.observed_at",
        "kind": "stored.kind",
        **{column: "CAST(NULL AS Nullable(String))" for column in tables.RAW_ADDRESS_COLUMNS},
    }
    tombstone_projection = ",\n".join(
        f"    {tombstone[column]} AS {column}" for column in ADDRESS_SELECT_COLUMNS
    )
    stored_keys = (
        f"SELECT company_id, {', '.join(ADDRESS_TOMBSTONE_COLUMNS)}\n"
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL\n"
        f"WHERE source = '{source}' AND {ADDRESS_LIVE_ROW_PREDICATE} "
        "AND company_id IN %(company_ids)s"
    )
    vanished = (
        "SELECT stored.company_id AS company_id, stored.slot AS slot, stored.kind AS kind\n"
        f"FROM (\n{stored_keys}\n) AS stored\n"
        "LEFT ANTI JOIN (SELECT company_id, slot FROM live) AS live_slots\n"
        "    ON live_slots.company_id = stored.company_id AND live_slots.slot = stored.slot"
    )
    return (
        f"WITH live AS (\n{live_sql}\n)\n"
        f"SELECT {live_projection}\n"
        "FROM live\n"
        "UNION ALL\n"
        f"SELECT\n{tombstone_projection}\n"
        f"FROM (\n{vanished}\n) AS stored\n"
        "INNER JOIN (\n"
        "SELECT company_id, max(observed_at) AS observed_at FROM live GROUP BY company_id\n"
        ") AS report ON report.company_id = stored.company_id"
    )


def define_address_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return define_suggestion_asset(target=ADDRESS_TARGET, **kwargs)
