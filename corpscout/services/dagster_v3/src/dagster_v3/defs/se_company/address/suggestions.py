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


def define_address_suggestion_asset(**kwargs: Any) -> dg.AssetsDefinition:
    return define_suggestion_asset(target=ADDRESS_TARGET, **kwargs)
