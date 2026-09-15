"""Stable source observations and per-slot withdrawal through the shared state scan."""

from collections.abc import Callable, Sequence

import dagster as dg

from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget, define_suggestion_asset
from dagster_v3.defs.se_company.domain import tables

STAMPED = ("suggestion_id", "suggested_at", "source_run_id", "extractor_version")
SELECT_COLUMNS = tuple(c for c in tables.SUGGESTION_COLUMNS if c not in STAMPED)
STATE_COLUMNS = tuple(c for c in SELECT_COLUMNS if c not in ("company_id", "source", "observed_at"))
TARGET = SuggestionTarget(
    database=tables.DATABASE, table=tables.SUGGESTION_TABLE,
    insert_columns=(*SELECT_COLUMNS, *STAMPED), select_columns=SELECT_COLUMNS,
    trailing_select_sql=(
        "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), "
        "'\\n', candidate.slot, '\\n', toString(stamp))))) AS suggestion_id, "
        "stamp AS suggested_at, %(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version"
    ),
    asset_prefix="se_company_domain_suggestions_", group_name=tables.GROUP_NAME,
    scratch_prefix="corpscout._tmp_domain_scope_", with_sql="WITH (SELECT now64(3, 'UTC')) AS stamp\n",
)


def source_scan(source: str) -> state_scan.StateScan:
    values = {column: "''" for column in SELECT_COLUMNS}
    values.update({
        "company_id": "stored.company_id", "source": f"'{source}'", "slot": "stored.slot",
        "root_domain": "stored.root_domain", "association": "'uncertain'",
        "is_primary": "toUInt8(0)", "confidence": "toFloat64(0)", "removed": "toUInt8(1)",
        "observed_at": "toDateTime64(0, 3, 'UTC')",
    })
    return state_scan.StateScan(
        target=TARGET, select_columns=SELECT_COLUMNS, state_columns=STATE_COLUMNS,
        key_column="slot", live_row_predicate="removed = 0",
        tombstone_columns=("slot", "root_domain"), tombstone_values=values,
    )


def define_domain_source(
    *, source: str, live_sql: Callable[..., str], deps: Sequence[dg.AssetKey], description: str,
) -> dg.AssetsDefinition:
    """Register a source's content comparison, scoped extraction and withdrawal SQL."""
    scan = source_scan(source)
    return define_suggestion_asset(
        source=source, extractor_version=f"{source}-domain-v1", target=TARGET,
        current_sql=f"SELECT company_id, max(observed_at) AS observed_at FROM ({live_sql(scoped=False)}) GROUP BY company_id",
        select_sql=state_scan.select_sql(scan, source=source, live_sql=live_sql(scoped=True)),
        changed_scope_override=state_scan.changed_scope_sql(scan, source=source, live_sql=live_sql(scoped=False)),
        deps=deps, description=description,
    )
