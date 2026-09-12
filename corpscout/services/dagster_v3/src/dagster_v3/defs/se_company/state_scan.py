"""The per-company state-hash change scan every entity without an observed_at watermark
shares: the person entity (slot-keyed) and the financial entity (period-keyed). Lifted from
person/suggestions.py on 2026-09-12 (financial slice 2); the person module keeps its names
and delegates here.

1. THE CHANGE SCAN IS A PER-COMPANY STATE HASH, not a timestamp. Each side -- what the
   source delivers now, and the company's live (non-tombstone) suggestion rows -- is
   reduced to one sha256 over its sorted, length-prefixed rows. A company is visited when
   only one side has it (never suggested, or gone from the source) or the two hashes
   differ; equal hashes mean there is nothing to write, so the scan converges after one
   pass. A timestamp would not: the source tables are rebuilt whole on every run.
2. TOMBSTONES ARE PER KEY (a slot, a period), not per company. The page select is
   `live UNION ALL tombstones`, the tombstones being the stored live keys the source no
   longer delivers -- a LEFT ANTI JOIN of the stored keys against the same `live` CTE.
   Writing the tombstone takes the row out of `live`, so the next scan sees equal hashes.

The `live` CTE is referenced twice but written once; each page select binds
%(company_ids)s exactly twice (the live CTE and the stored-key read), which is why the
entities page at 5,000 to 10,000 ids under ID_BOUND_QUERY_SETTINGS' 1 MiB max_query_size.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import dagster as dg

from dagster_v3.defs.se_company.basic_info.extract import SuggestionTarget, define_suggestion_asset


@dataclass(frozen=True)
class StateScan:
    """One entity's shape for the scan.

    select_columns: what a source supplies, in the target's select order (no id, stamp
    or run columns). state_columns: the subset hashed per company (everything but
    company_id and source). key_column: the per-row key inside a company (person: slot,
    financial: period_key). live_row_predicate: SQL over the suggestion table telling a
    live row from a tombstone. tombstone_columns: the stored columns a tombstone copies
    (the key, plus whatever the table's CHECKs derive it from). tombstone_values: the SQL
    for every select column of a tombstone row; it may name `stored.<column>` for any
    column in tombstone_columns.
    """

    target: SuggestionTarget
    select_columns: tuple[str, ...]
    state_columns: tuple[str, ...]
    key_column: str
    live_row_predicate: str
    tombstone_columns: tuple[str, ...]
    tombstone_values: Mapping[str, str]

    def __post_init__(self) -> None:
        missing = sorted(set(self.select_columns) - set(self.tombstone_values))
        extra = sorted(set(self.tombstone_values) - set(self.select_columns))
        if missing or extra:
            raise ValueError(f"tombstone_values: missing={missing} extra={extra}")
        if self.key_column not in self.tombstone_columns:
            raise ValueError(f"tombstone_columns must include the key column {self.key_column!r}")


def live_select_sql(
    scan: StateScan, *, columns: Mapping[str, str], from_sql: str, where_sql: str, with_sql: str = ""
) -> str:
    """One source's current rows, projected onto scan.select_columns in order.

    `columns` maps every select column to its SQL expression, so a source that forgets one
    fails at import instead of writing a short row into a UNION ALL whose other branch has
    them all.
    """
    missing = sorted(set(scan.select_columns) - set(columns))
    extra = sorted(set(columns) - set(scan.select_columns))
    if missing or extra:
        raise ValueError(f"live_select_sql columns: missing={missing} extra={extra}")
    projection = ",\n".join(f"    {columns[column]} AS {column}" for column in scan.select_columns)
    return f"{with_sql}SELECT\n{projection}\n{from_sql}\n{where_sql}"


def state_sql(scan: StateScan, alias: str) -> str:
    """One company's whole delivered state for one source, as a single hash.

    Length-prefixed fields so no value containing the separator can imitate another row;
    arraySort so the row order a scan happens to produce cannot change the hash;
    ifNull(toString(...), '') so a NULL and an empty string are told apart by the length
    prefix rather than by concat returning NULL.
    """
    fields: list[str] = []
    for column in scan.state_columns:
        value = f"ifNull(toString({alias}.{column}), '')"
        fields.append(f"toString(length({value})), ':', {value}")
    row = ", '\\n', ".join(fields)
    return f"lower(hex(SHA256(arrayStringConcat(arraySort(groupArray(concat({row}))), '\\n'))))"


def stored_live_sql(scan: StateScan, *, source: str, columns: Sequence[str], scoped: bool) -> str:
    """The company's live (non-tombstone) suggestion rows for one source.

    The source is a literal rather than %(source)s because `run_extractor` binds `source`
    only into the scope's params, never into the page select's.
    """
    scope = "\n    AND company_id IN %(company_ids)s" if scoped else ""
    return (
        f"SELECT company_id, {', '.join(columns)}\n"
        f"FROM {scan.target.qualified_table} FINAL\n"
        f"WHERE source = '{source}' AND {scan.live_row_predicate}{scope}"
    )


def changed_scope_sql(scan: StateScan, *, source: str, live_sql: str) -> str:
    """Companies whose delivered state differs from what is stored.

    Both sides are aliased `live` so the state expression is one identical text: any drift
    between them would re-extract every company on every run. Two aggregations and no join,
    so the result cannot depend on join_use_nulls. `count() < 2` catches a company only one
    side has -- new, or vanished from the source and due its tombstones.
    """
    state = state_sql(scan, "live")
    stored = stored_live_sql(scan, source=source, columns=scan.state_columns, scoped=False)
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


def select_sql(scan: StateScan, *, source: str, live_sql: str) -> str:
    """The page's rows: everything the source still delivers, plus one tombstone per stored
    live key it no longer delivers."""
    live_projection = ", ".join(f"live.{column} AS {column}" for column in scan.select_columns)
    tombstone_projection = ",\n".join(
        f"    {scan.tombstone_values[column]} AS {column}" for column in scan.select_columns
    )
    stored_keys = stored_live_sql(scan, source=source, columns=scan.tombstone_columns, scoped=True)
    key = scan.key_column
    alias = f"live_{key}s"  # live_slots for the person entity, live_period_keys for financials
    return (
        f"WITH live AS (\n{live_sql}\n)\n"
        f"SELECT {live_projection}\n"
        "FROM live\n"
        "UNION ALL\n"
        f"SELECT\n{tombstone_projection}\n"
        f"FROM (\n{stored_keys}\n) AS stored\n"
        f"LEFT ANTI JOIN (SELECT company_id, {key} FROM live) AS {alias}\n"
        f"    ON {alias}.company_id = stored.company_id AND {alias}.{key} = stored.{key}"
    )


def define_scan_asset(scan: StateScan, **kwargs: Any) -> dg.AssetsDefinition:
    return define_suggestion_asset(target=scan.target, **kwargs)
