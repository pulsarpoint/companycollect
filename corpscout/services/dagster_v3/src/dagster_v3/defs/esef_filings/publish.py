"""ClickHouse exports for esef_filings: filings index, facts, and the gleif
entity-registry map.

The filings-index and facts exports reuse the shared DuckDB->ClickHouse
exporter (``defs/clickhouse/resolved.py``): assert-exists, stage table +
``EXCHANGE TABLES``, refuse-on-empty. Both source DuckDB tables store
loosely-typed staging columns (raw API strings; see assets.py's
``_FILINGS_INDEX_COLUMN_TYPES`` / ``_FACTS_COLUMN_TYPES`` docstrings) -- the
``column_expressions`` below cast them to the migration 000149 types
(Date32/DateTime64/Decimal128) and coalesce nullable text fields that land in
non-nullable ClickHouse ``String`` columns (CLAUDE.md: a non-nullable
``String``/``LowCardinality(String)`` column must get ``''``, never ``NULL``
-- the native driver ``.encode()``s every value and dies on ``None``).

The entity map is different: it never touches DuckDB. It is built entirely
IN ClickHouse from ``corpscout.gleif_lei_records``, scoped to LEIs appearing
in ``corpscout.esef_filings``, via a stage table + ``INSERT ... SELECT`` +
``EXCHANGE`` (``dagster_v3.contact_extraction.replace_table_from_select``) --
with an explicit refuse-on-empty check this module owns (that shared helper
has no such guard -- see its docstring), run *before* the stage table is even
created.
"""

from collections.abc import Callable
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.contact_extraction import replace_table_from_select
from dagster_v3.defs.clickhouse.resolved import (
    RESOLVED_DATABASE,
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.gleif.tables import GLEIF_LEI_RECORDS_TABLE

GLEIF_LEI_RECORDS_QUALIFIED_TABLE = f"{RESOLVED_DATABASE}.{GLEIF_LEI_RECORDS_TABLE}"

# DuckDB `esef_filings.filings_index`.period_end/date_added/processed_at are
# staged as raw API strings (see assets.py's _FILINGS_INDEX_COLUMN_TYPES) but
# migration 000149's corpscout.esef_filings columns are
# Date32/Date32/Nullable(DateTime64(6)) -- cast at export time.
# period_end/date_added are declared NON-nullable Date32 in the migration
# (period_end is in the ORDER BY, so it can never become Nullable) -- a
# missing/malformed source date must never reach clickhouse_driver as Python
# None (it crashes the Date32 writer: `AttributeError: 'NoneType' object has
# no attribute 'year'`). Coalesce to the sentinel epoch DATE '1970-01-01'
# instead, marking "source date missing" without crashing the insert.
# processed_at IS genuinely Nullable(DateTime64(6)) in the migration -- NULL
# there is semantically meaningful, so it's left a plain try_cast.
# json_url/package_url/report_url/viewer_url/package_sha256 can be NULL in
# DuckDB (not every filing has a JSON export, package hash, etc.) but their
# ClickHouse columns are non-nullable String -- coalesce to ''.
ESEF_FILINGS_COLUMN_EXPRESSIONS: dict[str, str] = {
    "period_end": "coalesce(try_cast(period_end as date), DATE '1970-01-01')",
    "date_added": "coalesce(try_cast(date_added as date), DATE '1970-01-01')",
    "processed_at": "try_cast(processed_at as timestamp)",
    "json_url": "coalesce(json_url, '')",
    "package_url": "coalesce(package_url, '')",
    "report_url": "coalesce(report_url, '')",
    "viewer_url": "coalesce(viewer_url, '')",
    "package_sha256": "coalesce(package_sha256, '')",
}

# DuckDB `esef_filings.facts.amount_original` is staged as TEXT (Task 4
# decision -- the Decimal value is already validated in Python at parse time
# in facts.py; staging keeps it as text to avoid DuckDB DECIMAL precision
# pitfalls, see assets.py's _FACTS_COLUMN_TYPES docstring). Migration
# 000149's corpscout.esef_facts column is Nullable(Decimal128(2)) -- cast
# explicitly so ClickHouse receives a real decimal, not a string.
# period_end/period_start/period_instant are staged as text too but are
# Date32/Nullable(Date32)/Nullable(Date32) in the migration.
# period_end is NON-nullable Date32 (it's in the ORDER BY) -- a fact whose
# period_end is missing, or a year-prefix-valid but otherwise malformed
# string that try_cast alone would NULL (e.g. "2022-99-99"), must sentinel
# to DATE '1970-01-01' rather than reach clickhouse_driver as None (crashes
# the Date32 writer). period_start/period_instant ARE genuinely
# Nullable(Date32) in the migration, so they stay plain try_cast.
# `period_end_year` (local-only partition-scope column) is deliberately
# absent here -- ESEF_FACTS_EXPORT_COLUMNS already excludes it.
ESEF_FACTS_COLUMN_EXPRESSIONS: dict[str, str] = {
    "period_end": "coalesce(try_cast(period_end as date), DATE '1970-01-01')",
    "period_start": "try_cast(period_start as date)",
    "period_instant": "try_cast(period_instant as date)",
    "amount_original": "try_cast(amount_original as decimal(38,2))",
}


def export_esef_filings_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Full-replace corpscout.esef_filings from DuckDB esef_filings.filings_index.

    Full replace is correct here: the index crawl is itself a full sweep of
    filings.xbrl.org each run (see assets.py's esef_filings_index_duckdb), so
    the DuckDB table always holds the complete current index.
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(tables.ESEF_FILINGS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting ESEF filings index to ClickHouse: table=%s",
            tables.QUALIFIED_ESEF_FILINGS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table=tables.FILINGS_INDEX_TABLE,
            clickhouse_database=tables.ESEF_DATABASE,
            clickhouse_table=tables.ESEF_FILINGS_TABLE,
            columns=tables.ESEF_FILINGS_EXPORT_COLUMNS,
            truncate=True,
            column_expressions=ESEF_FILINGS_COLUMN_EXPRESSIONS,
            log=log,
        )
    if log is not None:
        log("Finished ESEF filings index ClickHouse export: rows=%s", rows)
    return rows


def export_esef_facts_clickhouse(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Full-replace corpscout.esef_facts from DuckDB esef_filings.facts.

    Full replace is correct here -- unlike sweden_financial, one DuckDB file
    holds the entire ESEF facts dataset (no split-file/backfill hazard), and
    ~15-40M rows is well inside full-replace comfort (see task brief).
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(tables.ESEF_FACTS_TABLE,),
    )
    if log is not None:
        log(
            "Exporting ESEF facts to ClickHouse: table=%s",
            tables.QUALIFIED_ESEF_FACTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=tables.DLT_DATASET_NAME,
            duckdb_table=tables.FACTS_TABLE,
            clickhouse_database=tables.ESEF_DATABASE,
            clickhouse_table=tables.ESEF_FACTS_TABLE,
            columns=tables.ESEF_FACTS_EXPORT_COLUMNS,
            truncate=True,
            column_expressions=ESEF_FACTS_COLUMN_EXPRESSIONS,
            log=log,
        )
    if log is not None:
        log("Finished ESEF facts ClickHouse export: rows=%s", rows)
    return rows


# --- Entity map: built entirely in ClickHouse, no DuckDB input -------------

MATCH_SOURCE_GLEIF_REGISTERED_AS = "gleif_registered_as"


def _escape_clickhouse_string_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def build_esef_entity_registry_map_select(source_run_id: str) -> str:
    """Build the ``INSERT ... SELECT`` body for corpscout.esef_entity_registry_map.

    Scope: only LEIs that appear in corpscout.esef_filings -- this map is
    ESEF-driven, not a general LEI -> registry-id table. Some ESEF filings
    carry a non-LEI national id (e.g. UA filings using EDRPOU) in the `lei`
    field; those simply won't match a gleif_lei_records row and are dropped
    by design (unmatched kept out, not raised as an error).

    Normalization v1 (per country; extend as backoffice consumers appear):
      - FI: lowercase, strip spaces; 8 bare digits -> NNNNNNN-N (insert a
        dash before the last digit). Already-dashed FI ids pass through
        unchanged (they don't match the 8-bare-digits pattern).
      - SE: digits only (10-digit org numbers expected; not enforced here).
      - all other countries: trimmed raw passthrough.

    `ORDER BY lei, resolved_at DESC` + `LIMIT 1 BY lei` guards against
    un-merged ReplacingMergeTree duplicates in gleif_lei_records, even though
    that table is already one-row-per-lei post-merge (ORDER BY (lei)).
    """
    run_id_literal = _escape_clickhouse_string_literal(source_run_id)
    return f"""
        SELECT
            lei,
            coalesce(primary_country_iso2, '') AS country_iso2,
            coalesce(registered_as, '') AS registry_id_raw,
            multiIf(
                primary_country_iso2 = 'FI',
                if(
                    match(replaceAll(lowerUTF8(trim(registered_as)), ' ', ''), '^[0-9]{{8}}$'),
                    concat(
                        substring(replaceAll(lowerUTF8(trim(registered_as)), ' ', ''), 1, 7),
                        '-',
                        substring(replaceAll(lowerUTF8(trim(registered_as)), ' ', ''), 8, 1)
                    ),
                    replaceAll(lowerUTF8(trim(registered_as)), ' ', '')
                ),
                primary_country_iso2 = 'SE',
                replaceRegexpAll(registered_as, '[^0-9]', ''),
                trim(registered_as)
            ) AS registry_id,
            '{MATCH_SOURCE_GLEIF_REGISTERED_AS}' AS match_source,
            '{run_id_literal}' AS source_run_id
        FROM {GLEIF_LEI_RECORDS_QUALIFIED_TABLE}
        WHERE registered_as != ''
          AND lei IN (SELECT DISTINCT lei FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE})
        ORDER BY lei, resolved_at DESC
        LIMIT 1 BY lei
    """


def replace_esef_entity_registry_map_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    source_run_id: str,
    log: Callable[..., object] | None = None,
) -> int:
    """Atomically rebuild corpscout.esef_entity_registry_map from gleif_lei_records.

    Built entirely IN ClickHouse -- no DuckDB input. Refuses to touch the
    existing table if the scoped SELECT would insert 0 rows, checked BEFORE
    the stage table is even created (mirrors the DuckDB-side exporters'
    refuse-on-empty guard for this ClickHouse-native path, which
    ``replace_table_from_select`` itself does not provide).
    """
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(
            tables.ESEF_ENTITY_REGISTRY_MAP_TABLE,
            GLEIF_LEI_RECORDS_TABLE,
            tables.ESEF_FILINGS_TABLE,
        ),
    )
    select_sql = build_esef_entity_registry_map_select(source_run_id)
    with clickhouse.get_connection() as client:
        [(would_be_row_count,)] = client.execute(
            f"SELECT count() FROM ({select_sql}) AS scoped"
        )
        if int(would_be_row_count) == 0:
            raise ValueError(
                "ESEF entity registry map SELECT would insert 0 rows -- "
                "refusing to replace "
                f"{tables.QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE} "
                "(refuse-to-replace-on-empty)."
            )
        rows = replace_table_from_select(
            client,
            qualified_table=tables.QUALIFIED_ESEF_ENTITY_REGISTRY_MAP_TABLE,
            columns=tables.ESEF_ENTITY_MAP_EXPORT_COLUMNS,
            select_sql=select_sql,
            log=log,
        )
    if log is not None:
        log("Finished ESEF entity registry map ClickHouse rebuild: rows=%s", rows)
    return rows
