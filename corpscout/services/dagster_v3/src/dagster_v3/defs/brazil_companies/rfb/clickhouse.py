import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.brazil_companies.rfb import history, source, tables
from dagster_v3.defs.brazil_companies.rfb.duckdb_attach import (
    attached_read_only_database,
)
from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)

DLT_DATASET_NAME = tables.DLT_DATASET_NAME
CLICKHOUSE_DATE32_EXPORT_EXPRESSIONS = {
    "status_date": (
        "case when status_date between date '1900-01-01' and date '2299-12-31' "
        "then status_date else null end"
    ),
    "activity_start_date": (
        "case when activity_start_date between date '1900-01-01' and date '2299-12-31' "
        "then activity_start_date else null end"
    ),
}
CLICKHOUSE_COMPANY_RELATIONS_DATE32_EXPORT_EXPRESSIONS = {
    "relation_since": (
        "case when relation_since between date '1900-01-01' and date '2299-12-31' "
        "then relation_since else null end"
    ),
}


def export_brazil_comp_rfb_clickhouse_companies(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_companies with the normalized DuckDB companies table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_RFB_DATABASE,
        tables=(tables.BR_COMPANIES_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB companies to ClickHouse: table=%s",
            tables.QUALIFIED_BR_COMPANIES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANIES_TABLE,
            clickhouse_database=tables.BRAZIL_COMP_RFB_DATABASE,
            clickhouse_table=tables.BR_COMPANIES_TABLE_CH,
            columns=tables.BR_COMPANIES_EXPORT_COLUMNS,
            truncate=True,
            column_expressions=CLICKHOUSE_DATE32_EXPORT_EXPRESSIONS,
        )
    if log is not None:
        log("Finished Brazil RFB companies ClickHouse export: rows=%s", rows)
    return rows


def export_brazil_comp_rfb_clickhouse_establishments(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_establishments with the normalized DuckDB establishments table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_RFB_DATABASE,
        tables=(tables.BR_ESTABLISHMENTS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB establishments to ClickHouse: table=%s",
            tables.QUALIFIED_BR_ESTABLISHMENTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.ESTABLISHMENTS_TABLE,
            clickhouse_database=tables.BRAZIL_COMP_RFB_DATABASE,
            clickhouse_table=tables.BR_ESTABLISHMENTS_TABLE_CH,
            columns=tables.BR_ESTABLISHMENTS_EXPORT_COLUMNS,
            truncate=True,
            column_expressions=CLICKHOUSE_DATE32_EXPORT_EXPRESSIONS,
        )
    if log is not None:
        log("Finished Brazil RFB establishments ClickHouse export: rows=%s", rows)
    return rows


def _qualified(table: str) -> str:
    return f"{tables.BRAZIL_COMP_RFB_DATABASE}.{table}"


# ClickHouse types for the snapshot stage table, keyed by column name so the
# DDL below is built FROM tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
# instead of hand-listing the columns a second time. A previous version of
# this export hand-listed 14 columns for that DDL and silently dropped
# resolved_at, which then landed at the DateTime64 type default (the epoch)
# on every row instead of failing loudly. Building the DDL from the same
# tuple the exporter ships columns from means a column added to that tuple
# without a matching entry here fails immediately with a KeyError instead of
# silently defaulting.
_SNAPSHOT_STAGE_COLUMN_TYPES: dict[str, str] = {
    "country_iso2": "LowCardinality(String)",
    "source_slug": "LowCardinality(String)",
    "cnpj_basico": "String",
    "related_entity_kind": "LowCardinality(String)",
    "related_tax_id": "String",
    "relation_code": "LowCardinality(String)",
    "relation_since_key": "String",
    "related_name": "String",
    "related_country": "String",
    "age_band": "LowCardinality(String)",
    "representative_tax_id": "String",
    "representative_name": "String",
    "representative_code": "LowCardinality(String)",
    "relation_since": "Nullable(Date32)",
    "resolved_at": "DateTime64(3, 'UTC')",
}


def _snapshot_stage_ddl(qualified_snapshot: str) -> str:
    column_lines = ",\n        ".join(
        f"{column} {_SNAPSHOT_STAGE_COLUMN_TYPES[column]}"
        for column in tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS
    )
    return f"""
        CREATE TABLE {qualified_snapshot}
        (
        {column_lines}
        )
        ENGINE = MergeTree
        ORDER BY (cnpj_basico, related_tax_id, relation_code)
        """


_SNAPSHOT_MANIFEST_ATTACH_ALIAS = "snapshot_manifest_db"


def _count_socios_manifest_parts(
    duckdb_connection: Any,
    snapshot_manifest_database_path: str | Path,
) -> int:
    """How many socios ZIP parts this month's manifest actually recorded.

    Reads tables.SNAPSHOT_FILES_TABLE (one row per downloaded file, with its
    family) from the manifest DuckDB file for this partition -- a SEPARATE
    file from `duckdb_connection` (the relations stage), attached read-only
    the same way relations.py attaches the socios raw DB. Feeds
    history.assert_snapshot_part_count_is_not_decreasing; see that function's
    docstring for why an exact count catches a failure
    MIN_SNAPSHOT_EDGE_RATIO cannot.
    """
    with attached_read_only_database(
        duckdb_connection,
        database_path=snapshot_manifest_database_path,
        alias=_SNAPSHOT_MANIFEST_ATTACH_ALIAS,
    ) as manifest_alias:
        return int(
            duckdb_connection.execute(
                f"select count(*) from {manifest_alias}.{DLT_DATASET_NAME}."
                f"{tables.SNAPSHOT_FILES_TABLE} where family = 'socios'"
            ).fetchone()[0]
        )


def export_brazil_comp_rfb_clickhouse_company_relations(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    snapshot_year_month: str,
    source_run_id: str,
    snapshot_manifest_database_path: str | Path,
    log: Callable[..., object] | None = None,
) -> dict[str, int]:
    """Merge this month's snapshot into the connection history.

    Not a replace: the previous months ARE the history. br_company_relations
    holds one row per SPELL (see brazil_rfb_socios_history-design.md); a plain
    truncate-and-replace would destroy every month but the latest, which is
    exactly the behaviour this function removes.

    `snapshot_manifest_database_path` is the partition's manifest DuckDB file
    (tables.SNAPSHOT_FILES_TABLE, one row per downloaded archive with its
    family) -- a SEPARATE file from `duckdb_connection`. It exists to count
    this month's socios ZIP parts exactly; see
    history.assert_snapshot_part_count_is_not_decreasing.

    Returns edges_in_snapshot / spells_opened / spells_closed / spells_total,
    the same counts written to the ledger (br_company_relations_snapshots).
    """
    # Validated ONCE, here, and reassigned: every later use of
    # snapshot_year_month in this function (the snapshot_date stamp, the
    # merge SQL, the ledger row) then reads the clean 'YYYY-MM' value,
    # instead of each call site having to separately reason about whether the
    # caller's raw string was ever checked. See history._validate_snapshot_stamp's
    # docstring for what an unvalidated value can do once interpolated into SQL.
    snapshot_year_month = source.validate_snapshot_year_month(snapshot_year_month)
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_RFB_DATABASE,
        tables=(
            tables.BR_COMPANY_RELATIONS_TABLE_CH,
            tables.BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE_CH,
        ),
    )
    if log is not None:
        log(
            "Merging Brazil RFB company relations snapshot into ClickHouse: "
            "table=%s snapshot_year_month=%s",
            tables.QUALIFIED_BR_COMPANY_RELATIONS_TABLE,
            snapshot_year_month,
        )
    snapshot_stage = f"_tmp_relations_snapshot_{uuid.uuid4().hex}"
    merge_stage = f"_tmp_{tables.BR_COMPANY_RELATIONS_TABLE_CH}_{uuid.uuid4().hex}"
    qualified_snapshot = _qualified(snapshot_stage)
    qualified_merge = _qualified(merge_stage)
    qualified_target = tables.QUALIFIED_BR_COMPANY_RELATIONS_TABLE
    qualified_ledger = tables.QUALIFIED_BR_COMPANY_RELATIONS_SNAPSHOTS_TABLE
    snapshot_date = f"{snapshot_year_month}-01"

    with clickhouse.get_connection() as client:
        ledger_rows = client.execute(
            "SELECT snapshot_year_month, edges_in_snapshot, socios_part_count "
            f"FROM {qualified_ledger}"
        )
        merged_months = [str(row[0]) for row in ledger_rows]
        edges_by_month = {str(row[0]): int(row[1]) for row in ledger_rows}
        parts_by_month = {str(row[0]): int(row[2]) for row in ledger_rows}
        # Before anything is written: refuse an out-of-order or repeated
        # month. See history.assert_snapshot_is_newer.
        history.assert_snapshot_is_newer(snapshot_year_month, merged_months)
        previous_edges_in_snapshot = (
            edges_by_month[max(merged_months)] if merged_months else None
        )
        previous_socios_part_count = (
            parts_by_month[max(merged_months)] if merged_months else None
        )

        # Exact guard, and deliberately run BEFORE any ClickHouse write below
        # (unlike the edge-count ratio guard, which needs the snapshot staged
        # in ClickHouse first): the manifest part count is available purely
        # from DuckDB, so a bad month is refused as early as possible. See
        # history.assert_snapshot_part_count_is_not_decreasing for why this
        # exact comparison catches a single missing socios part that
        # MIN_SNAPSHOT_EDGE_RATIO below cannot.
        socios_part_count = _count_socios_manifest_parts(
            duckdb_connection, snapshot_manifest_database_path
        )
        history.assert_snapshot_part_count_is_not_decreasing(
            socios_part_count, previous_socios_part_count
        )

        client.execute(_snapshot_stage_ddl(qualified_snapshot))
        try:
            edges_in_snapshot = export_duckdb_connection_table_to_clickhouse(
                duckdb_connection=duckdb_connection,
                clickhouse_client=client,
                duckdb_schema=DLT_DATASET_NAME,
                duckdb_table=tables.COMPANY_RELATIONS_TABLE,
                clickhouse_database=tables.BRAZIL_COMP_RFB_DATABASE,
                clickhouse_table=snapshot_stage,
                columns=tables.BR_COMPANY_RELATIONS_SNAPSHOT_INPUT_COLUMNS,
                truncate=False,
                column_expressions=(
                    CLICKHOUSE_COMPANY_RELATIONS_DATE32_EXPORT_EXPRESSIONS
                ),
            )
            # MINOR: truncate=False means the shared exporter's own
            # empty-input refusal never runs -- that check lives inside
            # export_duckdb_connection_table_to_clickhouse's `truncate`
            # branch (resolved.py), which this call does not take. Make the
            # guarantee local instead of relying on relations.py's own
            # "refuse a 0-row edge table" check in another file: an empty
            # snapshot merged into an empty history would publish an empty
            # table AND record edges_in_snapshot=0 in the ledger, after which
            # assert_snapshot_edge_count_is_plausible's own
            # previous_edges_in_snapshot <= 0 no-op leaves the NEXT month
            # completely unguarded.
            if int(edges_in_snapshot) == 0:
                raise ValueError(
                    "Brazil RFB connection snapshot has 0 edges; refusing to "
                    "merge an empty snapshot into the connection history"
                )
            # Refuse a short snapshot BEFORE it reaches the merge: RFB ships
            # socios as ~10 ZIP parts, and a truncated download produces a
            # well-formed but incomplete snapshot that relations.py's
            # empty-input guard cannot see. Fed into the merge, every
            # partner missing from the incomplete parts would read as "gone
            # by this snapshot" and get closed -- silently and permanently,
            # since the merge folds its own output back into itself every
            # run. See history.assert_snapshot_edge_count_is_plausible for
            # the threshold and its rationale, and
            # history.assert_snapshot_part_count_is_not_decreasing above for
            # the exact guard this ratio alone cannot provide.
            history.assert_snapshot_edge_count_is_plausible(
                int(edges_in_snapshot), previous_edges_in_snapshot
            )

            client.execute(f"CREATE TABLE {qualified_merge} AS {qualified_target}")
            client.execute(
                f"INSERT INTO {qualified_merge} "
                + history.build_merge_select_sql(
                    state_table=qualified_target,
                    snapshot_table=qualified_snapshot,
                    snapshot_year_month=snapshot_year_month,
                    snapshot_date=snapshot_date,
                ),
                settings={
                    # MANDATORY, not tuning: ClickHouse defaults to
                    # join_use_nulls=0, which fills the unmatched side of the
                    # FULL JOIN with type defaults ('') instead of NULL. On a
                    # real ClickHouse 26.5 run this did NOT error -- it wrote
                    # country_iso2, source_slug, cnpj_basico,
                    # related_entity_kind, related_tax_id, relation_code and
                    # relation_since_key as '' on every row, collapsing the
                    # entire spell identity onto one sort key. See
                    # history.build_merge_select_sql's docstring for the full
                    # failure mode. A bare SELECT cannot carry its own
                    # SETTINGS clause, so this dict is the ONLY place this is
                    # enforced -- do not move this INSERT without it.
                    "join_use_nulls": 1,
                    # No other settings here on purpose. history.py measured
                    # this query at ~3.2 GiB at 25M rows, plateauing rather
                    # than climbing, and found that manually forcing
                    # max_bytes_before_external_group_by LOWER made a 25M-row
                    # run WORSE (moved the failure into the join instead of
                    # avoiding it) -- ClickHouse 26.5's own default spill
                    # handling already outperforms a hand-tuned one here. Do
                    # not add settings that would read the merge's own
                    # output a second time (e.g. re-scanning qualified_merge
                    # to recompute these counts) -- that doubles the memory
                    # this budget was measured against.
                },
            )
            [(spells_total, spells_opened, spells_closed)] = client.execute(
                f"""
                SELECT
                    count(),
                    countIf(first_seen_snapshot = '{snapshot_year_month}'),
                    countIf(
                        is_current = 0
                        AND last_seen_snapshot != '{snapshot_year_month}'
                        AND end_at = toDate('{snapshot_date}')
                    )
                FROM {qualified_merge}
                """
            )
            if int(edges_in_snapshot) > 0 and int(spells_total) == 0:
                raise ValueError(
                    "Brazil RFB connection merge produced no spells from a "
                    f"non-empty snapshot ({edges_in_snapshot} edges); refusing "
                    "to replace the published history"
                )
            # Ledger BEFORE publish -- deliberately the opposite of the
            # intuitive "record what you published" order, and load-bearing:
            # do not move this back below EXCHANGE TABLES.
            #
            # Proven on a real ClickHouse by forcing the ledger INSERT to
            # raise with the OLD ordering (EXCHANGE first): the month was
            # published (new spells present, last_seen_snapshot advanced),
            # the ledger still listed only the prior month, and the `finally`
            # below had already dropped the pre-merge copy -- so there was no
            # rollback path. assert_snapshot_is_newer then happily let that
            # same month run again next time, silently bumping `observations`
            # a second time. Published-but-unrecorded is invisible and
            # permanent -- exactly the "a gap is invisible rather than
            # detectable" failure the ledger exists to prevent (see this
            # module's design doc, section 5).
            #
            # Inverting the order does not remove the failure mode, it
            # inverts it into a strictly better one: if EXCHANGE TABLES now
            # fails, the month is recorded but NOT published, and the next
            # run's assert_snapshot_is_newer refuses to re-run it with
            # "already merged" -- a loud failure that needs a human, instead
            # of a silent one that corrupts observations forever. A stuck
            # "recorded but unpublished" month is a recoverable, visible
            # state; a published-but-unrecorded one is neither.
            client.execute(
                f"INSERT INTO {qualified_ledger} "
                f"({', '.join(tables.BR_COMPANY_RELATIONS_SNAPSHOT_COLUMNS)}) VALUES",
                [
                    (
                        snapshot_year_month,
                        datetime.now(UTC),
                        source_run_id,
                        int(edges_in_snapshot),
                        int(spells_opened),
                        int(spells_closed),
                        int(spells_total),
                        int(socios_part_count),
                    )
                ],
            )
            client.execute(
                f"EXCHANGE TABLES {qualified_merge} AND {qualified_target}"
            )
            # Verify what got published actually matches what the ledger row
            # above just claimed, instead of trusting that EXCHANGE TABLES
            # did the right thing. This is what closes the loop on the
            # ledger-before-EXCHANGE ordering above: that ordering turns a
            # failed EXCHANGE into a loud, recoverable "recorded but
            # unpublished" state (an exception here propagates and the
            # ledger row stays orphaned until a human clears it -- see
            # history.assert_snapshot_is_newer's "already merged" message
            # for that recovery). This check exists for the OTHER case: a
            # "successful" EXCHANGE (no exception) whose result still does
            # not match the count computed just before it, which would
            # otherwise be silently trusted as correct simply because no
            # exception was raised.
            [(published_row_count,)] = client.execute(
                f"SELECT count() FROM {qualified_target}"
            )
            if int(published_row_count) != int(spells_total):
                raise ValueError(
                    "Brazil RFB connection history: after EXCHANGE TABLES "
                    f"for {snapshot_year_month}, {qualified_target} has "
                    f"{published_row_count} rows but the ledger just "
                    f"recorded spells_total={spells_total} for this month. "
                    "The publish and the ledger have diverged for this run "
                    "-- do not trust either without investigating."
                )
        finally:
            client.execute(f"DROP TABLE IF EXISTS {qualified_merge}")
            client.execute(f"DROP TABLE IF EXISTS {qualified_snapshot}")

    counts = {
        "edges_in_snapshot": int(edges_in_snapshot),
        "spells_opened": int(spells_opened),
        "spells_closed": int(spells_closed),
        "spells_total": int(spells_total),
    }
    if log is not None:
        log("Merged Brazil RFB connection history: counts=%s", counts)
    return counts


def export_brazil_comp_rfb_clickhouse_company_contacts(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_company_contacts with the canonical DuckDB contacts stage."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_RFB_DATABASE,
        tables=(tables.BR_COMPANY_CONTACTS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB company contacts to ClickHouse: table=%s",
            tables.QUALIFIED_BR_COMPANY_CONTACTS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANY_CONTACTS_STAGE_TABLE,
            clickhouse_database=tables.BRAZIL_COMP_RFB_DATABASE,
            clickhouse_table=tables.BR_COMPANY_CONTACTS_TABLE_CH,
            columns=tables.BR_COMPANY_CONTACTS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil RFB company contacts ClickHouse export: rows=%s", rows)
    return rows


def export_brazil_comp_rfb_clickhouse_company_domains(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_company_domains with the canonical DuckDB domains stage."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_RFB_DATABASE,
        tables=(tables.BR_COMPANY_DOMAINS_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB company domains to ClickHouse: table=%s",
            tables.QUALIFIED_BR_COMPANY_DOMAINS_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.COMPANY_DOMAINS_STAGE_TABLE,
            clickhouse_database=tables.BRAZIL_COMP_RFB_DATABASE,
            clickhouse_table=tables.BR_COMPANY_DOMAINS_TABLE_CH,
            columns=tables.BR_COMPANY_DOMAINS_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil RFB company domains ClickHouse export: rows=%s", rows)
    return rows


def export_brazil_comp_rfb_clickhouse_websites(
    *,
    duckdb_connection: Any,
    clickhouse: ClickhouseResource,
    log: Callable[..., object] | None = None,
) -> int:
    """Replace corpscout.br_websites with the DuckDB domain feeder table."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.BRAZIL_COMP_RFB_DATABASE,
        tables=(tables.BR_WEBSITES_TABLE_CH,),
    )
    if log is not None:
        log(
            "Exporting Brazil RFB websites to ClickHouse: table=%s",
            tables.QUALIFIED_BR_WEBSITES_TABLE,
        )
    with clickhouse.get_connection() as client:
        rows = export_duckdb_connection_table_to_clickhouse(
            duckdb_connection=duckdb_connection,
            clickhouse_client=client,
            duckdb_schema=DLT_DATASET_NAME,
            duckdb_table=tables.WEBSITES_TABLE,
            clickhouse_database=tables.BRAZIL_COMP_RFB_DATABASE,
            clickhouse_table=tables.BR_WEBSITES_TABLE_CH,
            columns=tables.BR_WEBSITES_EXPORT_COLUMNS,
            truncate=True,
        )
    if log is not None:
        log("Finished Brazil RFB websites ClickHouse export: rows=%s", rows)
    return rows
