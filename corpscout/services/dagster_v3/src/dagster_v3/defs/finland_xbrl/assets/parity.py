"""Legacy-vs-unified Finland XBRL fact parity report.

Compares numeric facts produced by the legacy extractor
(`list_xml_parse_duckdb_paths` / `read_xml_parse_duckdb_rows`) against the
unified extractor (`list_xml_unified_duckdb_paths` /
`read_xml_unified_duckdb_rows`), grouped per `statement_key`, using
`xbrl_common.parity.compare_document_facts`. Results replace
`corpscout.fi_xbrl_parity_report` via the same stage + `EXCHANGE TABLES`
pattern as `unified_clickhouse.py`'s `_next` table export.

This asset is launched manually during the migration review window (Task
10) — it is intentionally not wired into any job selection.
"""

from collections import Counter, defaultdict
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster import AssetExecutionContext
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.finland_xbrl.assets.common import FINLAND_XBRL_DUCKDB_POOL
from dagster_v3.defs.finland_xbrl.assets.data_daily_xml_duckdb import (
    data_daily_xml_unified_duckdb,
)
from dagster_v3.defs.finland_xbrl.assets.data_snapshot_xml_duckdb import (
    data_snapshot_xml_unified_duckdb,
    list_xml_parse_duckdb_paths,
    list_xml_unified_duckdb_paths,
    read_xml_parse_duckdb_rows,
    read_xml_unified_duckdb_rows,
)
from dagster_v3.defs.finland_xbrl.unified_clickhouse import (
    CLICKHOUSE_DATABASE,
    replace_clickhouse_table_with_rows,
)
from dagster_v3.defs.xbrl_common.parity import ParityResult, compare_document_facts

FINLAND_XBRL_PARITY_REPORT_TABLE = "fi_xbrl_parity_report"
FINLAND_XBRL_PARITY_REPORT_COLUMNS = (
    "document_key",
    "status",
    "old_fact_count",
    "new_fact_count",
    "value_mismatches",
    "missing_in_new",
    "missing_in_old",
    "details",
    "compared_at",
)

# Rules encoding documented, reviewed differences between the legacy and
# unified Finland XBRL extractors (e.g. a transform fixing a mangled value).
# Starts empty; rules are added by hand during the Task 10 review as
# differences are understood.
FINLAND_EXPLAINED_RULES: list = []


def _group_facts_by_statement_key(
    facts: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for fact in facts:
        grouped[str(fact.get("statement_key") or "")].append(fact)
    return grouped


def build_finland_parity_results(
    *,
    old_facts: list[dict[str, Any]],
    new_facts: list[dict[str, Any]],
    explained_rules: list = FINLAND_EXPLAINED_RULES,
) -> list[ParityResult]:
    old_by_statement = _group_facts_by_statement_key(old_facts)
    new_by_statement = _group_facts_by_statement_key(new_facts)
    statement_keys = sorted(set(old_by_statement) | set(new_by_statement))
    return [
        compare_document_facts(
            document_key=statement_key,
            old_facts=old_by_statement.get(statement_key, []),
            new_facts=new_by_statement.get(statement_key, []),
            explained_rules=explained_rules,
        )
        for statement_key in statement_keys
    ]


def _parity_row(result: ParityResult, *, compared_at: datetime) -> tuple[Any, ...]:
    return (
        result.document_key,
        result.status,
        result.old_fact_count,
        result.new_fact_count,
        result.value_mismatches,
        result.missing_in_new,
        result.missing_in_old,
        result.details,
        compared_at,
    )


@dg.asset(
    name="fi_xbrl_parity",
    group_name="finland_xbrl",
    pool=FINLAND_XBRL_DUCKDB_POOL,
    deps=[data_snapshot_xml_unified_duckdb, data_daily_xml_unified_duckdb],
    kinds={"python", "duckdb", "clickhouse", "xbrl"},
    description=(
        "Compares legacy vs. unified Finland XBRL numeric facts per statement "
        "and replaces corpscout.fi_xbrl_parity_report with the results. Launched "
        "manually during the migration review window, not part of any job."
    ),
)
def fi_xbrl_parity(
    context: AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    legacy_paths = list_xml_parse_duckdb_paths()
    if not legacy_paths:
        raise FileNotFoundError(
            "No legacy Finland XBRL parsed DuckDB files were found"
        )
    unified_paths = list_xml_unified_duckdb_paths()
    if not unified_paths:
        raise FileNotFoundError(
            "No unified Finland XBRL parsed DuckDB files were found"
        )

    old_rows = read_xml_parse_duckdb_rows(duckdb_paths=legacy_paths)
    new_rows = read_xml_unified_duckdb_rows(duckdb_paths=unified_paths)

    results = build_finland_parity_results(
        old_facts=old_rows.facts,
        new_facts=new_rows.facts,
        explained_rules=FINLAND_EXPLAINED_RULES,
    )
    if not results:
        raise ValueError(
            "Refusing to publish Finland XBRL parity report: no statements found "
            "on either side (would blank a populated table)"
        )

    status_counts = Counter(result.status for result in results)
    compared_at = datetime.now(UTC)

    assert_clickhouse_tables_exist(
        clickhouse,
        database=CLICKHOUSE_DATABASE,
        tables=(FINLAND_XBRL_PARITY_REPORT_TABLE,),
    )
    with clickhouse.get_connection() as client:
        inserted = replace_clickhouse_table_with_rows(
            clickhouse_client=client,
            table=FINLAND_XBRL_PARITY_REPORT_TABLE,
            columns=FINLAND_XBRL_PARITY_REPORT_COLUMNS,
            rows=results,
            converter=lambda result: _parity_row(result, compared_at=compared_at),
        )

    context.log.info(
        "Finland XBRL parity report published: total=%d match=%d explained=%d "
        "mismatch=%d",
        len(results),
        status_counts.get("match", 0),
        status_counts.get("explained", 0),
        status_counts.get("mismatch", 0),
    )
    return dg.MaterializeResult(
        metadata={
            "total_statements": len(results),
            "match_count": status_counts.get("match", 0),
            "explained_count": status_counts.get("explained", 0),
            "mismatch_count": status_counts.get("mismatch", 0),
            "rows_published": inserted,
            "legacy_duckdb_path_count": old_rows.duckdb_path_count,
            "unified_duckdb_path_count": new_rows.duckdb_path_count,
            "clickhouse_database": CLICKHOUSE_DATABASE,
        }
    )
