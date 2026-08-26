"""Partition-scoped ClickHouse publication for canonical ESEF parsing outputs.

No ``from __future__ import annotations``: Dagster inspects asset annotations.
"""

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import (
    assert_clickhouse_tables_exist,
    export_duckdb_connection_table_to_clickhouse,
)
from dagster_v3.defs.common.partition_duckdb import require_partition_duckdb
from dagster_v3.defs.esef_filings import tables
from dagster_v3.defs.esef_filings.partitioned_storage import (
    CONCEPT_LABELS_STORAGE,
    CONTACT_CANDIDATES_STORAGE,
    DISCLOSURES_STORAGE,
    FACTS_STORAGE,
    require_completed_partition,
)
from dagster_v3.defs.esef_filings.publish import ESEF_FACTS_COLUMN_EXPRESSIONS
from dagster_v3.defs.esef_filings.segment_assets import (
    ESEF_PROCESSED_WEEK_PARTITIONS,
)


GROUP_NAME = "esef"


@dataclass(frozen=True)
class PartitionPublishContract:
    dataset_name: str
    storage_source: str
    duckdb_table: str
    clickhouse_table: str
    columns: tuple[str, ...]
    column_expressions: dict[str, str] | None = None


FACTS_CONTRACT = PartitionPublishContract(
    dataset_name="facts",
    storage_source=FACTS_STORAGE,
    duckdb_table=tables.FACTS_TABLE,
    clickhouse_table=tables.ESEF_FACTS_TABLE,
    columns=tables.ESEF_FACTS_PARTITION_EXPORT_COLUMNS,
    column_expressions=ESEF_FACTS_COLUMN_EXPRESSIONS,
)
CONTACT_CANDIDATES_CONTRACT = PartitionPublishContract(
    dataset_name="contact_candidates",
    storage_source=CONTACT_CANDIDATES_STORAGE,
    duckdb_table=tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE,
    clickhouse_table=tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_TABLE,
    columns=tables.ESEF_DOCUMENT_CONTACT_CANDIDATES_PARTITION_EXPORT_COLUMNS,
)
CONCEPT_LABELS_CONTRACT = PartitionPublishContract(
    dataset_name="taxonomy_labels",
    storage_source=CONCEPT_LABELS_STORAGE,
    duckdb_table=tables.ESEF_DOCUMENT_CONCEPT_LABELS_TABLE,
    clickhouse_table=tables.ESEF_DOCUMENT_CONCEPT_LABELS_TABLE,
    columns=tables.ESEF_DOCUMENT_CONCEPT_LABELS_PARTITION_EXPORT_COLUMNS,
)
DISCLOSURES_CONTRACT = PartitionPublishContract(
    dataset_name="disclosures",
    storage_source=DISCLOSURES_STORAGE,
    duckdb_table=tables.ESEF_DISCLOSURES_TABLE,
    clickhouse_table=tables.ESEF_DISCLOSURES_TABLE,
    columns=tables.ESEF_DISCLOSURES_PARTITION_EXPORT_COLUMNS,
)


def replace_esef_partition_clickhouse(
    *,
    clickhouse: ClickhouseResource,
    partition_key: str,
    contract: PartitionPublishContract,
    log: Any = None,
) -> dict[str, object]:
    """Publish one validated DuckDB file into exactly one table partition."""
    assert_clickhouse_tables_exist(
        clickhouse,
        database=tables.ESEF_DATABASE,
        tables=(contract.clickhouse_table,),
    )
    with require_partition_duckdb(
        source=contract.storage_source,
        partition=partition_key,
    ) as duckdb_connection:
        expected_row_count = require_completed_partition(
            duckdb_connection,
            dataset_name=contract.dataset_name,
            table=contract.duckdb_table,
            partition_key=partition_key,
        )
        stage_name = f"_tmp_{contract.clickhouse_table}_{uuid.uuid4().hex}"
        target = f"`{tables.ESEF_DATABASE}`.`{contract.clickhouse_table}`"
        stage = f"`{tables.ESEF_DATABASE}`.`{stage_name}`"
        with clickhouse.get_connection() as client:
            client.execute(f"CREATE TABLE {stage} AS {target}")
            try:
                inserted_row_count = int(
                    export_duckdb_connection_table_to_clickhouse(
                        duckdb_connection=duckdb_connection,
                        clickhouse_client=client,
                        duckdb_schema=tables.DLT_DATASET_NAME,
                        duckdb_table=contract.duckdb_table,
                        clickhouse_database=tables.ESEF_DATABASE,
                        clickhouse_table=stage_name,
                        columns=contract.columns,
                        column_expressions=contract.column_expressions,
                        truncate=False,
                        log=log,
                    )
                )
                if inserted_row_count != expected_row_count:
                    raise ValueError(
                        f"ESEF {contract.dataset_name} stage row mismatch: "
                        f"expected={expected_row_count} inserted={inserted_row_count}"
                    )
                client.execute(
                    f"ALTER TABLE {target} REPLACE PARTITION "
                    f"'{partition_key}' FROM {stage}"
                )
                [(published_row_count,)] = client.execute(
                    f"SELECT count() FROM {target} "
                    "WHERE processed_week = %(processed_week)s",
                    {"processed_week": partition_key},
                )
                if int(published_row_count) != expected_row_count:
                    raise ValueError(
                        f"ESEF {contract.dataset_name} published row mismatch: "
                        f"expected={expected_row_count} actual={published_row_count}"
                    )
            finally:
                client.execute(f"DROP TABLE IF EXISTS {stage}")
    return {
        "dataset_name": contract.dataset_name,
        "partition_key": partition_key,
        "row_count": expected_row_count,
        "table": f"{tables.ESEF_DATABASE}.{contract.clickhouse_table}",
    }


PUBLISH_CONTRACTS = (
    (dg.AssetKey("esef_facts_clickhouse"), FACTS_CONTRACT),
    (
        dg.AssetKey("esef_document_contact_candidates_clickhouse"),
        CONTACT_CANDIDATES_CONTRACT,
    ),
    (dg.AssetKey("esef_document_concept_labels_clickhouse"), CONCEPT_LABELS_CONTRACT),
    (dg.AssetKey("esef_disclosures_clickhouse"), DISCLOSURES_CONTRACT),
)


@dg.multi_asset(
    specs=[
        dg.AssetSpec(
            "esef_facts_clickhouse",
            deps=["esef_filing_facts_duckdb"],
            group_name=GROUP_NAME,
            kinds={"duckdb", "clickhouse", "xbrl"},
        ),
        dg.AssetSpec(
            "esef_document_contact_candidates_clickhouse",
            deps=["esef_document_contact_candidates_duckdb"],
            group_name=GROUP_NAME,
            kinds={"duckdb", "clickhouse"},
        ),
        dg.AssetSpec(
            "esef_document_concept_labels_clickhouse",
            deps=["esef_document_concept_labels_duckdb"],
            group_name=GROUP_NAME,
            kinds={"duckdb", "clickhouse", "taxonomy"},
        ),
        dg.AssetSpec(
            "esef_disclosures_clickhouse",
            deps=["esef_disclosures_duckdb"],
            group_name=GROUP_NAME,
            kinds={"duckdb", "clickhouse", "xhtml"},
        ),
    ],
    partitions_def=ESEF_PROCESSED_WEEK_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool="esef_parsing_clickhouse",
)
def esef_parsing_clickhouse(
    context: dg.AssetExecutionContext,
    clickhouse: ClickhouseResource,
) -> Iterator[dg.MaterializeResult]:
    """Publish all four parsing outputs as one non-subsettable operation."""
    for asset_key, contract in PUBLISH_CONTRACTS:
        yield dg.MaterializeResult(
            asset_key=asset_key,
            metadata=replace_esef_partition_clickhouse(
                clickhouse=clickhouse,
                partition_key=context.partition_key,
                contract=contract,
                log=context.log.info,
            ),
        )


ESEF_PARSING_CLICKHOUSE_ASSETS = (esef_parsing_clickhouse,)


# --- esef_facts_clickhouse data-correctness checks --------------------------
#
# Both checks attach to "esef_facts_clickhouse" by string key -- one of four
# output keys of the `esef_parsing_clickhouse` multi-asset above. A check
# only needs an AssetKey to attach to, not a reference to the multi-asset's
# Python object (mirrors assets.py's `filings_index_non_empty`, which attaches
# to "esef_filings_index_duckdb" the same way).

# Guard 1 -- cross-partition fxo_id duplication (latent double-count bug).
# ReplacingMergeTree on esef_facts dedups only WITHIN a partition
# (PARTITION BY processed_week, migration 000309). If a filing's source
# `processed_at` ever moves it into a different week, its facts exist in two
# partitions simultaneously, and metrics.py joins esef_facts to filings on
# fxo_id alone (no FINAL, no week filter) -- both copies would aggregate into
# esef_financial_metrics silently. This is real corruption, not a coverage
# gap, so it fails the run at the default ERROR severity.
FXO_CROSS_WEEK_DUPLICATION_COUNT_SQL = f"""
SELECT count() FROM (
    SELECT fxo_id
    FROM {tables.QUALIFIED_ESEF_FACTS_TABLE}
    GROUP BY fxo_id
    HAVING uniqExact(processed_week) > 1
)
"""

FXO_CROSS_WEEK_DUPLICATION_SAMPLE_SQL = f"""
SELECT fxo_id, groupUniqArray(processed_week) AS weeks
FROM {tables.QUALIFIED_ESEF_FACTS_TABLE}
GROUP BY fxo_id
HAVING uniqExact(processed_week) > 1
ORDER BY fxo_id
LIMIT 10
"""


@dg.asset_check(
    asset="esef_facts_clickhouse",
    name="fxo_id_single_processed_week",
    description=(
        "Fails when any fxo_id's facts span more than one processed_week "
        "partition -- ReplacingMergeTree only dedups WITHIN a partition, and "
        "metrics.py joins esef_facts to filings by fxo_id alone (no FINAL, "
        "no week filter), so a cross-week duplicate would silently "
        "double-count in esef_financial_metrics."
    ),
)
def fxo_id_single_processed_week(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        [(offending_fxo_count,)] = client.execute(
            FXO_CROSS_WEEK_DUPLICATION_COUNT_SQL
        )
        offending_fxo_count = int(offending_fxo_count)
        sample_rows = (
            client.execute(FXO_CROSS_WEEK_DUPLICATION_SAMPLE_SQL)
            if offending_fxo_count
            else []
        )
    sample = [
        {
            "fxo_id": fxo_id,
            "processed_weeks": sorted(str(week) for week in weeks),
        }
        for fxo_id, weeks in sample_rows
    ]
    return dg.AssetCheckResult(
        passed=offending_fxo_count == 0,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "offending_fxo_count": offending_fxo_count,
            "sample": dg.MetadataValue.json(sample),
        },
    )


# Guard 2 -- per-week coverage (bounded runs masquerading as complete). The
# pipeline's dev-only `max_documents` bound produces green partitions that
# parsed a capped subset; partition status shows success, not coverage
# (observed in prod: week 2023-11-12 = 82 of 2,889 filings, 2025-11-30 = 24
# of 1,821). corpscout.esef_filings is the full source index (see publish.py
# `export_esef_filings_clickhouse`'s docstring); it carries no processed_week
# column of its own, so its filings are bucketed into the SAME Sunday-start
# week boundary the pipeline partitions on
# (ESEF_PROCESSED_WEEK_PARTITIONS: cron '0 0 * * 0' UTC) via
# `toStartOfWeek(date, 0)` (mode 0 = week starts Sunday).
FACTS_COVERAGE_BY_WEEK_SQL = f"""
WITH index_weeks AS (
    SELECT
        toStartOfWeek(toDate(processed_at), 0) AS week,
        uniqExact(fxo_id) AS index_filings
    FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE}
    WHERE processed_at IS NOT NULL
    GROUP BY week
),
fact_weeks AS (
    SELECT
        processed_week AS week,
        uniqExact(fxo_id) AS facts_filings
    FROM {tables.QUALIFIED_ESEF_FACTS_TABLE}
    GROUP BY week
)
SELECT
    index_weeks.week AS week,
    index_weeks.index_filings AS index_filings,
    coalesce(fact_weeks.facts_filings, 0) AS facts_filings
FROM index_weeks
LEFT JOIN fact_weeks ON fact_weeks.week = index_weeks.week
ORDER BY week
"""

# Overall totals are a SEPARATE query rather than a sum of the per-week
# counts above: summing per-week uniqExact(fxo_id) would double-count any
# fxo_id that Guard 1 would flag (present in more than one processed_week),
# inflating the facts side. A plain uniqExact over the whole table counts
# each fxo_id once regardless of how many partitions it appears in.
FACTS_COVERAGE_TOTALS_SQL = f"""
SELECT
    (SELECT uniqExact(fxo_id) FROM {tables.QUALIFIED_ESEF_FACTS_TABLE})
        AS facts_fxo_total,
    (
        SELECT uniqExact(fxo_id) FROM {tables.QUALIFIED_ESEF_FILINGS_TABLE}
        WHERE processed_at IS NOT NULL
    ) AS index_fxo_total
"""

FACTS_COVERAGE_THRESHOLD = 0.90
FACTS_COVERAGE_WORST_WEEK_LIMIT = 10


@dataclass(frozen=True)
class _WeekCoverage:
    week: str
    index_filings: int
    facts_filings: int

    @property
    def pct(self) -> float:
        return self.facts_filings / self.index_filings if self.index_filings else 0.0


@dg.asset_check(
    asset="esef_facts_clickhouse",
    name="per_week_facts_coverage",
    description=(
        "WARNs when a processed_week with index filings landed under 90% "
        "fact coverage in esef_facts -- the dev-only max_documents bound can "
        "leave a partition reporting success while it only parsed a capped "
        "subset. A week with zero facts at all is reported separately as "
        "never having run, not counted as an under-covered failure."
    ),
)
def per_week_facts_coverage(
    clickhouse: ClickhouseResource,
) -> dg.AssetCheckResult:
    with clickhouse.get_connection() as client:
        week_rows = client.execute(FACTS_COVERAGE_BY_WEEK_SQL)
        [(facts_fxo_total, index_fxo_total)] = client.execute(
            FACTS_COVERAGE_TOTALS_SQL
        )

    weeks = [
        _WeekCoverage(
            week=str(week),
            index_filings=int(index_filings),
            facts_filings=int(facts_filings),
        )
        for week, index_filings, facts_filings in week_rows
    ]
    never_run = [week for week in weeks if week.facts_filings == 0]
    under_covered = [
        week
        for week in weeks
        if week.facts_filings > 0 and week.pct < FACTS_COVERAGE_THRESHOLD
    ]
    covered = [
        week
        for week in weeks
        if week.facts_filings > 0 and week.pct >= FACTS_COVERAGE_THRESHOLD
    ]
    worst = sorted(under_covered, key=lambda week: week.pct)[
        :FACTS_COVERAGE_WORST_WEEK_LIMIT
    ]

    facts_fxo_total = int(facts_fxo_total)
    index_fxo_total = int(index_fxo_total)

    return dg.AssetCheckResult(
        passed=not under_covered,
        severity=dg.AssetCheckSeverity.WARN,
        metadata={
            "weeks_with_index_filings": len(weeks),
            "weeks_covered": len(covered),
            "weeks_under_covered": len(under_covered),
            "weeks_never_run": len(never_run),
            "worst_under_covered_weeks": dg.MetadataValue.json(
                [
                    {
                        "week": week.week,
                        "index_filings": week.index_filings,
                        "facts_filings": week.facts_filings,
                        "pct": round(week.pct * 100, 1),
                    }
                    for week in worst
                ]
            ),
            "facts_fxo_total": facts_fxo_total,
            "index_fxo_total": index_fxo_total,
            "overall_filing_coverage_pct": (
                round(facts_fxo_total / index_fxo_total * 100, 1)
                if index_fxo_total
                else None
            ),
        },
    )


defs = dg.Definitions(
    assets=list(ESEF_PARSING_CLICKHOUSE_ASSETS),
    asset_checks=[fxo_id_single_processed_week, per_week_facts_coverage],
)
