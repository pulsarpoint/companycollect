"""Dagster assets of the financial entity: the precedence export (slice 1) and the two fold
assets (slice 3). The extractors and the extract job live in their own modules (slice 2)."""

import re
from collections import Counter
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.se_company.financial import tables
from dagster_v3.defs.se_company.financial.batch import (
    BUCKET_COUNT,
    PAGE_SIZE as FOLD_PAGE_SIZE,
    FoldCounts,
    fold_bucket,
    fold_companies,
)
from dagster_v3.defs.se_company.financial.precedence import precedence_rows

GROUP_NAME = "se_company_financial"
# The bucket fold's page reads are primary-key seeks scattered over the whole suggestion table
# (the bucket hash spreads a page's ids across every granule); the instance defaults every pool
# to limit 1 (dagster.yaml), so this pool runs the 64 buckets one at a time and a backfill
# can never put sixty-four FINAL reads on the server at once (spec section 8). The targeted
# fold below (a few ids) stays unpooled.
FOLD_POOL = "se_company_financial_fold"
EXTRACTOR_SOURCES: tuple[str, ...] = ("bolagsverket", "bolagsverket_comparative", "esef", "ratsit")
EXTRACTOR_ASSET_NAMES: tuple[str, ...] = tuple(
    f"se_company_financial_suggestions_{source}" for source in EXTRACTOR_SOURCES
)


def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]:
    """Insert every (field, source, precedence) pair as a global rule (company_id '',
    period_key '', decided_by 'code'). Returns (pairs inserted, stale pairs): stale is the
    count of global (field, source) pairs present in ClickHouse that the dictionary no
    longer names at all -- they stay until removed by hand (the export never deletes). A
    pair whose number merely changed is not stale; the insert corrects it. Never touches a
    company-scoped row.

    Read the stored global rows first: when they already equal `precedence_rows()`, insert
    nothing and report 0 pairs (the caller reads that as `unchanged`); otherwise insert every
    pair."""
    wanted = precedence_rows()
    stored = {
        tuple(row)
        for row in client.execute(
            f"SELECT field, source, precedence FROM {tables.QUALIFIED_PRECEDENCE_TABLE} "
            "FINAL WHERE company_id = '' AND period_key = '' AND removed = 0"
        )
    }
    pairs = 0
    if stored != set(wanted):
        rows = [
            ("", "", field, source, precedence, 0, "code", "", exported_at)
            for field, source, precedence in wanted
        ]
        client.execute(
            f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
            f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
            rows,
        )
        pairs = len(rows)
    stale = len(
        {(field, source) for field, source, _ in stored}
        - {(field, source) for field, source, _ in wanted}
    )
    return pairs, stale


@dg.asset(
    name="se_company_financial_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports FINANCIAL_PRECEDENCE to se_company_financial_precedence as global rules "
        "(company_id '', period_key '') for the fold and the backoffice to read. The Python "
        "dictionary is the only source for these rows; re-run after changing it, then "
        "re-fold every bucket with changed_only false (a precedence change is not a "
        "per-company change). Idle re-materialisation is a no-op: when the stored rows "
        "already match the dictionary, nothing is written and the watermark does not move. "
        "Never touches a company-scoped row."
    ),
)
def se_company_financial_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,)
    )
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand",
            stale,
        )
    return dg.MaterializeResult(
        metadata={
            "pairs": pairs, "stale_pairs": stale, "unchanged": pairs == 0,
            "table": tables.QUALIFIED_PRECEDENCE_TABLE,
        }
    )


FINANCIAL_FOLD_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)]
)
_FOLD_TABLES = (
    tables.SUGGESTION_TABLE, tables.MAIN_TABLE, tables.HISTORY_TABLE,
    tables.PRECEDENCE_TABLE, tables.RULE_TABLE,
)


def financial_bucket_index(partition_key: str) -> int:
    match = re.fullmatch(r"bucket_(\d{2})", partition_key)
    if match is None:
        raise ValueError(f"invalid financial fold partition key: {partition_key!r}")
    bucket = int(match.group(1))
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"financial fold bucket out of range: {bucket}")
    return bucket


class FinancialFoldConfig(dg.Config):
    # True: only companies whose newest suggestion, precedence decision or hide decision is
    # newer than their last fold, plus companies never folded that have a live row. False
    # re-folds the whole bucket (what a precedence change needs, spec 5); history rows are
    # written either way only where values, sources or activity changed.
    changed_only: bool = True
    # Companies per page (spec 6: 5,000, about 65k suggestion rows in memory).
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=20_000)


class FinancialFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=20_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


def _fold_metadata(counts: FoldCounts, config: dg.Config, **extra: Any) -> dict[str, Any]:
    return {
        **counts.as_metadata(),
        "changed_only": config.changed_only,
        "page_size": config.page_size,
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
        **extra,
    }


@dg.asset(
    name="se_company_financial_fold",
    partitions_def=FINANCIAL_FOLD_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    group_name=GROUP_NAME,
    pool=FOLD_POOL,
    deps=[dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES],
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "Folds the current financial suggestion rows of the companies in one of 64 hash buckets "
        "into se_company_financial, one row per company, scope and period end: the currency is "
        "decided first by precedence (Ratsit, then the registers), each figure competes only "
        "among rows in that currency and brings its own USD twin, employees and the period "
        "attributes compete ungated, reviewer rules re-rank and hide rules deactivate, a period "
        "the sources stopped delivering is withdrawn, and every change is appended to "
        "se_company_financial_history first. changed_only=false re-folds the whole bucket (run "
        "over all 64 after a precedence change). Pooled at FOLD_POOL (instance default limit 1), "
        "so a backfill runs one bucket at a time. Manual: launch a partition or a backfill."
    ),
)
def se_company_financial_fold(
    context: dg.AssetExecutionContext, config: FinancialFoldConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    bucket = financial_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client:
        counts = fold_bucket(
            client, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config, bucket=bucket))


@dg.asset(
    group_name=GROUP_NAME,
    deps=[dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES],
    pool=FOLD_POOL,
    kinds={"clickhouse", "python"},
    description="Publishes financial across all companies after source processing, visiting fold buckets sequentially.",
)
def se_company_financial_publish(
    context: dg.AssetExecutionContext, config: FinancialFoldConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    totals: Counter[str] = Counter()
    with clickhouse.get_connection() as client:
        for bucket in range(BUCKET_COUNT):
            counts = fold_bucket(
                client, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
                folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
            )
            totals.update({key: value for key, value in counts.as_metadata().items() if isinstance(value, int)})
            context.log.info("Published bucket %d of %d", bucket + 1, BUCKET_COUNT)
    return dg.MaterializeResult(metadata={
        **totals, "buckets": BUCKET_COUNT, "changed_only": config.changed_only,
        "table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE,
    })


@dg.asset(
    name="se_company_financial_fold_companies",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "The targeted financial fold: the companies named in config.company_ids, whatever their "
        "bucket, changed_only false by default. The backoffice's Fold now button (slice 4) "
        "launches this asset for one company."
    ),
)
def se_company_financial_fold_companies(
    context: dg.AssetExecutionContext, config: FinancialFoldCompaniesConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    with clickhouse.get_connection() as client:
        counts = fold_companies(
            client, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config))
