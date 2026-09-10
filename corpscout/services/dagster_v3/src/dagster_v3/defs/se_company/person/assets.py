"""Dagster assets of the person entity. Slice 0 ships the normalize asset; the extractors
and the stopped weekly live in bolagsverket.py, esef.py, wikidata.py and jobs.py (slice 1),
the fold and the precedence export in slice 2."""

import re
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.batch import (
    BUCKET_COUNT,
    PAGE_SIZE as FOLD_PAGE_SIZE,
    FoldCounts,
    fold_bucket,
    fold_companies,
)
from dagster_v3.defs.se_company.person.normalize import (
    PAGE_SIZE,
    NormalizeCounts,
    normalize_all,
    normalize_companies,
)
from dagster_v3.defs.se_company.person.precedence import precedence_rows

GROUP_NAME = "se_company_person"
NORMALIZE_POOL = "se_company_person_normalize"
EXTRACTOR_SOURCES: tuple[str, ...] = ("bolagsverket", "esef", "wikidata")
EXTRACTOR_ASSET_NAMES: tuple[str, ...] = tuple(
    f"se_company_person_suggestions_{source}" for source in EXTRACTOR_SOURCES
)


class PersonNormalizeConfig(dg.Config):
    changed_only: bool = True
    company_ids: list[str] = Field(default_factory=list)
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


@dg.asset(
    name="se_company_person_normalize",
    group_name=GROUP_NAME,
    pool=NORMALIZE_POOL,
    deps=[dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES],
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_NORMALIZED_TABLE, "reads": tables.QUALIFIED_SUGGESTION_TABLE},
    description=(
        "Normalizes raw person suggestions into se_company_person_normalized: rows never "
        "normalized, computed from an older raw version, or computed by an older normalizer "
        "version. changed_only=false re-normalizes every raw row; company_ids targets companies."
    ),
)
def se_company_person_normalize(
    context: dg.AssetExecutionContext, config: PersonNormalizeConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE, tables=(tables.SUGGESTION_TABLE, tables.NORMALIZED_TABLE)
    )
    normalized_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        if config.company_ids:
            counts = normalize_companies(
                client, config.company_ids, changed_only=config.changed_only,
                normalized_at=normalized_at, page_size=config.page_size, log=context.log,
            )
        else:
            counts = normalize_all(
                client, changed_only=config.changed_only, normalized_at=normalized_at,
                page_size=config.page_size, log=context.log,
            )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "table": tables.QUALIFIED_NORMALIZED_TABLE}
    )


def _precedence_export_timestamp(exported_at: datetime) -> str:
    """``exported_at`` as a UTC ``%Y-%m-%d %H:%M:%S.mmm`` string for ``toDateTime64(..., 3,
    'UTC')``: a bare tz-aware datetime parameter would let the stale-pairs comparison depend
    on the server's default timezone and drop sub-second precision (basic info and the
    address entity do the same)."""
    return exported_at.strftime("%Y-%m-%d %H:%M:%S.") + f"{exported_at.microsecond // 1000:03d}"


def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]:
    """Insert every (field, source, precedence) pair as a global rule (company_id '',
    decided_by 'code') and count global rules exported before this run that the dictionary
    no longer names. Returns (pairs inserted, stale pairs remaining). Never touches a
    company-scoped row."""
    rows = [
        ("", field, source, precedence, 0, "code", "", exported_at)
        for field, source, precedence in precedence_rows()
    ]
    client.execute(
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} "
        f"({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
        rows,
    )
    stale = int(
        client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL "
            "WHERE company_id = '' AND decided_at < toDateTime64(%(exported_at)s, 3, 'UTC')",
            {"exported_at": _precedence_export_timestamp(exported_at)},
        )[0][0]
    )
    return len(rows), stale


@dg.asset(
    name="se_company_person_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports PERSON_PRECEDENCE to se_company_person_precedence as global rules "
        "(company_id '', field 'name') for the fold and the backoffice to read. The Python "
        "dictionary is the only source for these rows; re-run after changing it, which "
        "re-folds every company (the export's stamp is newer than their last fold). Never "
        "touches a company-scoped row."
    ),
)
def se_company_person_precedence_clickhouse(
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
        metadata={"pairs": pairs, "stale_pairs": stale, "table": tables.QUALIFIED_PRECEDENCE_TABLE}
    )


PERSON_FOLD_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)]
)
_FOLD_TABLES = (
    tables.NORMALIZED_TABLE, tables.MAIN_TABLE, tables.HISTORY_TABLE,
    tables.RULE_TABLE, tables.PRECEDENCE_TABLE,
)


def person_bucket_index(partition_key: str) -> int:
    match = re.fullmatch(r"bucket_(\d{2})", partition_key)
    if match is None:
        raise ValueError(f"invalid person fold partition key: {partition_key!r}")
    bucket = int(match.group(1))
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"person fold bucket out of range: {bucket}")
    return bucket


class PersonFoldConfig(dg.Config):
    # True: only companies whose newest normalized row, rule version or precedence row (their
    # own, or the global export) is newer than their last fold, plus companies never folded
    # that have a foldable row. False re-folds the whole bucket; history rows are written
    # either way only where a compared column changed.
    changed_only: bool = True
    # Companies per page. Lower it if a page's rows press the host's memory.
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)


class PersonFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


def _fold_metadata(counts: FoldCounts, config: dg.Config, **extra) -> dict:
    return {
        **counts.as_metadata(),
        "changed_only": config.changed_only,
        "page_size": config.page_size,
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
        **extra,
    }


def targeted_fold(
    client: Any,
    company_ids: Sequence[str],
    *,
    changed_only: bool,
    source_run_id: str,
    folded_at: datetime,
    page_size: int,
    log: Callable[..., object] | None,
    logger: Any = None,
) -> tuple[NormalizeCounts, FoldCounts]:
    """The targeted fold normalizes the companies' raw rows first (spec section 7: a
    reviewer's brand-new suggestion has to parse before Fold now can publish it), always
    changed_only so only rows never normalized, on another suggestion_id or on an older
    normalizer version are touched; then folds the same ids with the caller's changed_only.
    `log` (a plain callable) goes to fold_companies, which calls it directly; `logger` (an
    object with `.info`) goes to normalize_companies, which calls `log.info(...)` -- the two
    want different shapes, so the caller passes both."""
    normalized = normalize_companies(
        client, company_ids, changed_only=True, normalized_at=folded_at,
        page_size=page_size, log=logger,
    )
    folded = fold_companies(
        client, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
    return normalized, folded


@dg.asset(
    name="se_company_person_fold",
    partitions_def=PERSON_FOLD_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
    },
    description=(
        "Folds the current normalized person rows of the companies in one of 64 hash buckets "
        "into se_company_person_v2: observations of one person merge into one row with every "
        "source, reviewer rules hide, merge and split, previously published keys without a "
        "set are withdrawn, and every change is appended to se_company_person_history first. "
        "No pool: this fold opens no DuckDB, so buckets run in parallel. Manual: launch a "
        "partition or a backfill from the UI."
    ),
)
def se_company_person_fold(
    context: dg.AssetExecutionContext, config: PersonFoldConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    bucket = person_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client:
        counts = fold_bucket(
            client, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config, bucket=bucket))


@dg.asset(
    name="se_company_person_fold_companies",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={
        "table": tables.QUALIFIED_MAIN_TABLE,
        "history_table": tables.QUALIFIED_HISTORY_TABLE,
    },
    description=(
        "The targeted person fold: the companies named in config.company_ids, whatever their "
        "bucket. The backoffice's Fold now button launches this asset for one company. "
        "Normalizes the companies' raw rows first, so a reviewer's draft parses on Fold now."
    ),
)
def se_company_person_fold_companies(
    context: dg.AssetExecutionContext,
    config: PersonFoldCompaniesConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(
        clickhouse, database=tables.DATABASE,
        tables=(tables.SUGGESTION_TABLE, *_FOLD_TABLES),
    )
    folded_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        normalized, counts = targeted_fold(
            client, config.company_ids, changed_only=config.changed_only,
            source_run_id=context.run_id, folded_at=folded_at, page_size=config.page_size,
            log=context.log.info, logger=context.log,
        )
    return dg.MaterializeResult(
        metadata={
            **_fold_metadata(counts, config),
            **{f"normalize_{key}": value for key, value in normalized.as_metadata().items()},
        }
    )
