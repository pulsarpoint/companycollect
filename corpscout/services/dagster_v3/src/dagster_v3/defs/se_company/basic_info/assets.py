"""The fold assets and the precedence export (spec section 6). Everything manual: no
schedule, no sensor, until the fold has proven itself on production."""

import re
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import (
    BUCKET_COUNT,
    PAGE_SIZE,
    fold_bucket,
    fold_companies,
)
from dagster_v3.defs.se_company.basic_info.precedence import precedence_rows
from dagster_v3.defs.se_company.common import normalized_se_company_ids

GROUP_NAME = "se_company_basic_info"
FOLD_POOL = "se_company_basic_info_fold"

BASIC_INFO_FOLD_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)]
)


def basic_info_bucket_index(partition_key: str) -> int:
    match = re.fullmatch(r"bucket_(\d{2})", partition_key)
    if match is None:
        raise ValueError(f"invalid basic-info fold partition key: {partition_key!r}")
    bucket = int(match.group(1))
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"basic-info fold bucket out of range: {bucket}")
    return bucket


class BasicInfoFoldConfig(dg.Config):
    # True: only companies whose newest suggestion is later than their main row's
    # folded_at (or that have no main row). False re-folds the whole bucket and rewrites
    # every published row (history only where a value or source changed).
    changed_only: bool = True
    # Companies per page. A page holds every current suggestion row of its companies,
    # descriptions included, so lower this if a run presses the host's memory; it is also
    # the knob that tunes paging on prod without a redeploy.
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=50_000)


class BasicInfoFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        # Sorted, de-duplicated; raises on an id that is not 10 or 12 digits.
        return list(normalized_se_company_ids(value))


_FOLD_TABLES = (tables.SUGGESTION_TABLE, tables.MAIN_TABLE, tables.HISTORY_TABLE)


@dg.asset(
    name="se_company_basic_info_fold",
    partitions_def=BASIC_INFO_FOLD_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=FOLD_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE,
              "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "Folds every current suggestion row of the companies in one of 64 hash buckets "
        "into se_company_basic_info by the per-field precedence, rewriting every folded "
        "company's main row and adding a history row only when a value or source "
        "changed. Manual: launch a partition or a backfill from the UI."
    ),
)
def se_company_basic_info_fold(
    context: dg.AssetExecutionContext, config: BasicInfoFoldConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    bucket = basic_info_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client:
        counts = fold_bucket(
            client, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "bucket": bucket, "changed_only": config.changed_only,
                  "page_size": config.page_size, "table": tables.QUALIFIED_MAIN_TABLE,
                  "history_table": tables.QUALIFIED_HISTORY_TABLE}
    )


@dg.asset(
    name="se_company_basic_info_fold_companies",
    pool=FOLD_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE,
              "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "The targeted fold: the companies named in config.company_ids, whatever their "
        "bucket, rewriting every folded company's main row and adding a history row "
        "only when a value or source changed. The backoffice's Fold now button "
        "launches this asset for one company."
    ),
)
def se_company_basic_info_fold_companies(
    context: dg.AssetExecutionContext, config: BasicInfoFoldCompaniesConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    with clickhouse.get_connection() as client:
        counts = fold_companies(
            client, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "changed_only": config.changed_only,
                  "page_size": config.page_size, "table": tables.QUALIFIED_MAIN_TABLE,
                  "history_table": tables.QUALIFIED_HISTORY_TABLE}
    )


def _precedence_export_timestamp(exported_at: datetime) -> str:
    """``exported_at`` as a UTC ``%Y-%m-%d %H:%M:%S.mmm`` string, for binding into
    ``toDateTime64(..., 3, 'UTC')`` -- a bare tz-aware datetime parameter would let the
    stale-pairs comparison depend on the ClickHouse server's default timezone and drop
    sub-second precision."""
    return exported_at.strftime("%Y-%m-%d %H:%M:%S.") + f"{exported_at.microsecond // 1000:03d}"


def export_precedence(client: Any, exported_at: datetime) -> tuple[int, int]:
    """Insert every (field, source, precedence) pair from ``precedence_rows()`` and count
    pairs already in the table that were exported before this run but that the current
    dictionary no longer names. Returns ``(pairs inserted, stale pairs remaining)``.

    Factored out of the asset body so it can be exercised directly against a fake
    ClickHouse client in tests, without needing a Dagster asset execution context.
    """
    rows = [(field, source, precedence, exported_at) for field, source, precedence in precedence_rows()]
    client.execute(
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} ({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
        rows,
    )
    stale = int(
        client.execute(
            f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL "
            "WHERE exported_at < toDateTime64(%(exported_at)s, 3, 'UTC')",
            {"exported_at": _precedence_export_timestamp(exported_at)},
        )[0][0]
    )
    return len(rows), stale


@dg.asset(
    name="se_company_basic_info_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports BASIC_INFO_PRECEDENCE to se_company_basic_info_precedence for the "
        "backoffice to display and validate against. The Python dictionary is the only "
        "source; re-run after changing it."
    ),
)
def se_company_basic_info_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,))
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand", stale,
        )
    return dg.MaterializeResult(
        metadata={"pairs": pairs, "stale_pairs": stale, "table": tables.QUALIFIED_PRECEDENCE_TABLE}
    )
