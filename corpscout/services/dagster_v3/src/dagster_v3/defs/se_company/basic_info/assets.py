"""The fold assets and the precedence export (spec section 6). Everything manual: no
schedule, no sensor, until the fold has proven itself on production."""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.batch import BUCKET_COUNT, fold_bucket, fold_companies
from dagster_v3.defs.se_company.basic_info.precedence import precedence_rows
from dagster_v3.defs.se_company.common import normalized_se_company_ids

GROUP_NAME = "se_company_basic_info"
FOLD_POOL = "se_company_basic_info_fold"

BASIC_INFO_FOLD_PARTITIONS = dg.StaticPartitionsDefinition(
    [f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)]
)


def basic_info_bucket_index(partition_key: str) -> int:
    prefix, separator, suffix = partition_key.partition("_")
    if prefix != "bucket" or separator == "" or not suffix.isdigit():
        raise ValueError(f"invalid basic-info fold partition key: {partition_key!r}")
    bucket = int(suffix)
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"basic-info fold bucket out of range: {bucket}")
    return bucket


class BasicInfoFoldConfig(dg.Config):
    # True: only companies whose newest suggestion is later than their main row's
    # folded_at (or that have no main row). False re-folds the whole bucket, which still
    # writes only rows that differ.
    changed_only: bool = True


class BasicInfoFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False

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
    metadata={"table": tables.QUALIFIED_MAIN_TABLE},
    description=(
        "Folds every current suggestion row of the companies in one of 64 hash buckets "
        "into se_company_basic_info by the per-field precedence, writing only rows that "
        "differ and one history row per change. Manual: launch a partition or a backfill "
        "from the UI."
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
            folded_at=datetime.now(UTC), log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "bucket": bucket, "changed_only": config.changed_only,
                  "table": tables.QUALIFIED_MAIN_TABLE}
    )


@dg.asset(
    name="se_company_basic_info_fold_companies",
    pool=FOLD_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE},
    description=(
        "The targeted fold: the companies named in config.company_ids, whatever their "
        "bucket. The backoffice's Fold now button launches this asset for one company."
    ),
)
def se_company_basic_info_fold_companies(
    context: dg.AssetExecutionContext, config: BasicInfoFoldCompaniesConfig, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=_FOLD_TABLES)
    with clickhouse.get_connection() as client:
        counts = fold_companies(
            client, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), log=context.log.info,
        )
    return dg.MaterializeResult(
        metadata={**counts.as_metadata(), "changed_only": config.changed_only, "table": tables.QUALIFIED_MAIN_TABLE}
    )


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
    rows = [(field, source, precedence, exported_at) for field, source, precedence in precedence_rows()]
    with clickhouse.get_connection() as client:
        client.execute(
            f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} ({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
            rows,
        )
        stale = int(
            client.execute(
                f"SELECT count() FROM {tables.QUALIFIED_PRECEDENCE_TABLE} FINAL WHERE exported_at < %(exported_at)s",
                {"exported_at": exported_at},
            )[0][0]
        )
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; "
            "they stay until removed by hand", stale,
        )
    return dg.MaterializeResult(
        metadata={"pairs": len(rows), "stale_pairs": stale, "table": tables.QUALIFIED_PRECEDENCE_TABLE}
    )
