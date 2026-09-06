"""Dagster assets of the address entity. Slice 0 shipped the normalize asset; slice 2b adds
the fold (`se_company_address_fold`, `se_company_address_fold_companies`) and the
precedence export; the extractors follow in a later slice."""

import re
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.batch import BUCKET_COUNT, PAGE_SIZE as FOLD_PAGE_SIZE, fold_bucket, fold_companies
from dagster_v3.defs.se_company.address.normalize import PAGE_SIZE, normalize_all, normalize_companies
from dagster_v3.defs.se_company.address.precedence import precedence_rows
from dagster_v3.defs.se_company.common import normalized_se_company_ids
from dagster_v3.defs.sweden_address_osm import tables as osm_tables

GROUP_NAME = "se_company_address"
NORMALIZE_POOL = "se_company_address_normalize"

EXTRACTOR_SOURCES: tuple[str, ...] = ("scb", "bolagsverket", "ratsit")
EXTRACTOR_ASSET_NAMES: tuple[str, ...] = tuple(f"se_company_address_suggestions_{source}" for source in EXTRACTOR_SOURCES)


class AddressNormalizeConfig(dg.Config):
    changed_only: bool = True
    company_ids: list[str] = Field(default_factory=list)
    page_size: int = Field(default=PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return normalized_se_company_ids(value)


@dg.asset(
    name="se_company_address_normalize",
    group_name=GROUP_NAME,
    pool=NORMALIZE_POOL,
    deps=[dg.AssetKey(name) for name in EXTRACTOR_ASSET_NAMES],
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_NORMALIZED_TABLE, "reads": tables.QUALIFIED_SUGGESTION_TABLE},
    description=(
        "Normalizes raw address suggestions into se_company_address_normalized: rows never "
        "normalized, newer than their normalized row, or normalized on an older normalizer "
        "version. changed_only=false re-normalizes every raw row; company_ids targets companies."
    ),
)
def se_company_address_normalize(
    context: dg.AssetExecutionContext, config: AddressNormalizeConfig, clickhouse: ClickhouseResource
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
    return dg.MaterializeResult(metadata={**counts.as_metadata(), "table": tables.QUALIFIED_NORMALIZED_TABLE})


def _precedence_export_timestamp(exported_at: datetime) -> str:
    """``exported_at`` as a UTC ``%Y-%m-%d %H:%M:%S.mmm`` string for ``toDateTime64(..., 3,
    'UTC')``: a bare tz-aware datetime parameter would let the stale-pairs comparison depend
    on the server's default timezone and drop sub-second precision (basic info does the same)."""
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
        f"INSERT INTO {tables.QUALIFIED_PRECEDENCE_TABLE} ({', '.join(tables.PRECEDENCE_COLUMNS)}) VALUES",
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
    name="se_company_address_precedence_clickhouse",
    group_name=GROUP_NAME,
    kinds={"clickhouse", "python"},
    metadata={"table": tables.QUALIFIED_PRECEDENCE_TABLE},
    description=(
        "Exports ADDRESS_PRECEDENCE to se_company_address_precedence as global rules "
        "(company_id '', field 'text') for the backoffice to display. The Python dictionary "
        "is the only source for these rows; re-run after changing it. Never touches a "
        "company-scoped row."
    ),
)
def se_company_address_precedence_clickhouse(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(tables.PRECEDENCE_TABLE,))
    exported_at = datetime.now(UTC)
    with clickhouse.get_connection() as client:
        pairs, stale = export_precedence(client, exported_at)
    if stale:
        context.log.warning(
            "%d precedence pairs exist in ClickHouse that the dictionary no longer names; they stay until removed by hand",
            stale,
        )
    return dg.MaterializeResult(metadata={"pairs": pairs, "stale_pairs": stale, "table": tables.QUALIFIED_PRECEDENCE_TABLE})


ADDRESS_FOLD_PARTITIONS = dg.StaticPartitionsDefinition([f"bucket_{bucket:02d}" for bucket in range(BUCKET_COUNT)])


def address_bucket_index(partition_key: str) -> int:
    match = re.fullmatch(r"bucket_(\d{2})", partition_key)
    if match is None:
        raise ValueError(f"invalid address fold partition key: {partition_key!r}")
    bucket = int(match.group(1))
    if not 0 <= bucket < BUCKET_COUNT:
        raise ValueError(f"address fold bucket out of range: {bucket}")
    return bucket


class AddressFoldConfig(dg.Config):
    # True: only companies whose newest normalized row, rule version or geocode/normalizer
    # version is newer than their fold (or that have no main row). False re-folds the whole
    # bucket; history only where a compared field changed.
    changed_only: bool = True
    # Companies per page; also the geocode batch: a page's distinct location keys go to
    # the matcher together. Lower it if a run presses the host's memory.
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)


class AddressFoldCompaniesConfig(dg.Config):
    company_ids: list[str] = Field(min_length=1)
    changed_only: bool = False
    page_size: int = Field(default=FOLD_PAGE_SIZE, ge=1, le=50_000)

    @field_validator("company_ids")
    @classmethod
    def _valid_ids(cls, value: list[str]) -> list[str]:
        return list(normalized_se_company_ids(value))


_FOLD_TABLES = (tables.NORMALIZED_TABLE, tables.MAIN_TABLE, tables.HISTORY_TABLE, tables.RULE_TABLE, tables.PRECEDENCE_TABLE)
_GEOCODE_TABLES = ("se_address_geocodes", "se_postcode_centroids", "se_city_centroids")


def _fold_metadata(counts, config, **extra) -> dict:
    return {
        **counts.as_metadata(), "changed_only": config.changed_only, "page_size": config.page_size,
        "table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE, **extra,
    }


@dg.asset(
    name="se_company_address_fold",
    partitions_def=ADDRESS_FOLD_PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    pool=osm_tables.DUCKDB_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "duckdb", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "Folds the current normalized address rows of the companies in one of 64 hash "
        "buckets into se_company_address_v2: compatible suggestions merge into one "
        "published address, hide rules deactivate, previously published keys without a "
        "candidate are withdrawn, and every candidate gets its geocode from the cache or "
        "the OSM matcher inside the page. Takes the OSM workbench pool so an extract swap "
        "never races a fold. Manual: launch a partition or a backfill from the UI."
    ),
)
def se_company_address_fold(
    context: dg.AssetExecutionContext, config: AddressFoldConfig, clickhouse: ClickhouseResource,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(*_FOLD_TABLES, *_GEOCODE_TABLES))
    bucket = address_bucket_index(context.partition_key)
    with clickhouse.get_connection() as client, sweden_address_osm_duckdb.get_connection() as duckdb:
        counts = fold_bucket(
            client, duckdb, bucket, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config, bucket=bucket))


@dg.asset(
    name="se_company_address_fold_companies",
    pool=osm_tables.DUCKDB_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "duckdb", "python"},
    metadata={"table": tables.QUALIFIED_MAIN_TABLE, "history_table": tables.QUALIFIED_HISTORY_TABLE},
    description=(
        "The targeted address fold: the companies named in config.company_ids, whatever "
        "their bucket. The backoffice's Fold now button launches this asset for one company."
    ),
)
def se_company_address_fold_companies(
    context: dg.AssetExecutionContext, config: AddressFoldCompaniesConfig, clickhouse: ClickhouseResource,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(*_FOLD_TABLES, *_GEOCODE_TABLES))
    with clickhouse.get_connection() as client, sweden_address_osm_duckdb.get_connection() as duckdb:
        counts = fold_companies(
            client, duckdb, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info,
        )
    return dg.MaterializeResult(metadata=_fold_metadata(counts, config))
