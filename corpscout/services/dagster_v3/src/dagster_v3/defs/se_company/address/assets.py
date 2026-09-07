"""Dagster assets of the address entity. Slice 0 shipped the normalize asset; slice 2b adds
the fold (`se_company_address_fold`, `se_company_address_fold_companies`) and the
precedence export; the extractors follow in a later slice."""

import re
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from dagster_duckdb import DuckDBResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.batch import (
    BUCKET_COUNT,
    PAGE_SIZE as FOLD_PAGE_SIZE,
    FoldCounts,
    fold_bucket,
    fold_companies,
)
from dagster_v3.defs.se_company.address.normalize import PAGE_SIZE, NormalizeCounts, normalize_all, normalize_companies
from dagster_v3.defs.se_company.address.precedence import precedence_rows
from dagster_v3.defs.se_company.address.warm import CHUNK_SIZE as WARM_CHUNK_SIZE, warm_geocodes
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


def targeted_fold(
    client: Any, duckdb: Any, company_ids: Sequence[str], *, changed_only: bool, source_run_id: str,
    folded_at: datetime, page_size: int, log: Callable[..., object] | None, logger: Any = None,
) -> tuple[NormalizeCounts, FoldCounts]:
    """The targeted fold normalizes the companies' raw rows first (spec section 8: a
    reviewer's draft parses on Fold now), always changed_only, so only rows never
    normalized, newer than their normalized row or on an older normalizer version are
    touched; then folds the same ids with the caller's changed_only. `log` (a callable) goes
    to `fold_companies`, which calls it directly; `logger` (an object with `.info`) goes to
    `normalize_companies`, which calls `log.info(...)` -- the two functions want different
    shapes, so the caller passes both."""
    normalized = normalize_companies(
        client, company_ids, changed_only=True, normalized_at=folded_at, page_size=page_size, log=logger,
    )
    folded = fold_companies(
        client, duckdb, company_ids, changed_only=changed_only, source_run_id=source_run_id,
        folded_at=folded_at, page_size=page_size, log=log,
    )
    return normalized, folded


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
        "their bucket. The backoffice's Fold now button launches this asset for one company. "
        "Normalizes the companies' raw rows first, so a reviewer's draft parses on Fold now."
    ),
)
def se_company_address_fold_companies(
    context: dg.AssetExecutionContext, config: AddressFoldCompaniesConfig, clickhouse: ClickhouseResource,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(*_FOLD_TABLES, *_GEOCODE_TABLES))
    with clickhouse.get_connection() as client, sweden_address_osm_duckdb.get_connection() as duckdb:
        normalized, counts = targeted_fold(
            client, duckdb, config.company_ids, changed_only=config.changed_only, source_run_id=context.run_id,
            folded_at=datetime.now(UTC), page_size=config.page_size, log=context.log.info, logger=context.log,
        )
    return dg.MaterializeResult(
        metadata={**_fold_metadata(counts, config), **{f"normalize_{k}": v for k, v in normalized.as_metadata().items()}}
    )


class AddressWarmConfig(dg.Config):
    # Keys per geocode_addresses call. The engine's cost is mostly per call, so large chunks
    # are the point; lower it only if a chunk presses the host's memory.
    chunk_size: int = Field(default=WARM_CHUNK_SIZE, ge=10_000, le=5_000_000)
    # 0 = every key. A small limit times one engine call on prod without warming everything.
    limit: int = Field(default=0, ge=0)


@dg.asset(
    name="se_address_geocodes_warm",
    pool=osm_tables.DUCKDB_POOL,
    group_name=GROUP_NAME,
    kinds={"clickhouse", "duckdb", "python"},
    metadata={"table": "corpscout.se_address_geocodes", "reads": tables.QUALIFIED_NORMALIZED_TABLE},
    description=(
        "Geocodes every current address location key in bulk through the cache-then-matcher "
        "function, so the fold pages hit the cache. Run once before the first full fold and "
        "after every OSM extract refresh. Manual."
    ),
)
def se_address_geocodes_warm(
    context: dg.AssetExecutionContext, config: AddressWarmConfig, clickhouse: ClickhouseResource,
    sweden_address_osm_duckdb: DuckDBResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database=tables.DATABASE, tables=(tables.NORMALIZED_TABLE, *_GEOCODE_TABLES))
    with clickhouse.get_connection() as client, sweden_address_osm_duckdb.get_connection() as duckdb:
        counts = warm_geocodes(
            client, duckdb, run_id=context.run_id, matched_at=datetime.now(UTC),
            chunk_size=config.chunk_size, limit=config.limit, log=context.log.info,
        )
    return dg.MaterializeResult(metadata={**counts.as_metadata(), "chunk_size": config.chunk_size, "limit": config.limit})
