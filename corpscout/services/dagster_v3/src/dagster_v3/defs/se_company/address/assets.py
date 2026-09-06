"""Dagster assets of the address entity. Slice 0 ships the normalize asset; the fold, the
precedence export and the extractors follow in slices 1 and 2."""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.normalize import PAGE_SIZE, normalize_all, normalize_companies
from dagster_v3.defs.se_company.address.precedence import precedence_rows
from dagster_v3.defs.se_company.common import normalized_se_company_ids

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
