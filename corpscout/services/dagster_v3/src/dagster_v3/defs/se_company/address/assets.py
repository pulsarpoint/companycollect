"""Dagster assets of the address entity. Slice 0 ships the normalize asset; the fold, the
precedence export and the extractors follow in slices 1 and 2."""

from datetime import UTC, datetime

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field, field_validator

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company.address import tables
from dagster_v3.defs.se_company.address.normalize import PAGE_SIZE, normalize_all, normalize_companies
from dagster_v3.defs.se_company.common import normalized_se_company_ids

GROUP_NAME = "se_company_address"
NORMALIZE_POOL = "se_company_address_normalize"


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
