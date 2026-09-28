"""Explicit bounded copy of legacy claims, retaining withdrawals and original stamps."""

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.claims import insert_claims


class DomainClaimBackfillConfig(dg.Config):
    execute: bool = False
    after_company_id: str = ""
    page_size: int = Field(default=1000, ge=1, le=5000)
    max_companies: int = Field(default=10000, ge=1)


def copy_legacy_claims(client, *, config: DomainClaimBackfillConfig, log) -> dict:
    after = config.after_company_id
    companies = copied = 0
    while companies < config.max_companies:
        ids = [
            row[0]
            for row in client.execute(
                "SELECT DISTINCT company_id FROM corpscout.se_company_domain_suggestion WHERE company_id>%(after)s ORDER BY company_id LIMIT %(limit)s",
                {
                    "after": after,
                    "limit": min(config.page_size, config.max_companies - companies),
                },
            )
        ]
        if not ids:
            break
        rows = [
            dict(zip(tables.SUGGESTION_COLUMNS, row, strict=True))
            for row in client.execute(
                f"SELECT {','.join(tables.SUGGESTION_COLUMNS)} FROM corpscout.se_company_domain_suggestion FINAL WHERE company_id IN %(ids)s",
                {"ids": tuple(ids)},
            )
        ]
        if config.execute:
            insert_claims(client, rows)
        companies += len(ids)
        copied += len(rows)
        after = ids[-1]
        log(
            "Domain claim backfill: companies=%d claims=%d acknowledged_after=%s execute=%s",
            companies,
            copied,
            after,
            config.execute,
        )
    remaining = client.execute(
        "SELECT count() FROM corpscout.se_company_domain_suggestion WHERE company_id>%(after)s",
        {"after": after},
    )[0][0]
    return dict(
        companies=companies,
        claims=copied,
        after_company_id=after,
        complete=remaining == 0,
        execute=config.execute,
    )


@dg.asset(
    group_name=tables.GROUP_NAME,
    pool="se_company_domain_backfill",
    kinds={"clickhouse"},
    description="Cutover-only copy of current legacy claims, including removals/reviews, with original timestamps. Pause source writers first. Preview by default; resume from the last acknowledged company ID.",
)
def se_company_domain_sources_backfill(
    context: dg.AssetExecutionContext,
    config: DomainClaimBackfillConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        counts = copy_legacy_claims(client, config=config, log=context.log.info)
    return dg.MaterializeResult(metadata=counts)
