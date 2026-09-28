"""Stable source observations and per-slot withdrawal through the shared state scan."""

from collections.abc import Callable, Sequence
from contextlib import closing
from datetime import UTC, datetime, timedelta

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.se_company import state_scan
from dagster_v3.defs.se_company.basic_info.extract import (
    ExtractConfig,
    SuggestionTarget,
    scope_pages,
)
from dagster_v3.defs.se_company.domain import tables
from dagster_v3.defs.se_company.domain.claims import insert_claims
from dagster_v3.defs.se_company.domain.evidence import digest, json_text

STAMPED = ("suggestion_id", "suggested_at", "source_run_id", "extractor_version")
SELECT_COLUMNS = tuple(c for c in tables.SUGGESTION_COLUMNS if c not in STAMPED)
STATE_COLUMNS = tuple(
    c for c in SELECT_COLUMNS if c not in ("company_id", "source", "observed_at")
)
# This target is the read projection used by the shared state scan.
# Publication uses insert_claims, never INSERT into this view.
TARGET = SuggestionTarget(
    database=tables.DATABASE,
    table=tables.SOURCE_VIEW,
    insert_columns=(*SELECT_COLUMNS, *STAMPED),
    select_columns=SELECT_COLUMNS,
    trailing_select_sql=(
        "lower(hex(SHA256(concat(candidate.company_id, '\\n', toString(candidate.source), "
        "'\\n', candidate.slot, '\\n', toString(stamp))))) AS suggestion_id, "
        "stamp AS suggested_at, %(source_run_id)s AS source_run_id, %(extractor_version)s AS extractor_version"
    ),
    asset_prefix="se_company_domain_suggestions_",
    group_name=tables.GROUP_NAME,
    scratch_prefix="corpscout._tmp_domain_scope_",
    with_sql="WITH (SELECT now64(3, 'UTC')) AS stamp\n",
)


def source_scan(source: str) -> state_scan.StateScan:
    values = {column: f"stored.{column}" for column in SELECT_COLUMNS}
    values.update(
        {
            "company_id": "stored.company_id",
            "source": f"'{source}'",
            "slot": "stored.slot",
            "root_domain": "stored.root_domain",
            "association": "'uncertain'",
            "is_primary": "toUInt8(0)",
            "confidence": "toFloat64(0)",
            "removed": "toUInt8(1)",
        }
    )
    return state_scan.StateScan(
        target=TARGET,
        select_columns=SELECT_COLUMNS,
        state_columns=STATE_COLUMNS,
        key_column="slot",
        live_row_predicate="removed = 0",
        tombstone_columns=tuple(
            c for c in SELECT_COLUMNS if c not in ("company_id", "source")
        ),
        tombstone_values=values,
    )


def run_domain_source(
    client,
    *,
    source: str,
    live_sql: Callable,
    config: ExtractConfig,
    run_id: str,
    log: Callable,
) -> dict:
    scan = source_scan(source)
    scope_sql = state_scan.changed_scope_sql(
        scan, source=source, live_sql=live_sql(scoped=False)
    )
    params = {}
    if config.since:
        scope_sql = f"SELECT DISTINCT company_id FROM ({live_sql(scoped=False)}) WHERE observed_at>parseDateTime64BestEffort(%(since)s,3,'UTC')"
        params["since"] = config.since
    pages = (
        (
            config.company_ids[i : i + config.page_size]
            for i in range(0, len(config.company_ids), config.page_size)
        )
        if config.company_ids
        else scope_pages(
            client,
            scope_sql=scope_sql,
            params=params,
            page_size=config.page_size,
            settings={"max_execution_time": 1800, "max_threads": 4},
            prefix=TARGET.scratch_prefix,
        )
    )
    counts = dict(
        companies=0,
        candidates=0,
        inserted=0,
        execute=config.execute,
        stopped_at_cap=False,
    )
    selected = state_scan.select_sql(
        scan, source=source, live_sql=live_sql(scoped=True)
    )
    with closing(pages) as scope:
        for ids in scope:
            remaining = config.max_companies - counts["companies"]
            if len(ids) > remaining:
                ids = ids[:remaining]
                counts["stopped_at_cap"] = True
            if not ids:
                break
            bound = {"company_ids": tuple(ids)}
            candidates = [
                dict(zip(SELECT_COLUMNS, row, strict=True))
                for row in client.execute(
                    selected,
                    bound,
                    settings={"max_query_size": 1048576, "max_execution_time": 1800, "max_threads": 4},
                )
            ]
            counts["companies"] += len(ids)
            counts["candidates"] += len(candidates)
            if config.execute and candidates:
                latest = client.execute(
                    f"SELECT maxOrNull(suggested_at) FROM corpscout.{tables.SOURCE_TABLE} WHERE source=%(source)s AND company_id IN %(company_ids)s",
                    {**bound, "source": source},
                )[0][0]
                stamp = datetime.now(UTC)
                if latest is not None:
                    stamp = max(stamp, latest + timedelta(milliseconds=1))
                for row in candidates:
                    row.update(
                        suggestion_id=digest(json_text([row, stamp])),
                        suggested_at=stamp,
                        source_run_id=run_id,
                        extractor_version=f"{source}-domain-v2",
                    )
                insert_claims(client, candidates)
                counts["inserted"] += len(candidates)
            log(
                "Domain evidence: source=%s companies=%d claims=%d written=%d execute=%s",
                source,
                counts["companies"],
                counts["candidates"],
                counts["inserted"],
                config.execute,
            )
            if counts["stopped_at_cap"]:
                break
    return counts


def define_domain_source(
    *,
    source: str,
    live_sql: Callable[..., str],
    deps: Sequence[dg.AssetKey],
    description: str,
) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"se_company_domain_suggestions_{source}",
        group_name=tables.GROUP_NAME,
        pool=f"se_company_domain_{source}",
        deps=list(deps),
        kinds={"clickhouse", "python"},
        metadata={"table": f"corpscout.{tables.SOURCE_TABLE}", "source": source},
        description=description,
    )
    def evidence(
        context: dg.AssetExecutionContext,
        config: ExtractConfig,
        clickhouse: ClickhouseResource,
    ) -> dg.MaterializeResult:
        assert_clickhouse_tables_exist(
            clickhouse,
            database=tables.DATABASE,
            tables=(tables.SOURCE_TABLE, tables.SOURCE_VIEW),
        )
        with clickhouse.get_connection() as client:
            counts = run_domain_source(
                client,
                source=source,
                live_sql=live_sql,
                config=config,
                run_id=context.run_id,
                log=context.log.info,
            )
        return dg.MaterializeResult(metadata=counts)

    return evidence
