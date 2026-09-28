"""Bounded, replayable preparation of the compact index before table-name cutover."""

from uuid import uuid4

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.domains.registration import PUBLISH_POOL
from dagster_v3.defs.domains.sources import COLUMNS, SETTINGS, require_compact_index

DESTINATION = "corpscout.domains_sources_next"


class DomainSourcesBackfillConfig(dg.Config):
    execute: bool = False
    after_domain_id: str = ""
    page_size: int = Field(default=100_000, ge=1, le=100_000)
    max_domains: int = Field(default=1_000_000, ge=1)


def copy_source_index(client, *, config: DomainSourcesBackfillConfig, log) -> dict:
    require_compact_index(client, DESTINATION)
    after = config.after_domain_id
    if after and (len(after) != 64 or any(c not in "0123456789abcdef" for c in after)):
        raise ValueError("after_domain_id must be an acknowledged SHA-256 domain ID")
    domains = contributions = 0
    prefix = f"domain-source-backfill:{uuid4().hex}:"
    try:
        while domains < config.max_domains:
            ids = client.execute("""SELECT domain_id FROM corpscout.domains_sources
                WHERE domain_id>%(after)s GROUP BY domain_id ORDER BY domain_id LIMIT %(limit)s""",
                {"after": after, "limit": min(config.page_size, config.max_domains-domains)},
                settings=SETTINGS, query_id=f"{prefix}boundary-{domains}")
            if not ids:
                break
            through = ids[-1][0]
            params = {"after": after, "through": through}
            scope = "domain_id>%(after)s AND domain_id<=%(through)s"
            # Include every contributor, even ones not yet registered in the new
            # publisher. Multiple company/source records collapse to one row.
            expected = f"""SELECT domain_id,source_table,min(old.first_seen_at) AS first_seen_at,
                max(old.last_seen_at) AS last_seen_at,max(old.updated_at) AS updated_at,
                argMax(old.source_run_id,tuple(old.updated_at,old.source_run_id)) AS source_run_id
                FROM corpscout.domains_sources AS old FINAL WHERE {scope}
                GROUP BY domain_id,source_table"""
            [(count,)] = client.execute(f"SELECT count() FROM ({expected})", params, settings=SETTINGS)
            if config.execute:
                client.execute(f"""SELECT throwIf(count()>0,'Backfill contribution has no registered parent')
                    FROM ({expected}) WHERE domain_id NOT IN (
                        SELECT domain_id FROM corpscout.domains WHERE domain_id IN (
                            SELECT domain_id FROM corpscout.domains_sources WHERE {scope}))
                """, params, settings=SETTINGS, query_id=f"{prefix}parents-{domains}")
                client.execute(f"INSERT INTO {DESTINATION} ({','.join(COLUMNS)}) {expected}",
                               params, settings=SETTINGS, query_id=f"{prefix}copy-{domains}")
                # Both directions detect omissions, extras and timestamp drift.
                actual = f"SELECT {','.join(COLUMNS)} FROM {DESTINATION} FINAL WHERE {scope}"
                for left, right in ((expected, actual), (actual, expected)):
                    client.execute(f"SELECT throwIf(count()>0,'Compact source-index parity failed') FROM (({left}) EXCEPT DISTINCT ({right}))",
                                   params, settings=SETTINGS, query_id=f"{prefix}parity-{domains}-{int(left == actual)}")
            domains += len(ids)
            contributions += count
            after = through
            log("Source-index backfill: domains=%d contributions=%d after_domain_id=%s execute=%s",
                domains, contributions, after, config.execute)
        remaining = client.execute("SELECT count() FROM corpscout.domains_sources WHERE domain_id>%(after)s LIMIT 1",
                                   {"after": after}, settings=SETTINGS)[0][0]
    except BaseException:
        client.disconnect()
        client.execute("KILL QUERY WHERE startsWith(query_id,%(prefix)s) SYNC", {"prefix": prefix})
        raise
    return dict(domains=domains, contributions=contributions, after_domain_id=after,
                complete=remaining == 0, execute=config.execute)


@dg.asset(
    group_name="domains", kinds={"clickhouse"}, pool=PUBLISH_POOL,
    description="Cutover-only compact source-index copy. Pause old index writers first. Preview by default; resume acknowledged domain IDs. Validates references and full row parity. Does not swap/drop tables.",
)
def domains_sources_backfill(
    context: dg.AssetExecutionContext, config: DomainSourcesBackfillConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with clickhouse.get_connection() as client:
        result = copy_source_index(client, config=config, log=context.log.info)
    return dg.MaterializeResult(metadata=result)
