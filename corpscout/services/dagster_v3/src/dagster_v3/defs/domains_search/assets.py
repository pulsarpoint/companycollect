"""Precompute list filters, retaining exactly the canonical domain membership."""

import logging
from datetime import UTC, datetime
from uuid import uuid4

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist

TABLE = "corpscout.domains_search"
COLUMNS = (
    "root_domain", "sources", "has_dns_records", "dns_last_observed_at",
    "website_count", "observed_website_count", "company_count", "first_seen_at",
    "last_seen_at", "refreshed_at", "source_run_id",
)


class DomainsSearchConfig(dg.Config):
    batch_rows: int = Field(default=500_000, ge=1, le=1_000_000)
    max_threads: int = Field(default=4, ge=1, le=16)
    max_execution_time: int = Field(default=3600, ge=1, le=14400)


def publish_domains_search(
    clickhouse: ClickhouseResource, *, config: DomainsSearchConfig, run_id: str,
    log: logging.Logger,
) -> dict:
    assert_clickhouse_tables_exist(clickhouse, database="corpscout", tables=(
        "domains", "domains_search", "websites", "se_company_domain",
        "commoncrawl_domain_dns_records",
    ))
    suffix = uuid4().hex
    frozen = f"corpscout.domains_search_roots_{suffix}"
    companies = f"corpscout.domains_search_companies_{suffix}"
    stage = f"corpscout.domains_search_stage_{suffix}"
    prefix = f"domains-search:{suffix}:"
    settings = {
        "max_threads": config.max_threads,
        "max_execution_time": config.max_execution_time,
        "max_memory_usage": 4 * 1024**3,
        "max_bytes_before_external_group_by": 512 * 1024**2,
        "max_bytes_before_external_sort": 512 * 1024**2,
        "join_algorithm": "full_sorting_merge", "join_use_nulls": 1,
        "optimize_aggregation_in_order": 1, "async_insert": 0,
    }
    stamp = datetime.now(UTC)
    created = []
    with clickhouse.get_connection() as client:
        try:
            # Clone migration-owned schemas. Freeze membership once so a concurrent
            # domains EXCHANGE cannot mix inventory generations between batches.
            for temporary, base in ((frozen, "corpscout.domains"),
                                    (companies, TABLE), (stage, TABLE)):
                clone = "CLONE AS" if temporary == frozen else "AS"
                client.execute(f"CREATE TABLE {temporary} {clone} {base}")
                created.append(temporary)
            # Company aggregation is small but sorted by company_id upstream. Do
            # it once, not once per domain range. Country remains part of identity.
            client.execute(f"""INSERT INTO {companies}
                (root_domain,company_count,has_dns_records)
                SELECT root_domain,uniqExact(('SE',company_id)),0
                FROM corpscout.se_company_domain FINAL
                WHERE active=1 AND association='connected' GROUP BY root_domain
            """, settings={**settings, "optimize_aggregation_in_order": 0}, query_id=f"{prefix}companies")
            [(expected,)] = client.execute(f"SELECT count() FROM {frozen}")
            if not expected:
                raise ValueError("Domains inventory is empty; refusing search publication")
            after = ""
            batches = 0
            while True:
                boundary = client.execute(
                    f"SELECT root_domain FROM {frozen} WHERE root_domain > %(after)s "
                    "ORDER BY root_domain LIMIT 1 OFFSET %(batch)s",
                    {"after": after, "batch": config.batch_rows}, settings=settings,
                    query_id=f"{prefix}boundary-{batches}",
                )
                through = boundary[0][0] if boundary else None
                selection = "root_domain > %(after)s"
                if through is not None:
                    selection += " AND root_domain <= %(through)s"
                # DNS presence and max(last_seen) are idempotent over unmerged
                # physical rows. No FINAL, wide RDATA reads or distinct-ID state
                # is needed to answer these two questions.
                client.execute(f"""INSERT INTO {stage} ({','.join(COLUMNS)})
                    SELECT d.root_domain,d.sources,toUInt8(isNotNull(n.last_observed)),n.last_observed,
                        coalesce(w.websites,0),coalesce(w.observed,0),coalesce(c.company_count,0),
                        d.first_seen_at,d.last_seen_at,%(stamp)s,%(run)s
                    FROM (SELECT * FROM {frozen} WHERE {selection}) d
                    LEFT ANY JOIN (
                        SELECT root_domain,max(last_seen) AS last_observed
                        FROM corpscout.commoncrawl_domain_dns_records WHERE {selection}
                        GROUP BY root_domain ORDER BY root_domain
                    ) n ON d.root_domain=n.root_domain
                    LEFT ANY JOIN (
                        SELECT root_domain,count() AS websites,
                            countIf(evidence_status='observed') AS observed
                        FROM corpscout.websites WHERE {selection}
                        GROUP BY root_domain ORDER BY root_domain
                    ) w ON d.root_domain=w.root_domain
                    LEFT ANY JOIN (
                        SELECT root_domain,company_count FROM {companies} WHERE {selection}
                    ) c ON d.root_domain=c.root_domain
                """, {"after": after, "through": through, "stamp": stamp, "run": run_id},
                    settings=settings, query_id=f"{prefix}batch-{batches}")
                batches += 1
                log.info("Published staging range %s through %s", batches, through or "end")
                if through is None:
                    break
                after = through
            [(actual,)] = client.execute(f"SELECT count() FROM {stage}")
            if actual != expected:
                raise ValueError(f"Domain search membership changed: expected {expected}, got {actual}")
            client.execute(f"EXCHANGE TABLES {stage} AND {TABLE}", query_id=f"{prefix}publish")
            return {"dagster/row_count": actual, "batches": batches,
                    "refreshed_at": stamp.isoformat(), "source_run_id": run_id}
        except BaseException:
            client.disconnect()
            client.execute("KILL QUERY WHERE startsWith(query_id,%(prefix)s) SYNC", {"prefix": prefix})
            raise
        finally:
            for temporary in reversed(created):
                client.execute(f"DROP TABLE IF EXISTS {temporary}")


@dg.asset(
    group_name="domains", kinds={"clickhouse"}, pool="domains_search_publish",
    deps=["domains", "websites", "se_company_domain_publish",
          dg.AssetKey(["corpscout", "commoncrawl_domain_dns_records"])],
    metadata={"dagster/table_name": TABLE},
    description="Precomputed domain filters. Membership comes only from domains; DNS, websites and active connected Swedish companies enrich existing roots.",
)
def domains_search(
    context: dg.AssetExecutionContext, config: DomainsSearchConfig,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(metadata=publish_domains_search(
        clickhouse, config=config, run_id=context.run.run_id, log=context.log,
    ))


domains_search_job = dg.define_asset_job("domains_search_job", selection=dg.AssetSelection.assets(domains_search))
defs = dg.Definitions(assets=[domains_search], jobs=[domains_search_job])
