"""Publish a root-domain inventory from existing ClickHouse source datasets."""

from datetime import UTC, datetime
from uuid import uuid4

import dagster as dg
from corpscout_identity.coordination import DOMAIN_COLUMNS, inventory_publication_lock
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource
from dagster_v3.defs.domains.publication import retain_registered_identities
from dagster_v3.defs.domains.registration import PUBLISH_POOL
from dagster_v3.defs.domains.source_backfill import domains_sources_backfill
from dagster_v3.defs.domains.sources import domains_sources

TABLE = "corpscout.domains"
COLUMNS = DOMAIN_COLUMNS

# Bulk discovery sources. Country parents are registered before summary publication.
SOURCES = (
    (
        "commoncrawl",
        "SELECT root_domain FROM corpscout.commoncrawl_domains GROUP BY root_domain",
    ),
    (
        "commoncrawl_graph",
        "SELECT root_domain FROM corpscout.commoncrawl_domain_graph_nodes WHERE graph_release IN (SELECT graph_release FROM corpscout.commoncrawl_domain_graph_snapshots FINAL)",
    ),
)

VALID_DOMAIN = """length(root_domain) <= 253
    AND match(root_domain, '^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:[.][a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$')
    AND NOT match(root_domain, '^[0-9]+(?:[.][0-9]+){3}$')"""


class DomainsConfig(dg.Config):
    max_threads: int = Field(default=4, ge=1, le=16)
    max_execution_time: int = Field(default=3600, ge=1, le=14400)
    merge_batch_rows: int = Field(default=1_000_000, ge=1, le=1_000_000)


def publish_inventory(
    clickhouse: ClickhouseResource,
    *,
    run_id: str,
    config: DomainsConfig,
    log,
    graph_release: str | None = None,
) -> dict:
    assert_clickhouse_tables_exist(
        clickhouse, database="corpscout", tables=("domains", "domains_sources")
    )
    suffix = uuid4().hex
    contributions = f"{TABLE}_sources_{suffix}"
    stage = f"{TABLE}_stage_{suffix}"
    query_prefix = f"domains:{suffix}:"
    stamp = datetime.now(UTC)
    params = {"stamp": stamp, "run": run_id}
    settings = {
        "max_threads": config.max_threads,
        "max_execution_time": config.max_execution_time,
        "max_memory_usage": 4 * 1024**3,
        "max_bytes_before_external_group_by": 512 * 1024**2,
        "max_bytes_before_external_sort": 512 * 1024**2,
        "optimize_aggregation_in_order": 1,
        "async_insert": 0,
    }
    created = []
    source_counts = []
    with clickhouse.get_connection() as client:
        try:
            for table in (contributions, stage):
                client.execute(f"CREATE TABLE {table} AS {TABLE}")
                created.append(table)
            # Repeated input observations need only the natural key. Build the
            # central ID and its lookup projection once, on the final parent stage.
            client.execute(f"ALTER TABLE {contributions} DROP PROJECTION IF EXISTS domains_by_id")
            client.execute(f"ALTER TABLE {contributions} DROP COLUMN IF EXISTS domain_id")
            previous = 0
            for index, (source, query) in enumerate(SOURCES):
                if source == "commoncrawl_graph" and graph_release is not None:
                    query = "SELECT root_domain FROM corpscout.commoncrawl_domain_graph_nodes WHERE graph_release=%(graph_release)s"
                    params["graph_release"] = graph_release
                log.info("Reading domain inventory source %s: %s", source, query)
                client.execute(
                    f"""INSERT INTO {contributions} ({",".join(COLUMNS)})
                    SELECT canonical_domain AS root_domain,[%(source)s],%(stamp)s,%(stamp)s,%(run)s FROM (
                        SELECT lowerUTF8(trimRight(trimBoth(root_domain), '.')) AS canonical_domain
                        FROM ({query})
                    )""",
                    {**params, "source": source},
                    settings=settings,
                    query_id=f"{query_prefix}source-{index}",
                )
                [(count,)] = client.execute(f"SELECT count() FROM {contributions}")
                source_counts.append(
                    {"source": source, "relation": query, "rows": count - previous}
                )
                log.info(
                    "Inventory source %s contributed %s candidate roots",
                    source,
                    count - previous,
                )
                previous = count
            # Country contributions are read through the index, so adding another
            # country does not require another country-table scan here.
            client.execute(f"""INSERT INTO {contributions} ({','.join(COLUMNS)})
                SELECT d.root_domain,arraySort(groupUniqArray(s.source_table)),
                    min(d.first_seen_at),max(s.last_seen_at),%(run)s
                FROM (SELECT domain_id,source_table,last_seen_at FROM corpscout.domains_sources FINAL
                    WHERE source_table NOT IN ('commoncrawl_domains','commoncrawl_domain_graph_nodes')) AS s
                INNER JOIN (SELECT domain_id,root_domain,first_seen_at FROM corpscout.domains
                    WHERE domain_id IN (SELECT domain_id FROM corpscout.domains_sources FINAL
                        WHERE source_table NOT IN ('commoncrawl_domains','commoncrawl_domain_graph_nodes'))) AS d ON d.domain_id=s.domain_id
                GROUP BY d.root_domain
            """, params, settings={**settings, "optimize_aggregation_in_order": 0})
            [(previous,)] = client.execute(f"SELECT count() FROM {contributions}")
            if previous == 0:
                raise ValueError(
                    "All domain inventory sources are empty; refusing to replace the published inventory"
                )
            # Domain identities outlive any individual source. Keep every existing
            # parent, even after its last source is withdrawn. Late registrations
            # are reconciled under the shared guard just before EXCHANGE.
            client.execute(f"""INSERT INTO {contributions} ({','.join(COLUMNS)})
                SELECT root_domain,[],first_seen_at,last_seen_at,source_run_id FROM {TABLE}
            """, settings=settings)
            after = ""
            merge_batches = 0
            while True:
                boundary = client.execute(
                    f"""SELECT root_domain FROM {contributions}
                    WHERE root_domain > %(after)s ORDER BY root_domain
                    LIMIT 1 OFFSET %(batch)s""",
                    {"after": after, "batch": config.merge_batch_rows},
                    settings={**settings, "optimize_read_in_order": 1},
                    query_id=f"{query_prefix}boundary-{merge_batches}",
                )
                through = boundary[0][0] if boundary else None
                domain_range = "root_domain > %(after)s"
                if through is not None:
                    domain_range += " AND root_domain <= %(through)s"
                client.execute(
                    f"""INSERT INTO {stage} ({",".join(COLUMNS)})
                    SELECT roots.root_domain,roots.sources,coalesce(old.first_seen_at,%(stamp)s),
                        if(empty(roots.sources),coalesce(old.last_seen_at,%(stamp)s),%(stamp)s),%(run)s
                    FROM (
                        SELECT root_domain,arraySort(groupUniqArrayArray(sources)) AS sources
                        FROM {contributions} WHERE {domain_range} AND {VALID_DOMAIN}
                        GROUP BY root_domain ORDER BY root_domain
                    ) AS roots
                    LEFT ANY JOIN (
                        SELECT root_domain,first_seen_at,last_seen_at FROM {TABLE} WHERE {domain_range}
                    ) AS old ON roots.root_domain=old.root_domain""",
                    {**params, "after": after, "through": through},
                    settings={
                        **settings,
                        # Bound each merge as well as spilling: a whole-inventory
                        # merge of spilled files can itself exceed the memory cap.
                        "optimize_aggregation_in_order": 0,
                        "join_algorithm": "full_sorting_merge",
                        "join_use_nulls": 1,
                    },
                    query_id=f"{query_prefix}fold-{merge_batches}",
                )
                merge_batches += 1
                [(staged,)] = client.execute(f"SELECT count() FROM {stage}")
                log.info(
                    "Merged domain range %s through %s: %s staged domains",
                    merge_batches,
                    through or "end",
                    staged,
                )
                if through is None:
                    break
                after = through
            [(count,)] = client.execute(f"SELECT count() FROM {stage}")
            if count == 0:
                raise ValueError(
                    "Domain inventory build is empty; refusing publication"
                )
            with inventory_publication_lock():
                retained = retain_registered_identities(
                    client, stage=stage, table="domains", settings=settings,
                    query_id=f"{query_prefix}retain-registrations",
                )
                count += retained
                log.info("Preserved %s domains registered during inventory construction", retained)
                client.execute(
                    f"EXCHANGE TABLES {stage} AND {TABLE}",
                    query_id=f"{query_prefix}publish",
                )
                return {
                    "domains": count,
                    "source_contributions": source_counts,
                    "source_run_id": run_id,
                    "merge_batches": merge_batches,
                }
        except BaseException:
            # A disconnected caller does not prove a server-side INSERT stopped.
            # Fence this build before dropping only its own staging tables.
            client.disconnect()
            client.execute(
                "KILL QUERY WHERE startsWith(query_id,%(prefix)s) SYNC",
                {"prefix": query_prefix},
            )
            raise
        finally:
            for table in reversed(created):
                client.execute(f"DROP TABLE IF EXISTS {table}")


@dg.asset(
    group_name="domains", kinds={"clickhouse"}, pool=PUBLISH_POOL,
    deps=["se_company_domain_publish", "commoncrawl_domain_graph_active"],
    metadata={"dagster/table_name": TABLE},
)
def domains(
    context: dg.AssetExecutionContext,
    config: DomainsConfig,
    clickhouse: ClickhouseResource,
    graph_catalog: GraphCatalogResource,
) -> dg.MaterializeResult:
    with graph_catalog.get_store() as store, store.transaction() as cursor:
        cursor.execute(
            "SELECT active_graph_release FROM commoncrawl_graph_state WHERE singleton"
        )
        release = cursor.fetchone()["active_graph_release"]
    if release is None:
        raise ValueError(
            "Activate a validated Common Crawl graph before publishing the domain inventory"
        )
    result = publish_inventory(
        clickhouse,
        run_id=context.run_id,
        config=config,
        log=context.log,
        graph_release=release,
    )
    return dg.MaterializeResult(metadata=result)


@dg.asset(
    group_name="domains", kinds={"clickhouse"}, pool="domains_company_filter_refresh",
    deps=["domains", "se_company_domain_publish"],
    metadata={"dagster/table_name": "corpscout.domains_company_filter"},
    description="Refresh the derived domain/company filter and wait for its atomic publication. Company identity includes country; withdrawn associations disappear on refresh.",
)
def domains_company_filter(
    context: dg.AssetExecutionContext, clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    assert_clickhouse_tables_exist(clickhouse, database="corpscout", tables=(
        "domains", "se_company_domain_resolved", "domains_company_filter",
    ))
    with clickhouse.get_connection() as client:
        context.log.info("Refreshing the domain/company filter snapshot")
        client.execute("SYSTEM REFRESH VIEW corpscout.domains_company_filter")
        client.execute("SYSTEM WAIT VIEW corpscout.domains_company_filter")
        [(count,)] = client.execute("SELECT count() FROM corpscout.domains_company_filter")
    return dg.MaterializeResult(metadata={"dagster/row_count": count})


domains_job = dg.define_asset_job(
    "domains_job", selection=dg.AssetSelection.assets(domains, domains_sources, domains_company_filter)
)
domains_company_filter_job = dg.define_asset_job(
    "domains_company_filter_job", selection=dg.AssetSelection.assets(domains_company_filter),
)
defs = dg.Definitions(assets=[domains, domains_sources, domains_sources_backfill, domains_company_filter], jobs=[domains_job, domains_company_filter_job])
