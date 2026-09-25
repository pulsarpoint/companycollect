"""Publish a root-domain inventory from existing ClickHouse source datasets."""

from datetime import UTC, datetime
from uuid import uuid4

import dagster as dg
from dagster_clickhouse import ClickhouseResource
from pydantic import Field

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource

TABLE = "corpscout.domains"
COLUMNS = ("root_domain", "sources", "first_seen_at", "last_seen_at", "source_run_id")

# Inventory membership is limited to these three explicitly selected source tables.
SOURCES = (
    (
        "se_company_domain",
        "SELECT root_domain FROM corpscout.se_company_domain GROUP BY root_domain",
    ),
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
        clickhouse, database="corpscout", tables=("domains",)
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
            if previous == 0:
                raise ValueError(
                    "All domain inventory sources are empty; refusing to replace the published inventory"
                )
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
                    SELECT roots.root_domain,roots.sources,coalesce(old.first_seen_at,%(stamp)s),%(stamp)s,%(run)s
                    FROM (
                        SELECT root_domain,arraySort(groupUniqArrayArray(sources)) AS sources
                        FROM {contributions} WHERE {domain_range} AND {VALID_DOMAIN}
                        GROUP BY root_domain ORDER BY root_domain
                    ) AS roots
                    LEFT ANY JOIN (
                        SELECT root_domain,first_seen_at FROM {TABLE} WHERE {domain_range}
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
    group_name="domains",
    kinds={"clickhouse"},
    pool="commoncrawl_domain_graph",
    deps=["se_company_domain_publish", "commoncrawl_domain_graph_active"],
    metadata={"dagster/table_name": TABLE},
    description="Publish root domains from commoncrawl_domains, published Common Crawl graph nodes and se_company_domain, combining their source labels.",
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


domains_job = dg.define_asset_job(
    "domains_job", selection=dg.AssetSelection.assets(domains)
)
defs = dg.Definitions(assets=[domains], jobs=[domains_job])
