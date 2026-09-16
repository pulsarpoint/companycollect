from dataclasses import asdict

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.commoncrawl_domain_graph.load import (
    DATABASE,
    EDGES,
    NODES,
    SNAPSHOTS,
    load_graph_file,
    publish_snapshot,
)
from dagster_v3.defs.commoncrawl_domain_graph.source import (
    GraphSource,
    resolve_graph_source,
)

GROUP = "commoncrawl_domain_graph"
POOL = "commoncrawl_domain_graph"
PARTITIONS = dg.DynamicPartitionsDefinition(name="commoncrawl_domain_graph_release")


@dg.asset(
    group_name=GROUP,
    kinds={"dlt", "http"},
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
)
def commoncrawl_domain_graph_source(
    context: dg.AssetExecutionContext,
) -> GraphSource:
    """Published graph statistics, file URLs and immutable HTTP validators for one release."""
    source = resolve_graph_source(context.partition_key)
    context.add_output_metadata(asdict(source))
    return source


@dg.asset(
    group_name=GROUP,
    pool=POOL,
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"clickhouse"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def commoncrawl_domain_graph_nodes(
    context: dg.AssetExecutionContext,
    commoncrawl_domain_graph_source: GraphSource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """All pay-level domains, indexed by domain name and release-local numeric node ID."""
    if context.partition_key != commoncrawl_domain_graph_source.graph_release:
        raise ValueError(
            "Graph source does not match the materialized release partition"
        )
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(NODES, SNAPSHOTS)
    )
    with clickhouse.get_connection() as client:
        count, reused = load_graph_file(
            client,
            commoncrawl_domain_graph_source,
            "nodes",
            context.run_id,
            context.log,
        )
    return dg.MaterializeResult(
        metadata={
            "row_count": count,
            "reused": reused,
            "graph_release": commoncrawl_domain_graph_source.graph_release,
        }
    )


@dg.asset(
    group_name=GROUP,
    pool=POOL,
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"clickhouse"},
    retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
)
def commoncrawl_domain_graph_edges(
    context: dg.AssetExecutionContext,
    commoncrawl_domain_graph_source: GraphSource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Every directed domain link, indexed by source and target for adjacency queries."""
    if context.partition_key != commoncrawl_domain_graph_source.graph_release:
        raise ValueError(
            "Graph source does not match the materialized release partition"
        )
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(EDGES, SNAPSHOTS)
    )
    with clickhouse.get_connection() as client:
        count, reused = load_graph_file(
            client,
            commoncrawl_domain_graph_source,
            "edges",
            context.run_id,
            context.log,
        )
    return dg.MaterializeResult(
        metadata={
            "row_count": count,
            "reused": reused,
            "graph_release": commoncrawl_domain_graph_source.graph_release,
        }
    )


@dg.asset(
    group_name=GROUP,
    pool=POOL,
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"clickhouse"},
    deps=[commoncrawl_domain_graph_nodes, commoncrawl_domain_graph_edges],
)
def commoncrawl_domain_graph_snapshots(
    context: dg.AssetExecutionContext,
    commoncrawl_domain_graph_source: GraphSource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    """Publish only complete graph releases, making them available to the connections query view."""
    if context.partition_key != commoncrawl_domain_graph_source.graph_release:
        raise ValueError(
            "Graph source does not match the materialized release partition"
        )
    assert_clickhouse_tables_exist(
        clickhouse, database=DATABASE, tables=(NODES, EDGES, SNAPSHOTS)
    )
    with clickhouse.get_connection() as client:
        reused = publish_snapshot(
            client, commoncrawl_domain_graph_source, context.run_id
        )
    return dg.MaterializeResult(
        metadata={
            "graph_release": commoncrawl_domain_graph_source.graph_release,
            "nodes": commoncrawl_domain_graph_source.nodes,
            "edges": commoncrawl_domain_graph_source.edges,
            "reused": reused,
        }
    )


commoncrawl_domain_graph_job = dg.define_asset_job(
    "commoncrawl_domain_graph_job",
    selection=dg.AssetSelection.assets(commoncrawl_domain_graph_snapshots).upstream(),
    description="Load and validate one complete Common Crawl domain graph release into ClickHouse.",
)
