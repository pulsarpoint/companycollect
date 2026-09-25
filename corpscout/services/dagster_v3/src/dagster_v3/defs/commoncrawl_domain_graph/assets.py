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
    assert_immutable_release,
    file_summary,
    validate_file_rows,
    publish_snapshot,
)
from dagster_v3.defs.commoncrawl_domain_graph.source import (
    GraphSource,
    resolve_graph_source,
)

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.commoncrawl_domain_graph.download import (
    ArtifactSource,
    CachedArtifact,
    cache_artifact,
)
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource

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
    graph_catalog: GraphCatalogResource,
) -> GraphSource:
    """Published graph statistics, file URLs and immutable HTTP validators for one release."""
    source = resolve_graph_source(context.partition_key)
    with graph_catalog.get_store() as store:
        manifest = store.pin_run(
            context.partition_key,
            context.run_id,
            "full",
            context.run.tags.get("commoncrawl/request_id"),
        )
    for kind in ("nodes", "edges"):
        if manifest[kind] != asdict(graph_artifact(source, kind)):
            raise ValueError(
                "Graph source changed since the import manifest was pinned"
            )
    context.add_output_metadata(asdict(source))
    return source


def graph_artifact(source: GraphSource, kind: str) -> ArtifactSource:
    return ArtifactSource(
        source.graph_release,
        kind,
        source.vertices_url if kind == "nodes" else source.edges_url,
        source.vertices_etag if kind == "nodes" else source.edges_etag,
        source.vertices_bytes if kind == "nodes" else source.edges_bytes,
        source.nodes if kind == "nodes" else source.edges,
    )


def cache_graph_file(context, source, kind, objects, catalog, clickhouse):
    if context.partition_key != source.graph_release:
        raise ValueError(
            "Graph source does not match the materialized release partition"
        )
    with clickhouse.get_connection() as client:
        assert_immutable_release(client, source)
        table = NODES if kind == "nodes" else EDGES
        expected = source.nodes if kind == "nodes" else source.edges
        count = client.execute(
            f"SELECT count() FROM {DATABASE}.{table} WHERE graph_release=%(release)s",
            {"release": source.graph_release},
        )[0][0]
        if count == expected:
            validate_file_rows(
                kind, file_summary(client, table, kind, source.graph_release), source
            )
            context.add_output_metadata({"reused_loaded_partition": True})
            return None
    objects.ensure_bucket()
    artifact = cache_artifact(graph_artifact(source, kind), objects, context.log)
    with catalog.get_store() as store:
        store.record_cache(artifact)
    context.add_output_metadata({"cache_key": artifact.key, "sha256": artifact.sha256})
    return artifact


@dg.asset(
    group_name=GROUP,
    pool=POOL,
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"http", "s3"},
)
def commoncrawl_domain_graph_nodes_raw(
    context: dg.AssetExecutionContext,
    commoncrawl_domain_graph_source: GraphSource,
    graph_objects: ObjectStoreResource,
    graph_catalog: GraphCatalogResource,
    clickhouse: ClickhouseResource,
) -> CachedArtifact | None:
    return cache_graph_file(
        context,
        commoncrawl_domain_graph_source,
        "nodes",
        graph_objects,
        graph_catalog,
        clickhouse,
    )


@dg.asset(
    group_name=GROUP,
    pool=POOL,
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"http", "s3"},
)
def commoncrawl_domain_graph_edges_raw(
    context: dg.AssetExecutionContext,
    commoncrawl_domain_graph_source: GraphSource,
    graph_objects: ObjectStoreResource,
    graph_catalog: GraphCatalogResource,
    clickhouse: ClickhouseResource,
) -> CachedArtifact | None:
    return cache_graph_file(
        context,
        commoncrawl_domain_graph_source,
        "edges",
        graph_objects,
        graph_catalog,
        clickhouse,
    )


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
    commoncrawl_domain_graph_nodes_raw: CachedArtifact | None,
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
            cached=commoncrawl_domain_graph_nodes_raw,
            require_cached=True,
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
    commoncrawl_domain_graph_edges_raw: CachedArtifact | None,
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
            cached=commoncrawl_domain_graph_edges_raw,
            require_cached=True,
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
    selection=dg.AssetSelection.assets("commoncrawl_domain_graph_active").upstream(),
    description="Load and validate one complete Common Crawl domain graph release into ClickHouse.",
)
