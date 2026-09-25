from dataclasses import dataclass
import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.clickhouse.resolved import assert_clickhouse_tables_exist
from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.commoncrawl_domain_graph.assets import GROUP, PARTITIONS, POOL
from dagster_v3.defs.commoncrawl_domain_graph.download import (
    ArtifactSource,
    CachedArtifact,
    cache_artifact,
)
from dagster_v3.defs.commoncrawl_domain_graph.ranks import RANKS, load_ranks
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource


@dataclass(frozen=True)
class RankingInput:
    source: ArtifactSource
    cached: CachedArtifact | None


@dg.asset(
    group_name=GROUP,
    partitions_def=PARTITIONS,
    pool=POOL,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"http", "s3"},
)
def commoncrawl_domain_graph_ranks_raw(
    context: dg.AssetExecutionContext,
    graph_catalog: GraphCatalogResource,
    graph_objects: ObjectStoreResource,
    clickhouse: ClickhouseResource,
) -> RankingInput:
    """Verified, immutable gzip containing every published domain ranking row."""
    assert_clickhouse_tables_exist(clickhouse, database="corpscout", tables=(RANKS,))
    selection = (
        "full" if context.job_name == "commoncrawl_domain_graph_job" else "ranks"
    )
    with graph_catalog.get_store() as store:
        source = ArtifactSource(
            **store.pin_run(
                context.partition_key,
                context.run_id,
                selection,
                context.run.tags.get("commoncrawl/request_id"),
            )["ranks"]
        )
        with clickhouse.get_connection() as client:
            store.assert_rank_source(client, source)
            count = client.execute(
                "SELECT count() FROM corpscout.commoncrawl_domain_graph_ranks WHERE graph_release=%(release)s",
                {"release": source.graph_release},
            )[0][0]
            if count:
                if count != source.expected_rows:
                    raise ValueError(
                        "Existing ranking partition has unexpected row count"
                    )
                context.add_output_metadata({"reused_loaded_partition": True})
                return RankingInput(source, None)
        graph_objects.ensure_bucket()
        artifact = cache_artifact(source, graph_objects, context.log)
        store.record_cache(artifact)
    context.add_output_metadata(
        {
            "graph_release": source.graph_release,
            "compressed_bytes": source.source_bytes,
            "sha256": artifact.sha256,
            "cache_key": artifact.key,
        }
    )
    return RankingInput(source, artifact)


@dg.asset(
    group_name=GROUP,
    partitions_def=PARTITIONS,
    pool=POOL,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"clickhouse"},
)
def commoncrawl_domain_graph_ranks(
    context: dg.AssetExecutionContext,
    commoncrawl_domain_graph_ranks_raw: RankingInput,
    clickhouse: ClickhouseResource,
    graph_catalog: GraphCatalogResource,
) -> dg.MaterializeResult:
    """Complete ranking history, published by atomic replacement of one release."""
    artifact = commoncrawl_domain_graph_ranks_raw
    if context.partition_key != artifact.source.graph_release:
        raise ValueError("Rank artifact does not match materialization partition")
    assert_clickhouse_tables_exist(clickhouse, database="corpscout", tables=(RANKS,))
    with graph_catalog.get_store() as store, clickhouse.get_connection() as client:
        store.assert_rank_source(client, artifact.source)
        if artifact.cached is None:
            count = client.execute(
                "SELECT count() FROM corpscout.commoncrawl_domain_graph_ranks WHERE graph_release=%(release)s",
                {"release": artifact.source.graph_release},
            )[0][0]
            if count != artifact.source.expected_rows:
                raise ValueError(
                    "Previously published ranks disappeared; rematerialize the raw asset"
                )
            reused = True
        else:
            count, reused = load_ranks(
                client, artifact.cached, context.run_id, context.log
            )
    return dg.MaterializeResult(
        metadata={
            "row_count": count,
            "reused": reused,
            "graph_release": context.partition_key,
        }
    )


commoncrawl_domain_ranks_job = dg.define_asset_job(
    "commoncrawl_domain_ranks_job",
    selection=dg.AssetSelection.assets(commoncrawl_domain_graph_ranks).upstream(),
)
defs = dg.Definitions(
    assets=[commoncrawl_domain_graph_ranks_raw, commoncrawl_domain_graph_ranks],
    jobs=[commoncrawl_domain_ranks_job],
    resources={"graph_objects": ObjectStoreResource(bucket="commoncrawl-graphs")},
)
