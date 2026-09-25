"""Activate a complete release, then retire only its predecessor's full graph."""

import dagster as dg
from dagster_clickhouse import ClickhouseResource

from dagster_v3.defs.common.resources import ObjectStoreResource
from dagster_v3.defs.commoncrawl_domain_graph.assets import GROUP, PARTITIONS, POOL
from dagster_v3.defs.commoncrawl_domain_graph.load import (
    DATABASE,
    NODES,
    EDGES,
    SNAPSHOTS,
    assert_immutable_release,
)
from dagster_v3.defs.commoncrawl_domain_graph.ranks import RANKS
from dagster_v3.defs.commoncrawl_domain_graph.source import GraphSource
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource

# Backoffice queries time out at 20 seconds. Allow already-started readers to finish.
RETIREMENT_GRACE_SECONDS = 600


def activate_graph(store, client, source: GraphSource) -> bool:
    release = source.graph_release
    if not assert_immutable_release(client, source):
        raise ValueError("Graph snapshot must be validated before activation")
    for table, expected in (
        (NODES, source.nodes),
        (EDGES, source.edges),
        (RANKS, source.nodes),
    ):
        count = client.execute(
            f"SELECT count() FROM {DATABASE}.{table} WHERE graph_release=%(release)s",
            {"release": release},
        )[0][0]
        if count != expected:
            raise ValueError(f"Cannot activate {release}: incomplete {table}")
    for table, projection, expected in (
        (NODES, "by_node_id_lookup", source.nodes),
        (EDGES, "by_target", source.edges),
    ):
        count = client.execute(
            "SELECT sum(rows) FROM system.projection_parts WHERE database=%(db)s AND table=%(table)s AND name=%(projection)s AND partition=%(release)s AND active",
            {
                "db": DATABASE,
                "table": table,
                "projection": projection,
                "release": release,
            },
        )[0][0]
        if count != expected:
            raise ValueError(f"Incomplete graph projection {projection}")
    # Exercise both index directions using a real endpoint, with serving-query limits.
    seed = client.execute(
        f"SELECT source_node_id FROM {DATABASE}.{EDGES} WHERE graph_release=%(release)s LIMIT 1",
        {"release": release},
    )[0][0]
    for field in ("source_node_id", "target_node_id"):
        client.execute(
            f"SELECT source_node_id,target_node_id FROM {DATABASE}.{EDGES} WHERE graph_release=%(release)s AND {field}=%(seed)s LIMIT 10",
            {"release": release, "seed": seed},
            settings={
                "max_execution_time": 20,
                "max_memory_usage": 2_000_000_000,
                "max_threads": 4,
            },
        )
    with store.transaction() as cursor:
        cursor.execute(
            "SELECT active_graph_release FROM commoncrawl_graph_state WHERE singleton FOR UPDATE"
        )
        current = cursor.fetchone()["active_graph_release"]
        cursor.execute(
            "SELECT graph_release,coverage_end FROM commoncrawl_graph_releases WHERE graph_release=ANY(%s)",
            ([release, current],),
        )
        dates = {row["graph_release"]: row["coverage_end"] for row in cursor.fetchall()}
        if dates.get(release) is None or (
            current is not None and dates.get(current) is None
        ):
            raise ValueError(
                "Coverage dates are required before switching the active graph"
            )
        if current == release:
            return False
        if current is not None and dates[release] <= dates[current]:
            # A slower, older concurrent run must never move the pointer backwards.
            cursor.execute(
                "UPDATE commoncrawl_graph_releases SET graph_status='retired',graph_retired_at=now() WHERE graph_release=%s",
                (release,),
            )
            return False
        cursor.execute(
            "UPDATE commoncrawl_graph_releases SET graph_status='retained',graph_retired_at=NULL WHERE graph_release=%s",
            (release,),
        )
        if current is not None:
            cursor.execute(
                "UPDATE commoncrawl_graph_releases SET graph_status='retired',graph_retired_at=now() WHERE graph_release=%s",
                (current,),
            )
        cursor.execute(
            "UPDATE commoncrawl_graph_state SET active_graph_release=%s,updated_at=now(),updated_by='dagster' WHERE singleton",
            (release,),
        )
    return True


def cleanup_retired_graphs(
    store, client, objects, grace_seconds=RETIREMENT_GRACE_SECONDS
) -> list[str]:
    if grace_seconds < RETIREMENT_GRACE_SECONDS:
        raise ValueError("Graph retirement grace cannot be shorter than ten minutes")
    cleaned = []
    with store.transaction() as cursor:
        # Shared with activation; transaction-scoped locking also works through PgBouncer.
        cursor.execute(
            "SELECT active_graph_release FROM commoncrawl_graph_state WHERE singleton FOR UPDATE"
        )
        active = cursor.fetchone()["active_graph_release"]
        if active is None:
            return []
        cursor.execute(
            """SELECT graph_release FROM commoncrawl_graph_releases r WHERE graph_status='retired'
            AND graph_release<>%s AND graph_retired_at < now() - %s * interval '1 second'
            AND NOT EXISTS (SELECT 1 FROM commoncrawl_graph_import_requests q WHERE q.graph_release=r.graph_release
                AND q.status IN ('queued','launching','running'))""",
            (active, grace_seconds),
        )
        for row in cursor.fetchall():
            release = row["graph_release"]
            # Hide the marker first. Retrying after any partial cleanup is safe.
            client.execute(
                f"ALTER TABLE {DATABASE}.{SNAPSHOTS} DELETE WHERE graph_release=%(release)s",
                {"release": release},
                settings={"mutations_sync": 2},
            )
            for table in (NODES, EDGES):
                client.execute(
                    f"ALTER TABLE {DATABASE}.{table} DROP PARTITION %(release)s",
                    {"release": release},
                )
            cursor.execute(
                "SELECT cache_bucket,cache_key FROM commoncrawl_graph_release_files WHERE graph_release=%s AND artifact_kind IN ('nodes','edges') AND cache_key IS NOT NULL",
                (release,),
            )
            for cached in cursor.fetchall():
                # Single-object deletion raises on failure, unlike unchecked multi-delete.
                objects.client().delete_object(
                    Bucket=cached["cache_bucket"], Key=cached["cache_key"]
                )
            cursor.execute(
                """UPDATE commoncrawl_graph_release_files SET cache_bucket=NULL,cache_key=NULL,
                cached_source_etag=NULL,cached_bytes=NULL,cached_sha256=NULL,cached_at=NULL
                WHERE graph_release=%s AND artifact_kind IN ('nodes','edges')""",
                (release,),
            )
            cleaned.append(release)
    return cleaned


@dg.asset(
    group_name=GROUP,
    pool=POOL,
    partitions_def=PARTITIONS,
    backfill_policy=dg.BackfillPolicy.multi_run(max_partitions_per_run=1),
    kinds={"postgres", "clickhouse"},
    deps=["commoncrawl_domain_graph_snapshots", "commoncrawl_domain_graph_ranks"],
)
def commoncrawl_domain_graph_active(
    context: dg.AssetExecutionContext,
    commoncrawl_domain_graph_source: GraphSource,
    graph_catalog: GraphCatalogResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    if commoncrawl_domain_graph_source.graph_release != context.partition_key:
        raise ValueError("Graph source does not match release partition")
    with graph_catalog.get_store() as store, clickhouse.get_connection() as client:
        changed = activate_graph(store, client, commoncrawl_domain_graph_source)
    return dg.MaterializeResult(
        metadata={
            "graph_release": context.partition_key,
            "active_pointer_changed": changed,
        }
    )


@dg.asset(group_name=GROUP, pool=POOL, kinds={"postgres", "clickhouse", "s3"})
def commoncrawl_domain_graph_cleanup(
    context: dg.AssetExecutionContext,
    graph_catalog: GraphCatalogResource,
    graph_objects: ObjectStoreResource,
    clickhouse: ClickhouseResource,
) -> dg.MaterializeResult:
    with graph_catalog.get_store() as store, clickhouse.get_connection() as client:
        releases = cleanup_retired_graphs(store, client, graph_objects)
    return dg.MaterializeResult(metadata={"cleaned_releases": releases})


commoncrawl_graph_cleanup_job = dg.define_asset_job(
    "commoncrawl_graph_cleanup_job",
    selection=dg.AssetSelection.assets(commoncrawl_domain_graph_cleanup),
)
commoncrawl_graph_cleanup_schedule = dg.ScheduleDefinition(
    name="commoncrawl_graph_cleanup_schedule",
    job=commoncrawl_graph_cleanup_job,
    cron_schedule="43 * * * *",
    execution_timezone="UTC",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
