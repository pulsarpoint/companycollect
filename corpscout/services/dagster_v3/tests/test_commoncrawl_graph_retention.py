"""Switch and retire real graph partitions, retaining every rank partition."""

from dataclasses import replace
import logging

import pytest

from dagster_v3.defs.commoncrawl_domain_graph import source as source_module
from dagster_v3.defs.commoncrawl_domain_graph.assets import graph_artifact
from dagster_v3.defs.commoncrawl_domain_graph.download import cache_artifact
from dagster_v3.defs.commoncrawl_domain_graph.load import (
    load_graph_file,
    publish_snapshot,
)
from dagster_v3.defs.commoncrawl_domain_graph.retention import (
    activate_graph,
    cleanup_retired_graphs,
)
from dagster_v3.defs.commoncrawl_domain_graph.store import GraphStore
from tests.test_commoncrawl_domain_graph_integration import (
    graph_ch as graph_ch,
    graph_http as graph_http,
)
from tests.test_commoncrawl_graph_ranks import (
    rank_http as rank_http,
    graph_objects as graph_objects,
    rank_ch as rank_ch,
)
from tests.test_commoncrawl_graph_migrations import catalog_db as catalog_db
from tests.test_processing_store import (
    processing_postgres_url as processing_postgres_url,
)


def test_activation_requires_ranks_and_cleanup_keeps_history(
    rank_ch, graph_http, graph_objects, catalog_db, monkeypatch
):
    client = rank_ch
    release, port, _ = graph_http
    monkeypatch.setattr(source_module, "BASE_URL", f"http://127.0.0.1:{port}")
    first = source_module.resolve_graph_source(release)
    second = replace(first, graph_release="cc-main-2026-jul-aug-sep")
    store = GraphStore(catalog_db[0])
    objects = graph_objects[0]
    for source, date in ((first, "2026-08-01"), (second, "2026-09-01")):
        with store.transaction() as cursor:
            cursor.execute(
                "INSERT INTO commoncrawl_graph_releases (graph_release,source_index_url,coverage_end) VALUES (%s,%s,%s)",
                (source.graph_release, source.source_index_url, date),
            )
        for kind in ("nodes", "edges"):
            artifact = cache_artifact(
                graph_artifact(source, kind), objects, logging.getLogger(__name__)
            )
            load_graph_file(
                client,
                source,
                kind,
                "test",
                logging.getLogger(__name__),
                cached=artifact,
            )
            with store.transaction() as cursor:
                cursor.execute(
                    "INSERT INTO commoncrawl_graph_release_files (graph_release,artifact_kind,availability,source_url,source_etag) VALUES (%s,%s,'available',%s,%s)",
                    (
                        source.graph_release,
                        kind,
                        artifact.source.source_url,
                        artifact.source.source_etag,
                    ),
                )
            store.record_cache(artifact)
        publish_snapshot(client, source, "test")
        with pytest.raises(ValueError, match="incomplete"):
            activate_graph(store, client, source)
        client.execute(
            """INSERT INTO corpscout.commoncrawl_domain_graph_ranks
            SELECT %(release)s,root_domain,1,node_id+1,1,node_id+1,n_hosts,'test',now64(3)
            FROM corpscout.commoncrawl_domain_graph_nodes WHERE graph_release=%(release)s""",
            {"release": source.graph_release},
        )
        assert store.latest_ranking_release(client) == source.graph_release
        assert activate_graph(store, client, source)
        assert not activate_graph(store, client, source)
    assert not activate_graph(
        store, client, first
    )  # Late older run cannot rewind active pointer.
    assert cleanup_retired_graphs(store, client, objects) == []
    with store.transaction() as cursor:
        cursor.execute(
            "UPDATE commoncrawl_graph_releases SET graph_retired_at=now()-interval '11 minutes' WHERE graph_status='retired'"
        )
    assert cleanup_retired_graphs(store, client, objects) == [first.graph_release]
    assert cleanup_retired_graphs(store, client, objects) == [
        first.graph_release
    ]  # Retry is harmless.
    assert client.execute(
        "SELECT graph_release,count() FROM corpscout.commoncrawl_domain_graph_ranks GROUP BY graph_release ORDER BY graph_release"
    ) == sorted([(first.graph_release, 6), (second.graph_release, 6)])
    assert client.execute(
        "SELECT graph_release,count() FROM corpscout.commoncrawl_domain_graph_nodes GROUP BY graph_release"
    ) == [(second.graph_release, 6)]
    assert client.execute(
        "SELECT graph_release,count() FROM corpscout.commoncrawl_domain_graph_edges GROUP BY graph_release"
    ) == [(second.graph_release, 8)]
    assert client.execute(
        "SELECT graph_release FROM corpscout.commoncrawl_domain_graph_snapshots FINAL"
    ) == [(second.graph_release,)]
    assert not any(first.graph_release in key for key in objects.list_keys("domain/"))


def test_full_dagster_job_and_retry_reuse_loaded_partitions(
    rank_ch, graph_http, graph_objects, rank_http, catalog_db, monkeypatch
):
    import dagster as dg
    import gzip
    from datetime import UTC, datetime, date
    from dagster_clickhouse import ClickhouseResource
    from dagster_v3.defs.commoncrawl_domain_graph import assets, rank_assets, retention
    from dagster_v3.defs.commoncrawl_domain_graph.automation import reconcile_requests
    from dagster_v3.defs.commoncrawl_domain_graph.catalog import (
        GraphRelease,
        ReleaseFile,
    )
    from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource
    from tests.test_commoncrawl_graph_ranks import HEADER, rank_source

    for table in ("ranks", "snapshots", "edges", "nodes"):
        rank_ch.execute(f"TRUNCATE TABLE corpscout.commoncrawl_domain_graph_{table}")
    release, port, gets = graph_http
    monkeypatch.setattr(source_module, "BASE_URL", f"http://127.0.0.1:{port}")
    source = source_module.resolve_graph_source(release)
    state, url = rank_http
    state["body"] = gzip.compress(
        HEADER
        + b"".join(
            f"{i + 1}\t10\t{i + 1}\t0.4\t{domain}\t1\n".encode()
            for i, domain in enumerate(
                (
                    "com.original",
                    "se.related",
                    "com.oneway",
                    "com.cycle",
                    "com.return",
                    "com.inbound",
                )
            )
        )
    )
    rank = replace(rank_source(state, url, release), expected_rows=6)
    files = [graph_artifact(source, "nodes"), graph_artifact(source, "edges"), rank]
    store = GraphStore(catalog_db[0])
    store.save_catalog(
        [
            GraphRelease(
                release,
                source.source_index_url,
                [],
                date(2026, 6, 1),
                date(2026, 8, 1),
                [
                    ReleaseFile(
                        f.artifact_kind,
                        f.source_url,
                        f.source_etag,
                        f.source_bytes,
                        f.expected_rows,
                        "available",
                    )
                    for f in files
                ],
            )
        ],
        datetime.now(UTC),
    )
    all_assets = [
        value
        for module in (assets, rank_assets, retention)
        for value in vars(module).values()
        if isinstance(value, dg.AssetsDefinition)
    ]
    defs = dg.Definitions(
        assets=all_assets,
        jobs=[assets.commoncrawl_domain_graph_job],
        resources={
            "clickhouse": ClickhouseResource(
                host="127.0.0.1",
                port=rank_ch.connection.port,
                user="test",
                password="test",
                database="corpscout",
            ),
            "graph_catalog": GraphCatalogResource(postgres_url=catalog_db[1]),
            "graph_objects": graph_objects[0],
        },
    )
    with dg.DagsterInstance.ephemeral() as instance:
        instance.add_dynamic_partitions(assets.PARTITIONS.name, [release])
        job = defs.resolve_job_def("commoncrawl_domain_graph_job")
        assert job.execute_in_process(instance=instance, partition_key=release).success
        reconcile_requests(store, instance)
        before = (state["gets"], sum(path.endswith(".gz") for path in gets))
        # Expired raw objects do not force a redownload of already validated serving data.
        for key in graph_objects[0].list_keys(f"domain/{release}/"):
            graph_objects[0].client().delete_object(
                Bucket=graph_objects[0].bucket, Key=key
            )
        assert job.execute_in_process(instance=instance, partition_key=release).success
        assert before == (state["gets"], sum(path.endswith(".gz") for path in gets))
        reconcile_requests(store, instance)
    with store.transaction() as cursor:
        cursor.execute("SELECT status FROM commoncrawl_graph_import_requests")
        assert [row["status"] for row in cursor.fetchall()] == [
            "succeeded",
            "succeeded",
        ]
