from pathlib import Path

import pytest

from dagster_v3.defs.commoncrawl_domain_graph.source import (
    validate_graph_release,
    parse_graph_stats,
)


def test_graph_stats_use_exact_published_counts():
    assert parse_graph_stats(
        "nodes=119722885\narcs=2450405793\nloops=0\nother=3.4\n"
    ) == {"nodes": 119722885, "edges": 2450405793, "loops": 0}


@pytest.mark.parametrize(
    "text",
    [
        "nodes=2",
        "nodes=0\narcs=5\nloops=0",
        "nodes=2\narcs=4\nloops=5",
        "nodes=4294967297\narcs=4\nloops=0",
    ],
)
def test_invalid_stats_cannot_authorize_publication(text):
    with pytest.raises(ValueError):
        parse_graph_stats(text)


@pytest.mark.parametrize(
    "release",
    ["../other", "https://example.com", "cc-main-2026-jun-jul-aug; DROP TABLE x"],
)
def test_release_is_a_source_identifier_not_a_path_or_sql(release):
    with pytest.raises(ValueError):
        validate_graph_release(release)


def test_migration_indexes_both_edge_directions_and_domain_identifiers():
    migration = (
        Path(__file__).parents[3]
        / "clickhouse/migrations/000418_corpscout_commoncrawl_domain_graph.up.sql"
    )
    sql = migration.read_text()
    assert "PROJECTION by_node_id" in sql
    assert "PROJECTION by_target" in sql
    assert "ORDER BY (graph_release, target_node_id, source_node_id)" in sql
    assert "CREATE VIEW IF NOT EXISTS corpscout.commoncrawl_domain_connections" in sql
    assert "{domain:String}" in sql
    assert "commoncrawl_domain_graph_snapshots FINAL" in sql


def test_registered_job_selects_source_both_files_and_publication():
    import dagster as dg
    from dagster_v3.defs.commoncrawl_domain_graph import assets
    from dagster_clickhouse import ClickhouseResource

    from dagster_v3.defs.commoncrawl_domain_graph import rank_assets, retention
    from dagster_v3.defs.commoncrawl_domain_graph.store import GraphCatalogResource
    from dagster_v3.defs.common.resources import ObjectStoreResource

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
                host="localhost",
                port=9000,
                user="test",
                password="test",
                database="corpscout",
            ),
            "graph_catalog": GraphCatalogResource(
                postgres_url="postgresql://localhost/test"
            ),
            "graph_objects": ObjectStoreResource(bucket="commoncrawl-graphs"),
        },
    )
    job = defs.resolve_job_def("commoncrawl_domain_graph_job")
    assert {key.to_user_string() for key in job.asset_layer.executable_asset_keys} == {
        "commoncrawl_domain_graph_" + suffix
        for suffix in (
            "source",
            "nodes_raw",
            "edges_raw",
            "nodes",
            "edges",
            "snapshots",
            "ranks_raw",
            "ranks",
            "active",
        )
    }


@pytest.fixture
def tiny_source():
    from dagster_v3.defs.commoncrawl_domain_graph.source import GraphSource

    return GraphSource(
        graph_release="cc-main-2026-jun-jul-aug",
        nodes=6,
        edges=8,
        loops=1,
        vertices_url="https://example.com/vertices.gz",
        edges_url="https://example.com/edges.gz",
        vertices_etag='"vertices"',
        edges_etag='"edges"',
        vertices_bytes=100,
        edges_bytes=100,
        source_index_url="https://example.com/index.html",
    )


@pytest.mark.parametrize(
    ("kind", "row"),
    [
        ("nodes", (6, 5, 0, 5, 0, ['"vertices"'])),
        ("nodes", (6, 6, 1, 6, 0, ['"vertices"'])),
        ("nodes", (6, 6, 0, 5, 1, ['"vertices"'])),
        ("edges", (8, 0, 6, 0, 5, 1, ['"edges"'])),
        ("edges", (8, 0, 5, 0, 5, 0, ['"edges"'])),
        ("edges", (8, 0, 5, 0, 5, 1, ['"other"'])),
    ],
)
def test_rows_with_bad_ids_missing_domains_or_wrong_source_cannot_publish(
    kind, row, tiny_source
):
    from dagster_v3.defs.commoncrawl_domain_graph.load import validate_file_rows

    with pytest.raises(ValueError, match="validation"):
        validate_file_rows(kind, row, tiny_source)


@pytest.mark.parametrize(
    "name",
    [
        "commoncrawl_domain_graph_nodes",
        "commoncrawl_domain_graph_edges",
        "commoncrawl_domain_graph_snapshots",
    ],
)
def test_wrong_release_input_is_rejected_before_database_access(name, tiny_source):
    import dagster as dg
    from dagster_clickhouse import ClickhouseResource
    from dagster_v3.defs.commoncrawl_domain_graph import assets

    from dagster_v3.defs.commoncrawl_domain_graph.download import CachedArtifact

    kind = name.rsplit("_", 1)[1]
    artifact = (
        CachedArtifact(
            assets.graph_artifact(tiny_source, kind), "test", "test", "a" * 64, "test"
        )
        if kind in ("nodes", "edges")
        else None
    )
    with dg.build_asset_context(partition_key="cc-main-2025-jun-jul-aug") as context:
        with pytest.raises(ValueError, match="release partition"):
            getattr(assets, name)(
                context,
                tiny_source,
                ClickhouseResource(
                    host="localhost",
                    port=1,
                    user="test",
                    password="test",
                    database="corpscout",
                ),
                **(
                    {name + "_raw": artifact}
                    if name.endswith(("nodes", "edges"))
                    else {}
                ),
            )
