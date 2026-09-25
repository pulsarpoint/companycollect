"""Native bulk ingestion with release-scoped, validated partition publication."""

import logging
from time import monotonic
from typing import Literal
from uuid import uuid4

from clickhouse_driver import Client

from dagster_v3.defs.commoncrawl_domain_graph.source import GraphSource
from dagster_v3.defs.commoncrawl_domain_graph.download import CachedArtifact

DATABASE = "corpscout"
NODES = "commoncrawl_domain_graph_nodes"
EDGES = "commoncrawl_domain_graph_edges"
SNAPSHOTS = "commoncrawl_domain_graph_snapshots"
SETTINGS = {
    "max_threads": 4,
    "max_insert_threads": 2,
    "max_download_threads": 8,
    "max_download_buffer_size": 16_777_216,
    "max_memory_usage": 8_000_000_000,
    "max_execution_time": 7200,
    "use_query_condition_cache": 0,
    "input_format_allow_errors_num": 0,
    "input_format_allow_errors_ratio": 0,
}


def validate_file_rows(
    kind: Literal["nodes", "edges"], row: tuple, source: GraphSource
) -> int:
    if kind == "nodes":
        count, unique_ids, first, last, empty_domains, etags = row
        valid = (
            count == source.nodes == unique_ids
            and first == 0
            and last == source.nodes - 1
            and empty_domains == 0
            and etags == [source.vertices_etag]
        )
    else:
        count, first_source, last_source, first_target, last_target, loops, etags = row
        valid = (
            count == source.edges
            and loops == source.loops
            and 0 <= first_source <= last_source < source.nodes
            and 0 <= first_target <= last_target < source.nodes
            and etags == [source.edges_etag]
        )
    if not valid:
        raise ValueError(
            f"{source.graph_release} {kind} failed source count/ID/ETag validation: {row!r}"
        )
    return count


def file_summary(client: Client, table: str, kind: str, release: str) -> tuple:
    fields = (
        "count(), uniqExact(node_id), min(node_id), max(node_id), countIf(root_domain=''), groupUniqArray(2)(source_etag)"
        if kind == "nodes"
        else "count(), min(source_node_id), max(source_node_id), min(target_node_id), max(target_node_id), countIf(source_node_id=target_node_id), groupUniqArray(2)(source_etag)"
    )
    return client.execute(
        f"SELECT {fields} FROM {DATABASE}.{table} WHERE graph_release=%(release)s",
        {"release": release},
        settings=SETTINGS,
    )[0]


def assert_immutable_release(client: Client, source: GraphSource) -> bool:
    rows = client.execute(
        f"SELECT node_count,edge_count,loop_count,vertices_etag,edges_etag FROM {DATABASE}.{SNAPSHOTS} FINAL WHERE graph_release=%(release)s",
        {"release": source.graph_release},
        settings=SETTINGS,
    )
    expected = (
        source.nodes,
        source.edges,
        source.loops,
        source.vertices_etag,
        source.edges_etag,
    )
    if rows and rows[0] != expected:
        raise ValueError(
            "Published graph release changed upstream; refusing to overwrite release-local IDs"
        )
    return bool(rows)


def load_graph_file(
    client: Client,
    source: GraphSource,
    kind: Literal["nodes", "edges"],
    run_id: str,
    log: logging.Logger,
    cached: CachedArtifact | None = None,
    require_cached: bool = False,
) -> tuple[int, bool]:
    table = NODES if kind == "nodes" else EDGES
    expected = source.nodes if kind == "nodes" else source.edges
    published = assert_immutable_release(client, source)
    count = client.execute(
        f"SELECT count() FROM {DATABASE}.{table} WHERE graph_release=%(release)s",
        {"release": source.graph_release},
        settings=SETTINGS,
    )[0][0]
    if count == expected:
        # An unpublished successful file can also be reused after its sibling failed.
        if not published:
            validate_file_rows(
                kind, file_summary(client, table, kind, source.graph_release), source
            )
        log.info(
            "Reusing %s: %s verified rows for %s", table, count, source.graph_release
        )
        return count, True
    if published:
        raise ValueError(
            f"Published {table} has {count} rows, expected {expected}; investigate before replacing it"
        )

    if require_cached and cached is None:
        raise ValueError("Missing verified graph cache; rematerialize the raw asset")
    suffix = uuid4().hex
    stage = f"{table}_stage_{suffix}"
    query_id = f"commoncrawl-domain-graph-{kind}-{suffix}"
    params = {"release": source.graph_release, "run_id": run_id}
    if kind == "nodes":
        params.update(url=source.vertices_url, etag=source.vertices_etag)
        schema = "node_id UInt32, reversed_domain String, n_hosts UInt32"
        selection = "node_id,arrayStringConcat(arrayReverse(splitByChar('.',reversed_domain)),'.'),n_hosts"
    else:
        params.update(url=source.edges_url, etag=source.edges_etag)
        schema = "source_node_id UInt32, target_node_id UInt32"
        selection = "source_node_id,target_node_id"
    reader = f"url(%(url)s,'TSV','{schema}','gzip',headers('If-Match'=%(etag)s))"
    if cached is not None:
        if (
            cached.source.graph_release != source.graph_release
            or cached.source.artifact_kind != kind
            or cached.source.source_etag != params["etag"]
            or cached.source.source_url != params["url"]
            or cached.source.expected_rows != expected
        ):
            raise ValueError("Cached graph artifact does not match source")
        params["key"] = cached.key
        reader = f"s3(commoncrawl_graph_cache,filename=%(key)s,format='TSV',structure='{schema}',compression_method='gzip')"
    sql = f"""INSERT INTO {DATABASE}.{stage}
        SELECT %(release)s,{selection},%(etag)s,%(run_id)s
        FROM {reader}"""
    client.execute(f"CREATE TABLE {DATABASE}.{stage} AS {DATABASE}.{table}")
    try:
        log.info(
            "Loading %s from %s (%s expected rows)", table, params["url"], expected
        )
        progress = client.execute_with_progress(
            sql, params, settings=SETTINGS, query_id=query_id
        )
        last_log = monotonic()
        for rows_read, total_rows in progress:
            if monotonic() - last_log >= 30:
                log.info("%s: read %s rows (expected %s)", kind, rows_read, expected)
                last_log = monotonic()
        progress.get_result()
        count = validate_file_rows(
            kind, file_summary(client, stage, kind, source.graph_release), source
        )
        # Never replace from a missing/empty partition. Validation includes release-filtered count.
        client.execute(
            f"ALTER TABLE {DATABASE}.{table} REPLACE PARTITION %(release)s FROM {DATABASE}.{stage}",
            {"release": source.graph_release},
        )
        log.info("Published %s rows to %s for %s", count, table, source.graph_release)
        return count, False
    except BaseException:
        # A disconnected client does not necessarily stop a server-side INSERT SELECT.
        client.disconnect()
        try:
            client.execute(
                "KILL QUERY WHERE query_id=%(query_id)s SYNC", {"query_id": query_id}
            )
        except Exception:
            log.exception(
                "Could not cancel graph query %s; staging table is %s", query_id, stage
            )
        raise
    finally:
        try:
            client.execute(f"DROP TABLE IF EXISTS {DATABASE}.{stage} SYNC")
        except Exception:
            log.exception("Could not remove graph staging table %s", stage)


def publish_snapshot(client: Client, source: GraphSource, run_id: str) -> bool:
    if assert_immutable_release(client, source):
        for table, expected in ((NODES, source.nodes), (EDGES, source.edges)):
            count = client.execute(
                f"SELECT count() FROM {DATABASE}.{table} WHERE graph_release=%(release)s",
                {"release": source.graph_release},
                settings=SETTINGS,
            )[0][0]
            if count != expected:
                raise ValueError(
                    f"Published {table} count changed: {count} != {expected}"
                )
        return True
    for table, kind in ((NODES, "nodes"), (EDGES, "edges")):
        validate_file_rows(
            kind, file_summary(client, table, kind, source.graph_release), source
        )
    client.execute(
        f"""INSERT INTO {DATABASE}.{SNAPSHOTS}
        SELECT %(release)s,%(nodes)s,%(edges)s,%(loops)s,%(vertices_url)s,%(edges_url)s,
               %(vertices_etag)s,%(edges_etag)s,%(index_url)s,%(run_id)s,now64(3)""",
        {
            "release": source.graph_release,
            "nodes": source.nodes,
            "edges": source.edges,
            "loops": source.loops,
            "vertices_url": source.vertices_url,
            "edges_url": source.edges_url,
            "vertices_etag": source.vertices_etag,
            "edges_etag": source.edges_etag,
            "index_url": source.source_index_url,
            "run_id": run_id,
        },
    )
    return False
