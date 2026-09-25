"""One validated, atomic partition is the publication contract for ranking data."""

import logging
from time import monotonic
from uuid import uuid4

from clickhouse_driver import Client

from dagster_v3.defs.commoncrawl_domain_graph.download import CachedArtifact
from dagster_v3.defs.commoncrawl_domain_graph.load import DATABASE, SETTINGS

RANKS = "commoncrawl_domain_graph_ranks"
RANK_COLUMNS = (
    "graph_release",
    "root_domain",
    "cc_harmonic_centrality",
    "cc_harmonic_rank",
    "cc_pagerank",
    "cc_pagerank_rank",
    "n_hosts",
    "source_run_id",
    "loaded_at",
)
RANK_SCHEMA = "harmonicc_pos UInt64, harmonicc_val Float64, pr_pos UInt64, pr_val Float64, host_rev String, n_hosts UInt32"


def validate_ranks(client: Client, table: str, release: str, expected: int) -> int:
    row = client.execute(
        f"""SELECT sum(n),count(),sum(invalid) FROM (
        SELECT root_domain,count() AS n,
        countIf(root_domain='' OR NOT isFinite(cc_harmonic_centrality)
            OR NOT isFinite(cc_pagerank) OR cc_harmonic_centrality<0 OR cc_pagerank<0
            OR cc_harmonic_rank<1 OR cc_harmonic_rank>%(expected)s
            OR cc_pagerank_rank<1 OR cc_pagerank_rank>%(expected)s) AS invalid
        FROM {DATABASE}.{table} WHERE graph_release=%(release)s GROUP BY root_domain)""",
        {"release": release, "expected": expected},
        settings={
            **SETTINGS,
            "optimize_aggregation_in_order": 1,
            "max_bytes_before_external_group_by": 1_000_000_000,
        },
    )[0]
    if expected <= 0 or row != (expected, expected, 0):
        raise ValueError(
            f"{release} failed rank count/uniqueness/value validation: {row}"
        )
    return expected


def load_ranks(
    client: Client, artifact: CachedArtifact, run_id: str, log: logging.Logger
) -> tuple[int, bool]:
    source = artifact.source
    if source.artifact_kind != "ranks" or artifact.schema_version not in (
        "domain-ranks-tsv-v1",
        "domain-ranks-without-host-count-tsv-v1",
    ):
        raise ValueError("Expected a verified domain ranks artifact")
    count = client.execute(
        f"SELECT count() FROM {DATABASE}.{RANKS} WHERE graph_release=%(release)s",
        {"release": source.graph_release},
    )[0][0]
    if count:
        # The asset separately checks the prior run's pinned source manifest before reuse.
        if count != source.expected_rows:
            raise ValueError(
                "Existing rank partition has an unexpected count; refusing to replace it"
            )
        return count, True
    has_hosts = artifact.schema_version == "domain-ranks-tsv-v1"
    input_schema = (
        RANK_SCHEMA if has_hosts else RANK_SCHEMA.removesuffix(", n_hosts UInt32")
    )
    hosts = "n_hosts" if has_hosts else "CAST(NULL AS Nullable(UInt32))"
    suffix = uuid4().hex
    stage = f"{RANKS}_stage_{suffix}"
    query_id = f"commoncrawl-ranks-{suffix}"
    client.execute(f"CREATE TABLE {DATABASE}.{stage} AS {DATABASE}.{RANKS}")
    try:
        query = f"""INSERT INTO {DATABASE}.{stage} ({",".join(RANK_COLUMNS)})
            SELECT %(release)s,arrayStringConcat(arrayReverse(splitByChar('.',host_rev)),'.'),
                harmonicc_val,harmonicc_pos,pr_val,pr_pos,{hosts},%(run_id)s,now64(3)
            FROM s3(commoncrawl_graph_cache, filename=%(path)s, format='TSV',
                structure=%(schema)s, compression_method='gzip')"""
        progress = client.execute_with_progress(
            query,
            {
                "release": source.graph_release,
                "path": artifact.key,
                "schema": input_schema,
                "run_id": run_id,
            },
            settings={**SETTINGS, "input_format_tsv_skip_first_lines": 1},
            query_id=query_id,
        )
        last_log = monotonic()
        for rows, _ in progress:
            if monotonic() - last_log >= 30:
                log.info(
                    "Ranks: read %s rows (expected %s)", rows, source.expected_rows
                )
                last_log = monotonic()
        progress.get_result()
        count = validate_ranks(
            client, stage, source.graph_release, source.expected_rows
        )
        client.execute(
            f"ALTER TABLE {DATABASE}.{RANKS} REPLACE PARTITION %(release)s FROM {DATABASE}.{stage}",
            {"release": source.graph_release},
        )
        return count, False
    except BaseException:
        client.disconnect()
        try:
            client.execute(
                "KILL QUERY WHERE query_id=%(query_id)s SYNC", {"query_id": query_id}
            )
        except Exception:
            log.exception(
                "Could not cancel %s; investigate staging table %s", query_id, stage
            )
        raise
    finally:
        try:
            client.execute(f"DROP TABLE IF EXISTS {DATABASE}.{stage} SYNC")
        except Exception:
            log.exception("Could not remove rank staging table %s", stage)
