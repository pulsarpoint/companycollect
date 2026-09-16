"""Pilot: direct reciprocal domain links from one Common Crawl graph release.

Reads a frozen JSONL company/domain selection and the official gzip TSV files.
Retains only edges touching a seed; never imports the complete graph into ClickHouse.
Outputs candidates and crawl targets, not company ownership decisions.
"""

import argparse
import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic

import duckdb

LOG = logging.getLogger(__name__)
DEFAULT_RELEASE = "cc-main-2026-jun-jul-aug"
VERTICES = """read_csv(?, header=false, delim='\t', quote='', escape='', compression='gzip',
    columns={'node_id':'UINTEGER','reversed_domain':'VARCHAR','n_hosts':'UINTEGER'})"""
EDGES = """read_csv(?, header=false, delim='\t', quote='', escape='', compression='gzip',
    columns={'source_id':'UINTEGER','target_id':'UINTEGER'})"""


def profiled_scan(
    connection: duckdb.DuckDBPyConnection, sql: str, path: Path, profile: Path
) -> dict:
    connection.execute("SET enable_profiling='json'")
    connection.execute("SET profiling_output=?", [str(profile)])
    started = monotonic()
    try:
        connection.execute(sql, [str(path)]).fetchall()
    finally:
        connection.execute("SET enable_profiling='no_output'")
    metrics = json.loads(profile.read_text())
    pending, csv_rows = [metrics], 0
    while pending:
        node = pending.pop()
        if node.get("extra_info", {}).get("Function") == "READ_CSV":
            csv_rows += node["operator_cardinality"]
        pending.extend(node.get("children", []))
    return {
        "elapsed_seconds": round(monotonic() - started, 2),
        "csv_rows": csv_rows,
        "peak_memory_bytes": metrics["system_peak_buffer_memory"],
        "peak_temp_bytes": metrics["system_peak_temp_dir_size"],
    }


def prepare_seeds(
    connection: duckdb.DuckDBPyConnection, seed_file: Path, vertices: Path, output: Path
) -> dict:
    connection.execute(
        """CREATE OR REPLACE TABLE seeds AS SELECT DISTINCT company_id,company_name,
        lower(root_domain) AS root_domain,
        coalesce(root_domain!='' AND NOT regexp_matches(root_domain,'[/@: ]'),false) AS valid_domain,
        array_to_string(list_reverse(string_split(lower(root_domain),'.')),'.') AS reversed_domain
        FROM read_json(?,format='newline_delimited',columns={
          'company_id':'VARCHAR','company_name':'VARCHAR','root_domain':'VARCHAR'})""",
        [str(seed_file)],
    )
    if connection.execute("SELECT count() FROM seeds").fetchone()[0] == 0:
        raise ValueError("The seed snapshot must contain at least one company/domain.")
    invalid_ids = connection.execute(
        "SELECT count(*) FROM seeds WHERE company_id IS NULL OR company_id=''"
    ).fetchone()[0]
    if invalid_ids:
        raise ValueError(f"Seed rows without company IDs: {invalid_ids}")
    LOG.info("Mapping seed domains to graph node IDs")
    metrics = profiled_scan(
        connection,
        f"""CREATE OR REPLACE TABLE seed_nodes AS
        SELECT * FROM {VERTICES} WHERE reversed_domain IN (SELECT reversed_domain FROM seeds WHERE valid_domain)""",
        vertices,
        output / "seed-scan-profile.json",
    )
    LOG.info("Seed mapping: %s", metrics)
    return metrics


def extract_incident_edges(
    connection: duckdb.DuckDBPyConnection, edges: Path, output: Path
) -> dict:
    LOG.info(
        "Scanning the complete edge list; retaining incoming and outgoing edges for seeds"
    )
    metrics = profiled_scan(
        connection,
        f"""CREATE OR REPLACE TABLE incident_edges AS
        SELECT source_id,target_id FROM {EDGES}
        WHERE source_id IN (SELECT node_id FROM seed_nodes)
           OR target_id IN (SELECT node_id FROM seed_nodes)""",
        edges,
        output / "edge-scan-profile.json",
    )
    LOG.info("Edge scan: %s", metrics)
    connection.execute("""CREATE OR REPLACE TABLE reciprocal_pairs AS
        SELECT DISTINCT s.node_id AS seed_id,f.target_id AS neighbor_id
        FROM seed_nodes s
        JOIN incident_edges f ON f.source_id=s.node_id
        JOIN incident_edges b ON b.source_id=f.target_id AND b.target_id=s.node_id
        WHERE s.node_id!=f.target_id""")
    return metrics


def export_results(
    connection: duckdb.DuckDBPyConnection, vertices: Path, output: Path, release: str
) -> dict:
    LOG.info("Resolving reciprocal neighbor IDs to domains")
    metrics = profiled_scan(
        connection,
        f"""CREATE OR REPLACE TABLE neighbor_nodes AS
        SELECT * FROM {VERTICES} WHERE node_id IN (SELECT neighbor_id FROM reciprocal_pairs)""",
        vertices,
        output / "neighbor-scan-profile.json",
    )
    missing = connection.execute(
        "SELECT count(*) FROM reciprocal_pairs p ANTI JOIN neighbor_nodes n ON n.node_id=p.neighbor_id"
    ).fetchone()[0]
    if missing:
        raise ValueError(
            f"Graph edges refer to {missing} missing vertices; check release consistency"
        )
    connection.execute(
        """CREATE OR REPLACE TABLE candidates AS
        WITH popularity AS (SELECT neighbor_id,count(DISTINCT seed_id) AS reciprocal_seed_domains
            FROM reciprocal_pairs GROUP BY neighbor_id),
        degrees AS (SELECT s.node_id,
            (SELECT count(DISTINCT target_id) FROM incident_edges WHERE source_id=s.node_id) AS seed_outdegree,
            (SELECT count(DISTINCT source_id) FROM incident_edges WHERE target_id=s.node_id) AS seed_indegree
            FROM seed_nodes s)
        SELECT ? AS graph_release, company.company_id,company.company_name,company.root_domain AS seed_domain,
            array_to_string(list_reverse(string_split(neighbor.reversed_domain,'.')),'.') AS related_domain,
            p.seed_id,p.neighbor_id,neighbor.n_hosts AS related_domain_hosts,
            true AS seed_links_to_related,true AS related_links_to_seed,
            popularity.reciprocal_seed_domains,degrees.seed_indegree,degrees.seed_outdegree,
            'unverified_relationship' AS relationship_status
        FROM reciprocal_pairs p JOIN seed_nodes seed ON seed.node_id=p.seed_id
        JOIN seeds company ON company.reversed_domain=seed.reversed_domain
        JOIN neighbor_nodes neighbor ON neighbor.node_id=p.neighbor_id
        JOIN popularity ON popularity.neighbor_id=p.neighbor_id
        JOIN degrees ON degrees.node_id=p.seed_id""",
        [release],
    )
    connection.execute("""CREATE OR REPLACE TABLE seed_coverage AS
        WITH coverage AS (SELECT company.*,s.node_id,
            (SELECT count(*) FROM reciprocal_pairs p WHERE p.seed_id=s.node_id) AS reciprocal_domains
            FROM seeds company LEFT JOIN seed_nodes s USING(reversed_domain))
        SELECT *,CASE WHEN NOT valid_domain THEN 'invalid_domain'
                 WHEN node_id IS NULL THEN 'absent_from_graph'
                 WHEN reciprocal_domains=0 THEN 'no_reciprocal_links' ELSE 'has_reciprocal_links' END AS status
        FROM coverage""")
    connection.execute("""CREATE OR REPLACE TABLE crawl_targets AS
        SELECT related_domain AS root_domain,count(DISTINCT seed_domain) AS reciprocal_seed_domains,
            list(DISTINCT seed_domain ORDER BY seed_domain) AS seed_domains,
            list(DISTINCT company_id ORDER BY company_id) AS company_ids
        FROM candidates GROUP BY related_domain""")
    for table in ("candidates", "seed_coverage", "crawl_targets"):
        connection.execute(
            f"COPY (SELECT * FROM {table} ORDER BY ALL) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
            [str(output / f"{table}.parquet")],
        )
        connection.execute(
            f"COPY (SELECT * FROM {table} ORDER BY ALL) TO ? (FORMAT CSV, HEADER)",
            [str(output / f"{table}.csv")],
        )
    counts = dict(
        zip(
            (
                "seed_companies",
                "seed_domains",
                "found_seed_domains",
                "incident_edges",
                "reciprocal_pairs",
                "candidate_company_domains",
                "unique_related_domains",
                "companies_with_candidates",
                "invalid_seed_rows",
            ),
            connection.execute("""SELECT (SELECT count(DISTINCT company_id) FROM seeds),
            (SELECT count(DISTINCT root_domain) FROM seeds),(SELECT count(*) FROM seed_nodes),
            (SELECT count(*) FROM incident_edges),(SELECT count(*) FROM reciprocal_pairs),
            (SELECT count(*) FROM candidates),(SELECT count(*) FROM crawl_targets),
            (SELECT count(DISTINCT company_id) FROM candidates),
            (SELECT count(*) FROM seeds WHERE NOT valid_domain)""").fetchone(),
            strict=True,
        )
    )
    LOG.info("Results: %s", counts)
    return {"counts": counts, "neighbor_scan": metrics}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        type=Path,
        required=True,
        help="Directory containing this release's completed graph files",
    )
    parser.add_argument(
        "--seed-file",
        type=Path,
        required=True,
        help="Frozen JSONL with company_id, company_name and root_domain",
    )
    parser.add_argument("--release", default=DEFAULT_RELEASE)
    parser.add_argument("--stage", choices=("all", "prepare", "analyze"), default="all")
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    root = args.data_dir.resolve()
    root.mkdir(parents=True, exist_ok=True)
    vertices = root / f"{args.release}-domain-vertices.txt.gz"
    edges = root / f"{args.release}-domain-edges.txt.gz"
    manifest_path = root / "pilot-manifest.json"
    identity = {
        "graph_release": args.release,
        "seed_sha256": hashlib.sha256(args.seed_file.read_bytes()).hexdigest(),
    }
    if args.stage == "analyze":
        manifest = json.loads(manifest_path.read_text())
        if any(manifest[key] != value for key, value in identity.items()):
            raise ValueError(
                "Seed snapshot or graph release differs from prepared input"
            )
    else:
        manifest = {**identity, "started_at": datetime.now(UTC).isoformat()}
    with duckdb.connect(str(root / "reciprocal.duckdb")) as connection:
        connection.execute("SET threads=?", [args.threads])
        connection.execute("SET memory_limit='8GB'")
        connection.execute("SET preserve_insertion_order=false")
        if args.stage in ("all", "prepare"):
            manifest["seed_scan"] = prepare_seeds(
                connection, args.seed_file, vertices, root
            )
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
        if args.stage in ("all", "analyze"):
            manifest["edge_scan"] = extract_incident_edges(connection, edges, root)
            manifest.update(export_results(connection, vertices, root, args.release))
            manifest["completed_at"] = datetime.now(UTC).isoformat()
            manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
        connection.execute("CHECKPOINT")


if __name__ == "__main__":
    main()
