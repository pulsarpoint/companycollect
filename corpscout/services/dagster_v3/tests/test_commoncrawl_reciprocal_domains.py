"""Require both direct edges, retain shared-company lineage and report missing seeds."""

import gzip
import json

import duckdb

from scripts.commoncrawl_reciprocal_domains import (
    prepare_seeds,
    extract_incident_edges,
    export_results,
)


def test_direct_reciprocity_is_not_a_transitive_cycle_or_one_way_link(tmp_path):
    vertices = tmp_path / "vertices.gz"
    edges = tmp_path / "edges.gz"
    seeds = tmp_path / "seeds.jsonl"
    with gzip.open(vertices, "wt") as stream:
        stream.write(
            "0\tcom.original\t2\n1\tse.related\t4\n2\tcom.oneway\t1\n3\tcom.cycle\t1\n4\tcom.return\t1\n5\tcom.inbound\t1\n"
        )
    # Mutual, duplicated, self, one-way, inbound-only, and a three-node cycle.
    with gzip.open(edges, "wt") as stream:
        stream.write("0\t1\n1\t0\n0\t1\n0\t0\n0\t2\n0\t3\n3\t4\n4\t0\n5\t0\n")
    seeds.write_text(
        "\n".join(
            json.dumps(row)
            for row in [
                {
                    "company_id": "001",
                    "company_name": "Original",
                    "root_domain": "original.com",
                },
                {
                    "company_id": "002",
                    "company_name": "Shares site",
                    "root_domain": "original.com",
                },
                {
                    "company_id": "003",
                    "company_name": "Absent",
                    "root_domain": "absent.se",
                },
                {
                    "company_id": "004",
                    "company_name": "One-way",
                    "root_domain": "oneway.com",
                },
                {
                    "company_id": "005",
                    "company_name": "Malformed published domain",
                    "root_domain": "http:",
                },
            ]
        )
        + "\n"
    )
    with duckdb.connect() as connection:
        prepare = prepare_seeds(connection, seeds, vertices, tmp_path)
        scan = extract_incident_edges(connection, edges, tmp_path)
        result = export_results(connection, vertices, tmp_path, "test-release")
        assert prepare["csv_rows"] == 6 and scan["csv_rows"] == 9
        assert connection.execute(
            "SELECT company_id,seed_domain,related_domain,graph_release FROM candidates ORDER BY company_id"
        ).fetchall() == [
            ("001", "original.com", "related.se", "test-release"),
            ("002", "original.com", "related.se", "test-release"),
        ]
        assert connection.execute(
            "SELECT root_domain,reciprocal_seed_domains,company_ids FROM crawl_targets"
        ).fetchall() == [("related.se", 1, ["001", "002"])]
        assert (
            connection.execute(
                "SELECT status FROM seed_coverage WHERE company_id='003'"
            ).fetchone()[0]
            == "absent_from_graph"
        )
        assert (
            connection.execute(
                "SELECT status FROM seed_coverage WHERE company_id='004'"
            ).fetchone()[0]
            == "no_reciprocal_links"
        )
        assert result["counts"]["companies_with_candidates"] == 2
        assert result["counts"]["invalid_seed_rows"] == 1
        assert (
            connection.execute(
                "SELECT status FROM seed_coverage WHERE company_id='005'"
            ).fetchone()[0]
            == "invalid_domain"
        )
        assert (
            connection.execute(
                "SELECT count(*) FROM read_parquet(?)",
                [str(tmp_path / "candidates.parquet")],
            ).fetchone()[0]
            == 2
        )
