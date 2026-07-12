from pathlib import Path

import duckdb

from warc_index_builder.catalog import build_candidate, build_catalog, open_duckdb
from warc_index_builder.manifests import IndexSource, WarcObject, WarcSize
from warc_index_builder.selection import candidate_query


def test_duplicate_capture_does_not_consume_two_domain_slots() -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE source_rows (
                url_host_registered_domain VARCHAR,
                url_host_name VARCHAR,
                url VARCHAR,
                url_path VARCHAR,
                fetch_status BIGINT,
                content_mime_type VARCHAR,
                warc_filename VARCHAR,
                warc_record_offset BIGINT,
                warc_record_length BIGINT
            )
            """
        )
        rows = [
            (
                "example.com",
                "example.com",
                "https://example.com/",
                "/",
                200,
                "text/html",
                "capture.warc.gz",
                10,
                100,
            ),
            (
                "example.com",
                "example.com",
                "https://example.com/",
                "/",
                200,
                "text/html",
                "capture.warc.gz",
                10,
                100,
            ),
            (
                "example.com",
                "example.com",
                "https://example.com/contact",
                "/contact",
                200,
                "text/html",
                "capture.warc.gz",
                110,
                80,
            ),
            (
                "example.com",
                "example.com",
                "https://example.com/news",
                "/news",
                200,
                "text/html",
                "capture.warc.gz",
                190,
                70,
            ),
        ]
        connection.executemany(
            "INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )

        query = candidate_query(
            "source_rows",
            source_index=0,
            pages_per_domain=2,
            has_detected_mime=False,
            has_languages=False,
        )
        urls = [
            row[0]
            for row in connection.execute(
                f"SELECT url FROM ({query}) ORDER BY url"
            ).fetchall()
        ]
    finally:
        connection.close()

    assert urls == ["https://example.com/", "https://example.com/contact"]


def _write_source(path: Path, rows: list[tuple[object, ...]]) -> None:
    connection = duckdb.connect()
    try:
        connection.execute(
            """
            CREATE TABLE source_rows (
                url_host_registered_domain VARCHAR, url_host_name VARCHAR,
                url VARCHAR, url_path VARCHAR, fetch_status BIGINT,
                content_mime_type VARCHAR, warc_filename VARCHAR,
                warc_record_offset BIGINT, warc_record_length BIGINT
            )
            """
        )
        connection.executemany(
            "INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )
        connection.execute("COPY source_rows TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        connection.close()


def test_duplicate_capture_across_candidates_does_not_consume_global_slot(
    tmp_path: Path,
) -> None:
    home = (
        "example.com",
        "example.com",
        "https://example.com/",
        "/",
        200,
        "text/html",
        "shared.warc.gz",
        10,
        100,
    )
    source_rows = (
        [
            home,
            (
                "example.com",
                "example.com",
                "https://example.com/contact",
                "/contact",
                200,
                "text/html",
                "shared.warc.gz",
                110,
                80,
            ),
        ],
        [
            home,
            (
                "example.com",
                "example.com",
                "https://example.com/news",
                "/news",
                200,
                "text/html",
                "shared.warc.gz",
                190,
                70,
            ),
        ],
    )
    connection = open_duckdb(
        None, tmp_path / "candidate-temp", threads=1, memory_limit=None
    )
    candidate_paths = []
    try:
        for source_index, rows in enumerate(source_rows):
            source_path = tmp_path / f"source-{source_index}.parquet"
            candidate_path = tmp_path / f"candidate-{source_index}.parquet"
            _write_source(source_path, rows)
            build_candidate(
                connection,
                IndexSource(source_index, str(source_path), str(source_path)),
                candidate_path,
                pages_per_domain=2,
                attempts=1,
            )
            candidate_paths.append(candidate_path)
    finally:
        connection.close()

    catalog_path = tmp_path / "catalog.duckdb"
    build_catalog(
        catalog_path,
        candidate_paths,
        (WarcObject(0, "shared.warc.gz"),),
        (WarcSize(0, "shared.warc.gz", 1_000),),
        crawl="CC-MAIN-2026-25",
        pages_per_domain=2,
        index_manifest_sha256="11" * 32,
        warc_manifest_sha256="22" * 32,
        temp_directory=tmp_path / "catalog-temp",
        threads=1,
        memory_limit=None,
    )
    result = duckdb.connect(str(catalog_path), read_only=True)
    try:
        urls = result.execute(
            "SELECT url FROM pages ORDER BY domain_page_rank"
        ).fetchall()
    finally:
        result.close()

    assert urls == [
        ("https://example.com/",),
        ("https://example.com/contact",),
    ]
