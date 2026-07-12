from pathlib import Path

import duckdb
import pytest

from warc_index_builder.catalog import publish_parquets


def _write_tiny_catalog(path: Path) -> None:
    connection = duckdb.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE metadata (
                warc_count UINTEGER NOT NULL,
                selected_page_count UBIGINT NOT NULL
            );
            INSERT INTO metadata VALUES (3, 3);

            CREATE TABLE warcs (
                warc_index UINTEGER PRIMARY KEY,
                warc_filename VARCHAR NOT NULL UNIQUE
            );
            INSERT INTO warcs VALUES
                (2, 'third.warc.gz'),
                (0, 'first.warc.gz'),
                (1, 'second.warc.gz');

            CREATE TABLE pages (
                warc_index UINTEGER NOT NULL,
                root_domain VARCHAR NOT NULL,
                url VARCHAR NOT NULL,
                domain_page_rank USMALLINT NOT NULL,
                content_languages VARCHAR,
                warc_record_offset UBIGINT NOT NULL,
                warc_record_length UBIGINT NOT NULL
            );
            INSERT INTO pages VALUES
                (2, 'other.test', 'https://other.test/', 1, 'eng', 20, 30),
                (0, 'example.com', 'https://example.com/about', 2, NULL, 100, 25),
                (0, 'example.com', 'https://example.com/', 1, 'deu', 5, 15);

            CREATE TABLE warc_stats (
                warc_index UINTEGER NOT NULL,
                warc_filename VARCHAR NOT NULL,
                selected_pages UBIGINT NOT NULL,
                selected_bytes HUGEINT NOT NULL
            );
            INSERT INTO warc_stats VALUES
                (2, 'third.warc.gz', 1, 30),
                (0, 'first.warc.gz', 2, 40),
                (1, 'second.warc.gz', 0, 0);
            """
        )
    finally:
        connection.close()


def test_publish_parquets_has_portable_schema_order_and_compression(
    tmp_path: Path,
) -> None:
    catalog_path = tmp_path / "catalog.duckdb"
    _write_tiny_catalog(catalog_path)

    warcs_path, pages_path = publish_parquets(catalog_path)

    connection = duckdb.connect()
    try:
        warc_schema = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(warcs_path)]
        ).fetchall()
        page_schema = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(pages_path)]
        ).fetchall()
        warcs = connection.execute(
            "SELECT * FROM read_parquet(?)", [str(warcs_path)]
        ).fetchall()
        pages = connection.execute(
            "SELECT * FROM read_parquet(?)", [str(pages_path)]
        ).fetchall()
        compression = {
            path.name: connection.execute(
                "SELECT DISTINCT compression FROM parquet_metadata(?)", [str(path)]
            ).fetchall()
            for path in (warcs_path, pages_path)
        }
    finally:
        connection.close()

    assert [(row[0], row[1]) for row in warc_schema] == [
        ("warc_index", "UINTEGER"),
        ("warc_filename", "VARCHAR"),
        ("selected_pages", "UBIGINT"),
        ("selected_bytes", "UBIGINT"),
    ]
    assert [(row[0], row[1]) for row in page_schema] == [
        ("warc_index", "UINTEGER"),
        ("root_domain", "VARCHAR"),
        ("url", "VARCHAR"),
        ("domain_page_rank", "USMALLINT"),
        ("content_languages", "VARCHAR"),
        ("warc_record_offset", "UBIGINT"),
        ("warc_record_length", "UBIGINT"),
    ]
    assert warcs == [
        (0, "first.warc.gz", 2, 40),
        (1, "second.warc.gz", 0, 0),
        (2, "third.warc.gz", 1, 30),
    ]
    assert pages == [
        (0, "example.com", "https://example.com/", 1, "deu", 5, 15),
        (0, "example.com", "https://example.com/about", 2, None, 100, 25),
        (2, "other.test", "https://other.test/", 1, "eng", 20, 30),
    ]
    assert compression == {"warcs.parquet": [("ZSTD",)], "pages.parquet": [("ZSTD",)]}
    assert not (tmp_path / "warcs.parquet.partial").exists()
    assert not (tmp_path / "pages.parquet.partial").exists()


def test_failed_validation_preserves_published_parquets(tmp_path: Path) -> None:
    catalog_path = tmp_path / "catalog.duckdb"
    _write_tiny_catalog(catalog_path)
    warcs_path, pages_path = publish_parquets(catalog_path)
    original_warcs = warcs_path.read_bytes()
    original_pages = pages_path.read_bytes()

    connection = duckdb.connect(str(catalog_path))
    try:
        connection.execute(
            "UPDATE warc_stats SET selected_pages = 9 WHERE warc_index = 0"
        )
    finally:
        connection.close()

    with pytest.raises(ValueError, match="totals differ"):
        publish_parquets(catalog_path)

    assert warcs_path.read_bytes() == original_warcs
    assert pages_path.read_bytes() == original_pages
    assert not (tmp_path / "warcs.parquet.partial").exists()
    assert not (tmp_path / "pages.parquet.partial").exists()


@pytest.mark.parametrize(
    "mutation",
    [
        "UPDATE warc_stats SET warc_filename = ' ' WHERE warc_index = 0",
        "UPDATE pages SET root_domain = ' ' WHERE warc_index = 0",
        "UPDATE pages SET url = '' WHERE warc_index = 0",
        "UPDATE pages SET warc_record_length = 0 WHERE warc_index = 2",
    ],
)
def test_publish_rejects_blank_identity_and_zero_length(
    tmp_path: Path,
    mutation: str,
) -> None:
    catalog_path = tmp_path / "catalog.duckdb"
    _write_tiny_catalog(catalog_path)
    connection = duckdb.connect(str(catalog_path))
    try:
        connection.execute(mutation)
    finally:
        connection.close()

    with pytest.raises(ValueError, match="invalid required values"):
        publish_parquets(catalog_path)

    assert not (tmp_path / "warcs.parquet").exists()
    assert not (tmp_path / "pages.parquet").exists()
    assert not (tmp_path / "warcs.parquet.partial").exists()
    assert not (tmp_path / "pages.parquet.partial").exists()
