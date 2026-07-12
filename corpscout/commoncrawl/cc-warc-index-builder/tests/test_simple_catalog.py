from pathlib import Path

import duckdb
import pytest

from warc_index_builder.catalog import (
    build_candidate,
    build_catalog,
    open_duckdb,
)
from warc_index_builder.manifests import IndexSource, WarcObject, WarcSize


BASE_COLUMNS = (
    ("url_host_registered_domain", "VARCHAR"),
    ("url_host_name", "VARCHAR"),
    ("url", "VARCHAR"),
    ("url_path", "VARCHAR"),
    ("fetch_status", "BIGINT"),
    ("content_mime_type", "VARCHAR"),
    ("warc_filename", "VARCHAR"),
    ("warc_record_offset", "BIGINT"),
    ("warc_record_length", "BIGINT"),
)
OPTIONAL_COLUMNS = (
    ("content_mime_detected", "VARCHAR"),
    ("content_languages", "VARCHAR"),
)

WARCS = (
    WarcObject(0, "zero.warc.gz"),
    WarcObject(1, "d-about.warc.gz"),
    WarcObject(2, "a-home.warc.gz"),
    WarcObject(3, "c-empty.warc.gz"),
    WarcObject(4, "a-about.warc.gz"),
    WarcObject(5, "d-empty.warc.gz"),
    WarcObject(6, "b-home.warc.gz"),
)
WARC_SIZES = (
    WarcSize(0, "zero.warc.gz", 1_000),
    WarcSize(3, "c-empty.warc.gz", 3_000),
)


def _row(
    domain: str,
    url: str,
    path: str,
    warc_filename: str,
    offset: int,
    length: int,
    *,
    detected_mime: str | None = None,
    languages: str | None = None,
) -> dict[str, object]:
    return {
        "url_host_registered_domain": domain,
        "url_host_name": domain,
        "url": url,
        "url_path": path,
        "fetch_status": 200,
        "content_mime_type": (
            "application/octet-stream" if detected_mime else "text/html"
        ),
        "warc_filename": warc_filename,
        "warc_record_offset": offset,
        "warc_record_length": length,
        "content_mime_detected": detected_mime,
        "content_languages": languages,
    }


def _source_rows() -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    legacy = [
        _row("example.com", "https://example.com/", "/", "a-home.warc.gz", 10, 100),
        _row(
            "example.com",
            "https://example.com/about",
            "/about",
            "a-about.warc.gz",
            120,
            80,
        ),
        _row(
            "example.com",
            "https://example.com/news",
            "/news",
            "a-news.warc.gz",
            220,
            70,
        ),
        _row(
            "example.com",
            "https://example.com/deep/page",
            "/deep/page",
            "a-deep.warc.gz",
            310,
            60,
        ),
        _row("other.test", "https://other.test/", "/", "b-home.warc.gz", 20, 60),
        _row(
            "other.test",
            "https://other.test/contact",
            "/contact",
            "b-contact.warc.gz",
            100,
            50,
        ),
        _row(
            "other.test", "https://other.test/news", "/news", "b-news.warc.gz", 170, 40
        ),
        _row(
            "other.test",
            "https://other.test/deep/page",
            "/deep/page",
            "b-deep.warc.gz",
            230,
            30,
        ),
    ]
    modern = [
        _row(
            "example.com",
            "https://example.com",
            "",
            "c-empty.warc.gz",
            30,
            110,
            detected_mime="text/html",
            languages="fra",
        ),
        _row(
            "example.com",
            "https://example.com/contact",
            "/contact",
            "c-contact.warc.gz",
            160,
            90,
            detected_mime="text/html",
            languages="eng",
        ),
        _row(
            "example.com",
            "https://example.com/terms",
            "/terms",
            "c-terms.warc.gz",
            270,
            75,
            detected_mime="text/html",
            languages="eng",
        ),
        _row(
            "example.com",
            "https://example.com/blog",
            "/blog",
            "c-blog.warc.gz",
            365,
            65,
            detected_mime="text/html",
            languages="eng",
        ),
        _row(
            "other.test",
            "https://other.test",
            "",
            "d-empty.warc.gz",
            40,
            65,
            detected_mime="text/html",
            languages="deu",
        ),
        _row(
            "other.test",
            "https://other.test/about",
            "/about",
            "d-about.warc.gz",
            125,
            55,
            detected_mime="text/html",
            languages="spa",
        ),
        _row(
            "other.test",
            "https://other.test/privacy",
            "/privacy",
            "d-privacy.warc.gz",
            200,
            45,
            detected_mime="text/html",
            languages="deu",
        ),
        _row(
            "other.test",
            "https://other.test/blog",
            "/blog",
            "d-blog.warc.gz",
            265,
            35,
            detected_mime="text/html",
            languages="deu",
        ),
    ]
    return legacy, modern


def _write_source(
    path: Path,
    rows: list[dict[str, object]],
    *,
    include_optional: bool,
) -> IndexSource:
    columns = BASE_COLUMNS + (OPTIONAL_COLUMNS if include_optional else ())
    connection = duckdb.connect()
    try:
        definition = ", ".join(f"{name} {kind}" for name, kind in columns)
        connection.execute(f"CREATE TABLE source_rows ({definition})")
        placeholders = ", ".join("?" for _ in columns)
        connection.executemany(
            f"INSERT INTO source_rows VALUES ({placeholders})",
            [tuple(row[name] for name, _kind in columns) for row in rows],
        )
        connection.execute(
            "COPY source_rows TO ? (FORMAT PARQUET)",
            [str(path)],
        )
    finally:
        connection.close()
    source_index = 1 if include_optional else 0
    return IndexSource(source_index, str(path), str(path))


def _write_sources(directory: Path) -> tuple[IndexSource, IndexSource]:
    legacy, modern = _source_rows()
    return (
        _write_source(directory / "legacy.parquet", legacy, include_optional=False),
        _write_source(directory / "modern.parquet", modern, include_optional=True),
    )


def _build_candidates(
    directory: Path,
    sources: tuple[IndexSource, IndexSource],
    pages_per_domain: int,
) -> tuple[Path, Path]:
    connection = open_duckdb(
        None,
        directory / "query-temp",
        threads=1,
        memory_limit=None,
    )
    paths: list[Path] = []
    try:
        for source in sources:
            path = directory / f"candidate-{source.source_index}.parquet"
            result = build_candidate(
                connection,
                source,
                path,
                pages_per_domain=pages_per_domain,
                attempts=1,
            )
            assert result.rows == 2 * pages_per_domain
            paths.append(path)
    finally:
        connection.close()
    return paths[0], paths[1]


def _build(
    path: Path,
    candidates: tuple[Path, Path],
    pages_per_domain: int,
):
    return build_catalog(
        path,
        candidates,
        WARCS,
        WARC_SIZES,
        crawl="CC-MAIN-2026-25",
        pages_per_domain=pages_per_domain,
        index_manifest_sha256="11" * 32,
        warc_manifest_sha256="22" * 32,
        temp_directory=path.parent / f"catalog-temp-{pages_per_domain}",
        threads=1,
        memory_limit=None,
    )


@pytest.mark.parametrize(
    "pages_per_domain,expected",
    [
        (
            1,
            [
                ("example.com", "https://example.com", 1, "fra", 3),
                ("other.test", "https://other.test", 1, "deu", 5),
            ],
        ),
        (
            3,
            [
                ("example.com", "https://example.com", 1, "fra", 3),
                ("example.com", "https://example.com/", 2, None, 2),
                ("example.com", "https://example.com/about", 3, None, 4),
                ("other.test", "https://other.test", 1, "deu", 5),
                ("other.test", "https://other.test/", 2, None, 6),
                ("other.test", "https://other.test/about", 3, "spa", 1),
            ],
        ),
    ],
)
def test_local_then_global_top_n_and_legacy_columns(
    tmp_path: Path,
    pages_per_domain: int,
    expected: list[tuple[object, ...]],
) -> None:
    sources = _write_sources(tmp_path)
    candidates = _build_candidates(
        tmp_path / f"n{pages_per_domain}", sources, pages_per_domain
    )
    connection = duckdb.connect()
    try:
        assert (
            connection.execute(
                "SELECT count(*) FROM read_parquet(?) WHERE content_languages IS NULL",
                [str(candidates[0])],
            ).fetchone()[0]
            == 2 * pages_per_domain
        )
    finally:
        connection.close()

    catalog_path = tmp_path / f"catalog-{pages_per_domain}.duckdb"
    result = _build(catalog_path, candidates, pages_per_domain)
    connection = duckdb.connect(str(catalog_path), read_only=True)
    try:
        rows = connection.execute(
            """
            SELECT root_domain, url, domain_page_rank, content_languages, warc_index
            FROM pages ORDER BY root_domain, domain_page_rank
            """
        ).fetchall()
    finally:
        connection.close()

    assert rows == expected
    assert result.selected_page_count == 2 * pages_per_domain


def test_warc_stats_and_estimate_fields_include_zero_page_warc(tmp_path: Path) -> None:
    sources = _write_sources(tmp_path)
    candidates = _build_candidates(tmp_path / "n3", sources, 3)
    catalog_path = tmp_path / "catalog.duckdb"
    result = _build(catalog_path, candidates, 3)

    connection = duckdb.connect(str(catalog_path), read_only=True)
    try:
        zero = connection.execute(
            """
            SELECT selected_pages, selected_bytes, estimated_warc_bytes,
                   estimated_utilization_percent
            FROM warc_stats WHERE warc_index = 0
            """
        ).fetchone()
        metadata = connection.execute(
            """
            SELECT selection_version, index_shard_count, warc_count, selected_warc_count,
                   selected_page_count, selected_bytes, warc_sample_count,
                   estimated_average_warc_bytes
            FROM metadata
            """
        ).fetchone()
        mapped = connection.execute(
            "SELECT warc_index, estimated_utilization_percent FROM warc_stats WHERE warc_index = 3"
        ).fetchone()
    finally:
        connection.close()

    assert zero == (0, 0, 2_000.0, 0.0)
    assert metadata == (1, 2, 7, 6, 6, 470, 2, 2_000.0)
    assert mapped[0] == 3
    assert mapped[1] == pytest.approx(5.5)
    assert result.warc_count == 7
    assert result.selected_warc_count == 6
    assert result.selected_bytes == 470
    assert result.estimated_average_warc_bytes == 2_000.0


def test_catalog_preserves_published_file_on_failure_and_replaces_on_success(
    tmp_path: Path,
) -> None:
    sources = _write_sources(tmp_path)
    candidates3 = _build_candidates(tmp_path / "n3", sources, 3)
    candidates1 = _build_candidates(tmp_path / "n1", sources, 1)
    catalog_path = tmp_path / "catalog.duckdb"
    first = _build(catalog_path, candidates3, 3)
    before = catalog_path.read_bytes()
    before_inode = catalog_path.stat().st_ino

    missing = tmp_path / "missing-candidate.parquet"
    with pytest.raises(duckdb.Error):
        _build(catalog_path, (candidates3[0], missing), 3)
    assert catalog_path.stat().st_ino == before_inode
    assert catalog_path.read_bytes() == before

    rebuilt = _build(catalog_path, candidates1, 1)
    assert first.reused is False and rebuilt.reused is False
    assert rebuilt.selected_page_count == 2
    assert catalog_path.stat().st_ino != before_inode
    assert not Path(f"{catalog_path}.partial").exists()
    assert not Path(f"{catalog_path}.partial.wal").exists()
    assert not (tmp_path / "catalog-temp-1").exists()
    connection = duckdb.connect(str(catalog_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT pages_per_domain, selected_page_count FROM metadata"
        ).fetchone() == (1, 2)
    finally:
        connection.close()
