import duckdb
import pytest

from warc_index_builder.selection import (
    RANKING_COLUMN_NAMES,
    eligibility_predicate,
    ranking_column_names,
    ranking_order_clause,
    ranking_order_terms,
    ranking_projection,
)


PAGE_COLUMNS = (
    "root_domain",
    "url_host_name",
    "url",
    "url_path",
    "fetch_status",
    "content_mime_type",
    "content_mime_detected",
    "content_languages",
    "warc_filename",
    "warc_record_offset",
    "warc_record_length",
)


def _connection() -> duckdb.DuckDBPyConnection:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE pages (
            root_domain VARCHAR,
            url_host_name VARCHAR,
            url VARCHAR,
            url_path VARCHAR,
            fetch_status BIGINT,
            content_mime_type VARCHAR,
            content_mime_detected VARCHAR,
            content_languages VARCHAR,
            warc_filename VARCHAR,
            warc_record_offset HUGEINT,
            warc_record_length HUGEINT
        )
        """
    )
    return connection


def _page(
    domain: str | None,
    url: str | None,
    path: str | None,
    *,
    host: str | None = None,
    status: int | None = 200,
    reported_mime: str | None = "text/html",
    detected_mime: str | None = "text/html",
    languages: str | None = "eng",
    warc_filename: str | None = "crawl-data/example.warc.gz",
    offset: int | None = 100,
    length: int | None = 50,
) -> tuple[object, ...]:
    return (
        domain,
        host if host is not None else domain,
        url,
        path,
        status,
        reported_mime,
        detected_mime,
        languages,
        warc_filename,
        offset,
        length,
    )


def _insert(connection: duckdb.DuckDBPyConnection, rows: list[tuple[object, ...]]) -> None:
    placeholders = ", ".join("?" for _column in PAGE_COLUMNS)
    connection.executemany(f"INSERT INTO pages VALUES ({placeholders})", rows)


def _selected(
    connection: duckdb.DuckDBPyConnection, pages_per_domain: int
) -> list[tuple[object, ...]]:
    query = f"""
        WITH eligible AS (
            SELECT *, {ranking_projection()}
            FROM pages
            WHERE {eligibility_predicate()}
        ), ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY root_domain
                ORDER BY {ranking_order_clause(pages_per_domain)}
            ) AS selection_rank
            FROM eligible
        )
        SELECT root_domain, url, selection_rank
        FROM ranked
        WHERE selection_rank <= ?
        ORDER BY root_domain ASC NULLS LAST, selection_rank ASC NULLS LAST
    """
    return connection.execute(query, [pages_per_domain]).fetchall()


def test_one_page_selects_main_homepage() -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page("ex.com", "https://ex.com/deep/page", "/deep/page", offset=30),
                _page("ex.com", "https://ex.com/contact", "/contact", offset=20),
                _page("ex.com", "https://ex.com/", "/", offset=10),
                _page(
                    "ex.com",
                    "https://shop.ex.com/",
                    "/",
                    host="shop.ex.com",
                    offset=40,
                ),
            ],
        )

        assert _selected(connection, 1) == [("ex.com", "https://ex.com/", 1)]
    finally:
        connection.close()


@pytest.mark.parametrize(
    "path",
    [
        "/impressum",
        "/IMPRESSUM",
        "/aviso-legal",
        "/chi-siamo",
        "/o-nas",
        "/hakkimizda",
        "/confidentialite",
        "/voorwaarden",
    ],
)
def test_multiple_pages_prioritize_multilingual_company_paths(path: str) -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page("ex.com", "https://ex.com/", "/", offset=10),
                _page("ex.com", f"https://ex.com{path}", path, offset=20),
                _page("ex.com", "https://ex.com/news", "/news", offset=30),
            ],
        )

        assert [row[1] for row in _selected(connection, 2)] == [
            "https://ex.com/",
            f"https://ex.com{path}",
        ]
    finally:
        connection.close()


def test_single_and_multi_page_modes_use_their_distinct_policy() -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page("ex.com", "https://ex.com/a", "/a", offset=10),
                _page("ex.com", "https://ex.com/deep/contact", "/deep/contact", offset=20),
            ],
        )

        assert [row[1] for row in _selected(connection, 1)] == ["https://ex.com/a"]
        assert [row[1] for row in _selected(connection, 2)] == [
            "https://ex.com/deep/contact",
            "https://ex.com/a",
        ]
    finally:
        connection.close()


def test_apex_and_www_rank_before_functional_subdomains() -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page("ex.com", "https://shop.ex.com/", "/", host="shop.ex.com", offset=10),
                _page("ex.com", "https://www.ex.com/", "/", host="www.ex.com", offset=20),
                _page("ex.com", "https://ex.com/", "/", host="ex.com", offset=30),
            ],
        )

        assert [row[1] for row in _selected(connection, 3)] == [
            "https://ex.com/",
            "https://www.ex.com/",
            "https://shop.ex.com/",
        ]
    finally:
        connection.close()


def test_eligibility_preserves_detected_mime_fallback_for_old_crawls() -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page(
                    "old.example",
                    "https://old.example/",
                    "/",
                    reported_mime="text/html",
                    detected_mime=None,
                    languages=None,
                ),
                _page(
                    "xhtml.example",
                    "https://xhtml.example/",
                    "/",
                    reported_mime="application/xhtml+xml",
                    detected_mime=None,
                ),
                _page(
                    "ignored.example",
                    "https://ignored.example/image",
                    "/image",
                    reported_mime="text/html",
                    detected_mime="image/png",
                ),
            ],
        )

        assert [(row[0], row[1]) for row in _selected(connection, 1)] == [
            ("old.example", "https://old.example/"),
            ("xhtml.example", "https://xhtml.example/"),
        ]
    finally:
        connection.close()


def test_ineligible_rows_are_rejected() -> None:
    connection = _connection()
    try:
        valid = _page("valid.example", "https://valid.example/", "/")
        invalid = [
            _page("status.example", "https://status.example/", "/", status=404),
            _page(
                "mime.example",
                "https://mime.example/",
                "/",
                reported_mime="image/png",
                detected_mime="image/png",
            ),
            _page(None, "https://missing-domain.example/", "/"),
            _page("  ", "https://blank-domain.example/", "/"),
            _page("missing-url.example", None, "/"),
            _page("blank-url.example", "  ", "/"),
            _page(
                "missing-warc.example",
                "https://missing-warc.example/",
                "/",
                warc_filename=None,
            ),
            _page(
                "blank-warc.example",
                "https://blank-warc.example/",
                "/",
                warc_filename=" ",
            ),
            _page("negative-offset.example", "https://negative-offset.example/", "/", offset=-1),
            _page("missing-offset.example", "https://missing-offset.example/", "/", offset=None),
            _page("zero-length.example", "https://zero-length.example/", "/", length=0),
            _page("missing-length.example", "https://missing-length.example/", "/", length=None),
        ]
        _insert(connection, [valid, *invalid])

        assert _selected(connection, 1) == [
            ("valid.example", "https://valid.example/", 1)
        ]
    finally:
        connection.close()


def test_null_path_ranks_after_non_null_path() -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page("ex.com", "https://ex.com/no-path", None, offset=10),
                _page("ex.com", "https://ex.com/path", "/path", offset=20),
            ],
        )

        assert [row[1] for row in _selected(connection, 2)] == [
            "https://ex.com/path",
            "https://ex.com/no-path",
        ]
    finally:
        connection.close()


def test_empty_path_preserves_legacy_priority_over_slash_homepage() -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page("ex.com", "https://ex.com/slash", "/", offset=10),
                _page("ex.com", "https://ex.com/empty", "", offset=20),
            ],
        )

        assert [row[1] for row in _selected(connection, 2)] == [
            "https://ex.com/empty",
            "https://ex.com/slash",
        ]
    finally:
        connection.close()


def test_selection_cap_is_applied_independently_per_domain() -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page("a.example", "https://a.example/", "/", offset=10),
                _page("a.example", "https://a.example/contact", "/contact", offset=20),
                _page("a.example", "https://a.example/news", "/news", offset=30),
                _page("b.example", "https://b.example/", "/", offset=40),
                _page("b.example", "https://b.example/about", "/about", offset=50),
                _page("b.example", "https://b.example/news", "/news", offset=60),
            ],
        )

        rows = _selected(connection, 2)
        assert [row[0] for row in rows] == [
            "a.example",
            "a.example",
            "b.example",
            "b.example",
        ]
        assert [row[2] for row in rows] == [1, 2, 1, 2]
    finally:
        connection.close()


def test_final_ties_are_total_and_deterministic() -> None:
    connection = _connection()
    try:
        _insert(
            connection,
            [
                _page("ex.com", "https://ex.com/a", "/same", warc_filename="w2", offset=20),
                _page("ex.com", "https://ex.com/a", "/same", warc_filename="w1", offset=30),
                _page("ex.com", "https://ex.com/a", "/same", warc_filename="w1", offset=10, length=60),
                _page("ex.com", "https://ex.com/a", "/same", warc_filename="w1", offset=10, length=50),
                _page("ex.com", "https://ex.com/b", "/same", warc_filename="w0", offset=1),
            ],
        )

        rows = connection.execute(
            f"""
            SELECT url, warc_filename, warc_record_offset, warc_record_length
            FROM (
                SELECT *, {ranking_projection()}
                FROM pages
                WHERE {eligibility_predicate()}
            )
            ORDER BY {ranking_order_clause(25)}
            """
        ).fetchall()

        assert rows == [
            ("https://ex.com/a", "w1", 10, 50),
            ("https://ex.com/a", "w1", 10, 60),
            ("https://ex.com/a", "w1", 30, 50),
            ("https://ex.com/a", "w2", 20, 50),
            ("https://ex.com/b", "w0", 1, 50),
        ]
    finally:
        connection.close()


def test_ranking_columns_are_named_and_shared_by_both_stages() -> None:
    projection = ranking_projection()

    assert RANKING_COLUMN_NAMES == (
        "rank_main_site",
        "rank_homepage",
        "rank_priority_path",
        "rank_path_depth",
        "rank_path_length",
        "rank_apex",
    )
    assert all(f"AS {name}" in projection for name in RANKING_COLUMN_NAMES)
    assert ranking_column_names(1) == (
        "rank_main_site",
        "rank_path_depth",
        "rank_path_length",
        "rank_apex",
    )
    assert ranking_column_names(25) == RANKING_COLUMN_NAMES


def test_every_ordering_term_has_explicit_null_order() -> None:
    for pages_per_domain in (1, 25):
        terms = ranking_order_terms(pages_per_domain)
        assert terms
        assert all(term.endswith(" ASC NULLS LAST") for term in terms)


def test_generated_policy_sql_is_path_free() -> None:
    generated_sql = "\n".join(
        (eligibility_predicate(), ranking_projection(), ranking_order_clause(25))
    ).lower()

    assert "read_parquet" not in generated_sql
    assert "http://" not in generated_sql
    assert "https://" not in generated_sql
    assert "/tmp/" not in generated_sql


@pytest.mark.parametrize("pages_per_domain", [0, -1])
def test_invalid_selection_size_is_rejected(pages_per_domain: int) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        ranking_order_clause(pages_per_domain)
