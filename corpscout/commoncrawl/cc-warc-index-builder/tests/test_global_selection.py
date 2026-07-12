from pathlib import Path

import duckdb
import pytest

from warc_index_builder.catalog import (
    GLOBAL_PAGE_COLUMNS,
    BuildStateCorrupt,
    CandidateArtifactError,
    candidate_artifact_path,
    global_selected_pages_query,
    ready_candidate_paths,
)
from warc_index_builder.selection import CANDIDATE_COLUMNS


def _candidate_row(
    source_index: int,
    root_domain: str,
    url: str,
    warc_filename: str,
    offset: int,
    *,
    languages: str | None = None,
    rank_main_site: int = 0,
    rank_homepage: int = 1,
    rank_priority_path: int = 1,
    rank_path_depth: int = 1,
    rank_path_length: int = 10,
    rank_apex: int = 0,
) -> tuple[object, ...]:
    return (
        source_index,
        root_domain,
        url,
        languages,
        warc_filename,
        offset,
        100,
        rank_main_site,
        rank_homepage,
        rank_priority_path,
        rank_path_depth,
        rank_path_length,
        rank_apex,
    )


def _write_candidates(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    rows: list[tuple[object, ...]],
) -> None:
    columns = ", ".join(f"{name} {column_type}" for name, column_type in CANDIDATE_COLUMNS)
    connection.execute(f"CREATE OR REPLACE TABLE candidate_fixture ({columns})")
    if rows:
        placeholders = ", ".join("?" for _ in CANDIDATE_COLUMNS)
        connection.executemany(
            f"INSERT INTO candidate_fixture VALUES ({placeholders})",
            rows,
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        "COPY candidate_fixture TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
        [str(path)],
    )


def _create_inventory(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[int, str]],
) -> None:
    connection.execute(
        "CREATE TABLE warc_inventory(warc_index UINTEGER, warc_filename VARCHAR)"
    )
    if rows:
        connection.executemany("INSERT INTO warc_inventory VALUES (?, ?)", rows)


def _global_rows(
    connection: duckdb.DuckDBPyConnection,
    paths: list[Path],
    pages_per_domain: int,
) -> list[tuple[object, ...]]:
    return connection.execute(
        global_selected_pages_query(pages_per_domain),
        [[str(path) for path in paths]],
    ).fetchall()


def test_global_selection_repeats_total_ranking_and_stable_output_types(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect()
    paths = [tmp_path / "source_00000.parquet", tmp_path / "source_00001.parquet"]
    try:
        _write_candidates(
            connection,
            paths[0],
            [
                _candidate_row(
                    0,
                    "example.com",
                    "https://example.com/",
                    "a.warc.gz",
                    10,
                    rank_homepage=0,
                    rank_path_length=1,
                ),
                _candidate_row(
                    0,
                    "example.com",
                    "https://example.com/ordinary",
                    "a.warc.gz",
                    20,
                    rank_path_length=9,
                ),
            ],
        )
        _write_candidates(
            connection,
            paths[1],
            [
                _candidate_row(
                    1,
                    "example.com",
                    "https://example.com/about",
                    "b.warc.gz",
                    30,
                    rank_priority_path=0,
                    rank_path_length=6,
                )
            ],
        )
        _create_inventory(connection, [(7, "a.warc.gz"), (8, "b.warc.gz")])

        query = global_selected_pages_query(2)
        described = connection.execute(
            f"DESCRIBE SELECT * FROM ({query})",
            [[str(path) for path in paths]],
        ).fetchall()
        rows = connection.execute(
            f"SELECT * FROM ({query}) ORDER BY domain_page_rank",
            [[str(path) for path in paths]],
        ).fetchall()

        assert tuple((str(row[0]), str(row[1])) for row in described) == GLOBAL_PAGE_COLUMNS
        assert rows == [
            (7, "example.com", "https://example.com/", 1, None, 10, 100),
            (8, "example.com", "https://example.com/about", 2, None, 30, 100),
        ]
    finally:
        connection.close()


def test_global_selection_canonicalizes_cross_source_language_winner(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect()
    paths = [tmp_path / "source_00000.parquet", tmp_path / "source_00001.parquet"]
    duplicate = {
        "root_domain": "example.com",
        "url": "https://example.com/about",
        "warc_filename": "a.warc.gz",
        "offset": 10,
        "rank_priority_path": 0,
    }
    try:
        _write_candidates(
            connection,
            paths[0],
            [_candidate_row(0, languages="z", **duplicate)],
        )
        _write_candidates(
            connection,
            paths[1],
            [_candidate_row(1, languages="a", **duplicate)],
        )
        _create_inventory(connection, [(3, "a.warc.gz")])

        assert _global_rows(connection, paths, 25) == [
            (3, "example.com", "https://example.com/about", 1, "a", 10, 100)
        ]
    finally:
        connection.close()


def test_global_selection_rejects_retained_conflict_below_global_top_n(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect()
    paths = [tmp_path / "source_00000.parquet", tmp_path / "source_00001.parquet"]
    try:
        _write_candidates(
            connection,
            paths[0],
            [
                _candidate_row(
                    0,
                    "example.com",
                    "https://example.com/",
                    "a.warc.gz",
                    1,
                    rank_homepage=0,
                    rank_path_length=1,
                ),
                _candidate_row(
                    0,
                    "example.com",
                    "https://example.com/first",
                    "a.warc.gz",
                    99,
                ),
            ],
        )
        _write_candidates(
            connection,
            paths[1],
            [
                _candidate_row(
                    1,
                    "example.com",
                    "https://example.com/conflict",
                    "a.warc.gz",
                    99,
                )
            ],
        )
        _create_inventory(connection, [(0, "a.warc.gz")])

        with pytest.raises(duckdb.Error, match="conflicting selection values"):
            _global_rows(connection, paths, 1)
    finally:
        connection.close()


@pytest.mark.parametrize("ambiguous", [False, True])
def test_global_selection_rejects_missing_or_ambiguous_warc_mapping(
    tmp_path: Path,
    ambiguous: bool,
) -> None:
    connection = duckdb.connect()
    path = tmp_path / "source_00000.parquet"
    try:
        _write_candidates(
            connection,
            path,
            [
                _candidate_row(
                    0,
                    "example.com",
                    "https://example.com/",
                    "missing.warc.gz",
                    10,
                )
            ],
        )
        inventory = (
            [(0, "missing.warc.gz"), (1, "missing.warc.gz")]
            if ambiguous
            else []
        )
        _create_inventory(connection, inventory)

        message = "multiple inventory mappings" if ambiguous else "absent from inventory"
        with pytest.raises(duckdb.Error, match=message):
            _global_rows(connection, [path], 25)
    finally:
        connection.close()


def test_global_selection_rejects_duplicate_mapped_coordinate(tmp_path: Path) -> None:
    connection = duckdb.connect()
    path = tmp_path / "source_00000.parquet"
    try:
        _write_candidates(
            connection,
            path,
            [
                _candidate_row(
                    0,
                    "one.example",
                    "https://one.example/",
                    "a.warc.gz",
                    10,
                ),
                _candidate_row(
                    0,
                    "two.example",
                    "https://two.example/",
                    "b.warc.gz",
                    10,
                ),
            ],
        )
        _create_inventory(connection, [(4, "a.warc.gz"), (4, "b.warc.gz")])

        with pytest.raises(duckdb.Error, match="mapped WARC coordinate is duplicated"):
            _global_rows(connection, [path], 25)
    finally:
        connection.close()


@pytest.mark.parametrize(
    "column_index,value,message",
    [
        (0, None, "source_index is null"),
        (1, "", "root_domain is blank"),
        (2, "", "URL is blank"),
        (4, "", "WARC filename is blank"),
        (5, None, "record offset is null"),
        (6, 0, "record length is not positive"),
    ],
)
def test_global_selection_rejects_invalid_candidate_values(
    tmp_path: Path,
    column_index: int,
    value: object,
    message: str,
) -> None:
    connection = duckdb.connect()
    path = tmp_path / "source_00000.parquet"
    row = list(
        _candidate_row(
            0,
            "example.com",
            "https://example.com/",
            "a.warc.gz",
            10,
        )
    )
    row[column_index] = value
    try:
        _write_candidates(connection, path, [tuple(row)])
        _create_inventory(connection, [(0, "a.warc.gz")])

        with pytest.raises(duckdb.Error, match=message):
            _global_rows(connection, [path], 25)
    finally:
        connection.close()


def test_ready_candidate_paths_requires_complete_validated_ready_set(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    path = candidate_artifact_path(build_directory, 0)
    try:
        _write_candidates(
            validation_connection,
            path,
            [
                _candidate_row(
                    0,
                    "example.com",
                    "https://example.com/",
                    "a.warc.gz",
                    10,
                )
            ],
        )
        state_connection.execute(
            """
            CREATE TABLE source_shards(
                source_index UINTEGER,
                status VARCHAR,
                candidate_rows UBIGINT,
                candidate_bytes UBIGINT
            )
            """
        )
        state_connection.execute(
            "INSERT INTO source_shards VALUES (0, 'ready', 1, ?)",
            [path.stat().st_size],
        )

        assert ready_candidate_paths(
            state_connection,
            validation_connection,
            build_directory,
        ) == (path,)

        state_connection.execute(
            "UPDATE source_shards SET candidate_bytes = candidate_bytes + 1"
        )
        with pytest.raises(CandidateArtifactError, match="conflicts with ready metadata"):
            ready_candidate_paths(
                state_connection,
                validation_connection,
                build_directory,
            )
    finally:
        state_connection.close()
        validation_connection.close()


def test_ready_candidate_paths_rejects_unready_source(tmp_path: Path) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    try:
        state_connection.execute(
            """
            CREATE TABLE source_shards(
                source_index UINTEGER,
                status VARCHAR,
                candidate_rows UBIGINT,
                candidate_bytes UBIGINT
            )
            """
        )
        state_connection.execute(
            "INSERT INTO source_shards VALUES (0, 'pending', NULL, NULL)"
        )

        with pytest.raises(BuildStateCorrupt, match="not ready"):
            ready_candidate_paths(
                state_connection,
                validation_connection,
                tmp_path / "build",
            )
    finally:
        state_connection.close()
        validation_connection.close()


def test_global_selection_rejects_invalid_page_limit() -> None:
    for pages_per_domain in (0, 65536):
        with pytest.raises(ValueError):
            global_selected_pages_query(pages_per_domain)
