from pathlib import Path

import duckdb
import pytest

import warc_index_builder.catalog as catalog
from warc_index_builder.catalog import (
    CATALOG_SCHEMA_VERSION,
    BuildIdentity,
    BuildStateConflict,
    BuildStateCorrupt,
    FinalCatalogBuildError,
    SourceShardSeed,
    candidate_artifact_path,
    checkpoint_warc_size_batch,
    create_partial_catalog,
    initialize_build_state,
    materialize_final_pages,
    partial_catalog_path,
    warc_inventory_sha256,
)
from warc_index_builder.manifests import WarcObject
from warc_index_builder.object_sizes import ProbeMetrics, WarcSizeSuccess
from warc_index_builder.selection import CANDIDATE_COLUMNS, SELECTION_POLICY_VERSION


_WARCS = (
    WarcObject(0, "crawl-data/CC-MAIN-2026-25/segments/a/warc/a.warc.gz"),
    WarcObject(1, "crawl-data/CC-MAIN-2026-25/segments/b/warc/b.warc.gz"),
)


def _identity() -> BuildIdentity:
    return BuildIdentity(
        catalog_schema_version=CATALOG_SCHEMA_VERSION,
        crawl_id="CC-MAIN-2026-25",
        pages_per_domain=25,
        selection_policy_version=SELECTION_POLICY_VERSION,
        selection_policy_sha256="00" * 32,
        source_schema_sha256="11" * 32,
        warc_manifest_sha256="22" * 32,
        index_manifest_sha256="33" * 32,
    )


def _initialize_state(
    connection: duckdb.DuckDBPyConnection,
    *,
    sized: bool = True,
) -> None:
    initialize_build_state(
        connection,
        _identity(),
        _WARCS,
        (SourceShardSeed(0, "https://example/index.parquet", "44" * 32),),
    )
    if sized:
        checkpoint_warc_size_batch(
            connection,
            tuple(
                WarcSizeSuccess(
                    warc=warc,
                    object_bytes=(warc.warc_index + 1) * 1000,
                    attempts=1,
                    retries=0,
                    metrics=ProbeMetrics(head_requests=1),
                )
                for warc in _WARCS
            ),
        )


def _described_columns(
    connection: duckdb.DuckDBPyConnection,
    table: str,
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (str(row[0]), str(row[1]), str(row[2]))
        for row in connection.execute(f"DESCRIBE {table}").fetchall()
    )


def _candidate_row(
    root_domain: str,
    url: str,
    warc_filename: str,
    offset: int,
    length: int,
    *,
    rank_homepage: int = 1,
) -> tuple[object, ...]:
    return (
        0,
        root_domain,
        url,
        None,
        warc_filename,
        offset,
        length,
        0,
        rank_homepage,
        1,
        1,
        len(url),
        0,
    )


def _write_ready_candidates(
    state_connection: duckdb.DuckDBPyConnection,
    validation_connection: duckdb.DuckDBPyConnection,
    build_directory: Path,
    rows: list[tuple[object, ...]],
) -> Path:
    path = candidate_artifact_path(build_directory, 0)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    columns = ", ".join(
        f"{name} {column_type}" for name, column_type in CANDIDATE_COLUMNS
    )
    validation_connection.execute(
        f"CREATE OR REPLACE TABLE final_page_candidates ({columns})"
    )
    if rows:
        validation_connection.executemany(
            "INSERT INTO final_page_candidates VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    validation_connection.execute(
        "COPY final_page_candidates TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
        [str(path)],
    )
    state_connection.execute(
        """
        UPDATE source_shards
        SET status = 'ready', candidate_rows = ?, candidate_bytes = ?,
            attempts = attempts + 1, last_error = NULL,
            completed_at = current_timestamp
        WHERE source_index = 0
        """,
        [len(rows), path.stat().st_size],
    )
    return path


def test_final_warcs_partial_contains_exact_schema_and_complete_inventory(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _initialize_state(state_connection)

        result = create_partial_catalog(state_connection, build_directory)

        expected_hash = warc_inventory_sha256(
            (
                (0, _WARCS[0].warc_filename, 1000),
                (1, _WARCS[1].warc_filename, 2000),
            )
        )
        assert result.path == build_directory / "catalog.duckdb.partial"
        assert result.warc_count == 2
        assert result.total_bytes == 3000
        assert result.inventory_sha256 == expected_hash
        assert result.path.is_file()

        final_connection = duckdb.connect(str(result.path), read_only=True)
        try:
            assert final_connection.execute(
                "SELECT table_name FROM information_schema.tables ORDER BY table_name"
            ).fetchall() == [
                ("catalog_metadata",),
                ("pages",),
                ("warcs",),
            ]
            assert _described_columns(final_connection, "warcs") == (
                ("warc_index", "UINTEGER", "NO"),
                ("warc_filename", "VARCHAR", "NO"),
                ("object_bytes", "UBIGINT", "NO"),
            )
            assert _described_columns(final_connection, "pages") == (
                ("warc_index", "UINTEGER", "NO"),
                ("root_domain", "VARCHAR", "NO"),
                ("url", "VARCHAR", "NO"),
                ("domain_page_rank", "USMALLINT", "NO"),
                ("content_languages", "VARCHAR", "YES"),
                ("warc_record_offset", "UBIGINT", "NO"),
                ("warc_record_length", "UBIGINT", "NO"),
            )
            assert _described_columns(final_connection, "catalog_metadata") == (
                ("singleton", "BOOLEAN", "NO"),
                ("schema_version", "USMALLINT", "NO"),
                ("catalog_id", "VARCHAR", "NO"),
                ("crawl_id", "VARCHAR", "NO"),
                ("selection_name", "VARCHAR", "NO"),
                ("pages_per_domain", "USMALLINT", "NO"),
                ("selection_policy_version", "VARCHAR", "NO"),
                ("selection_policy_sha256", "VARCHAR", "NO"),
                ("source_schema_sha256", "VARCHAR", "NO"),
                ("warc_manifest_sha256", "VARCHAR", "NO"),
                ("index_manifest_sha256", "VARCHAR", "NO"),
                ("warc_inventory_sha256", "VARCHAR", "NO"),
                ("warc_count", "UINTEGER", "NO"),
                ("selected_page_count", "UBIGINT", "NO"),
                ("distinct_domain_count", "UBIGINT", "NO"),
                ("source_index_shard_count", "UINTEGER", "NO"),
                ("duckdb_version", "VARCHAR", "NO"),
                ("builder_version", "VARCHAR", "NO"),
                ("created_at", "TIMESTAMP WITH TIME ZONE", "NO"),
            )
            assert final_connection.execute(
                "SELECT * FROM warcs ORDER BY warc_index"
            ).fetchall() == [
                (0, _WARCS[0].warc_filename, 1000),
                (1, _WARCS[1].warc_filename, 2000),
            ]
            assert final_connection.execute(
                "SELECT (SELECT count(*) FROM pages), "
                "       (SELECT count(*) FROM catalog_metadata)"
            ).fetchone() == (0, 0)
        finally:
            final_connection.close()
    finally:
        state_connection.close()


def test_final_warcs_replaces_only_stale_partial_and_wal(tmp_path: Path) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    partial = partial_catalog_path(build_directory)
    partial.write_bytes(b"stale")
    Path(f"{partial}.wal").write_bytes(b"stale wal")
    sibling = build_directory / "keep.me"
    sibling.write_text("keep")
    published = tmp_path / "catalog.duckdb"
    published.write_bytes(b"published sentinel")
    try:
        _initialize_state(state_connection)

        result = create_partial_catalog(state_connection, build_directory)

        assert result.path.stat().st_size > len(b"stale")
        assert Path(f"{partial}.wal").exists() is False
        assert sibling.read_text() == "keep"
        assert published.read_bytes() == b"published sentinel"
    finally:
        state_connection.close()


def test_final_warcs_supports_apostrophe_in_partial_path(tmp_path: Path) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "builder's files"
    build_directory.mkdir()
    try:
        _initialize_state(state_connection)

        result = create_partial_catalog(state_connection, build_directory)

        assert result.path.is_file()
    finally:
        state_connection.close()


@pytest.mark.parametrize("sized", [False, True])
def test_final_warcs_requires_finalized_matching_state_hash(
    tmp_path: Path,
    sized: bool,
) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    partial = partial_catalog_path(build_directory)
    partial.write_bytes(b"existing sentinel")
    try:
        _initialize_state(state_connection, sized=sized)
        if sized:
            state_connection.execute(
                "UPDATE warc_inventory SET object_bytes = object_bytes + 1 WHERE warc_index = 0"
            )

        error = BuildStateConflict if sized else BuildStateCorrupt
        with pytest.raises(error):
            create_partial_catalog(state_connection, build_directory)

        assert partial.read_bytes() == b"existing sentinel"
    finally:
        state_connection.close()


def test_final_warcs_finalizes_hash_after_last_sizes_were_already_committed(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _initialize_state(state_connection, sized=False)
        state_connection.execute(
            """
            UPDATE warc_inventory
            SET object_bytes = CASE warc_index WHEN 0 THEN 1000 ELSE 2000 END
            """
        )

        result = create_partial_catalog(state_connection, build_directory)

        assert result.warc_count == 2
        assert state_connection.execute(
            "SELECT warc_inventory_sha256 FROM build_identity"
        ).fetchone()[0] == result.inventory_sha256
    finally:
        state_connection.close()


def test_final_warcs_rejects_partial_directory_without_removing_contents(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    partial = partial_catalog_path(build_directory)
    partial.mkdir()
    sentinel = partial / "keep"
    sentinel.write_text("keep")
    try:
        _initialize_state(state_connection)

        with pytest.raises(FinalCatalogBuildError, match="is a directory"):
            create_partial_catalog(state_connection, build_directory)

        assert sentinel.read_text() == "keep"
    finally:
        state_connection.close()


def test_final_warcs_rejects_symlink_without_touching_target(tmp_path: Path) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    outside = tmp_path / "outside.duckdb"
    outside.write_bytes(b"outside sentinel")
    partial = partial_catalog_path(build_directory)
    partial.symlink_to(outside)
    try:
        _initialize_state(state_connection)

        with pytest.raises(ValueError, match="escapes base"):
            create_partial_catalog(state_connection, build_directory)

        assert partial.is_symlink()
        assert outside.read_bytes() == b"outside sentinel"
    finally:
        state_connection.close()


def test_final_warcs_validation_failure_rolls_back_and_removes_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    published = tmp_path / "catalog.duckdb"
    published.write_bytes(b"published sentinel")
    try:
        _initialize_state(state_connection)
        original_validation = catalog._final_warc_inventory

        def fail_validation(_connection: duckdb.DuckDBPyConnection):
            raise RuntimeError("simulated final inventory validation failure")

        monkeypatch.setattr(catalog, "_final_warc_inventory", fail_validation)
        with pytest.raises(RuntimeError, match="simulated final inventory"):
            create_partial_catalog(state_connection, build_directory)

        assert partial_catalog_path(build_directory).exists() is False
        assert Path(f"{partial_catalog_path(build_directory)}.wal").exists() is False
        assert state_connection.execute(
            "SELECT object_bytes FROM warc_inventory ORDER BY warc_index"
        ).fetchall() == [(1000,), (2000,)]
        assert published.read_bytes() == b"published sentinel"

        monkeypatch.setattr(catalog, "_final_warc_inventory", original_validation)
        result = create_partial_catalog(state_connection, build_directory)
        assert result.warc_count == 2
        assert state_connection.execute(
            """
            SELECT count(*) FROM duckdb_databases()
            WHERE database_name = 'final_catalog'
            """
        ).fetchone() == (0,)
        assert published.read_bytes() == b"published sentinel"
    finally:
        state_connection.close()


def test_final_warcs_rebuilds_committed_partial_instead_of_resuming(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _initialize_state(state_connection)

        first = create_partial_catalog(state_connection, build_directory)
        stale_connection = duckdb.connect(str(first.path))
        stale_connection.execute("CREATE TABLE stale_marker(value INTEGER)")
        stale_connection.close()
        second = create_partial_catalog(state_connection, build_directory)

        assert second == first
        assert state_connection.execute(
            """
            SELECT count(*) FROM duckdb_databases()
            WHERE database_name = 'final_catalog'
            """
        ).fetchone() == (0,)
        final_connection = duckdb.connect(str(second.path), read_only=True)
        try:
            assert final_connection.execute(
                """
                SELECT count(*) FROM information_schema.tables
                WHERE table_name = 'stale_marker'
                """
            ).fetchone() == (0,)
            assert final_connection.execute(
                "SELECT warc_index, object_bytes FROM warcs ORDER BY warc_index"
            ).fetchall() == [(0, 1000), (1, 2000)]
        finally:
            final_connection.close()
    finally:
        state_connection.close()


def test_final_pages_are_bulk_materialized_in_physical_warc_offset_order(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    rows = [
        _candidate_row("four.example", "https://four.example/", _WARCS[1].warc_filename, 100, 50),
        _candidate_row("two.example", "https://two.example/", _WARCS[0].warc_filename, 500, 50),
        _candidate_row("one.example", "https://one.example/", _WARCS[0].warc_filename, 20, 50),
        _candidate_row("three.example", "https://three.example/", _WARCS[1].warc_filename, 50, 50),
    ]
    try:
        _initialize_state(state_connection)
        create_partial_catalog(state_connection, build_directory)
        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            rows,
        )

        result = materialize_final_pages(
            state_connection,
            validation_connection,
            build_directory,
        )

        assert result.path == partial_catalog_path(build_directory)
        assert result.selected_page_count == 4
        assert result.distinct_domain_count == 4
        assert result.selected_bytes == 200
        final_connection = duckdb.connect(str(result.path), read_only=True)
        try:
            assert final_connection.execute(
                """
                SELECT warc_index, warc_record_offset
                FROM pages
                ORDER BY rowid
                """
            ).fetchall() == [(0, 20), (0, 500), (1, 50), (1, 100)]
            assert final_connection.execute(
                "SELECT count(*) FROM warcs"
            ).fetchone() == (2,)
            assert final_connection.execute(
                "SELECT count(*) FROM catalog_metadata"
            ).fetchone() == (0,)
        finally:
            final_connection.close()
    finally:
        state_connection.close()
        validation_connection.close()


@pytest.mark.parametrize(
    "offset,length,message",
    [
        (1001, 1, "record offset exceeds object size"),
        (950, 100, "record exceeds object size"),
        (1000, 1, "record exceeds object size"),
    ],
)
def test_final_pages_reject_overflow_safe_out_of_warc_bounds(
    tmp_path: Path,
    offset: int,
    length: int,
    message: str,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _initialize_state(state_connection)
        create_partial_catalog(state_connection, build_directory)
        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            [
                _candidate_row(
                    "example.com",
                    "https://example.com/",
                    _WARCS[0].warc_filename,
                    offset,
                    length,
                )
            ],
        )

        with pytest.raises(FinalCatalogBuildError, match=message):
            materialize_final_pages(
                state_connection,
                validation_connection,
                build_directory,
            )

        final_connection = duckdb.connect(
            str(partial_catalog_path(build_directory)),
            read_only=True,
        )
        try:
            assert final_connection.execute("SELECT count(*) FROM pages").fetchone() == (0,)
            assert final_connection.execute("SELECT count(*) FROM warcs").fetchone() == (2,)
        finally:
            final_connection.close()
        assert state_connection.execute(
            """
            SELECT count(*) FROM duckdb_databases()
            WHERE database_name = 'final_catalog'
            """
        ).fetchone() == (0,)
    finally:
        state_connection.close()
        validation_connection.close()


@pytest.mark.parametrize("length,valid", [(2, True), (3, False)])
def test_final_pages_uint64_max_bounds_do_not_overflow(
    tmp_path: Path,
    length: int,
    valid: bool,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    maximum = 2**64 - 1
    try:
        _initialize_state(state_connection, sized=False)
        checkpoint_warc_size_batch(
            state_connection,
            (
                WarcSizeSuccess(
                    warc=_WARCS[0],
                    object_bytes=maximum,
                    attempts=1,
                    retries=0,
                    metrics=ProbeMetrics(head_requests=1),
                ),
                WarcSizeSuccess(
                    warc=_WARCS[1],
                    object_bytes=2000,
                    attempts=1,
                    retries=0,
                    metrics=ProbeMetrics(head_requests=1),
                ),
            ),
        )
        create_partial_catalog(state_connection, build_directory)
        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            [
                _candidate_row(
                    "maximum.example",
                    "https://maximum.example/",
                    _WARCS[0].warc_filename,
                    maximum - 2,
                    length,
                )
            ],
        )

        if valid:
            result = materialize_final_pages(
                state_connection,
                validation_connection,
                build_directory,
            )
            assert result.selected_page_count == 1
            assert result.selected_bytes == length
        else:
            with pytest.raises(FinalCatalogBuildError, match="record exceeds object size"):
                materialize_final_pages(
                    state_connection,
                    validation_connection,
                    build_directory,
                )
            final_connection = duckdb.connect(
                str(partial_catalog_path(build_directory)),
                read_only=True,
            )
            try:
                assert final_connection.execute(
                    "SELECT count(*) FROM pages"
                ).fetchone() == (0,)
            finally:
                final_connection.close()
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_pages_missing_mapping_rolls_back_and_corrected_retry_succeeds(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    valid_row = _candidate_row(
        "example.com",
        "https://example.com/",
        _WARCS[0].warc_filename,
        10,
        100,
    )
    try:
        _initialize_state(state_connection)
        create_partial_catalog(state_connection, build_directory)
        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            [
                _candidate_row(
                    "missing.example",
                    "https://missing.example/",
                    "missing.warc.gz",
                    10,
                    100,
                )
            ],
        )

        with pytest.raises(FinalCatalogBuildError, match="absent from inventory"):
            materialize_final_pages(
                state_connection,
                validation_connection,
                build_directory,
            )

        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            [valid_row],
        )
        result = materialize_final_pages(
            state_connection,
            validation_connection,
            build_directory,
        )

        assert result.selected_page_count == 1
        final_connection = duckdb.connect(str(result.path), read_only=True)
        try:
            assert final_connection.execute(
                "SELECT root_domain, warc_record_offset FROM pages"
            ).fetchall() == [("example.com", 10)]
        finally:
            final_connection.close()
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_pages_rejects_nonempty_committed_pages(tmp_path: Path) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _initialize_state(state_connection)
        create_partial_catalog(state_connection, build_directory)
        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            [
                _candidate_row(
                    "first.example",
                    "https://first.example/",
                    _WARCS[0].warc_filename,
                    10,
                    100,
                )
            ],
        )
        materialize_final_pages(
            state_connection,
            validation_connection,
            build_directory,
        )
        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            [
                _candidate_row(
                    "second.example",
                    "https://second.example/",
                    _WARCS[1].warc_filename,
                    20,
                    100,
                )
            ],
        )

        with pytest.raises(FinalCatalogBuildError, match="already materialized"):
            materialize_final_pages(
                state_connection,
                validation_connection,
                build_directory,
            )

        final_connection = duckdb.connect(
            str(partial_catalog_path(build_directory)),
            read_only=True,
        )
        try:
            assert final_connection.execute(
                "SELECT warc_index, root_domain, warc_record_offset FROM pages"
            ).fetchall() == [(0, "first.example", 10)]
        finally:
            final_connection.close()
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_pages_rejects_final_warc_hash_divergence_before_insert(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _initialize_state(state_connection)
        result = create_partial_catalog(state_connection, build_directory)
        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            [
                _candidate_row(
                    "example.com",
                    "https://example.com/",
                    _WARCS[0].warc_filename,
                    10,
                    100,
                )
            ],
        )
        corrupt_connection = duckdb.connect(str(result.path))
        corrupt_connection.execute(
            "UPDATE warcs SET object_bytes = object_bytes + 1 WHERE warc_index = 0"
        )
        corrupt_connection.close()

        with pytest.raises(FinalCatalogBuildError, match="hash differs"):
            materialize_final_pages(
                state_connection,
                validation_connection,
                build_directory,
            )

        final_connection = duckdb.connect(str(result.path), read_only=True)
        try:
            assert final_connection.execute("SELECT count(*) FROM pages").fetchone() == (0,)
            assert final_connection.execute(
                "SELECT object_bytes FROM warcs WHERE warc_index = 0"
            ).fetchone() == (1001,)
        finally:
            final_connection.close()
    finally:
        state_connection.close()
        validation_connection.close()
