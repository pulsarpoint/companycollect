import hashlib
import importlib.metadata
from dataclasses import replace
from pathlib import Path

import duckdb
import httpx
import pytest

import warc_index_builder.__main__ as command
import warc_index_builder.catalog as catalog
from warc_index_builder.catalog import (
    CATALOG_SCHEMA_VERSION,
    BuildIdentity,
    BuildStateConflict,
    BuildStateCorrupt,
    CatalogPublicationError,
    CatalogValidationError,
    FinalCatalogBuildError,
    SourceShardSeed,
    build_final_catalog_partial,
    candidate_artifact_path,
    checkpoint_warc_size_batch,
    create_partial_catalog,
    initialize_build_state,
    materialize_final_metadata,
    materialize_final_pages,
    partial_catalog_path,
    inspect_published_catalog,
    publish_catalog,
    validate_catalog,
    warc_inventory_sha256,
)
from warc_index_builder.manifests import (
    ManifestChangedError,
    ManifestDownloadError,
    WarcObject,
)
from warc_index_builder.object_sizes import ProbeMetrics, WarcSizeSuccess
from warc_index_builder.selection import (
    CANDIDATE_COLUMNS,
    SELECTION_POLICY_VERSION,
    selection_policy_sha256,
)


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
    identity: BuildIdentity | None = None,
) -> None:
    initialize_build_state(
        connection,
        identity or _identity(),
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


def _prepare_final_pages(
    state_connection: duckdb.DuckDBPyConnection,
    validation_connection: duckdb.DuckDBPyConnection,
    build_directory: Path,
    rows: list[tuple[object, ...]],
) -> None:
    _initialize_state(state_connection)
    create_partial_catalog(state_connection, build_directory)
    _write_ready_candidates(
        state_connection,
        validation_connection,
        build_directory,
        rows,
    )
    materialize_final_pages(
        state_connection,
        validation_connection,
        build_directory,
    )


def _completed_catalog_path(tmp_path: Path) -> Path:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    rows = [
        _candidate_row(
            "example.com",
            "https://example.com/",
            _WARCS[0].warc_filename,
            10,
            100,
        ),
        _candidate_row(
            "example.com",
            "https://example.com/about",
            _WARCS[0].warc_filename,
            200,
            50,
            rank_homepage=0,
        ),
        _candidate_row(
            "other.example",
            "https://other.example/",
            _WARCS[1].warc_filename,
            20,
            75,
        ),
    ]
    try:
        _prepare_final_pages(
            state_connection,
            validation_connection,
            build_directory,
            rows,
        )
        return materialize_final_metadata(
            state_connection,
            build_directory,
        ).path
    finally:
        state_connection.close()
        validation_connection.close()


def _replace_validation_warcs_without_constraints(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    connection.execute(
        """
        CREATE TABLE replacement_warcs (
            warc_index UINTEGER NOT NULL,
            warc_filename VARCHAR NOT NULL,
            object_bytes UBIGINT NOT NULL
        )
        """
    )
    connection.execute("INSERT INTO replacement_warcs SELECT * FROM warcs")
    connection.execute("DROP TABLE warcs")
    connection.execute("ALTER TABLE replacement_warcs RENAME TO warcs")


def _synchronize_validation_identity(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    inventory = tuple(
        (int(index), str(filename), int(byte_count))
        for index, filename, byte_count in connection.execute(
            """
            SELECT warc_index, warc_filename, object_bytes
            FROM warcs ORDER BY warc_index
            """
        ).fetchall()
    )
    inventory_hash = warc_inventory_sha256(inventory)
    (
        schema_version,
        crawl_id,
        pages_per_domain,
        policy_version,
        policy_hash,
        schema_hash,
        warc_manifest_hash,
        index_manifest_hash,
    ) = connection.execute(
        """
        SELECT schema_version, crawl_id, pages_per_domain,
               selection_policy_version, selection_policy_sha256,
               source_schema_sha256, warc_manifest_sha256,
               index_manifest_sha256
        FROM catalog_metadata
        """
    ).fetchone()
    identity = catalog.catalog_id(
        schema_version=int(schema_version),
        crawl_id=str(crawl_id),
        pages_per_domain=int(pages_per_domain),
        selection_policy_version=str(policy_version),
        selection_policy_sha256=str(policy_hash),
        source_schema_sha256=str(schema_hash),
        warc_manifest_sha256=str(warc_manifest_hash),
        index_manifest_sha256=str(index_manifest_hash),
        warc_inventory_sha256=inventory_hash,
    )
    connection.execute(
        """
        UPDATE catalog_metadata
        SET warc_inventory_sha256 = ?, catalog_id = ?
        """,
        [inventory_hash, identity],
    )


def _prepare_final_build_inputs(
    state_connection: duckdb.DuckDBPyConnection,
    validation_connection: duckdb.DuckDBPyConnection,
    build_directory: Path,
    *,
    identity: BuildIdentity | None = None,
) -> None:
    _initialize_state(state_connection, identity=identity)
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


def _completed_final_build(
    catalog_directory: Path,
    *,
    identity: BuildIdentity | None = None,
):
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    catalog_directory.mkdir(parents=True, exist_ok=True)
    build_directory = catalog_directory / ".build"
    build_directory.mkdir()
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
            identity=identity,
        )
        result = build_final_catalog_partial(
            state_connection,
            validation_connection,
            build_directory,
        )
        return result, build_directory
    finally:
        state_connection.close()
        validation_connection.close()


class _CatalogConnectionProxy:
    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        fail_checkpoint: bool = False,
        fail_close: bool = False,
        wal_after_close: Path | None = None,
    ) -> None:
        self.connection = connection
        self.fail_checkpoint = fail_checkpoint
        self.fail_close = fail_close
        self.wal_after_close = wal_after_close
        self.closed = False

    def execute(self, query: str, *args: object, **kwargs: object):
        if self.fail_checkpoint and query.strip().upper() == "FORCE CHECKPOINT":
            raise duckdb.IOException("simulated checkpoint failure")
        return self.connection.execute(query, *args, **kwargs)

    def close(self) -> None:
        self.connection.close()
        self.closed = True
        if self.wal_after_close is not None:
            self.wal_after_close.write_bytes(b"simulated WAL")
        if self.fail_close:
            raise RuntimeError("simulated close failure")

    def __getattr__(self, name: str):
        return getattr(self.connection, name)


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
                ("_build_progress",),
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
                "       (SELECT count(*) FROM catalog_metadata), "
                "       (SELECT pages_materialized FROM _build_progress)"
            ).fetchone() == (0, 0, False)
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
            assert final_connection.execute(
                "SELECT pages_materialized FROM _build_progress"
            ).fetchone() == (True,)
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


def test_final_metadata_contains_exact_identity_versions_and_actual_counts(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    rows = [
        _candidate_row(
            "example.com",
            "https://example.com/",
            _WARCS[0].warc_filename,
            10,
            100,
        ),
        _candidate_row(
            "example.com",
            "https://example.com/about",
            _WARCS[0].warc_filename,
            200,
            50,
            rank_homepage=0,
        ),
        _candidate_row(
            "other.example",
            "https://other.example/",
            _WARCS[1].warc_filename,
            20,
            75,
        ),
    ]
    try:
        _prepare_final_pages(
            state_connection,
            validation_connection,
            build_directory,
            rows,
        )
        expected_inventory_hash = warc_inventory_sha256(
            (
                (0, _WARCS[0].warc_filename, 1000),
                (1, _WARCS[1].warc_filename, 2000),
            )
        )
        expected_id = catalog.catalog_id(
            schema_version=CATALOG_SCHEMA_VERSION,
            crawl_id=_identity().crawl_id,
            pages_per_domain=_identity().pages_per_domain,
            selection_policy_version=_identity().selection_policy_version,
            selection_policy_sha256=_identity().selection_policy_sha256,
            source_schema_sha256=_identity().source_schema_sha256,
            warc_manifest_sha256=_identity().warc_manifest_sha256,
            index_manifest_sha256=_identity().index_manifest_sha256,
            warc_inventory_sha256=expected_inventory_hash,
        )

        result = materialize_final_metadata(state_connection, build_directory)

        assert result.path == partial_catalog_path(build_directory)
        assert result.catalog_id == expected_id
        final_connection = duckdb.connect(str(result.path), read_only=True)
        try:
            metadata = final_connection.execute(
                """
                SELECT singleton, schema_version, catalog_id, crawl_id,
                       selection_name, pages_per_domain,
                       selection_policy_version, selection_policy_sha256,
                       source_schema_sha256, warc_manifest_sha256,
                       index_manifest_sha256, warc_inventory_sha256,
                       warc_count, selected_page_count, distinct_domain_count,
                       source_index_shard_count, duckdb_version, builder_version,
                       created_at IS NOT NULL
                FROM catalog_metadata
                """
            ).fetchall()
            assert metadata == [
                (
                    True,
                    CATALOG_SCHEMA_VERSION,
                    expected_id,
                    _identity().crawl_id,
                    "pages25",
                    25,
                    _identity().selection_policy_version,
                    _identity().selection_policy_sha256,
                    _identity().source_schema_sha256,
                    _identity().warc_manifest_sha256,
                    _identity().index_manifest_sha256,
                    expected_inventory_hash,
                    2,
                    3,
                    2,
                    1,
                    state_connection.execute("SELECT version()").fetchone()[0],
                    importlib.metadata.version("cc-warc-index-builder"),
                    True,
                )
            ]
            assert final_connection.execute(
                "SELECT table_name FROM information_schema.tables ORDER BY table_name"
            ).fetchall() == [
                ("catalog_metadata",),
                ("pages",),
                ("warcs",),
            ]
        finally:
            final_connection.close()
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_metadata_catalog_id_is_stable_across_build_paths(
    tmp_path: Path,
) -> None:
    results = []
    for name in ("first", "second"):
        state_connection = duckdb.connect()
        validation_connection = duckdb.connect()
        build_directory = tmp_path / name
        build_directory.mkdir()
        try:
            _prepare_final_pages(
                state_connection,
                validation_connection,
                build_directory,
                [],
            )
            results.append(
                materialize_final_metadata(state_connection, build_directory)
            )
        finally:
            state_connection.close()
            validation_connection.close()

    assert results[0].path != results[1].path
    assert results[0].catalog_id == results[1].catalog_id
    for result in results:
        connection = duckdb.connect(str(result.path), read_only=True)
        try:
            assert connection.execute(
                """
                SELECT selected_page_count, distinct_domain_count,
                       created_at IS NOT NULL
                FROM catalog_metadata
                """
            ).fetchone() == (0, 0, True)
        finally:
            connection.close()


def test_final_metadata_failure_after_progress_drop_rolls_back_and_retry_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _prepare_final_pages(
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
        original_result_type = catalog.FinalMetadataResult

        def fail_result(**_values: object) -> None:
            raise RuntimeError("simulated metadata result failure")

        monkeypatch.setattr(
            catalog,
            "FinalMetadataResult",
            fail_result,
        )
        with pytest.raises(RuntimeError, match="simulated metadata result"):
            materialize_final_metadata(state_connection, build_directory)

        connection = duckdb.connect(
            str(partial_catalog_path(build_directory)), read_only=True
        )
        try:
            assert connection.execute(
                """
                SELECT (SELECT count(*) FROM catalog_metadata),
                       (SELECT count(*) FROM warcs),
                       (SELECT count(*) FROM pages),
                       (SELECT pages_materialized FROM _build_progress)
                """
            ).fetchone() == (0, 2, 1, True)
        finally:
            connection.close()

        monkeypatch.setattr(
            catalog,
            "FinalMetadataResult",
            original_result_type,
        )
        result = materialize_final_metadata(state_connection, build_directory)
        assert result.catalog_id
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_metadata_rejects_nonready_source_and_existing_metadata(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _initialize_state(state_connection)
        create_partial_catalog(state_connection, build_directory)
        with pytest.raises(BuildStateCorrupt, match="is not ready"):
            materialize_final_metadata(state_connection, build_directory)

        _write_ready_candidates(
            state_connection,
            validation_connection,
            build_directory,
            [],
        )
        materialize_final_pages(
            state_connection,
            validation_connection,
            build_directory,
        )
        first = materialize_final_metadata(state_connection, build_directory)
        first_connection = duckdb.connect(str(first.path), read_only=True)
        try:
            stored_before = first_connection.execute(
                "SELECT catalog_id, CAST(created_at AS VARCHAR) FROM catalog_metadata"
            ).fetchone()
        finally:
            first_connection.close()

        with pytest.raises(FinalCatalogBuildError, match="already materialized"):
            materialize_final_metadata(state_connection, build_directory)

        final_connection = duckdb.connect(str(first.path), read_only=True)
        try:
            assert final_connection.execute(
                "SELECT catalog_id, CAST(created_at AS VARCHAR) FROM catalog_metadata"
            ).fetchone() == stored_before
        finally:
            final_connection.close()
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_metadata_requires_committed_page_materialization(
    tmp_path: Path,
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
                    10,
                    100,
                )
            ],
        )

        with pytest.raises(
            FinalCatalogBuildError,
            match="pages have not been materialized",
        ):
            materialize_final_metadata(state_connection, build_directory)

        partial_connection = duckdb.connect(
            str(partial_catalog_path(build_directory)), read_only=True
        )
        try:
            assert partial_connection.execute(
                """
                SELECT (SELECT count(*) FROM pages),
                       (SELECT count(*) FROM catalog_metadata),
                       (SELECT pages_materialized FROM _build_progress)
                """
            ).fetchone() == (0, 0, False)
        finally:
            partial_connection.close()

        materialize_final_pages(
            state_connection,
            validation_connection,
            build_directory,
        )
        result = materialize_final_metadata(state_connection, build_directory)
        assert result.catalog_id
    finally:
        state_connection.close()
        validation_connection.close()


def test_catalog_validation_accepts_completed_catalog_read_only_without_changes(
    tmp_path: Path,
) -> None:
    path = _completed_catalog_path(tmp_path)
    bytes_before = path.read_bytes()
    connection = duckdb.connect(str(path), read_only=True)
    try:
        result = validate_catalog(connection)
    finally:
        connection.close()

    assert result.catalog_id
    assert result.crawl_id == "CC-MAIN-2026-25"
    assert result.selection_name == "pages25"
    assert result.pages_per_domain == 25
    assert result.warc_count == 2
    assert result.selected_page_count == 3
    assert result.distinct_domain_count == 2
    assert result.source_index_shard_count == 1
    assert path.read_bytes() == bytes_before


def test_catalog_validation_accepts_completed_catalog_with_zero_pages(
    tmp_path: Path,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _prepare_final_pages(
            state_connection,
            validation_connection,
            build_directory,
            [],
        )
        path = materialize_final_metadata(
            state_connection,
            build_directory,
        ).path
    finally:
        state_connection.close()
        validation_connection.close()

    connection = duckdb.connect(str(path), read_only=True)
    try:
        result = validate_catalog(connection)
    finally:
        connection.close()
    assert result.selected_page_count == 0
    assert result.distinct_domain_count == 0


@pytest.mark.parametrize(
    "statement,message",
    [
        ("DELETE FROM catalog_metadata", "exactly one metadata row"),
        (
            "UPDATE catalog_metadata SET schema_version = 2",
            "schema version is unsupported",
        ),
        (
            "UPDATE catalog_metadata SET selection_name = 'pages1'",
            "selection name conflicts",
        ),
        (
            "UPDATE catalog_metadata SET catalog_id = repeat('0', 64)",
            "catalog ID differs",
        ),
        (
            "UPDATE catalog_metadata SET selection_policy_sha256 = 'invalid'",
            "noncanonical hash",
        ),
        (
            "UPDATE catalog_metadata SET crawl_id = 'invalid'",
            "identity fields are invalid",
        ),
        (
            "UPDATE catalog_metadata SET source_index_shard_count = 0",
            "source shard count must be positive",
        ),
        (
            "UPDATE catalog_metadata SET builder_version = '   '",
            "runtime versions must not be blank",
        ),
    ],
)
def test_catalog_validation_rejects_invalid_metadata(
    tmp_path: Path,
    statement: str,
    message: str,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        connection.execute(statement)

        with pytest.raises(CatalogValidationError, match=message) as error:
            validate_catalog(connection)

        assert error.value.samples
    finally:
        connection.close()


@pytest.mark.parametrize(
    "statement,message",
    [
        (
            "UPDATE warcs SET warc_index = 2 WHERE warc_index = 1",
            "indexes are not contiguous",
        ),
        (
            "UPDATE warcs SET object_bytes = object_bytes + 1 WHERE warc_index = 0",
            "inventory hash differs",
        ),
    ],
)
def test_catalog_validation_rejects_warc_identity_or_hash_corruption(
    tmp_path: Path,
    statement: str,
    message: str,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        connection.execute(statement)

        with pytest.raises(CatalogValidationError, match=message) as error:
            validate_catalog(connection)

        assert error.value.samples
    finally:
        connection.close()


@pytest.mark.parametrize(
    "statement,message",
    [
        (
            """
            UPDATE warcs
            SET warc_filename = (
                SELECT warc_filename FROM warcs WHERE warc_index = 0
            )
            WHERE warc_index = 1
            """,
            "filenames are not unique",
        ),
        (
            "UPDATE warcs SET object_bytes = 0 WHERE warc_index = 0",
            "object sizes must be positive",
        ),
    ],
)
def test_catalog_validation_rejects_invalid_warc_rows(
    tmp_path: Path,
    statement: str,
    message: str,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        _replace_validation_warcs_without_constraints(connection)
        connection.execute(statement)

        with pytest.raises(CatalogValidationError, match=message) as error:
            validate_catalog(connection)

        assert error.value.samples
    finally:
        connection.close()


@pytest.mark.parametrize(
    "statement,message",
    [
        (
            "UPDATE pages SET warc_index = 99 WHERE root_domain = 'other.example'",
            "WARC absent from the inventory",
        ),
        (
            "UPDATE pages SET warc_record_length = 0 WHERE root_domain = 'other.example'",
            "zero-length WARC record",
        ),
        (
            "UPDATE pages SET root_domain = '' WHERE root_domain = 'other.example'",
            "blank domain or URL",
        ),
        (
            "UPDATE pages SET warc_record_offset = 2001 WHERE root_domain = 'other.example'",
            "offset exceeds object size",
        ),
        (
            """
            UPDATE pages
            SET warc_record_offset = 1950, warc_record_length = 100
            WHERE root_domain = 'other.example'
            """,
            "record exceeds object size",
        ),
    ],
)
def test_catalog_validation_rejects_invalid_page_mapping_or_bounds(
    tmp_path: Path,
    statement: str,
    message: str,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        connection.execute(statement)

        with pytest.raises(CatalogValidationError, match=message) as error:
            validate_catalog(connection)

        assert error.value.samples
    finally:
        connection.close()


def test_catalog_validation_rejects_duplicate_page_coordinates(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        connection.execute(
            """
            INSERT INTO pages
            SELECT * FROM pages WHERE root_domain = 'other.example'
            """
        )

        with pytest.raises(
            CatalogValidationError,
            match="duplicate WARC coordinates",
        ) as error:
            validate_catalog(connection)

        assert error.value.samples[0][3] == 2
    finally:
        connection.close()


@pytest.mark.parametrize(
    "statement",
    [
        """
        UPDATE pages SET domain_page_rank = 0
        WHERE root_domain = 'example.com' AND domain_page_rank = 1
        """,
        """
        UPDATE pages SET domain_page_rank = 3
        WHERE root_domain = 'example.com' AND domain_page_rank = 2
        """,
        """
        UPDATE pages SET domain_page_rank = 1
        WHERE root_domain = 'example.com' AND domain_page_rank = 2
        """,
        """
        UPDATE pages SET domain_page_rank = 26
        WHERE root_domain = 'example.com' AND domain_page_rank = 2
        """,
    ],
)
def test_catalog_validation_rejects_invalid_domain_rank_sequences(
    tmp_path: Path,
    statement: str,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        connection.execute(statement)

        with pytest.raises(
            CatalogValidationError,
            match="domain ranks are not unique, gapless, and bounded",
        ) as error:
            validate_catalog(connection)

        assert error.value.samples[0][0] == "example.com"
    finally:
        connection.close()


@pytest.mark.parametrize(
    "field",
    ["warc_count", "selected_page_count", "distinct_domain_count"],
)
def test_catalog_validation_rejects_incorrect_stored_counts(
    tmp_path: Path,
    field: str,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        connection.execute(f"UPDATE catalog_metadata SET {field} = {field} + 1")

        with pytest.raises(
            CatalogValidationError,
            match="stored counts differ",
        ) as error:
            validate_catalog(connection)

        assert error.value.samples[0][0] == field
    finally:
        connection.close()


def test_catalog_validation_samples_are_bounded_and_deterministic(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        connection.executemany(
            """
            INSERT INTO pages VALUES (?, ?, ?, ?, NULL, ?, ?)
            """,
            [
                (99, f"invalid-{index}.example", f"https://invalid-{index}.example/", 1, 0, 10)
                for index in range(5)
            ],
        )

        with pytest.raises(CatalogValidationError) as error:
            validate_catalog(connection)

        assert len(error.value.samples) == 3
        assert error.value.has_more_samples is True
        assert [sample[3] for sample in error.value.samples] == [
            "invalid-0.example",
            "invalid-1.example",
            "invalid-2.example",
        ]
        assert "additional invalid rows omitted" in str(error.value)
    finally:
        connection.close()


def test_catalog_validation_uses_overflow_safe_uint64_bounds(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    maximum = 2**64 - 1
    try:
        connection.execute(
            "UPDATE warcs SET object_bytes = ? WHERE warc_index = 0",
            [maximum],
        )
        _synchronize_validation_identity(connection)
        connection.execute(
            """
            UPDATE pages
            SET warc_record_offset = ?, warc_record_length = 1
            WHERE url = 'https://example.com/'
            """,
            [maximum - 1],
        )

        assert validate_catalog(connection).selected_page_count == 3

        connection.execute(
            """
            UPDATE pages
            SET warc_record_length = 2
            WHERE url = 'https://example.com/'
            """
        )
        with pytest.raises(
            CatalogValidationError,
            match="record exceeds object size",
        ) as error:
            validate_catalog(connection)
        assert error.value.samples[0][3] == maximum
    finally:
        connection.close()


def test_catalog_validation_rejects_incompatible_table_set(tmp_path: Path) -> None:
    connection = duckdb.connect(str(_completed_catalog_path(tmp_path)))
    try:
        connection.execute("CREATE TABLE unexpected(value INTEGER)")

        with pytest.raises(
            CatalogValidationError,
            match="incompatible table set",
        ) as error:
            validate_catalog(connection)

        assert error.value.samples
    finally:
        connection.close()


def test_final_build_checkpoints_closes_and_reopens_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    published = tmp_path / "catalog.duckdb"
    published.write_bytes(b"published sentinel")
    published_inode = published.stat().st_ino
    access_modes: list[str] = []
    original_validation = catalog.validate_catalog

    def record_access_mode(connection: duckdb.DuckDBPyConnection):
        access_modes.append(
            str(
                connection.execute(
                    "SELECT current_setting('access_mode')"
                ).fetchone()[0]
            )
        )
        return original_validation(connection)

    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )
        monkeypatch.setattr(catalog, "validate_catalog", record_access_mode)

        result = build_final_catalog_partial(
            state_connection,
            validation_connection,
            build_directory,
        )

        assert result.path == partial_catalog_path(build_directory)
        assert result.validation.selected_page_count == 1
        assert access_modes == ["automatic", "read_only"]
        assert Path(f"{result.path}.wal").exists() is False
        assert state_connection.execute(
            """
            SELECT count(*) FROM duckdb_databases()
            WHERE database_name = 'final_catalog'
            """
        ).fetchone() == (0,)
        read_only_connection = duckdb.connect(str(result.path), read_only=True)
        try:
            with pytest.raises(duckdb.Error):
                read_only_connection.execute("CREATE TABLE forbidden(value INTEGER)")
        finally:
            read_only_connection.close()
        assert published.read_bytes() == b"published sentinel"
        assert published.stat().st_ino == published_inode
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_build_failure_preserves_published_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    published = tmp_path / "catalog.duckdb"
    published.write_bytes(b"published sentinel")
    published_inode = published.stat().st_ino
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )

        def fail_pages(*_args: object, **_kwargs: object) -> None:
            raise RuntimeError("simulated final page build failure")

        monkeypatch.setattr(catalog, "materialize_final_pages", fail_pages)
        with pytest.raises(RuntimeError, match="simulated final page build"):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )

        assert published.read_bytes() == b"published sentinel"
        assert published.stat().st_ino == published_inode
    finally:
        state_connection.close()
        validation_connection.close()


@pytest.mark.parametrize(
    "phase,field",
    [
        ("warcs", "warc_count"),
        ("warcs", "warc_inventory_sha256"),
        ("pages", "selected_page_count"),
        ("pages", "distinct_domain_count"),
    ],
)
def test_final_build_rejects_phase_results_that_differ_from_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
    field: str,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )
        if phase == "warcs":
            original = catalog.create_partial_catalog

            def change_warcs(*args: object, **kwargs: object):
                result = original(*args, **kwargs)
                if field == "warc_count":
                    return replace(result, warc_count=result.warc_count + 1)
                return replace(result, inventory_sha256="00" * 32)

            monkeypatch.setattr(catalog, "create_partial_catalog", change_warcs)
        else:
            original_pages = catalog.materialize_final_pages

            def change_pages(*args: object, **kwargs: object):
                result = original_pages(*args, **kwargs)
                if field == "selected_page_count":
                    return replace(
                        result,
                        selected_page_count=result.selected_page_count + 1,
                    )
                return replace(
                    result,
                    distinct_domain_count=result.distinct_domain_count + 1,
                )

            monkeypatch.setattr(catalog, "materialize_final_pages", change_pages)

        with pytest.raises(
            FinalCatalogBuildError,
            match=field,
        ):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_build_checkpoint_failure_closes_handle_and_stops_before_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    real_connect = duckdb.connect
    proxies: list[_CatalogConnectionProxy] = []
    read_only_opens = 0
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )

        def connect(*args: object, **kwargs: object):
            nonlocal read_only_opens
            if kwargs.get("read_only"):
                read_only_opens += 1
                return real_connect(*args, **kwargs)
            proxy = _CatalogConnectionProxy(
                real_connect(*args, **kwargs),
                fail_checkpoint=True,
            )
            proxies.append(proxy)
            return proxy

        monkeypatch.setattr(catalog.duckdb, "connect", connect)
        with pytest.raises(
            FinalCatalogBuildError,
            match="force checkpoint completed partial catalog",
        ):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )

        assert len(proxies) == 1
        assert proxies[0].closed is True
        assert read_only_opens == 0
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_build_writable_close_failure_blocks_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    real_connect = duckdb.connect
    proxies: list[_CatalogConnectionProxy] = []
    read_only_opens = 0
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )

        def connect(*args: object, **kwargs: object):
            nonlocal read_only_opens
            if kwargs.get("read_only"):
                read_only_opens += 1
                return real_connect(*args, **kwargs)
            proxy = _CatalogConnectionProxy(
                real_connect(*args, **kwargs),
                fail_close=True,
            )
            proxies.append(proxy)
            return proxy

        monkeypatch.setattr(catalog.duckdb, "connect", connect)
        with pytest.raises(
            FinalCatalogBuildError,
            match="close checkpointed partial catalog",
        ):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )

        assert proxies[0].closed is True
        assert read_only_opens == 0
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_build_remaining_wal_blocks_read_only_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    real_connect = duckdb.connect
    read_only_opens = 0
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )
        wal_path = Path(f"{partial_catalog_path(build_directory)}.wal")

        def connect(*args: object, **kwargs: object):
            nonlocal read_only_opens
            if kwargs.get("read_only"):
                read_only_opens += 1
                return real_connect(*args, **kwargs)
            return _CatalogConnectionProxy(
                real_connect(*args, **kwargs),
                wal_after_close=wal_path,
            )

        monkeypatch.setattr(catalog.duckdb, "connect", connect)
        with pytest.raises(
            FinalCatalogBuildError,
            match="WAL remains after forced checkpoint and close",
        ):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )

        assert wal_path.read_bytes() == b"simulated WAL"
        assert read_only_opens == 0
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_build_read_only_reopen_failure_is_contextual(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    real_connect = duckdb.connect
    writable_connection: duckdb.DuckDBPyConnection | None = None
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )

        def connect(*args: object, **kwargs: object):
            nonlocal writable_connection
            if kwargs.get("read_only"):
                raise duckdb.IOException("simulated read-only reopen failure")
            writable_connection = real_connect(*args, **kwargs)
            return writable_connection

        monkeypatch.setattr(catalog.duckdb, "connect", connect)
        with pytest.raises(
            FinalCatalogBuildError,
            match="reopen completed partial catalog read-only",
        ):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )

        assert writable_connection is not None
        with pytest.raises(duckdb.ConnectionException):
            writable_connection.execute("SELECT 1")
        assert Path(f"{partial_catalog_path(build_directory)}.wal").exists() is False
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_build_reopened_validation_failure_closes_reader(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    original_validation = catalog.validate_catalog
    real_connect = duckdb.connect
    validations = 0
    read_only_proxy: _CatalogConnectionProxy | None = None
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )

        def fail_second_validation(connection: duckdb.DuckDBPyConnection):
            nonlocal validations
            validations += 1
            if validations == 2:
                raise CatalogValidationError("simulated reopened validation failure")
            return original_validation(connection)

        def connect(*args: object, **kwargs: object):
            nonlocal read_only_proxy
            connection = real_connect(*args, **kwargs)
            if not kwargs.get("read_only"):
                return connection
            read_only_proxy = _CatalogConnectionProxy(connection)
            return read_only_proxy

        monkeypatch.setattr(
            catalog,
            "validate_catalog",
            fail_second_validation,
        )
        monkeypatch.setattr(catalog.duckdb, "connect", connect)
        with pytest.raises(
            CatalogValidationError,
            match="simulated reopened validation failure",
        ):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )

        assert validations == 2
        assert read_only_proxy is not None
        assert read_only_proxy.closed is True
        connection = real_connect(
            str(partial_catalog_path(build_directory)), read_only=True
        )
        try:
            assert original_validation(connection).selected_page_count == 1
        finally:
            connection.close()
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_build_read_only_close_failure_rejects_partial(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    real_connect = duckdb.connect
    read_only_proxy: _CatalogConnectionProxy | None = None
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )

        def connect(*args: object, **kwargs: object):
            nonlocal read_only_proxy
            connection = real_connect(*args, **kwargs)
            if not kwargs.get("read_only"):
                return connection
            read_only_proxy = _CatalogConnectionProxy(
                connection,
                fail_close=True,
            )
            return read_only_proxy

        monkeypatch.setattr(catalog.duckdb, "connect", connect)
        with pytest.raises(
            FinalCatalogBuildError,
            match="close read-only partial catalog",
        ):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )

        assert read_only_proxy is not None
        assert read_only_proxy.closed is True
    finally:
        state_connection.close()
        validation_connection.close()


def test_final_build_rejects_changed_validation_after_reopen(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_connection = duckdb.connect()
    validation_connection = duckdb.connect()
    build_directory = tmp_path / "build"
    build_directory.mkdir()
    original_validation = catalog.validate_catalog
    validations = 0
    try:
        _prepare_final_build_inputs(
            state_connection,
            validation_connection,
            build_directory,
        )

        def change_second_validation(connection: duckdb.DuckDBPyConnection):
            nonlocal validations
            validations += 1
            result = original_validation(connection)
            if validations == 2:
                return replace(
                    result,
                    selected_page_count=result.selected_page_count + 1,
                )
            return result

        monkeypatch.setattr(
            catalog,
            "validate_catalog",
            change_second_validation,
        )
        with pytest.raises(
            FinalCatalogBuildError,
            match="validation changed after checkpoint and reopen",
        ):
            build_final_catalog_partial(
                state_connection,
                validation_connection,
                build_directory,
            )

        assert validations == 2
    finally:
        state_connection.close()
        validation_connection.close()


def _install_old_published_catalog(catalog_directory: Path) -> tuple[int, bytes]:
    old_result, _old_build = _completed_final_build(
        catalog_directory.parent / "old-catalog"
    )
    published = catalog_directory / "catalog.duckdb"
    old_result.path.replace(published)
    connection = duckdb.connect(str(published))
    try:
        connection.execute(
            "UPDATE catalog_metadata SET builder_version = 'old-version'"
        )
        connection.execute("FORCE CHECKPOINT")
    finally:
        connection.close()
    return published.stat().st_ino, published.read_bytes()


def _publication_fsync_label(
    descriptor: int,
    final_catalog,
    build_directory: Path,
    catalog_directory: Path,
) -> str:
    value = catalog.os.fstat(descriptor)
    identity = (value.st_dev, value.st_ino)
    if identity == final_catalog.file_identity[:2]:
        return "partial_file"
    if identity == (
        catalog_directory.stat().st_dev,
        catalog_directory.stat().st_ino,
    ):
        return "catalog_directory"
    if identity == (build_directory.stat().st_dev, build_directory.stat().st_ino):
        return "build_directory"
    return "target_marker"


def test_publish_catalog_atomically_replaces_old_inode_and_cleans_staging(
    tmp_path: Path,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, _old_bytes = _install_old_published_catalog(catalog_directory)
    old_reader = duckdb.connect(
        str(catalog_directory / "catalog.duckdb"), read_only=True
    )
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    try:
        result = publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )

        assert result.path == catalog_directory / "catalog.duckdb"
        assert result.path.stat().st_ino != old_inode
        assert build_directory.exists() is False
        assert old_reader.execute(
            "SELECT builder_version FROM catalog_metadata"
        ).fetchone() == ("old-version",)
        old_reader.close()
        new_reader = duckdb.connect(str(result.path), read_only=True)
        try:
            assert validate_catalog(new_reader) == result.validation
            assert new_reader.execute(
                "SELECT builder_version FROM catalog_metadata"
            ).fetchone() == (importlib.metadata.version("cc-warc-index-builder"),)
        finally:
            new_reader.close()
    finally:
        old_reader.close()


def test_publish_catalog_without_existing_final(tmp_path: Path) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)

    result = publish_catalog(
        final_catalog,
        catalog_directory,
        build_directory,
        replace_existing=False,
    )

    assert result.path.is_file()
    assert build_directory.exists() is False


def test_publish_catalog_fsyncs_file_source_and_destination_boundaries(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    original_fsync = catalog.os.fsync
    fsync_labels: list[str] = []

    def record_fsync(descriptor: int) -> None:
        fsync_labels.append(
            _publication_fsync_label(
                descriptor,
                final_catalog,
                build_directory,
                catalog_directory,
            )
        )
        original_fsync(descriptor)

    monkeypatch.setattr(catalog.os, "fsync", record_fsync)

    publish_catalog(
        final_catalog,
        catalog_directory,
        build_directory,
        replace_existing=False,
    )

    assert fsync_labels == [
        "target_marker",
        "build_directory",
        "partial_file",
        "build_directory",
        "catalog_directory",
        "catalog_directory",
        "catalog_directory",
    ]


@pytest.mark.parametrize(
    "boundary",
    [
        "marker_open",
        "marker_write",
        "marker_fsync",
        "marker_replace",
        "marker_directory_fsync",
    ],
)
def test_publish_catalog_marker_failures_preserve_old_catalog_and_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)

    if boundary == "marker_open":
        original_open = catalog.os.open

        def fail_marker_open(
            path: object,
            flags: int,
            mode: int = 0o777,
            *,
            dir_fd: int | None = None,
        ) -> int:
            if (
                path == catalog._PUBLICATION_TARGET_PARTIAL_FILENAME
                and flags & catalog.os.O_WRONLY
            ):
                raise OSError("simulated marker open failure")
            return original_open(path, flags, mode, dir_fd=dir_fd)

        monkeypatch.setattr(catalog.os, "open", fail_marker_open)
    elif boundary == "marker_write":
        monkeypatch.setattr(
            catalog.os,
            "write",
            lambda _descriptor, _data: (_ for _ in ()).throw(
                OSError("simulated marker write failure")
            ),
        )
    elif boundary == "marker_replace":
        original_replace = catalog.os.replace

        def fail_marker_replace(
            source: object,
            destination: object,
            **kwargs: object,
        ) -> None:
            if (
                source == catalog._PUBLICATION_TARGET_PARTIAL_FILENAME
                and destination == catalog._PUBLICATION_TARGET_FILENAME
            ):
                raise OSError("simulated marker replace failure")
            original_replace(source, destination, **kwargs)

        monkeypatch.setattr(catalog.os, "replace", fail_marker_replace)
    else:
        original_fsync = catalog.os.fsync

        def fail_marker_boundary(descriptor: int) -> None:
            label = _publication_fsync_label(
                descriptor,
                final_catalog,
                build_directory,
                catalog_directory,
            )
            if boundary == "marker_fsync" and label == "target_marker":
                raise OSError("simulated marker fsync failure")
            if boundary == "marker_directory_fsync" and label == "build_directory":
                raise OSError("simulated marker directory fsync failure")
            original_fsync(descriptor)

        monkeypatch.setattr(catalog.os, "fsync", fail_marker_boundary)

    with pytest.raises(CatalogPublicationError) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )

    published = catalog_directory / "catalog.duckdb"
    assert error.value.published is False
    assert published.stat().st_ino == old_inode
    assert published.read_bytes() == old_bytes
    assert build_directory.is_dir()
    assert final_catalog.path.is_file()


@pytest.mark.parametrize("boundary", ["partial_fsync", "replace"])
def test_publish_catalog_precommit_failures_preserve_old_inode_and_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    if boundary == "partial_fsync":
        original_fsync = catalog.os.fsync

        def fail_partial_fsync(descriptor: int) -> None:
            if _publication_fsync_label(
                descriptor,
                final_catalog,
                build_directory,
                catalog_directory,
            ) == "partial_file":
                raise OSError("simulated partial fsync failure")
            original_fsync(descriptor)

        monkeypatch.setattr(catalog.os, "fsync", fail_partial_fsync)
    else:
        original_replace = catalog.os.replace

        def fail_catalog_replace(
            source: object,
            destination: object,
            **kwargs: object,
        ) -> None:
            if source == "catalog.duckdb.partial" and destination == "catalog.duckdb":
                raise OSError("simulated catalog replace failure")
            original_replace(source, destination, **kwargs)

        monkeypatch.setattr(
            catalog.os,
            "replace",
            fail_catalog_replace,
        )

    with pytest.raises(CatalogPublicationError) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )

    published = catalog_directory / "catalog.duckdb"
    assert error.value.published is False
    assert published.stat().st_ino == old_inode
    assert published.read_bytes() == old_bytes
    assert build_directory.is_dir()
    assert final_catalog.path.is_file()


def test_recovery_rejects_same_catalog_id_marker_when_replace_never_committed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    original_replace = catalog.os.replace

    def fail_catalog_replace(
        source: object,
        destination: object,
        **kwargs: object,
    ) -> None:
        if source == "catalog.duckdb.partial" and destination == "catalog.duckdb":
            raise OSError("simulated replace failure after marker write")
        original_replace(source, destination, **kwargs)

    monkeypatch.setattr(
        catalog.os,
        "replace",
        fail_catalog_replace,
    )

    with pytest.raises(CatalogPublicationError) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )
    assert error.value.published is False

    monkeypatch.setattr(catalog.os, "replace", original_replace)
    recovered = catalog.recover_published_catalog_cleanup(
        catalog_directory,
        build_directory,
        final_catalog.validation.catalog_id,
    )

    published = catalog_directory / "catalog.duckdb"
    assert recovered is False
    assert published.stat().st_ino == old_inode
    assert published.read_bytes() == old_bytes
    assert final_catalog.path.is_file()
    assert (build_directory / catalog._PUBLICATION_TARGET_FILENAME).is_file()


def test_publish_catalog_rejects_partial_path_swap_before_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    original_fsync = catalog.os.fsync
    swapped = False

    def swap_after_partial_fsync(descriptor: int) -> None:
        nonlocal swapped
        label = _publication_fsync_label(
            descriptor,
            final_catalog,
            build_directory,
            catalog_directory,
        )
        original_fsync(descriptor)
        if label == "partial_file" and not swapped:
            replacement = build_directory / "replacement.duckdb"
            replacement.write_bytes(b"unvalidated replacement")
            replacement.replace(final_catalog.path)
            swapped = True

    monkeypatch.setattr(catalog.os, "fsync", swap_after_partial_fsync)

    with pytest.raises(CatalogPublicationError, match="partial catalog path changed") as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )

    published = catalog_directory / "catalog.duckdb"
    assert error.value.published is False
    assert published.stat().st_ino == old_inode
    assert published.read_bytes() == old_bytes


def test_publish_catalog_rejects_existing_destination_symlink(
    tmp_path: Path,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    outside = tmp_path / "outside.duckdb"
    outside.write_bytes(b"outside")
    published = catalog_directory / "catalog.duckdb"
    published.symlink_to(outside)

    with pytest.raises(CatalogPublicationError) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )

    assert error.value.published is False
    assert published.is_symlink()
    assert outside.read_bytes() == b"outside"
    assert final_catalog.path.is_file()


def test_publish_catalog_rejects_partial_wal_symlink(
    tmp_path: Path,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    outside = tmp_path / "outside.wal"
    outside.write_bytes(b"outside")
    wal = Path(f"{final_catalog.path}.wal")
    wal.symlink_to(outside)

    with pytest.raises(CatalogPublicationError, match="WAL prevents publication") as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=False,
        )

    assert error.value.published is False
    assert wal.is_symlink()
    assert outside.read_bytes() == b"outside"
    assert final_catalog.path.is_file()


@pytest.mark.parametrize("sidecar_kind", ["file", "symlink"])
def test_publish_catalog_rejects_destination_wal_sidecar(
    tmp_path: Path,
    sidecar_kind: str,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    wal = catalog_directory / "catalog.duckdb.wal"
    if sidecar_kind == "file":
        wal.write_bytes(b"unexpected wal")
    else:
        outside = tmp_path / "outside.wal"
        outside.write_bytes(b"outside")
        wal.symlink_to(outside)

    with pytest.raises(
        CatalogPublicationError,
        match="published catalog WAL prevents atomic replacement",
    ) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=False,
        )

    assert error.value.published is False
    assert wal.exists() or wal.is_symlink()
    assert final_catalog.path.is_file()


def test_publish_catalog_rejects_destination_swap_before_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    _old_inode, _old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    published = catalog_directory / "catalog.duckdb"
    outside = tmp_path / "outside.duckdb"
    outside.write_bytes(b"outside")
    original_fsync = catalog.os.fsync
    swapped = False

    def swap_destination_after_partial_fsync(descriptor: int) -> None:
        nonlocal swapped
        label = _publication_fsync_label(
            descriptor,
            final_catalog,
            build_directory,
            catalog_directory,
        )
        original_fsync(descriptor)
        if label == "partial_file" and not swapped:
            published.unlink()
            published.symlink_to(outside)
            swapped = True

    monkeypatch.setattr(catalog.os, "fsync", swap_destination_after_partial_fsync)

    with pytest.raises(CatalogPublicationError, match="published catalog path changed") as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )

    assert error.value.published is False
    assert published.is_symlink()
    assert outside.read_bytes() == b"outside"
    assert final_catalog.path.is_file()


def test_publish_catalog_rejects_build_directory_swap_before_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    saved_build_directory = catalog_directory / ".build.saved"
    original_fsync = catalog.os.fsync
    swapped = False

    def swap_build_after_partial_fsync(descriptor: int) -> None:
        nonlocal swapped
        label = _publication_fsync_label(
            descriptor,
            final_catalog,
            build_directory,
            catalog_directory,
        )
        original_fsync(descriptor)
        if label == "partial_file" and not swapped:
            build_directory.rename(saved_build_directory)
            build_directory.mkdir()
            (build_directory / "replacement-sentinel").write_text("replacement")
            swapped = True

    monkeypatch.setattr(catalog.os, "fsync", swap_build_after_partial_fsync)

    with pytest.raises(CatalogPublicationError, match="build directory changed") as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )

    published = catalog_directory / "catalog.duckdb"
    assert error.value.published is False
    assert published.stat().st_ino == old_inode
    assert published.read_bytes() == old_bytes
    assert (saved_build_directory / "catalog.duckdb.partial").is_file()
    assert (build_directory / "replacement-sentinel").read_text() == "replacement"


@pytest.mark.parametrize(
    (
        "boundary,fail_label,fail_occurrence,"
        "active_staging_remains,tombstone_remains"
    ),
    [
        ("source_directory_fsync", "build_directory", 2, True, False),
        ("catalog_directory_fsync", "catalog_directory", 1, True, False),
        ("detach_directory_fsync", "catalog_directory", 2, False, True),
        ("staging_cleanup", None, None, False, True),
        ("cleanup_directory_fsync", "catalog_directory", 3, False, False),
    ],
)
def test_publish_catalog_postcommit_failures_report_visible_new_catalog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    fail_label: str | None,
    fail_occurrence: int | None,
    active_staging_remains: bool,
    tombstone_remains: bool,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, _old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    if fail_label is not None:
        original_fsync = catalog.os.fsync
        label_counts: dict[str, int] = {}

        def fail_selected_fsync(descriptor: int) -> None:
            label = _publication_fsync_label(
                descriptor,
                final_catalog,
                build_directory,
                catalog_directory,
            )
            label_counts[label] = label_counts.get(label, 0) + 1
            if label == fail_label and label_counts[label] == fail_occurrence:
                raise OSError(f"simulated {boundary} failure")
            original_fsync(descriptor)

        monkeypatch.setattr(catalog.os, "fsync", fail_selected_fsync)
    else:
        monkeypatch.setattr(
            catalog,
            "_remove_open_directory_tree",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("simulated staging cleanup failure")
            ),
        )

    with pytest.raises(CatalogPublicationError) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=True,
        )

    published = catalog_directory / "catalog.duckdb"
    assert error.value.published is True
    assert "atomically replaced" in str(error.value)
    assert published.stat().st_ino != old_inode
    assert build_directory.exists() is active_staging_remains
    assert (
        catalog_directory / catalog._PUBLISHED_BUILD_DIRECTORY
    ).exists() is tombstone_remains
    connection = duckdb.connect(str(published), read_only=True)
    try:
        assert validate_catalog(connection) == final_catalog.validation
    finally:
        connection.close()


@pytest.mark.parametrize(
    "boundary,fail_label,fail_occurrence",
    [
        ("source_directory_fsync", "build_directory", 2),
        ("catalog_directory_fsync", "catalog_directory", 1),
        ("staging_cleanup", None, None),
    ],
)
def test_run_recovers_committed_publication_and_removes_stale_staging(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    fail_label: str | None,
    fail_occurrence: int | None,
) -> None:
    catalog_directory = (
        tmp_path / "CC-MAIN-2026-25" / "catalog" / "pages25"
    )
    identity = replace(
        _identity(),
        selection_policy_sha256=selection_policy_sha256(),
    )
    final_catalog, build_directory = _completed_final_build(
        catalog_directory,
        identity=identity,
    )
    original_fsync = catalog.os.fsync
    original_remove_tree = catalog._remove_open_directory_tree
    label_counts: dict[str, int] = {}

    def fail_selected_fsync(descriptor: int) -> None:
        label = _publication_fsync_label(
            descriptor,
            final_catalog,
            build_directory,
            catalog_directory,
        )
        label_counts[label] = label_counts.get(label, 0) + 1
        if label == fail_label and label_counts[label] == fail_occurrence:
            raise OSError(f"simulated {boundary} failure")
        original_fsync(descriptor)

    if fail_label is not None:
        monkeypatch.setattr(catalog.os, "fsync", fail_selected_fsync)
    else:
        monkeypatch.setattr(
            catalog,
            "_remove_open_directory_tree",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("simulated staging cleanup failure")
            ),
        )
    with pytest.raises(CatalogPublicationError) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=False,
        )
    assert error.value.published is True
    tombstone = catalog_directory / catalog._PUBLISHED_BUILD_DIRECTORY
    if boundary == "staging_cleanup":
        assert build_directory.exists() is False
        assert tombstone.is_dir()
    else:
        assert build_directory.is_dir()
        assert tombstone.exists() is False

    monkeypatch.setattr(catalog.os, "fsync", original_fsync)
    monkeypatch.setattr(catalog, "_remove_open_directory_tree", original_remove_tree)
    options = command.parse_options(
        ["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25"]
    )
    assert command.run(options) == 0
    assert build_directory.exists() is False
    assert tombstone.exists() is False


def test_run_recovers_partially_cleaned_published_staging_tombstone(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_directory = (
        tmp_path / "CC-MAIN-2026-25" / "catalog" / "pages25"
    )
    identity = replace(
        _identity(),
        selection_policy_sha256=selection_policy_sha256(),
    )
    final_catalog, build_directory = _completed_final_build(
        catalog_directory,
        identity=identity,
    )
    (build_directory / "removed-before-crash").write_text("remove first")
    (build_directory / "remaining-after-crash").write_text("recover later")
    original_remove_tree = catalog._remove_open_directory_tree

    def remove_one_entry_then_fail(
        _parent_descriptor: int,
        directory_descriptor: int,
        name: str,
        _expected_identity: tuple[int, int, int, int, int, int],
    ) -> None:
        assert name == catalog._PUBLISHED_BUILD_DIRECTORY
        catalog.os.unlink("removed-before-crash", dir_fd=directory_descriptor)
        raise OSError("simulated failure during tombstone cleanup")

    monkeypatch.setattr(
        catalog,
        "_remove_open_directory_tree",
        remove_one_entry_then_fail,
    )
    with pytest.raises(CatalogPublicationError) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=False,
        )
    assert error.value.published is True

    tombstone = catalog_directory / catalog._PUBLISHED_BUILD_DIRECTORY
    assert build_directory.exists() is False
    assert (tombstone / "removed-before-crash").exists() is False
    assert (tombstone / "remaining-after-crash").is_file()

    monkeypatch.setattr(catalog, "_remove_open_directory_tree", original_remove_tree)
    options = command.parse_options(
        ["--base", str(tmp_path), "--crawl", "CC-MAIN-2026-25"]
    )
    assert command.run(options) == 0
    assert build_directory.exists() is False
    assert tombstone.exists() is False


def test_rebuild_removes_published_staging_tombstone_and_preserves_catalog(
    tmp_path: Path,
) -> None:
    base = tmp_path / "data"
    catalog_directory = base / "CC-MAIN-2026-25" / "catalog" / "pages25"
    catalog_directory.mkdir(parents=True)
    published = catalog_directory / "catalog.duckdb"
    published.write_bytes(b"published catalog")
    tombstone = catalog_directory / catalog._PUBLISHED_BUILD_DIRECTORY
    (tombstone / "nested").mkdir(parents=True)
    (tombstone / "nested" / "stale").write_text("stale")

    build_directory = catalog.prepare_build_directory(
        base,
        catalog_directory,
        rebuild=True,
    )

    assert build_directory == catalog_directory / ".build"
    assert build_directory.is_dir()
    assert list(build_directory.iterdir()) == []
    assert tombstone.exists() is False
    assert published.read_bytes() == b"published catalog"


@pytest.mark.parametrize("marker_state", ["absent", "mismatch", "invalid"])
def test_recovery_without_matching_valid_marker_never_deletes_staging(
    tmp_path: Path,
    marker_state: str,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    published = publish_catalog(
        final_catalog,
        catalog_directory,
        build_directory,
        replace_existing=False,
    )
    build_directory.mkdir()
    sentinel = build_directory / "must-remain"
    sentinel.write_text("staging")

    if marker_state == "mismatch":
        descriptor = catalog.os.open(build_directory, catalog.os.O_RDONLY)
        try:
            catalog._write_publication_target(
                descriptor,
                catalog._PublicationTarget(
                    catalog_id="00" * 32,
                    device=published.path.stat().st_dev,
                    inode=published.path.stat().st_ino,
                ),
            )
        finally:
            catalog.os.close(descriptor)
    elif marker_state == "invalid":
        (build_directory / catalog._PUBLICATION_TARGET_FILENAME).write_text(
            "invalid marker"
        )

    if marker_state == "invalid":
        with pytest.raises(CatalogPublicationError, match="marker is invalid"):
            catalog.recover_published_catalog_cleanup(
                catalog_directory,
                build_directory,
                published.validation.catalog_id,
            )
    else:
        assert (
            catalog.recover_published_catalog_cleanup(
                catalog_directory,
                build_directory,
                published.validation.catalog_id,
            )
            is False
        )

    assert sentinel.read_text() == "staging"
    assert build_directory.is_dir()
    assert published.path.is_file()


@pytest.mark.parametrize(
    (
        "boundary,initial_state,fail_label,fail_occurrence,"
        "active_staging_remains,tombstone_remains"
    ),
    [
        ("catalog_file_fsync", "tombstone", "partial_file", 1, False, True),
        ("build_directory_fsync", "active", "build_directory", 1, True, False),
        (
            "catalog_directory_fsync",
            "tombstone",
            "catalog_directory",
            1,
            False,
            True,
        ),
        ("staging_cleanup", "tombstone", None, None, False, True),
        (
            "cleanup_directory_fsync",
            "tombstone",
            "catalog_directory",
            2,
            False,
            False,
        ),
    ],
)
def test_recovery_faults_report_published_catalog_and_preserve_safe_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
    initial_state: str,
    fail_label: str | None,
    fail_occurrence: int | None,
    active_staging_remains: bool,
    tombstone_remains: bool,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    tombstone = catalog_directory / catalog._PUBLISHED_BUILD_DIRECTORY
    original_fsync = catalog.os.fsync
    original_remove_tree = catalog._remove_open_directory_tree
    if initial_state == "active":
        initial_build_fsyncs = 0

        def fail_source_directory_fsync(descriptor: int) -> None:
            nonlocal initial_build_fsyncs
            label = _publication_fsync_label(
                descriptor,
                final_catalog,
                build_directory,
                catalog_directory,
            )
            if label == "build_directory":
                initial_build_fsyncs += 1
                if initial_build_fsyncs == 2:
                    raise OSError("leave active staging for recovery")
            original_fsync(descriptor)

        monkeypatch.setattr(catalog.os, "fsync", fail_source_directory_fsync)
    else:
        monkeypatch.setattr(
            catalog,
            "_remove_open_directory_tree",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("leave detached staging for recovery")
            ),
        )
    with pytest.raises(CatalogPublicationError) as publication_error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=False,
        )
    assert publication_error.value.published is True
    monkeypatch.setattr(catalog.os, "fsync", original_fsync)
    monkeypatch.setattr(catalog, "_remove_open_directory_tree", original_remove_tree)

    label_counts: dict[str, int] = {}

    def fail_selected_fsync(descriptor: int) -> None:
        label = _publication_fsync_label(
            descriptor,
            final_catalog,
            build_directory,
            catalog_directory,
        )
        label_counts[label] = label_counts.get(label, 0) + 1
        if label == fail_label and label_counts[label] == fail_occurrence:
            raise OSError(f"simulated recovery {boundary} failure")
        original_fsync(descriptor)

    if fail_label is not None:
        monkeypatch.setattr(catalog.os, "fsync", fail_selected_fsync)
    else:
        monkeypatch.setattr(
            catalog,
            "_remove_open_directory_tree",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("simulated recovery staging cleanup failure")
            ),
        )

    with pytest.raises(CatalogPublicationError) as recovery_error:
        catalog.recover_published_catalog_cleanup(
            catalog_directory,
            build_directory,
            final_catalog.validation.catalog_id,
        )

    assert recovery_error.value.published is True
    assert build_directory.exists() is active_staging_remains
    assert tombstone.exists() is tombstone_remains
    connection = duckdb.connect(
        str(catalog_directory / "catalog.duckdb"),
        read_only=True,
    )
    try:
        assert validate_catalog(connection) == final_catalog.validation
    finally:
        connection.close()


def test_publication_recursively_removes_nested_staging_without_following_symlinks(
    tmp_path: Path,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    nested = build_directory / "duckdb-temp" / "nested"
    nested.mkdir(parents=True)
    (nested / "spill.bin").write_bytes(b"spill")
    outside = tmp_path / "outside"
    outside.mkdir()
    outside_sentinel = outside / "must-remain"
    outside_sentinel.write_text("outside")
    (nested / "outside-link").symlink_to(outside, target_is_directory=True)

    result = publish_catalog(
        final_catalog,
        catalog_directory,
        build_directory,
        replace_existing=False,
    )

    assert result.path.is_file()
    assert build_directory.exists() is False
    assert outside_sentinel.read_text() == "outside"


def test_publish_catalog_replaces_invalid_existing_final_only_when_explicit(
    tmp_path: Path,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    published = catalog_directory / "catalog.duckdb"
    published.write_bytes(b"invalid existing catalog")
    old_inode = published.stat().st_ino

    with pytest.raises(CatalogPublicationError) as error:
        publish_catalog(
            final_catalog,
            catalog_directory,
            build_directory,
            replace_existing=False,
        )

    assert error.value.published is False
    assert published.stat().st_ino == old_inode
    assert published.read_bytes() == b"invalid existing catalog"
    assert final_catalog.path.is_file()

    result = publish_catalog(
        final_catalog,
        catalog_directory,
        build_directory,
        replace_existing=True,
    )
    assert result.path.stat().st_ino != old_inode
    assert build_directory.exists() is False


def test_inspect_published_catalog_is_quick_and_detects_reuse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    published = publish_catalog(
        final_catalog,
        catalog_directory,
        build_directory,
        replace_existing=False,
    ).path

    def forbid_full_warc_validation(_connection: duckdb.DuckDBPyConnection):
        raise AssertionError("quick reuse must not scan the WARC inventory")

    monkeypatch.setattr(
        catalog,
        "_validated_catalog_warcs",
        forbid_full_warc_validation,
    )
    matching = inspect_published_catalog(
        published,
        crawl_id="CC-MAIN-2026-25",
        pages_per_domain=25,
        selection_policy_version=_identity().selection_policy_version,
        selection_policy_sha256=_identity().selection_policy_sha256,
    )
    mismatching = inspect_published_catalog(
        published,
        crawl_id="CC-MAIN-2016-22",
        pages_per_domain=25,
        selection_policy_version=_identity().selection_policy_version,
        selection_policy_sha256=_identity().selection_policy_sha256,
    )

    assert matching is not None and matching.reusable is True
    assert mismatching is not None and mismatching.reusable is False


@pytest.mark.parametrize("sidecar_kind", ["file", "symlink"])
def test_inspect_published_catalog_rejects_wal_sidecar(
    tmp_path: Path,
    sidecar_kind: str,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    published = publish_catalog(
        final_catalog,
        catalog_directory,
        build_directory,
        replace_existing=False,
    ).path
    wal = Path(f"{published}.wal")
    if sidecar_kind == "file":
        wal.write_bytes(b"unexpected wal")
    else:
        outside = tmp_path / "outside.wal"
        outside.write_bytes(b"outside")
        wal.symlink_to(outside)

    with pytest.raises(FinalCatalogBuildError, match="WAL prevents reuse"):
        inspect_published_catalog(
            published,
            crawl_id="CC-MAIN-2026-25",
            pages_per_domain=25,
            selection_policy_version=_identity().selection_policy_version,
            selection_policy_sha256=_identity().selection_policy_sha256,
        )

    assert published.is_file()
    assert wal.exists() or wal.is_symlink()


def test_inspect_published_catalog_rejects_impossible_metadata_counts(
    tmp_path: Path,
) -> None:
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(catalog_directory)
    published = publish_catalog(
        final_catalog,
        catalog_directory,
        build_directory,
        replace_existing=False,
    ).path
    connection = duckdb.connect(str(published))
    try:
        connection.execute("UPDATE catalog_metadata SET warc_count = 0")
        connection.execute("FORCE CHECKPOINT")
    finally:
        connection.close()

    with pytest.raises(FinalCatalogBuildError, match="invalid build information"):
        inspect_published_catalog(
            published,
            crawl_id="CC-MAIN-2026-25",
            pages_per_domain=25,
            selection_policy_version=_identity().selection_policy_version,
            selection_policy_sha256=_identity().selection_policy_sha256,
        )


def test_recheck_and_publish_catalog_uses_fresh_matching_manifests(
    tmp_path: Path,
) -> None:
    warc_payload = b"fresh warc manifest"
    index_payload = b"fresh index manifest"
    identity = replace(
        _identity(),
        warc_manifest_sha256=hashlib.sha256(warc_payload).hexdigest(),
        index_manifest_sha256=hashlib.sha256(index_payload).hexdigest(),
    )
    catalog_directory = tmp_path / "catalog"
    final_catalog, build_directory = _completed_final_build(
        catalog_directory,
        identity=identity,
    )
    requests: list[str] = []

    def respond(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        payload = (
            index_payload
            if request.url.path.endswith("cc-index-table.paths.gz")
            else warc_payload
        )
        return httpx.Response(200, stream=httpx.ByteStream(payload))

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        result = command.recheck_and_publish_catalog(
            client,
            final_catalog,
            catalog_directory,
            build_directory,
            http_attempts=1,
            replace_existing=False,
        )

    assert result.path.is_file()
    assert build_directory.exists() is False
    assert len(requests) == 2


def test_recheck_and_publish_catalog_blocks_changed_manifest_before_replace(
    tmp_path: Path,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)

    def respond(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            stream=httpx.ByteStream(request.url.path.encode()),
        )

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ManifestChangedError):
            command.recheck_and_publish_catalog(
                client,
                final_catalog,
                catalog_directory,
                build_directory,
                http_attempts=1,
                replace_existing=True,
            )

    published = catalog_directory / "catalog.duckdb"
    assert published.stat().st_ino == old_inode
    assert published.read_bytes() == old_bytes
    assert final_catalog.path.is_file()


@pytest.mark.parametrize("failed_manifest", ["warc", "index"])
def test_recheck_and_publish_catalog_network_failure_preserves_old_catalog(
    tmp_path: Path,
    failed_manifest: str,
) -> None:
    catalog_directory = tmp_path / "catalog"
    catalog_directory.mkdir()
    old_inode, old_bytes = _install_old_published_catalog(catalog_directory)
    final_catalog, build_directory = _completed_final_build(catalog_directory)

    def respond(request: httpx.Request) -> httpx.Response:
        is_index = request.url.path.endswith("cc-index-table.paths.gz")
        if (failed_manifest == "index") == is_index:
            return httpx.Response(404)
        return httpx.Response(200, stream=httpx.ByteStream(b"unchanged attempt"))

    with httpx.Client(transport=httpx.MockTransport(respond)) as client:
        with pytest.raises(ManifestDownloadError, match="HTTP 404"):
            command.recheck_and_publish_catalog(
                client,
                final_catalog,
                catalog_directory,
                build_directory,
                http_attempts=1,
                replace_existing=True,
            )

    published = catalog_directory / "catalog.duckdb"
    assert published.stat().st_ino == old_inode
    assert published.read_bytes() == old_bytes
    assert final_catalog.path.is_file()
