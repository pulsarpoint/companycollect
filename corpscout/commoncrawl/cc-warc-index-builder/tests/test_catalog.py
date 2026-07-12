import os
import subprocess
import sys
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

import warc_index_builder.catalog as catalog
from warc_index_builder.catalog import (
    CATALOG_SCHEMA_VERSION,
    BuildIdentity,
    BuildStateConflict,
    BuildStateCorrupt,
    SourceShardSeed,
    catalog_id,
    initialize_build_state,
    local_candidate_query,
    prepare_build_directory,
    require_path_within,
    source_shard_seeds,
    warc_inventory_sha256,
)
from warc_index_builder.manifests import (
    IndexSource,
    SourceSchema,
    WarcObject,
    inspect_source_schema,
    source_schema_sha256,
)
from warc_index_builder.selection import (
    CANDIDATE_COLUMNS,
    SELECTION_POLICY_VERSION,
    selection_policy_sha256,
)


_IDENTITY_HASHES = {
    "selection_policy_sha256": "00" * 32,
    "source_schema_sha256": "11" * 32,
    "warc_manifest_sha256": "22" * 32,
    "index_manifest_sha256": "33" * 32,
    "warc_inventory_sha256": "44" * 32,
}


def _catalog_identity_values() -> dict[str, object]:
    return {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "crawl_id": "CC-MAIN-2026-25",
        "pages_per_domain": 25,
        "selection_policy_version": SELECTION_POLICY_VERSION,
        **_IDENTITY_HASHES,
    }


def _build_identity() -> BuildIdentity:
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


def _warc_seeds() -> tuple[WarcObject, ...]:
    return (
        WarcObject(0, "crawl-data/CC-MAIN-2026-25/segments/a/warc/a.warc.gz"),
        WarcObject(1, "crawl-data/CC-MAIN-2026-25/segments/b/warc/b.warc.gz"),
    )


def _source_seeds() -> tuple[SourceShardSeed, ...]:
    columns = (
        ("url", "VARCHAR"),
        ("url_host_name", "VARCHAR"),
        ("url_host_registered_domain", "VARCHAR"),
        ("url_path", "VARCHAR"),
        ("content_mime_type", "VARCHAR"),
        ("warc_filename", "VARCHAR"),
        ("fetch_status", "SMALLINT"),
        ("warc_record_offset", "BIGINT"),
        ("warc_record_length", "UBIGINT"),
    )
    sources = (
        IndexSource(0, "ignored/local/part-000", "https://data.commoncrawl.org/index/part-000.parquet"),
        IndexSource(1, "ignored/local/part-001", "https://data.commoncrawl.org/index/part-001.parquet"),
    )
    schemas = (
        SourceSchema(0, columns),
        SourceSchema(
            1,
            columns
            + (
                ("content_mime_detected", "VARCHAR"),
                ("content_languages", "VARCHAR"),
            ),
        ),
    )
    return source_shard_seeds(sources, schemas)


def _candidate_page(
    domain: str,
    url: str,
    path: str | None,
    offset: int,
    *,
    host: str | None = None,
    status: int = 200,
    reported_mime: str = "text/html",
    detected_mime: str | None = "text/html",
    warc_filename: str = "crawl-data/fixture.warc.gz",
    length: int = 100,
    languages: str | None = "eng",
) -> tuple[object, ...]:
    return (
        domain,
        host or domain,
        url,
        path,
        status,
        reported_mime,
        detected_mime,
        warc_filename,
        offset,
        length,
        languages,
    )


def _candidate_fixture(
    tmp_path: Path,
    rows: list[tuple[object, ...]],
) -> tuple[duckdb.DuckDBPyConnection, Path, SourceSchema]:
    connection = duckdb.connect()
    connection.execute(
        """
        CREATE TABLE source_rows (
            url_host_registered_domain VARCHAR,
            url_host_name VARCHAR,
            url VARCHAR,
            url_path VARCHAR,
            fetch_status BIGINT,
            content_mime_type VARCHAR,
            content_mime_detected VARCHAR,
            warc_filename VARCHAR,
            warc_record_offset BIGINT,
            warc_record_length BIGINT,
            content_languages VARCHAR
        )
        """
    )
    connection.executemany(
        "INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    path = tmp_path / "source.parquet"
    connection.execute("COPY source_rows TO ? (FORMAT PARQUET)", [str(path)])
    schema = inspect_source_schema(
        connection,
        IndexSource(7, str(path), str(path)),
    )
    return connection, path, schema


def _local_candidates(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    schema: SourceSchema,
    *,
    pages_per_domain: int = 25,
) -> list[tuple[object, ...]]:
    query = local_candidate_query(schema, pages_per_domain)
    return connection.execute(
        f"""
        SELECT * FROM ({query})
        ORDER BY root_domain, url, warc_filename, warc_record_offset,
                 warc_record_length
        """,
        [str(path)],
    ).fetchall()


def test_require_path_within_rejects_outside_target(tmp_path: Path) -> None:
    base = tmp_path / "base"
    base.mkdir()

    with pytest.raises(ValueError, match="escapes base"):
        require_path_within(base, tmp_path / "outside")


def test_prepare_build_directory_rejects_file(tmp_path: Path) -> None:
    catalog_directory = tmp_path / "CC-MAIN-2026-25/catalog/pages25"
    catalog_directory.mkdir(parents=True)
    build_path = catalog_directory / ".build"
    build_path.write_text("not a directory")

    with pytest.raises(ValueError, match="not a directory"):
        prepare_build_directory(tmp_path, catalog_directory, rebuild=True)

    assert build_path.read_text() == "not a directory"


def test_warc_inventory_identity_matches_golden_hash() -> None:
    inventory = (
        (
            0,
            "crawl-data/CC-MAIN-2026-25/segments/a/warc/a:alpha.warc.gz",
            2**32 - 1,
        ),
        (
            1,
            "crawl-data/CC-MAIN-2026-25/segments/b/warc/beta-β.warc.gz",
            2**32 + 1,
        ),
    )

    assert warc_inventory_sha256(inventory) == (
        "472da7d2cf3483de1aec457858e72b88769190ec51c4c6f65c277c9c580279da"
    )


@pytest.mark.parametrize(
    "inventory",
    [
        (),
        ((1, "b.warc.gz", 1),),
        ((0, "a.warc.gz", 1), (0, "b.warc.gz", 2)),
        ((1, "b.warc.gz", 2), (0, "a.warc.gz", 1)),
        ((0, " ", 1),),
        ((0, "same.warc.gz", 1), (1, "same.warc.gz", 2)),
        ((0, "zero.warc.gz", 0),),
        ((0, "negative.warc.gz", -1),),
        ((0, "overflow.warc.gz", 2**64),),
    ],
)
def test_warc_inventory_identity_rejects_invalid_rows(
    inventory: tuple[tuple[int, str, int], ...],
) -> None:
    with pytest.raises(ValueError):
        warc_inventory_sha256(inventory)


def test_warc_inventory_identity_is_length_delimited() -> None:
    first = ((0, "a:1.warc.gz", 23),)
    second = ((0, "a.warc.gz", 123),)

    assert warc_inventory_sha256(first) != warc_inventory_sha256(second)


def test_catalog_identity_matches_golden_hash() -> None:
    assert catalog_id(**_catalog_identity_values()) == (
        "41d6768f649157ac34b244e8903662a59674782d9fa941e6e69396db458e04dc"
    )


def test_every_logical_input_changes_catalog_identity() -> None:
    original = _catalog_identity_values()
    changes: dict[str, object] = {
        "schema_version": 2,
        "crawl_id": "CC-MAIN-2016-22",
        "pages_per_domain": 1,
        "selection_policy_version": "page-selection-v2",
        "selection_policy_sha256": "55" * 32,
        "source_schema_sha256": "66" * 32,
        "warc_manifest_sha256": "77" * 32,
        "index_manifest_sha256": "88" * 32,
        "warc_inventory_sha256": "99" * 32,
    }

    original_id = catalog_id(**original)
    for name, value in changes.items():
        changed = {**original, name: value}
        assert catalog_id(**changed) != original_id


@pytest.mark.parametrize("value", ["A" * 64, "0" * 63, "0" * 65, "z" * 64])
def test_catalog_identity_rejects_noncanonical_hashes(value: str) -> None:
    for hash_name in _IDENTITY_HASHES:
        identity = _catalog_identity_values()
        identity[hash_name] = value
        with pytest.raises(ValueError, match=hash_name):
            catalog_id(**identity)


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", 0),
        ("schema_version", 2**16),
        ("crawl_id", ""),
        ("pages_per_domain", 0),
        ("pages_per_domain", 2**16),
        ("selection_policy_version", ""),
    ],
)
def test_catalog_identity_rejects_invalid_fields(field: str, value: object) -> None:
    identity = _catalog_identity_values()
    identity[field] = value

    with pytest.raises(ValueError):
        catalog_id(**identity)


def test_pages_one_and_pages_twenty_five_share_policy_but_not_catalog_identity() -> None:
    policy_hash = selection_policy_sha256()
    pages_one = _catalog_identity_values()
    pages_twenty_five = _catalog_identity_values()
    pages_one["pages_per_domain"] = 1
    pages_one["selection_policy_sha256"] = policy_hash
    pages_twenty_five["selection_policy_sha256"] = policy_hash

    assert catalog_id(**pages_one) != catalog_id(**pages_twenty_five)


def test_all_catalog_identities_are_stable_across_processes_and_paths(
    tmp_path: Path,
) -> None:
    script = """
from warc_index_builder.catalog import catalog_id, warc_inventory_sha256
from warc_index_builder.manifests import SourceSchema, source_schemas_sha256
from warc_index_builder.selection import SELECTION_POLICY_VERSION, selection_policy_sha256

required = (
    ('url', 'VARCHAR'),
    ('url_host_name', 'VARCHAR'),
    ('url_host_registered_domain', 'VARCHAR'),
    ('url_path', 'VARCHAR'),
    ('content_mime_type', 'VARCHAR'),
    ('warc_filename', 'VARCHAR'),
    ('fetch_status', 'SMALLINT'),
    ('warc_record_offset', 'INTEGER'),
    ('warc_record_length', 'UBIGINT'),
)
current = tuple(
    (
        name,
        {
            'fetch_status': 'USMALLINT',
            'warc_record_offset': 'UINTEGER',
            'warc_record_length': 'BIGINT',
        }.get(name, column_type),
    )
    for name, column_type in required
)
schemas = (
    SourceSchema(0, required),
    SourceSchema(
        1,
        current + (
            ('content_mime_detected', 'VARCHAR'),
            ('content_languages', 'VARCHAR'),
        ),
    ),
)
inventory = (
    (0, 'crawl-data/CC-MAIN-2026-25/segments/a/warc/a:alpha.warc.gz', 2**32 - 1),
    (1, 'crawl-data/CC-MAIN-2026-25/segments/b/warc/beta-β.warc.gz', 2**32 + 1),
)
values = (
    selection_policy_sha256(),
    source_schemas_sha256(schemas),
    warc_inventory_sha256(inventory),
    catalog_id(
        schema_version=1,
        crawl_id='CC-MAIN-2026-25',
        pages_per_domain=25,
        selection_policy_version=SELECTION_POLICY_VERSION,
        selection_policy_sha256='00' * 32,
        source_schema_sha256='11' * 32,
        warc_manifest_sha256='22' * 32,
        index_manifest_sha256='33' * 32,
        warc_inventory_sha256='44' * 32,
    ),
)
print('\\n'.join(values))
"""
    results: list[str] = []
    for name, hash_seed in (("first", "1"), ("second", "8675309")):
        working_directory = tmp_path / name
        temporary_directory = working_directory / "temp"
        temporary_directory.mkdir(parents=True)
        environment = os.environ.copy()
        environment["PYTHONHASHSEED"] = hash_seed
        environment["TMPDIR"] = str(temporary_directory)
        results.append(
            subprocess.check_output(
                [sys.executable, "-c", script],
                cwd=working_directory,
                env=environment,
                text=True,
            ).strip()
        )

    assert results[0] == results[1]


def test_build_state_initialization_creates_typed_tables_and_seeds_rows(
    tmp_path: Path,
) -> None:
    connection = duckdb.connect(str(tmp_path / "state.duckdb"))
    try:
        result = initialize_build_state(
            connection,
            _build_identity(),
            _warc_seeds(),
            _source_seeds(),
        )

        assert result.reused is False
        assert result.recovered_source_shards == 0
        assert connection.execute(
            "SELECT table_name FROM information_schema.tables ORDER BY table_name"
        ).fetchall() == [
            ("build_identity",),
            ("source_shards",),
            ("warc_inventory",),
        ]
        assert connection.execute(
            """
            SELECT state_schema_version, catalog_schema_version, crawl_id,
                   pages_per_domain, selection_policy_version,
                   selection_policy_sha256, source_schema_sha256,
                   warc_manifest_sha256, index_manifest_sha256,
                   warc_inventory_sha256
            FROM build_identity
            """
        ).fetchall() == [
            (
                1,
                1,
                "CC-MAIN-2026-25",
                25,
                SELECTION_POLICY_VERSION,
                "00" * 32,
                "11" * 32,
                "22" * 32,
                "33" * 32,
                None,
            )
        ]
        assert connection.execute(
            """
            SELECT warc_index, warc_filename, object_bytes, attempts, last_error
            FROM warc_inventory ORDER BY warc_index
            """
        ).fetchall() == [
            (0, _warc_seeds()[0].warc_filename, None, 0, None),
            (1, _warc_seeds()[1].warc_filename, None, 0, None),
        ]
        assert connection.execute(
            """
            SELECT source_index, source_url, source_schema_sha256, status,
                   candidate_rows, candidate_bytes, attempts, last_error, completed_at
            FROM source_shards ORDER BY source_index
            """
        ).fetchall() == [
            (
                0,
                _source_seeds()[0].source_url,
                _source_seeds()[0].source_schema_sha256,
                "pending",
                None,
                None,
                0,
                None,
                None,
            ),
            (
                1,
                _source_seeds()[1].source_url,
                _source_seeds()[1].source_schema_sha256,
                "pending",
                None,
                None,
                0,
                None,
                None,
            ),
        ]
    finally:
        connection.close()


def test_source_shard_seed_hashes_are_derived_from_inspected_schemas() -> None:
    seeds = _source_seeds()

    assert seeds[0].source_schema_sha256 == source_schema_sha256(
        SourceSchema(
            99,
            (
                ("url", "VARCHAR"),
                ("url_host_name", "VARCHAR"),
                ("url_host_registered_domain", "VARCHAR"),
                ("url_path", "VARCHAR"),
                ("content_mime_type", "VARCHAR"),
                ("warc_filename", "VARCHAR"),
                ("fetch_status", "SMALLINT"),
                ("warc_record_offset", "BIGINT"),
                ("warc_record_length", "UBIGINT"),
            ),
        )
    )
    assert seeds[0].source_schema_sha256 != seeds[1].source_schema_sha256


@pytest.mark.parametrize("stored_field", ["state_schema_version", "catalog_schema_version"])
def test_build_state_rejects_stored_schema_version_mismatch(stored_field: str) -> None:
    connection = duckdb.connect()
    try:
        initialize_build_state(connection, _build_identity(), _warc_seeds(), _source_seeds())
        connection.execute(f"UPDATE build_identity SET {stored_field} = 2")

        with pytest.raises(BuildStateConflict, match=rf"{stored_field}.*--rebuild"):
            initialize_build_state(
                connection,
                _build_identity(),
                _warc_seeds(),
                _source_seeds(),
            )
    finally:
        connection.close()


def test_build_state_rejects_unsupported_requested_catalog_schema() -> None:
    connection = duckdb.connect()
    try:
        with pytest.raises(ValueError, match="catalog_schema_version must be 1"):
            initialize_build_state(
                connection,
                replace(_build_identity(), catalog_schema_version=2),
                _warc_seeds(),
                _source_seeds(),
            )
        assert connection.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall() == []
    finally:
        connection.close()


@pytest.mark.parametrize("partial", [True, False])
def test_build_state_rejects_partial_or_malformed_existing_tables(partial: bool) -> None:
    connection = duckdb.connect()
    try:
        connection.execute("CREATE TABLE build_identity(unexpected VARCHAR)")
        if not partial:
            connection.execute("CREATE TABLE warc_inventory(unexpected VARCHAR)")
            connection.execute("CREATE TABLE source_shards(unexpected VARCHAR)")

        with pytest.raises(BuildStateCorrupt, match="--rebuild"):
            initialize_build_state(
                connection,
                _build_identity(),
                _warc_seeds(),
                _source_seeds(),
            )
    finally:
        connection.close()


def test_build_state_reopen_recovers_only_running_sources_and_preserves_progress(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.duckdb"
    sources = (
        *_source_seeds(),
        SourceShardSeed(2, "https://data.commoncrawl.org/index/part-002.parquet", "66" * 32),
    )
    connection = duckdb.connect(str(state_path))
    initialize_build_state(connection, _build_identity(), _warc_seeds(), sources)
    connection.execute(
        "UPDATE warc_inventory SET object_bytes = 4096, attempts = 2 WHERE warc_index = 0"
    )
    connection.execute(
        "UPDATE source_shards SET attempts = 1, last_error = 'retry later' WHERE source_index = 0"
    )
    connection.execute(
        "UPDATE source_shards SET status = 'running', attempts = 2 WHERE source_index = 1"
    )
    connection.execute(
        """
        UPDATE source_shards
        SET status = 'ready', candidate_rows = 0, candidate_bytes = 128,
            attempts = 3, completed_at = TIMESTAMPTZ '2026-07-12 10:00:00+00'
        WHERE source_index = 2
        """
    )
    connection.close()

    connection = duckdb.connect(str(state_path))
    try:
        result = initialize_build_state(
            connection,
            _build_identity(),
            _warc_seeds(),
            sources,
        )
        assert result.reused is True
        assert result.recovered_source_shards == 1
        assert connection.execute(
            """
            SELECT source_index, status, candidate_rows, candidate_bytes,
                   attempts, last_error, completed_at IS NOT NULL
            FROM source_shards ORDER BY source_index
            """
        ).fetchall() == [
            (0, "pending", None, None, 1, "retry later", False),
            (1, "pending", None, None, 2, None, False),
            (2, "ready", 0, 128, 3, None, True),
        ]
        assert connection.execute(
            "SELECT object_bytes, attempts FROM warc_inventory WHERE warc_index = 0"
        ).fetchone() == (4096, 2)
    finally:
        connection.close()

    connection = duckdb.connect(str(state_path), read_only=True)
    try:
        assert connection.execute(
            "SELECT status FROM source_shards ORDER BY source_index"
        ).fetchall() == [("pending",), ("pending",), ("ready",)]
    finally:
        connection.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("crawl_id", "CC-MAIN-2016-22"),
        ("pages_per_domain", 1),
        ("selection_policy_version", "page-selection-v2"),
        ("selection_policy_sha256", "aa" * 32),
        ("source_schema_sha256", "bb" * 32),
        ("warc_manifest_sha256", "cc" * 32),
        ("index_manifest_sha256", "dd" * 32),
    ],
)
def test_build_state_identity_conflict_is_exact_and_happens_before_recovery(
    field: str,
    value: object,
) -> None:
    connection = duckdb.connect()
    initialize_build_state(connection, _build_identity(), _warc_seeds(), _source_seeds())
    connection.execute("UPDATE source_shards SET status = 'running' WHERE source_index = 0")
    try:
        with pytest.raises(BuildStateConflict, match=rf"{field}.*--rebuild"):
            initialize_build_state(
                connection,
                replace(_build_identity(), **{field: value}),
                _warc_seeds(),
                _source_seeds(),
            )
        assert connection.execute(
            "SELECT status FROM source_shards WHERE source_index = 0"
        ).fetchone() == ("running",)
    finally:
        connection.close()


@pytest.mark.parametrize(
    "warcs,sources,error",
    [
        (
            (
                WarcObject(0, "crawl-data/CC-MAIN-2026-25/changed.warc.gz"),
                _warc_seeds()[1],
            ),
            _source_seeds(),
            BuildStateConflict,
        ),
        (
            _warc_seeds(),
            (
                SourceShardSeed(0, "https://data.commoncrawl.org/index/changed.parquet", "44" * 32),
                _source_seeds()[1],
            ),
            BuildStateConflict,
        ),
        (_warc_seeds()[:1], _source_seeds(), BuildStateCorrupt),
        (_warc_seeds(), _source_seeds()[:1], BuildStateCorrupt),
        (
            (*_warc_seeds(), WarcObject(2, "crawl-data/CC-MAIN-2026-25/extra.warc.gz")),
            _source_seeds(),
            BuildStateCorrupt,
        ),
        (
            _warc_seeds(),
            (
                *_source_seeds(),
                SourceShardSeed(2, "https://data.commoncrawl.org/index/extra.parquet", "66" * 32),
            ),
            BuildStateCorrupt,
        ),
    ],
)
def test_build_state_rejects_changed_missing_or_extra_seeds(
    warcs: tuple[WarcObject, ...],
    sources: tuple[SourceShardSeed, ...],
    error: type[Exception],
) -> None:
    connection = duckdb.connect()
    try:
        initialize_build_state(connection, _build_identity(), _warc_seeds(), _source_seeds())
        with pytest.raises(error, match="--rebuild"):
            initialize_build_state(connection, _build_identity(), warcs, sources)
    finally:
        connection.close()


def test_build_state_reports_persisted_incomplete_seed_tables_as_corrupt() -> None:
    connection = duckdb.connect()
    try:
        initialize_build_state(connection, _build_identity(), _warc_seeds(), _source_seeds())
        connection.execute("DELETE FROM warc_inventory WHERE warc_index = 1")

        with pytest.raises(BuildStateCorrupt, match="incomplete.*--rebuild"):
            initialize_build_state(
                connection,
                _build_identity(),
                _warc_seeds(),
                _source_seeds(),
            )
    finally:
        connection.close()


def test_build_state_first_seed_is_atomic(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = duckdb.connect()

    def fail_source_seed(
        _connection: duckdb.DuckDBPyConnection,
        _sources: tuple[SourceShardSeed, ...],
    ) -> None:
        raise RuntimeError("simulated source seed failure")

    monkeypatch.setattr(catalog, "_seed_source_shards", fail_source_seed)
    try:
        with pytest.raises(RuntimeError, match="simulated"):
            initialize_build_state(
                connection,
                _build_identity(),
                _warc_seeds(),
                _source_seeds(),
            )
        assert connection.execute(
            "SELECT table_name FROM information_schema.tables"
        ).fetchall() == []
    finally:
        connection.close()


@pytest.mark.parametrize(
    "statement,parameters",
    [
        (
            "UPDATE build_identity SET selection_policy_sha256 = ?",
            ["not-a-hash"],
        ),
        (
            "INSERT INTO warc_inventory(warc_index, warc_filename) VALUES (2, ?)",
            [_warc_seeds()[0].warc_filename],
        ),
        (
            "UPDATE warc_inventory SET object_bytes = 0 WHERE warc_index = 0",
            [],
        ),
        (
            "INSERT INTO source_shards(source_index, source_url, source_schema_sha256) "
            "VALUES (2, ?, ?)",
            [_source_seeds()[0].source_url, "66" * 32],
        ),
        (
            "UPDATE source_shards SET status = 'invalid' WHERE source_index = 0",
            [],
        ),
        (
            "UPDATE source_shards SET status = 'ready' WHERE source_index = 0",
            [],
        ),
    ],
)
def test_build_state_database_constraints_reject_invalid_rows(
    statement: str,
    parameters: list[str],
) -> None:
    connection = duckdb.connect()
    try:
        initialize_build_state(connection, _build_identity(), _warc_seeds(), _source_seeds())
        with pytest.raises(duckdb.ConstraintException):
            connection.execute(statement, parameters)
    finally:
        connection.close()


def test_local_candidate_query_emits_the_fixed_schema(tmp_path: Path) -> None:
    connection, path, schema = _candidate_fixture(
        tmp_path,
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
    )
    try:
        query = local_candidate_query(schema, 25)
        columns = connection.execute(
            f"DESCRIBE SELECT * FROM ({query})",
            [str(path)],
        ).fetchall()
        rows = _local_candidates(
            connection,
            path,
            schema,
        )

        assert tuple((row[0], row[1]) for row in columns) == CANDIDATE_COLUMNS
        assert len(rows) == 1
        assert rows[0][0] == 7
        assert len(rows[0]) == len(CANDIDATE_COLUMNS)
    finally:
        connection.close()


def test_local_candidate_query_filters_ineligible_and_negative_coordinates(
    tmp_path: Path,
) -> None:
    rows = [
        _candidate_page("valid.example", "https://valid.example/", "/", 10),
        _candidate_page("status.example", "https://status.example/", "/", 20, status=404),
        _candidate_page(
            "mime.example",
            "https://mime.example/",
            "/",
            30,
            reported_mime="image/png",
            detected_mime="image/png",
        ),
        _candidate_page("negative.example", "https://negative.example/", "/", -1),
    ]
    connection, path, schema = _candidate_fixture(tmp_path, rows)
    try:
        candidates = _local_candidates(connection, path, schema)

        assert [row[1] for row in candidates] == ["valid.example"]
    finally:
        connection.close()


def test_local_candidate_coordinate_canonicalization_precedes_top_n(
    tmp_path: Path,
) -> None:
    rows = [
        _candidate_page(
            "example.com",
            "https://example.com/",
            "/",
            10,
            languages="fra",
        ),
        _candidate_page(
            "example.com",
            "https://example.com/",
            "/",
            10,
            languages=None,
        ),
        _candidate_page(
            "example.com",
            "https://example.com/",
            "/",
            10,
            languages="eng",
        ),
        _candidate_page(
            "example.com",
            "https://example.com/contact",
            "/contact",
            20,
            languages="deu",
        ),
        _candidate_page(
            "example.com",
            "https://example.com/news",
            "/news",
            30,
            languages="eng",
        ),
    ]
    connection, path, schema = _candidate_fixture(tmp_path, rows)
    try:
        candidates = _local_candidates(
            connection,
            path,
            schema,
            pages_per_domain=2,
        )

        assert [(row[2], row[3]) for row in candidates] == [
            ("https://example.com/", "eng"),
            ("https://example.com/contact", "deu"),
        ]
    finally:
        connection.close()


def test_local_candidate_coordinate_identity_includes_record_length(
    tmp_path: Path,
) -> None:
    rows = [
        _candidate_page("example.com", "https://example.com/a", "/a", 10, length=100),
        _candidate_page("example.com", "https://example.com/b", "/b", 10, length=101),
    ]
    connection, path, schema = _candidate_fixture(tmp_path, rows)
    try:
        candidates = _local_candidates(
            connection,
            path,
            schema,
            pages_per_domain=2,
        )

        assert [(row[2], row[6]) for row in candidates] == [
            ("https://example.com/a", 100),
            ("https://example.com/b", 101),
        ]
    finally:
        connection.close()


def test_local_candidate_top_n_is_applied_per_domain(tmp_path: Path) -> None:
    rows = [
        _candidate_page("a.example", "https://a.example/", "/", 10),
        _candidate_page("a.example", "https://a.example/about", "/about", 20),
        _candidate_page("a.example", "https://a.example/news", "/news", 30),
        _candidate_page("b.example", "https://b.example/", "/", 40),
        _candidate_page("b.example", "https://b.example/contact", "/contact", 50),
        _candidate_page("b.example", "https://b.example/news", "/news", 60),
    ]
    connection, path, schema = _candidate_fixture(tmp_path, rows)
    try:
        candidates = _local_candidates(
            connection,
            path,
            schema,
            pages_per_domain=2,
        )

        assert [(row[1], row[2]) for row in candidates] == [
            ("a.example", "https://a.example/"),
            ("a.example", "https://a.example/about"),
            ("b.example", "https://b.example/"),
            ("b.example", "https://b.example/contact"),
        ]
    finally:
        connection.close()


def test_local_candidate_duplicate_null_ranks_do_not_create_false_conflict(
    tmp_path: Path,
) -> None:
    rows = [
        _candidate_page(
            "example.com",
            "https://example.com/no-path",
            None,
            10,
            languages="fra",
        ),
        _candidate_page(
            "example.com",
            "https://example.com/no-path",
            None,
            10,
            languages="eng",
        ),
    ]
    connection, path, schema = _candidate_fixture(tmp_path, rows)
    try:
        candidates = _local_candidates(connection, path, schema)

        assert len(candidates) == 1
        assert candidates[0][3] == "eng"
        assert candidates[0][10:12] == (None, None)
    finally:
        connection.close()


@pytest.mark.parametrize(
    "conflicting_row",
    [
        _candidate_page("other.example", "https://example.com/", "/", 10),
        _candidate_page("example.com", "https://example.com/different", "/", 10),
        _candidate_page("example.com", "https://example.com/", "/contact", 10),
        _candidate_page("example.com", "https://example.com/", None, 10),
    ],
)
def test_local_candidate_rejects_coordinate_selection_conflicts(
    tmp_path: Path,
    conflicting_row: tuple[object, ...],
) -> None:
    connection, path, schema = _candidate_fixture(
        tmp_path,
        [
            _candidate_page("example.com", "https://example.com/", "/", 10),
            conflicting_row,
        ],
    )
    try:
        with pytest.raises(
            duckdb.Error,
            match="duplicate capture coordinate has conflicting selection values",
        ):
            _local_candidates(connection, path, schema)
    finally:
        connection.close()


def test_local_candidate_detects_conflict_below_top_n_boundary(tmp_path: Path) -> None:
    rows = [
        _candidate_page("example.com", "https://example.com/", "/", 1),
        _candidate_page("example.com", "https://example.com/ordinary", "/ordinary", 2),
        _candidate_page(
            "example.com",
            "https://example.com/ordinary",
            "/deep/ordinary",
            2,
        ),
    ]
    connection, path, schema = _candidate_fixture(tmp_path, rows)
    try:
        with pytest.raises(
            duckdb.Error,
            match="duplicate capture coordinate has conflicting selection values",
        ):
            _local_candidates(
                connection,
                path,
                schema,
                pages_per_domain=1,
            )
    finally:
        connection.close()


@pytest.mark.parametrize(
    "source_index,pages_per_domain",
    [(-1, 25), (2**32, 25), (0, 0), (0, 2**16)],
)
def test_local_candidate_query_rejects_invalid_limits(
    source_index: int,
    pages_per_domain: int,
) -> None:
    schema = SourceSchema(0, ())

    with pytest.raises(ValueError):
        local_candidate_query(
            SourceSchema(source_index, schema.column_types),
            pages_per_domain,
        )


def test_local_candidate_query_contains_no_source_or_output_path() -> None:
    query = local_candidate_query(SourceSchema(0, ()), 25).lower()

    assert "read_parquet(?)" in query
    assert "https://" not in query
    assert "/tmp/" not in query
    assert ".partial" not in query
