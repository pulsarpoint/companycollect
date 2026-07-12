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
    prepare_build_directory,
    require_path_within,
    source_shard_seeds,
    warc_inventory_sha256,
)
from warc_index_builder.manifests import (
    IndexSource,
    SourceSchema,
    WarcObject,
    source_schema_sha256,
)
from warc_index_builder.selection import (
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
