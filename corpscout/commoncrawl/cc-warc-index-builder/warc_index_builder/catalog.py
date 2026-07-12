"""Local catalog build paths and exclusive build lifecycle."""

import fcntl
import os
import re
import shutil
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, fields
from pathlib import Path

import duckdb

from ._identity import decode_sha256, new_identity_digest, update_text
from .manifests import IndexSource, SourceSchema, WarcObject, source_schema_sha256


CATALOG_SCHEMA_VERSION = 1
_BUILD_STATE_SCHEMA_VERSION = 1
_CRAWL_ID = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}")


class CatalogBuildLocked(RuntimeError):
    pass


class BuildStateConflict(RuntimeError):
    pass


class BuildStateCorrupt(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class BuildIdentity:
    catalog_schema_version: int
    crawl_id: str
    pages_per_domain: int
    selection_policy_version: str
    selection_policy_sha256: str
    source_schema_sha256: str
    warc_manifest_sha256: str
    index_manifest_sha256: str


@dataclass(frozen=True, slots=True)
class SourceShardSeed:
    source_index: int
    source_url: str
    source_schema_sha256: str


@dataclass(frozen=True, slots=True)
class BuildStateResult:
    reused: bool
    recovered_source_shards: int


_BUILD_STATE_DDL = (
    """
    CREATE TABLE IF NOT EXISTS build_identity (
        singleton BOOLEAN PRIMARY KEY CHECK (singleton),
        state_schema_version USMALLINT NOT NULL CHECK (state_schema_version > 0),
        catalog_schema_version USMALLINT NOT NULL CHECK (catalog_schema_version > 0),
        crawl_id VARCHAR NOT NULL
            CHECK (regexp_full_match(crawl_id, 'CC-MAIN-[0-9]{4}-[0-9]{2}')),
        pages_per_domain USMALLINT NOT NULL CHECK (pages_per_domain > 0),
        selection_policy_version VARCHAR NOT NULL
            CHECK (length(trim(selection_policy_version)) > 0),
        selection_policy_sha256 VARCHAR NOT NULL
            CHECK (regexp_full_match(selection_policy_sha256, '[0-9a-f]{64}')),
        source_schema_sha256 VARCHAR NOT NULL
            CHECK (regexp_full_match(source_schema_sha256, '[0-9a-f]{64}')),
        warc_manifest_sha256 VARCHAR NOT NULL
            CHECK (regexp_full_match(warc_manifest_sha256, '[0-9a-f]{64}')),
        index_manifest_sha256 VARCHAR NOT NULL
            CHECK (regexp_full_match(index_manifest_sha256, '[0-9a-f]{64}')),
        warc_inventory_sha256 VARCHAR CHECK (
            warc_inventory_sha256 IS NULL
            OR regexp_full_match(warc_inventory_sha256, '[0-9a-f]{64}')
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS warc_inventory (
        warc_index UINTEGER PRIMARY KEY,
        warc_filename VARCHAR NOT NULL UNIQUE
            CHECK (length(trim(warc_filename)) > 0),
        object_bytes UBIGINT CHECK (object_bytes IS NULL OR object_bytes > 0),
        attempts UINTEGER NOT NULL DEFAULT 0,
        last_error VARCHAR CHECK (last_error IS NULL OR length(trim(last_error)) > 0),
        CHECK (object_bytes IS NULL OR last_error IS NULL)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS source_shards (
        source_index UINTEGER PRIMARY KEY,
        source_url VARCHAR NOT NULL UNIQUE CHECK (length(trim(source_url)) > 0),
        source_schema_sha256 VARCHAR NOT NULL
            CHECK (regexp_full_match(source_schema_sha256, '[0-9a-f]{64}')),
        status VARCHAR NOT NULL DEFAULT 'pending'
            CHECK (status IN ('pending', 'running', 'ready')),
        candidate_rows UBIGINT,
        candidate_bytes UBIGINT CHECK (candidate_bytes IS NULL OR candidate_bytes > 0),
        attempts UINTEGER NOT NULL DEFAULT 0,
        last_error VARCHAR CHECK (last_error IS NULL OR length(trim(last_error)) > 0),
        completed_at TIMESTAMPTZ,
        CHECK (
            (
                status = 'ready'
                AND candidate_rows IS NOT NULL
                AND candidate_bytes IS NOT NULL
                AND completed_at IS NOT NULL
                AND last_error IS NULL
            )
            OR
            (
                status <> 'ready'
                AND candidate_rows IS NULL
                AND candidate_bytes IS NULL
                AND completed_at IS NULL
            )
        )
    )
    """,
)

_BUILD_STATE_COLUMNS = {
    "build_identity": (
        ("singleton", "BOOLEAN", "NO"),
        ("state_schema_version", "USMALLINT", "NO"),
        ("catalog_schema_version", "USMALLINT", "NO"),
        ("crawl_id", "VARCHAR", "NO"),
        ("pages_per_domain", "USMALLINT", "NO"),
        ("selection_policy_version", "VARCHAR", "NO"),
        ("selection_policy_sha256", "VARCHAR", "NO"),
        ("source_schema_sha256", "VARCHAR", "NO"),
        ("warc_manifest_sha256", "VARCHAR", "NO"),
        ("index_manifest_sha256", "VARCHAR", "NO"),
        ("warc_inventory_sha256", "VARCHAR", "YES"),
    ),
    "warc_inventory": (
        ("warc_index", "UINTEGER", "NO"),
        ("warc_filename", "VARCHAR", "NO"),
        ("object_bytes", "UBIGINT", "YES"),
        ("attempts", "UINTEGER", "NO"),
        ("last_error", "VARCHAR", "YES"),
    ),
    "source_shards": (
        ("source_index", "UINTEGER", "NO"),
        ("source_url", "VARCHAR", "NO"),
        ("source_schema_sha256", "VARCHAR", "NO"),
        ("status", "VARCHAR", "NO"),
        ("candidate_rows", "UBIGINT", "YES"),
        ("candidate_bytes", "UBIGINT", "YES"),
        ("attempts", "UINTEGER", "NO"),
        ("last_error", "VARCHAR", "YES"),
        ("completed_at", "TIMESTAMP WITH TIME ZONE", "YES"),
    ),
}


def source_shard_seeds(
    sources: Sequence[IndexSource], schemas: Sequence[SourceSchema]
) -> tuple[SourceShardSeed, ...]:
    """Pair manifest sources with their inspected, path-free schema fingerprints."""
    if len(sources) != len(schemas):
        raise ValueError("index sources and source schemas must have the same length")
    seeds: list[SourceShardSeed] = []
    for expected_index, (source, schema) in enumerate(zip(sources, schemas)):
        if source.source_index != expected_index or schema.source_index != expected_index:
            raise ValueError("sources and schemas must be in matching source_index order")
        seeds.append(
            SourceShardSeed(
                source_index=expected_index,
                source_url=source.url,
                source_schema_sha256=source_schema_sha256(schema),
            )
        )
    return tuple(seeds)


def initialize_build_state(
    connection: duckdb.DuckDBPyConnection,
    identity: BuildIdentity,
    warcs: Sequence[WarcObject],
    sources: Sequence[SourceShardSeed],
) -> BuildStateResult:
    """Create or exactly reopen one typed, transactionally seeded build state."""
    _validate_build_state_inputs(identity, warcs, sources)
    connection.execute("BEGIN TRANSACTION")
    try:
        _require_complete_build_state_table_set(connection)
        for statement in _BUILD_STATE_DDL:
            connection.execute(statement)
        _require_build_state_table_shapes(connection)

        rows = connection.execute(
            """
            SELECT state_schema_version, catalog_schema_version,
                   crawl_id, pages_per_domain, selection_policy_version,
                   selection_policy_sha256, source_schema_sha256,
                   warc_manifest_sha256, index_manifest_sha256
            FROM build_identity
            """
        ).fetchall()
        if not rows:
            _require_empty_seed_tables(connection)
            _insert_build_identity(connection, identity)
            _seed_warc_inventory(connection, warcs)
            _seed_source_shards(connection, sources)
            result = BuildStateResult(reused=False, recovered_source_shards=0)
        elif len(rows) == 1:
            state_schema_version, *identity_values = rows[0]
            if state_schema_version != _BUILD_STATE_SCHEMA_VERSION:
                raise BuildStateConflict(
                    "build identity conflicts for: state_schema_version; use --rebuild"
                )
            stored_identity = BuildIdentity(*identity_values)
            _require_matching_identity(identity, stored_identity)
            _require_matching_seeds(connection, warcs, sources)
            recovered_sources = connection.execute(
                "UPDATE source_shards SET status = 'pending' WHERE status = 'running' RETURNING 1"
            ).fetchall()
            result = BuildStateResult(
                reused=True,
                recovered_source_shards=len(recovered_sources),
            )
        else:
            raise BuildStateCorrupt("build_identity must contain exactly one row; use --rebuild")
        connection.execute("COMMIT")
        return result
    except BaseException:
        connection.execute("ROLLBACK")
        raise


def _validate_build_state_inputs(
    identity: BuildIdentity,
    warcs: Sequence[WarcObject],
    sources: Sequence[SourceShardSeed],
) -> None:
    if identity.catalog_schema_version != CATALOG_SCHEMA_VERSION:
        raise ValueError(
            f"catalog_schema_version must be {CATALOG_SCHEMA_VERSION}"
        )
    if _CRAWL_ID.fullmatch(identity.crawl_id) is None:
        raise ValueError("crawl_id must match CC-MAIN-YYYY-NN")
    if not identity.selection_policy_version.strip():
        raise ValueError("selection_policy_version must not be blank")
    if not 1 <= identity.pages_per_domain <= 0xFFFF:
        raise ValueError("pages_per_domain must be between 1 and uint16 max")
    identity_hashes = (
        ("selection_policy_sha256", identity.selection_policy_sha256),
        ("source_schema_sha256", identity.source_schema_sha256),
        ("warc_manifest_sha256", identity.warc_manifest_sha256),
        ("index_manifest_sha256", identity.index_manifest_sha256),
    )
    for name, value in identity_hashes:
        decode_sha256(value, name)

    if not warcs:
        raise ValueError("WARC inventory must not be empty")
    warc_filenames: set[str] = set()
    for expected_index, warc in enumerate(warcs):
        if warc.warc_index != expected_index:
            raise ValueError("WARC inventory must be in contiguous warc_index order")
        if not warc.warc_filename.strip():
            raise ValueError("WARC filenames must not be blank")
        if warc.warc_filename in warc_filenames:
            raise ValueError("WARC filenames must be unique")
        warc_filenames.add(warc.warc_filename)

    if not sources:
        raise ValueError("source shards must not be empty")
    source_urls: set[str] = set()
    for expected_index, source in enumerate(sources):
        if source.source_index != expected_index:
            raise ValueError("source shards must be in contiguous source_index order")
        if not source.source_url.strip():
            raise ValueError("source URLs must not be blank")
        if source.source_url in source_urls:
            raise ValueError("source URLs must be unique")
        decode_sha256(source.source_schema_sha256, "source_schema_sha256")
        source_urls.add(source.source_url)


def _require_complete_build_state_table_set(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    existing = {
        row[0]
        for row in connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    state_tables = set(_BUILD_STATE_COLUMNS)
    present = existing & state_tables
    if present and present != state_tables:
        raise BuildStateCorrupt(
            "build state has only some required tables; use --rebuild"
        )


def _require_build_state_table_shapes(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    for table, expected_columns in _BUILD_STATE_COLUMNS.items():
        try:
            rows = connection.execute(f"DESCRIBE {table}").fetchall()
        except duckdb.Error as error:
            raise BuildStateCorrupt(
                f"cannot inspect build state table {table}; use --rebuild"
            ) from error
        actual_columns = tuple((str(row[0]), str(row[1]), str(row[2])) for row in rows)
        if actual_columns != expected_columns:
            raise BuildStateCorrupt(
                f"build state table {table} has an incompatible schema; use --rebuild"
            )


def _require_empty_seed_tables(connection: duckdb.DuckDBPyConnection) -> None:
    warc_count, source_count = connection.execute(
        """
        SELECT (SELECT count(*) FROM warc_inventory),
               (SELECT count(*) FROM source_shards)
        """
    ).fetchone()
    if warc_count or source_count:
        raise BuildStateCorrupt(
            "state tables contain rows without a build identity; use --rebuild"
        )


def _insert_build_identity(
    connection: duckdb.DuckDBPyConnection, identity: BuildIdentity
) -> None:
    connection.execute(
        """
        INSERT INTO build_identity VALUES (
            true, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL
        )
        """,
        [
            _BUILD_STATE_SCHEMA_VERSION,
            identity.catalog_schema_version,
            identity.crawl_id,
            identity.pages_per_domain,
            identity.selection_policy_version,
            identity.selection_policy_sha256,
            identity.source_schema_sha256,
            identity.warc_manifest_sha256,
            identity.index_manifest_sha256,
        ],
    )


def _seed_warc_inventory(
    connection: duckdb.DuckDBPyConnection, warcs: Sequence[WarcObject]
) -> None:
    connection.executemany(
        "INSERT INTO warc_inventory(warc_index, warc_filename) VALUES (?, ?)",
        [(warc.warc_index, warc.warc_filename) for warc in warcs],
    )


def _seed_source_shards(
    connection: duckdb.DuckDBPyConnection, sources: Sequence[SourceShardSeed]
) -> None:
    connection.executemany(
        """
        INSERT INTO source_shards(source_index, source_url, source_schema_sha256)
        VALUES (?, ?, ?)
        """,
        [
            (source.source_index, source.source_url, source.source_schema_sha256)
            for source in sources
        ],
    )


def _require_matching_identity(expected: BuildIdentity, stored: BuildIdentity) -> None:
    conflicts = [
        field.name
        for field in fields(BuildIdentity)
        if getattr(expected, field.name) != getattr(stored, field.name)
    ]
    if conflicts:
        raise BuildStateConflict(
            "build identity conflicts for: " + ", ".join(conflicts) + "; use --rebuild"
        )


def _require_matching_seeds(
    connection: duckdb.DuckDBPyConnection,
    warcs: Sequence[WarcObject],
    sources: Sequence[SourceShardSeed],
) -> None:
    stored_warcs = tuple(
        connection.execute(
            "SELECT warc_index, warc_filename FROM warc_inventory ORDER BY warc_index"
        ).fetchall()
    )
    expected_warcs = tuple((warc.warc_index, warc.warc_filename) for warc in warcs)
    if len(stored_warcs) != len(expected_warcs):
        raise BuildStateCorrupt(
            "stored WARC inventory row count is incomplete; use --rebuild"
        )
    if stored_warcs != expected_warcs:
        raise BuildStateConflict(
            "stored WARC inventory conflicts with the manifest; use --rebuild"
        )

    stored_sources = tuple(
        connection.execute(
            """
            SELECT source_index, source_url, source_schema_sha256
            FROM source_shards
            ORDER BY source_index
            """
        ).fetchall()
    )
    expected_sources = tuple(
        (source.source_index, source.source_url, source.source_schema_sha256)
        for source in sources
    )
    if len(stored_sources) != len(expected_sources):
        raise BuildStateCorrupt(
            "stored source shard row count is incomplete; use --rebuild"
        )
    if stored_sources != expected_sources:
        raise BuildStateConflict(
            "stored source shards conflict with the index manifest; use --rebuild"
        )


def warc_inventory_sha256(inventory: Sequence[tuple[int, str, int]]) -> str:
    """Hash the complete index-ordered WARC inventory and exact object sizes."""
    if not inventory:
        raise ValueError("WARC inventory must not be empty")
    digest = new_identity_digest("warc-inventory")
    digest.update(len(inventory).to_bytes(4, byteorder="big"))
    filenames: set[str] = set()
    for expected_index, (warc_index, warc_filename, object_bytes) in enumerate(inventory):
        if warc_index != expected_index:
            raise ValueError(
                "WARC inventory must be in contiguous warc_index order starting at 0"
            )
        if not warc_filename.strip():
            raise ValueError("WARC filenames must not be blank")
        if warc_filename in filenames:
            raise ValueError("WARC filenames must be unique")
        if not 1 <= object_bytes <= 0xFFFFFFFFFFFFFFFF:
            raise ValueError("WARC object sizes must be between 1 and uint64 max")
        filenames.add(warc_filename)
        digest.update(warc_index.to_bytes(4, byteorder="big"))
        update_text(digest, warc_filename)
        digest.update(object_bytes.to_bytes(8, byteorder="big"))
    return digest.hexdigest()


def catalog_id(
    *,
    schema_version: int,
    crawl_id: str,
    pages_per_domain: int,
    selection_policy_version: str,
    selection_policy_sha256: str,
    source_schema_sha256: str,
    warc_manifest_sha256: str,
    index_manifest_sha256: str,
    warc_inventory_sha256: str,
) -> str:
    """Hash every logical input that determines one published catalog."""
    if not 1 <= schema_version <= 0xFFFF:
        raise ValueError("schema_version must be between 1 and uint16 max")
    if not 1 <= pages_per_domain <= 0xFFFF:
        raise ValueError("pages_per_domain must be between 1 and uint16 max")
    if not crawl_id or not selection_policy_version:
        raise ValueError("catalog identity strings must not be empty")

    hashes = (
        ("selection_policy_sha256", selection_policy_sha256),
        ("source_schema_sha256", source_schema_sha256),
        ("warc_manifest_sha256", warc_manifest_sha256),
        ("index_manifest_sha256", index_manifest_sha256),
        ("warc_inventory_sha256", warc_inventory_sha256),
    )
    digest = new_identity_digest("catalog")
    digest.update(schema_version.to_bytes(2, byteorder="big"))
    update_text(digest, crawl_id)
    digest.update(pages_per_domain.to_bytes(2, byteorder="big"))
    update_text(digest, selection_policy_version)
    for name, value in hashes:
        digest.update(decode_sha256(value, name))
    return digest.hexdigest()


def require_path_within(base: Path, target: Path) -> None:
    resolved_base = base.resolve()
    resolved_target = target.resolve()
    try:
        resolved_target.relative_to(resolved_base)
    except ValueError as error:
        raise ValueError(f"path {resolved_target} escapes base {resolved_base}") from error


@contextmanager
def catalog_build_lock(catalog_directory: Path) -> Iterator[None]:
    catalog_directory.mkdir(parents=True, exist_ok=True)
    lock_path = catalog_directory / "build.lock"
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(lock_path, flags, 0o644)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise CatalogBuildLocked(f"another builder holds {lock_path}") from error
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


def prepare_build_directory(base: Path, catalog_directory: Path, *, rebuild: bool) -> Path:
    require_path_within(base, catalog_directory)
    build_directory = catalog_directory / ".build"
    if build_directory.is_symlink():
        raise ValueError(f"build directory must not be a symlink: {build_directory}")
    require_path_within(base, build_directory)
    if rebuild and build_directory.exists():
        if not build_directory.is_dir():
            raise ValueError(f"build path is not a directory: {build_directory}")
        shutil.rmtree(build_directory)
    build_directory.mkdir(parents=True, exist_ok=True)
    return build_directory
