"""Local catalog build paths and exclusive build lifecycle."""

import fcntl
import os
import re
import shutil
import time
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, fields
from pathlib import Path

import duckdb

from ._identity import decode_sha256, new_identity_digest, update_text
from .manifests import IndexSource, SourceSchema, WarcObject, source_schema_sha256
from .selection import (
    CANDIDATE_COLUMNS,
    RANKING_COLUMN_NAMES,
    candidate_output_projection,
    eligibility_predicate,
    normalized_source_projection,
    ranking_order_clause,
    ranking_projection,
    validated_candidate_coordinate_projection,
)


CATALOG_SCHEMA_VERSION = 1
_BUILD_STATE_SCHEMA_VERSION = 1
_CRAWL_ID = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}")
_CANDIDATE_HTTP_STATUS = re.compile(
    r"(?:\bhttp\s+|statuscode:\s*)([1-5][0-9]{2})\b",
    re.IGNORECASE,
)
_TRANSIENT_CANDIDATE_HTTP_STATUSES = frozenset({429, 500, 502, 503, 504})
_TRANSIENT_CANDIDATE_NETWORK_MARKERS = (
    "connection reset",
    "connection closed",
    "connection refused",
    "broken pipe",
    "could not connect",
    "failed to connect",
    "network is unreachable",
    "temporary failure",
    "temporary name resolution failure",
    "timed out",
    "timeout",
    "unexpected eof",
)


class CatalogBuildLocked(RuntimeError):
    pass


class BuildStateConflict(RuntimeError):
    pass


class BuildStateCorrupt(RuntimeError):
    pass


class CandidateArtifactError(RuntimeError):
    pass


class CandidateBuildError(RuntimeError):
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


@dataclass(frozen=True, slots=True)
class CandidateMetadata:
    rows: int
    byte_count: int


@dataclass(frozen=True, slots=True)
class CandidateShardResult:
    source_index: int
    path: Path
    rows: int
    byte_count: int
    reused: bool
    attempts_this_run: int
    attempts_total: int
    retries: int
    http_429: int
    http_503: int
    elapsed_seconds: float


@dataclass(frozen=True, slots=True)
class _CandidateSourceState:
    status: str
    candidate_rows: int | None
    candidate_bytes: int | None
    attempts: int


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


def local_candidate_query(
    schema: SourceSchema,
    pages_per_domain: int,
) -> str:
    """Build the parameterized query for one URL-index Parquet source."""
    source_index = schema.source_index
    if not 0 <= source_index <= 0xFFFFFFFF:
        raise ValueError("schema source_index must be between 0 and uint32 max")
    if not 1 <= pages_per_domain <= 0xFFFF:
        raise ValueError("pages_per_domain must be between 1 and uint16 max")

    conflict_columns = ("root_domain", "url", *RANKING_COLUMN_NAMES)
    conflict_expression = "\nOR ".join(
        f"(min({column}) IS DISTINCT FROM max({column}) "
        f"OR (count({column}) > 0 AND count({column}) < count(*)))"
        for column in conflict_columns
    )
    ranking_aggregates = ",\n".join(
        f"min({column}) AS {column}" for column in RANKING_COLUMN_NAMES
    )
    return f"""
        WITH normalized AS (
            SELECT {normalized_source_projection(schema)}
            FROM read_parquet(?)
        ),
        eligible AS (
            SELECT
                CAST({source_index} AS UINTEGER) AS source_index,
                CAST(root_domain AS VARCHAR) AS root_domain,
                CAST(url AS VARCHAR) AS url,
                CAST(content_languages AS VARCHAR) AS content_languages,
                CAST(warc_filename AS VARCHAR) AS warc_filename,
                {validated_candidate_coordinate_projection()},
                {ranking_projection()}
            FROM normalized
            WHERE {eligibility_predicate()}
        ),
        coordinate_groups AS (
            SELECT
                min(source_index) AS source_index,
                min(root_domain) AS root_domain,
                min(url) AS url,
                first(
                    content_languages
                    ORDER BY content_languages COLLATE "binary" ASC NULLS LAST,
                             source_index ASC
                ) AS content_languages,
                warc_filename,
                warc_record_offset,
                warc_record_length,
                {ranking_aggregates},
                ({conflict_expression}) AS has_conflict
            FROM eligible
            GROUP BY warc_filename, warc_record_offset, warc_record_length
        ),
        ranked AS (
            SELECT coordinate_groups.*,
                   row_number() OVER (
                       PARTITION BY root_domain
                       ORDER BY {ranking_order_clause(pages_per_domain)}
                   ) AS local_rank,
                   bool_or(has_conflict) OVER () AS has_any_conflict
            FROM coordinate_groups
        )
        SELECT {candidate_output_projection()}
        FROM ranked
        WHERE local_rank <= {pages_per_domain}
          AND CASE
              WHEN has_any_conflict
                  THEN error('duplicate capture coordinate has conflicting selection values')
              ELSE true
          END
    """.strip()


def candidate_artifact_path(build_directory: Path, source_index: int) -> Path:
    if not 0 <= source_index <= 0xFFFFFFFF:
        raise ValueError("source_index must be between 0 and uint32 max")
    candidate_directory = build_directory / "candidates"
    require_path_within(build_directory, candidate_directory)
    return candidate_directory / f"source_{source_index:05d}.parquet"


def inspect_candidate_artifact(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    source_index: int,
) -> CandidateMetadata:
    """Validate one closed candidate Parquet and return persisted metadata."""
    if path.is_symlink() or not path.is_file():
        raise CandidateArtifactError(f"candidate artifact is not a regular file: {path}")
    try:
        byte_count = path.stat().st_size
    except OSError as error:
        raise CandidateArtifactError(f"stat candidate artifact {path}: {error}") from error
    if byte_count <= 0:
        raise CandidateArtifactError(f"candidate artifact is empty: {path}")

    try:
        described = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]
        ).fetchall()
        actual_columns = tuple((str(row[0]), str(row[1]).upper()) for row in described)
        if actual_columns != CANDIDATE_COLUMNS:
            raise CandidateArtifactError(
                f"candidate artifact has an incompatible schema: {path}"
            )
        rows, minimum_source, maximum_source = connection.execute(
            """
            SELECT count(*), min(source_index), max(source_index)
            FROM read_parquet(?)
            """,
            [str(path)],
        ).fetchone()
    except CandidateArtifactError:
        raise
    except duckdb.Error as error:
        raise CandidateArtifactError(f"read candidate artifact {path}: {error}") from error

    if rows > 0 and (minimum_source != source_index or maximum_source != source_index):
        raise CandidateArtifactError(
            f"candidate artifact contains a different source_index: {path}"
        )
    return CandidateMetadata(rows=int(rows), byte_count=byte_count)


def build_candidate_shard(
    state_connection: duckdb.DuckDBPyConnection,
    query_connection: duckdb.DuckDBPyConnection,
    build_directory: Path,
    source: IndexSource,
    schema: SourceSchema,
    pages_per_domain: int,
    http_attempts: int,
) -> CandidateShardResult:
    """Reuse or atomically build one source candidate while checkpointing state."""
    if state_connection is query_connection:
        raise ValueError("state and candidate query connections must be distinct")
    if http_attempts <= 0:
        raise ValueError("http_attempts must be positive")
    if source.source_index != schema.source_index:
        raise ValueError("source and schema indexes must match")
    started = time.monotonic()
    candidate_path = candidate_artifact_path(build_directory, source.source_index)
    candidate_directory = candidate_path.parent
    _prepare_candidate_directory(build_directory, candidate_directory)
    partial_path = candidate_path.with_name(f"{candidate_path.name}.partial")

    state = _candidate_source_state(state_connection, source, schema)
    if state.status == "ready":
        expected_rows, expected_bytes = state.candidate_rows, state.candidate_bytes
        try:
            metadata = inspect_candidate_artifact(
                query_connection, candidate_path, source.source_index
            )
        except CandidateArtifactError:
            _invalidate_candidate_state(state_connection, source.source_index)
        else:
            if metadata.rows == expected_rows and metadata.byte_count == expected_bytes:
                partial_removed = _remove_candidate_file(partial_path)
                if partial_removed:
                    _fsync_directory(candidate_directory)
                return CandidateShardResult(
                    source_index=source.source_index,
                    path=candidate_path,
                    rows=metadata.rows,
                    byte_count=metadata.byte_count,
                    reused=True,
                    attempts_this_run=0,
                    attempts_total=state.attempts,
                    retries=0,
                    http_429=0,
                    http_503=0,
                    elapsed_seconds=time.monotonic() - started,
                )
            _invalidate_candidate_state(state_connection, source.source_index)
    elif state.status != "pending":
        raise BuildStateCorrupt(
            f"source {source.source_index} is {state.status!r}; reopen state before building"
        )

    removed_final = _remove_candidate_file(candidate_path)
    removed_partial = _remove_candidate_file(partial_path)
    if removed_final or removed_partial:
        _fsync_directory(candidate_directory)
    attempts = 0
    retries = 0
    http_429 = 0
    http_503 = 0
    attempts_total = state.attempts
    for attempt in range(1, http_attempts + 1):
        attempts += 1
        attempts_total = _record_candidate_attempt(
            state_connection, source.source_index
        )
        try:
            _write_candidate_artifact(
                query_connection,
                source,
                schema,
                pages_per_domain,
                partial_path,
            )
        except duckdb.Error as error:
            _remove_candidate_file(partial_path)
            statuses = _candidate_http_statuses(error)
            http_429 += int(429 in statuses)
            http_503 += int(503 in statuses)
            if _is_transient_candidate_error(error) and attempt < http_attempts:
                retries += 1
                _record_candidate_retry(
                    state_connection, source.source_index, error
                )
                time.sleep(min(float(2 ** (attempt - 1)), 30.0))
                continue
            _record_candidate_failure(state_connection, source.source_index, error)
            raise CandidateBuildError(
                f"build candidate source {source.source_index}: {error}"
            ) from error

        try:
            metadata = inspect_candidate_artifact(
                query_connection, partial_path, source.source_index
            )
            _fsync_file(partial_path)
            os.replace(partial_path, candidate_path)
            _fsync_directory(candidate_directory)
            attempts_total = _record_candidate_ready(
                state_connection,
                source.source_index,
                metadata,
            )
        except (CandidateArtifactError, OSError, duckdb.Error) as error:
            _remove_candidate_file(partial_path)
            _record_candidate_failure(state_connection, source.source_index, error)
            raise CandidateBuildError(
                f"finalize candidate source {source.source_index}: {error}"
            ) from error

        return CandidateShardResult(
            source_index=source.source_index,
            path=candidate_path,
            rows=metadata.rows,
            byte_count=metadata.byte_count,
            reused=False,
            attempts_this_run=attempts,
            attempts_total=attempts_total,
            retries=retries,
            http_429=http_429,
            http_503=http_503,
            elapsed_seconds=time.monotonic() - started,
        )
    raise AssertionError("candidate attempt loop ended without a result")


def _prepare_candidate_directory(
    build_directory: Path, candidate_directory: Path
) -> None:
    require_path_within(build_directory, candidate_directory)
    if candidate_directory.is_symlink():
        raise CandidateArtifactError(
            f"candidate directory must not be a symlink: {candidate_directory}"
        )
    candidate_directory.mkdir(parents=True, exist_ok=True)


def _remove_candidate_file(path: Path) -> bool:
    if path.is_dir() and not path.is_symlink():
        raise CandidateArtifactError(f"candidate artifact path is a directory: {path}")
    try:
        existed = path.exists() or path.is_symlink()
        path.unlink(missing_ok=True)
    except OSError as error:
        raise CandidateArtifactError(f"remove candidate artifact {path}: {error}") from error
    return existed


def _candidate_source_state(
    connection: duckdb.DuckDBPyConnection,
    source: IndexSource,
    schema: SourceSchema,
) -> _CandidateSourceState:
    row = connection.execute(
        """
        SELECT source_url, source_schema_sha256, status,
               candidate_rows, candidate_bytes, attempts
        FROM source_shards
        WHERE source_index = ?
        """,
        [source.source_index],
    ).fetchone()
    if row is None:
        raise BuildStateCorrupt(f"source {source.source_index} is missing from build state")
    expected_schema_hash = source_schema_sha256(schema)
    if row[0] != source.url or row[1] != expected_schema_hash:
        raise BuildStateConflict(
            f"source {source.source_index} conflicts with build state; use --rebuild"
        )
    return _CandidateSourceState(
        status=str(row[2]),
        candidate_rows=row[3],
        candidate_bytes=row[4],
        attempts=int(row[5]),
    )


def _invalidate_candidate_state(
    connection: duckdb.DuckDBPyConnection, source_index: int
) -> None:
    updated = connection.execute(
        """
        UPDATE source_shards
        SET status = 'pending', candidate_rows = NULL, candidate_bytes = NULL,
            last_error = 'candidate artifact failed reuse validation',
            completed_at = NULL
        WHERE source_index = ? AND status = 'ready'
        RETURNING 1
        """,
        [source_index],
    ).fetchall()
    if len(updated) != 1:
        raise BuildStateCorrupt(
            f"cannot invalidate candidate source {source_index}; use --rebuild"
        )


def _record_candidate_attempt(
    connection: duckdb.DuckDBPyConnection, source_index: int
) -> int:
    updated = connection.execute(
        """
        UPDATE source_shards
        SET status = 'running', attempts = attempts + 1, last_error = NULL
        WHERE source_index = ? AND status IN ('pending', 'running')
        RETURNING attempts
        """,
        [source_index],
    ).fetchall()
    if len(updated) != 1:
        raise BuildStateCorrupt(f"cannot claim candidate source {source_index}")
    return int(updated[0][0])


def _record_candidate_retry(
    connection: duckdb.DuckDBPyConnection,
    source_index: int,
    error: BaseException,
) -> None:
    message = str(error).strip() or type(error).__name__
    updated = connection.execute(
        """
        UPDATE source_shards
        SET last_error = ?
        WHERE source_index = ? AND status = 'running'
        RETURNING 1
        """,
        [message[:2000], source_index],
    ).fetchall()
    if len(updated) != 1:
        raise BuildStateCorrupt(f"cannot record retry for source {source_index}")


def _record_candidate_ready(
    connection: duckdb.DuckDBPyConnection,
    source_index: int,
    metadata: CandidateMetadata,
) -> int:
    updated = connection.execute(
        """
        UPDATE source_shards
        SET status = 'ready', candidate_rows = ?, candidate_bytes = ?,
            last_error = NULL, completed_at = current_timestamp
        WHERE source_index = ? AND status = 'running'
        RETURNING attempts
        """,
        [metadata.rows, metadata.byte_count, source_index],
    ).fetchall()
    if len(updated) != 1:
        raise BuildStateCorrupt(f"cannot commit candidate source {source_index}")
    return int(updated[0][0])


def _record_candidate_failure(
    connection: duckdb.DuckDBPyConnection,
    source_index: int,
    error: BaseException,
) -> None:
    message = str(error).strip() or type(error).__name__
    updated = connection.execute(
        """
        UPDATE source_shards
        SET status = 'pending', candidate_rows = NULL, candidate_bytes = NULL,
            last_error = ?, completed_at = NULL
        WHERE source_index = ? AND status = 'running'
        RETURNING 1
        """,
        [message[:2000], source_index],
    ).fetchall()
    if len(updated) != 1:
        raise BuildStateCorrupt(f"cannot record failure for source {source_index}")


def _write_candidate_artifact(
    connection: duckdb.DuckDBPyConnection,
    source: IndexSource,
    schema: SourceSchema,
    pages_per_domain: int,
    partial_path: Path,
) -> None:
    query = local_candidate_query(schema, pages_per_domain)
    connection.execute(
        f"COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
        [str(partial_path), source.url],
    )


def _fsync_file(path: Path) -> None:
    try:
        with path.open("rb") as artifact:
            os.fsync(artifact.fileno())
    except OSError as error:
        raise CandidateArtifactError(f"fsync candidate artifact {path}: {error}") from error


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _candidate_http_statuses(error: BaseException) -> set[int]:
    message = str(error).lower()
    if "http" not in message and "statuscode" not in message:
        return set()
    return {int(status) for status in _CANDIDATE_HTTP_STATUS.findall(message)}


def _is_transient_candidate_error(error: BaseException) -> bool:
    message = str(error).lower()
    statuses = _candidate_http_statuses(error)
    if statuses:
        return bool(statuses & _TRANSIENT_CANDIDATE_HTTP_STATUSES)
    return any(
        marker in message for marker in _TRANSIENT_CANDIDATE_NETWORK_MARKERS
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
