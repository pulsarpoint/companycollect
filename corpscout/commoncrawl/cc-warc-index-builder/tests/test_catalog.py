import io
import json
import os
import subprocess
import sys
import threading
from contextlib import contextmanager
from dataclasses import replace
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import duckdb
import httpx
import pytest

import warc_index_builder.__main__ as command
import warc_index_builder.catalog as catalog
from warc_index_builder.__main__ import (
    WarcSizeBuildError,
    build_candidate_shards,
    build_warc_sizes,
)
from warc_index_builder.catalog import (
    CATALOG_SCHEMA_VERSION,
    BuildIdentity,
    BuildStateConflict,
    BuildStateCorrupt,
    CandidateArtifactError,
    CandidateBuildError,
    WarcSizeCheckpointError,
    SourceShardSeed,
    build_candidate_shard,
    candidate_artifact_path,
    catalog_id,
    initialize_build_state,
    inspect_candidate_artifact,
    local_candidate_query,
    checkpoint_warc_size_batch,
    prepare_build_directory,
    prepare_warc_size_resume,
    require_path_within,
    source_shard_seeds,
    verify_or_finalize_warc_inventory,
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
from warc_index_builder.object_sizes import (
    PermanentWarcSizeError,
    ProbeMetrics,
    TransientWarcSizeError,
    WarcSizeFailure,
    WarcSizeSuccess,
)


_IDENTITY_HASHES = {
    "selection_policy_sha256": "00" * 32,
    "source_schema_sha256": "11" * 32,
    "warc_manifest_sha256": "22" * 32,
    "index_manifest_sha256": "33" * 32,
    "warc_inventory_sha256": "44" * 32,
}


@contextmanager
def _parquet_http_server(payload: bytes, failures: int):
    state = {"failures_remaining": failures, "failures_sent": 0, "requests": 0}

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_HEAD(self) -> None:
            self._serve(send_body=False)

        def do_GET(self) -> None:
            self._serve(send_body=True)

        def _serve(self, *, send_body: bool) -> None:
            state["requests"] += 1
            if state["failures_remaining"] > 0:
                state["failures_remaining"] -= 1
                state["failures_sent"] += 1
                self.send_response(503)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            start = 0
            end = len(payload) - 1
            range_header = self.headers.get("Range")
            status = 200
            if range_header:
                start_text, end_text = range_header.removeprefix("bytes=").split("-", 1)
                if start_text:
                    start = int(start_text)
                    end = int(end_text) if end_text else end
                else:
                    start = max(0, len(payload) - int(end_text))
                end = min(end, len(payload) - 1)
                status = 206
            body = payload[start : end + 1]
            self.send_response(status)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(body)))
            if status == 206:
                self.send_header(
                    "Content-Range", f"bytes {start}-{end}/{len(payload)}"
                )
            self.end_headers()
            if send_body:
                self.wfile.write(body)

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/source.parquet", state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


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


def _warc_size_success(
    warc: WarcObject,
    object_bytes: int,
    *,
    attempts: int = 1,
    metrics: ProbeMetrics | None = None,
) -> WarcSizeSuccess:
    return WarcSizeSuccess(
        warc=warc,
        object_bytes=object_bytes,
        attempts=attempts,
        retries=attempts - 1,
        metrics=metrics or ProbeMetrics(head_requests=attempts),
    )


def _warc_size_failure(
    warc: WarcObject,
    *,
    attempts: int = 1,
    permanent: bool = True,
    metrics: ProbeMetrics | None = None,
) -> WarcSizeFailure:
    outcome_metrics = metrics or ProbeMetrics(head_requests=attempts)
    error_type = PermanentWarcSizeError if permanent else TransientWarcSizeError
    arguments = {
        "warc": warc,
        "method": "HEAD",
        "status_code": 404 if permanent else 503,
        "metrics": outcome_metrics,
    }
    error = (
        error_type("missing WARC", **arguments)
        if permanent
        else error_type(
            "throttled WARC",
            **arguments,
            retry_after_seconds=None,
        )
    )
    return WarcSizeFailure(
        warc=warc,
        attempts=attempts,
        retries=attempts - 1,
        metrics=outcome_metrics,
        error=error,
        permanent=permanent,
    )


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
    *,
    source_index: int = 7,
) -> tuple[duckdb.DuckDBPyConnection, Path, SourceSchema]:
    tmp_path.mkdir(parents=True, exist_ok=True)
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
    if rows:
        connection.executemany(
            "INSERT INTO source_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    path = tmp_path / "source.parquet"
    connection.execute("COPY source_rows TO ? (FORMAT PARQUET)", [str(path)])
    schema = inspect_source_schema(
        connection,
        IndexSource(source_index, str(path), str(path)),
    )
    return connection, path, schema


def _initialize_candidate_state(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    schema: SourceSchema,
) -> IndexSource:
    source = IndexSource(schema.source_index, str(path), str(path))
    initialize_build_state(
        connection,
        _build_identity(),
        _warc_seeds(),
        source_shard_seeds((source,), (schema,)),
    )
    return source


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


def test_candidate_artifact_path_is_fixed_and_contained(tmp_path: Path) -> None:
    build_directory = tmp_path / ".build"

    assert candidate_artifact_path(build_directory, 7) == (
        build_directory / "candidates/source_00007.parquet"
    )
    with pytest.raises(ValueError):
        candidate_artifact_path(build_directory, -1)


def test_candidate_build_requires_distinct_state_and_query_connections(
    tmp_path: Path,
) -> None:
    connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    source = _initialize_candidate_state(connection, source_path, schema)
    try:
        with pytest.raises(ValueError, match="connections must be distinct"):
            build_candidate_shard(
                connection,
                connection,
                tmp_path / "build",
                source,
                schema,
                25,
                1,
            )
    finally:
        connection.close()


@pytest.mark.parametrize("rows", [[], [_candidate_page("example.com", "https://example.com/", "/", 10)]])
def test_candidate_build_writes_valid_atomic_artifact_and_ready_state(
    tmp_path: Path,
    rows: list[tuple[object, ...]],
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        rows,
        source_index=0,
    )
    state_connection = duckdb.connect()
    source = _initialize_candidate_state(
        state_connection,
        source_path,
        schema,
    )
    build_directory = tmp_path / "build"
    try:
        result = build_candidate_shard(
            state_connection,
            query_connection,
            build_directory,
            source,
            schema,
            25,
            3,
        )

        assert result.reused is False
        assert result.rows == len(rows)
        assert result.attempts_this_run == 1
        assert result.attempts_total == 1
        assert result.path.is_file()
        assert result.path.with_name(f"{result.path.name}.partial").exists() is False
        assert inspect_candidate_artifact(
            query_connection,
            result.path,
            source.source_index,
        ) == catalog.CandidateMetadata(result.rows, result.byte_count)
        assert state_connection.execute(
            """
            SELECT status, candidate_rows, candidate_bytes, attempts,
                   last_error, completed_at IS NOT NULL
            FROM source_shards WHERE source_index = 0
            """
        ).fetchone() == (
            "ready",
            len(rows),
            result.byte_count,
            1,
            None,
            True,
        )
        if rows:
            with pytest.raises(CandidateArtifactError, match="different source_index"):
                inspect_candidate_artifact(query_connection, result.path, 1)
    finally:
        state_connection.close()
        query_connection.close()


def test_ready_candidate_is_reused_and_stale_partial_is_removed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    state_connection = duckdb.connect()
    source = _initialize_candidate_state(state_connection, source_path, schema)
    build_directory = tmp_path / "build"
    try:
        first = build_candidate_shard(
            state_connection,
            query_connection,
            build_directory,
            source,
            schema,
            25,
            3,
        )
        partial_path = first.path.with_name(f"{first.path.name}.partial")
        partial_path.write_bytes(b"stale")

        def reject_copy(*_args, **_kwargs) -> None:
            raise AssertionError("ready candidate must not access its source")

        monkeypatch.setattr(catalog, "_write_candidate_artifact", reject_copy)
        reused = build_candidate_shard(
            state_connection,
            query_connection,
            build_directory,
            source,
            schema,
            25,
            3,
        )

        assert reused.reused is True
        assert reused.attempts_this_run == 0
        assert reused.attempts_total == 1
        assert partial_path.exists() is False
        assert state_connection.execute(
            "SELECT attempts FROM source_shards WHERE source_index = 0"
        ).fetchone() == (1,)
    finally:
        state_connection.close()
        query_connection.close()


@pytest.mark.parametrize("corruption", ["file", "row_count"])
def test_invalid_ready_candidate_rebuilds_only_that_artifact(
    tmp_path: Path,
    corruption: str,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    state_connection = duckdb.connect()
    source = _initialize_candidate_state(state_connection, source_path, schema)
    build_directory = tmp_path / "build"
    try:
        first = build_candidate_shard(
            state_connection,
            query_connection,
            build_directory,
            source,
            schema,
            25,
            2,
        )
        if corruption == "file":
            first.path.write_bytes(b"truncated")
        else:
            state_connection.execute(
                "UPDATE source_shards SET candidate_rows = candidate_rows + 1 "
                "WHERE source_index = 0"
            )

        rebuilt = build_candidate_shard(
            state_connection,
            query_connection,
            build_directory,
            source,
            schema,
            25,
            2,
        )

        assert rebuilt.reused is False
        assert rebuilt.attempts_this_run == 1
        assert rebuilt.attempts_total == 2
        assert inspect_candidate_artifact(query_connection, rebuilt.path, 0).rows == 1
    finally:
        state_connection.close()
        query_connection.close()


def test_transient_candidate_http_failure_retries_then_succeeds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    state_connection = duckdb.connect()
    source = _initialize_candidate_state(state_connection, source_path, schema)
    original_write = catalog._write_candidate_artifact
    calls = 0
    delays: list[float] = []

    def transient_then_write(*args, **kwargs) -> None:
        nonlocal calls
        calls += 1
        if calls <= 2:
            raise duckdb.IOException("HTTP 503 Service Unavailable")
        original_write(*args, **kwargs)

    monkeypatch.setattr(catalog, "_write_candidate_artifact", transient_then_write)
    monkeypatch.setattr(catalog.time, "sleep", delays.append)
    try:
        result = build_candidate_shard(
            state_connection,
            query_connection,
            tmp_path / "build",
            source,
            schema,
            25,
            3,
        )

        assert result.attempts_this_run == 3
        assert result.attempts_total == 3
        assert result.retries == 2
        assert result.http_503 == 2
        assert delays == [1.0, 2.0]
        assert state_connection.execute(
            "SELECT status, attempts, last_error FROM source_shards"
        ).fetchone() == ("ready", 3, None)
    finally:
        state_connection.close()
        query_connection.close()


def test_candidate_duckdb_http_503_then_success_uses_outer_retry_and_copy_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    query_connection.execute("LOAD httpfs")
    query_connection.execute("SET http_retries = 0")
    state_connection = duckdb.connect()
    delays: list[float] = []
    monkeypatch.setattr(catalog.time, "sleep", delays.append)
    with _parquet_http_server(source_path.read_bytes(), failures=1) as (url, server_state):
        source = IndexSource(0, "fixture", url)
        initialize_build_state(
            state_connection,
            _build_identity(),
            _warc_seeds(),
            source_shard_seeds((source,), (schema,)),
        )
        try:
            result = build_candidate_shard(
                state_connection,
                query_connection,
                tmp_path / "build",
                source,
                schema,
                25,
                2,
            )

            assert result.rows == 1
            assert result.attempts_this_run == 2
            assert result.retries == 1
            assert result.http_503 == 1
            assert delays == [1.0]
            assert server_state["failures_sent"] == 1
        finally:
            state_connection.close()
            query_connection.close()


def test_candidate_duckdb_http_503_exhaustion_preserves_pending_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    query_connection.execute("LOAD httpfs")
    query_connection.execute("SET http_retries = 0")
    state_connection = duckdb.connect()
    monkeypatch.setattr(catalog.time, "sleep", lambda _delay: None)
    with _parquet_http_server(source_path.read_bytes(), failures=10) as (url, server_state):
        source = IndexSource(0, "fixture", url)
        initialize_build_state(
            state_connection,
            _build_identity(),
            _warc_seeds(),
            source_shard_seeds((source,), (schema,)),
        )
        try:
            with pytest.raises(CandidateBuildError, match="source 0.*503"):
                build_candidate_shard(
                    state_connection,
                    query_connection,
                    tmp_path / "build",
                    source,
                    schema,
                    25,
                    2,
                )

            assert server_state["failures_sent"] == 2
            status, attempts, error = state_connection.execute(
                "SELECT status, attempts, last_error FROM source_shards"
            ).fetchone()
            assert (status, attempts) == ("pending", 2)
            assert "503" in error
        finally:
            state_connection.close()
            query_connection.close()


def test_candidate_orphan_final_after_interrupted_ready_commit_is_rebuilt(
    tmp_path: Path,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    state_connection = duckdb.connect()
    source = _initialize_candidate_state(state_connection, source_path, schema)
    build_directory = tmp_path / "build"
    try:
        first = build_candidate_shard(
            state_connection,
            query_connection,
            build_directory,
            source,
            schema,
            25,
            2,
        )
        state_connection.execute(
            """
            UPDATE source_shards
            SET status = 'running', candidate_rows = NULL, candidate_bytes = NULL,
                completed_at = NULL
            WHERE source_index = 0
            """
        )
        reopened = initialize_build_state(
            state_connection,
            _build_identity(),
            _warc_seeds(),
            source_shard_seeds((source,), (schema,)),
        )

        assert reopened.recovered_source_shards == 1
        assert first.path.is_file()
        rebuilt = build_candidate_shard(
            state_connection,
            query_connection,
            build_directory,
            source,
            schema,
            25,
            2,
        )
        assert rebuilt.reused is False
        assert rebuilt.attempts_total == 2
    finally:
        state_connection.close()
        query_connection.close()


def test_permanent_candidate_http_failure_does_not_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    state_connection = duckdb.connect()
    source = _initialize_candidate_state(state_connection, source_path, schema)
    delays: list[float] = []

    def fail_permanently(*_args, **_kwargs) -> None:
        raise duckdb.IOException("HTTP 404 Not Found")

    monkeypatch.setattr(catalog, "_write_candidate_artifact", fail_permanently)
    monkeypatch.setattr(catalog.time, "sleep", delays.append)
    build_directory = tmp_path / "build"
    try:
        with pytest.raises(CandidateBuildError, match="source 0.*404"):
            build_candidate_shard(
                state_connection,
                query_connection,
                build_directory,
                source,
                schema,
                25,
                3,
            )

        assert delays == []
        assert state_connection.execute(
            "SELECT status, attempts, last_error FROM source_shards"
        ).fetchone() == ("pending", 1, "HTTP 404 Not Found")
        assert candidate_artifact_path(build_directory, 0).exists() is False
    finally:
        state_connection.close()
        query_connection.close()


def test_candidate_http_404_stays_permanent_even_with_network_words() -> None:
    error = duckdb.IOException("HTTP 404 Not Found: connection closed")

    assert catalog._is_transient_candidate_error(error) is False


def test_exhausted_transient_candidate_failure_returns_source_to_pending(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    state_connection = duckdb.connect()
    source = _initialize_candidate_state(state_connection, source_path, schema)
    delays: list[float] = []

    def stay_throttled(*_args, **_kwargs) -> None:
        raise duckdb.HTTPException("HTTP 503 Service Unavailable")

    monkeypatch.setattr(catalog, "_write_candidate_artifact", stay_throttled)
    monkeypatch.setattr(catalog.time, "sleep", delays.append)
    try:
        with pytest.raises(CandidateBuildError, match="source 0.*503"):
            build_candidate_shard(
                state_connection,
                query_connection,
                tmp_path / "build",
                source,
                schema,
                25,
                2,
            )

        assert delays == [1.0]
        assert state_connection.execute(
            "SELECT status, attempts, last_error FROM source_shards"
        ).fetchone() == ("pending", 2, "HTTP 503 Service Unavailable")
    finally:
        state_connection.close()
        query_connection.close()


def test_candidate_coordinator_emits_one_event_for_build_and_reuse(
    tmp_path: Path,
) -> None:
    query_connection, source_path, schema = _candidate_fixture(
        tmp_path / "source",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    state_connection = duckdb.connect()
    source = _initialize_candidate_state(state_connection, source_path, schema)
    build_directory = tmp_path / "build"
    try:
        built_stream = io.StringIO()
        reused_stream = io.StringIO()
        built = build_candidate_shards(
            state_connection,
            query_connection,
            build_directory,
            (source,),
            (schema,),
            crawl_id="CC-MAIN-2026-25",
            pages_per_domain=25,
            http_attempts=2,
            stream=built_stream,
        )
        reused = build_candidate_shards(
            state_connection,
            query_connection,
            build_directory,
            (source,),
            (schema,),
            crawl_id="CC-MAIN-2026-25",
            pages_per_domain=25,
            http_attempts=2,
            stream=reused_stream,
        )

        built_event = json.loads(built_stream.getvalue())
        reused_event = json.loads(reused_stream.getvalue())
        assert len(built) == len(reused) == 1
        assert built_event["msg"] == "candidate shard ready"
        assert built_event["sources_completed"] == 1
        assert built_event["sources_total"] == 1
        assert built_event["candidate_rows"] == 1
        assert built_event["candidate_size"].endswith("KiB")
        assert built_event["attempts"] == 1
        assert built_event["attempts_total"] == 1
        assert built_event["reused"] is False
        assert reused_event["attempts"] == 0
        assert reused_event["attempts_total"] == 1
        assert reused_event["rows_per_second"] is None
        assert reused_event["reused"] is True
    finally:
        state_connection.close()
        query_connection.close()


def test_later_candidate_failure_preserves_earlier_ready_source_and_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    query_connection, first_path, first_schema = _candidate_fixture(
        tmp_path / "first",
        [_candidate_page("example.com", "https://example.com/", "/", 10)],
        source_index=0,
    )
    second_directory = tmp_path / "second"
    second_directory.mkdir()
    second_path = second_directory / "source.parquet"
    query_connection.execute(
        "COPY source_rows TO ? (FORMAT PARQUET)", [str(second_path)]
    )
    second_schema = inspect_source_schema(
        query_connection,
        IndexSource(1, str(second_path), str(second_path)),
    )
    sources = (
        IndexSource(0, str(first_path), str(first_path)),
        IndexSource(1, str(second_path), str(second_path)),
    )
    schemas = (first_schema, second_schema)
    state_connection = duckdb.connect()
    initialize_build_state(
        state_connection,
        _build_identity(),
        _warc_seeds(),
        source_shard_seeds(sources, schemas),
    )
    original_write = catalog._write_candidate_artifact

    def fail_second(*args, **kwargs) -> None:
        source = args[1]
        if source.source_index == 1:
            raise duckdb.IOException("HTTP 404 Not Found")
        original_write(*args, **kwargs)

    monkeypatch.setattr(catalog, "_write_candidate_artifact", fail_second)
    stream = io.StringIO()
    build_directory = tmp_path / "build"
    try:
        with pytest.raises(CandidateBuildError, match="source 1"):
            build_candidate_shards(
                state_connection,
                query_connection,
                build_directory,
                sources,
                schemas,
                crawl_id="CC-MAIN-2026-25",
                pages_per_domain=25,
                http_attempts=1,
                stream=stream,
            )

        assert state_connection.execute(
            "SELECT source_index, status FROM source_shards ORDER BY source_index"
        ).fetchall() == [(0, "ready"), (1, "pending")]
        assert candidate_artifact_path(build_directory, 0).is_file()
        assert candidate_artifact_path(build_directory, 1).exists() is False
        events = [json.loads(line) for line in stream.getvalue().splitlines()]
        assert [event["source_index"] for event in events] == [0]
    finally:
        state_connection.close()
        query_connection.close()


def test_candidate_builder_does_not_depend_on_pyarrow() -> None:
    pyproject = Path(__file__).parents[1] / "pyproject.toml"

    assert "pyarrow" not in pyproject.read_text().lower()


def test_warc_size_resume_submits_only_null_rows_and_preserves_attempts() -> None:
    connection = duckdb.connect()
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            _warc_seeds(),
            _source_seeds(),
        )
        connection.execute(
            """
            UPDATE warc_inventory
            SET object_bytes = 4096, attempts = 3
            WHERE warc_index = 0
            """
        )
        connection.execute(
            """
            UPDATE warc_inventory
            SET attempts = 2, last_error = 'retry me'
            WHERE warc_index = 1
            """
        )

        resume = prepare_warc_size_resume(connection)

        assert resume.total_objects == 2
        assert resume.reused_objects == 1
        assert resume.reused_bytes == 4096
        assert resume.attempts_total == 5
        assert resume.pending == (_warc_seeds()[1],)
    finally:
        connection.close()


def test_warc_size_resume_rejects_hash_with_pending_rows() -> None:
    connection = duckdb.connect()
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            _warc_seeds(),
            _source_seeds(),
        )
        connection.execute(
            "UPDATE build_identity SET warc_inventory_sha256 = ?",
            ["aa" * 32],
        )

        with pytest.raises(BuildStateCorrupt, match="hash exists.*missing"):
            prepare_warc_size_resume(connection)
    finally:
        connection.close()


def test_warc_size_checkpoint_rejects_hash_with_pending_rows_before_update() -> None:
    connection = duckdb.connect()
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            _warc_seeds(),
            _source_seeds(),
        )
        connection.execute(
            "UPDATE build_identity SET warc_inventory_sha256 = ?",
            ["aa" * 32],
        )

        with pytest.raises(BuildStateCorrupt, match="hash exists.*missing"):
            checkpoint_warc_size_batch(
                connection,
                (_warc_size_success(_warc_seeds()[0], 1000),),
            )

        assert connection.execute(
            "SELECT object_bytes, attempts FROM warc_inventory ORDER BY warc_index"
        ).fetchall() == [(None, 0), (None, 0)]
    finally:
        connection.close()


def test_warc_size_checkpoint_persists_mixed_outcomes_and_exact_metrics() -> None:
    connection = duckdb.connect()
    warcs = _warc_seeds()
    success = _warc_size_success(
        warcs[0],
        8192,
        attempts=2,
        metrics=ProbeMetrics(
            head_requests=2,
            range_requests=1,
            http_503=1,
        ),
    )
    failure = _warc_size_failure(
        warcs[1],
        attempts=3,
        permanent=False,
        metrics=ProbeMetrics(
            head_requests=3,
            range_requests=1,
            http_429=1,
            http_503=1,
        ),
    )
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            warcs,
            _source_seeds(),
        )
        connection.execute(
            "UPDATE warc_inventory SET last_error = 'old error' WHERE warc_index = 0"
        )

        result = checkpoint_warc_size_batch(connection, (failure, success))

        assert result.objects == 2
        assert result.successes == 1
        assert result.failures == 1
        assert result.object_bytes == 8192
        assert result.attempts == 5
        assert result.retries == 3
        assert result.head_requests == 5
        assert result.range_requests == 2
        assert result.http_requests == 7
        assert result.http_429 == 1
        assert result.http_503 == 2
        assert result.objects_completed == 1
        assert result.objects_pending == 1
        assert result.known_bytes == 8192
        assert result.attempts_total == 5
        assert result.inventory_sha256 is None
        assert connection.execute(
            """
            SELECT warc_index, object_bytes, attempts, last_error
            FROM warc_inventory ORDER BY warc_index
            """
        ).fetchall() == [
            (0, 8192, 2, None),
            (1, None, 3, "throttled WARC"),
        ]
        assert connection.execute(
            "SELECT warc_inventory_sha256 FROM build_identity"
        ).fetchone() == (None,)
    finally:
        connection.close()


def test_warc_size_checkpoint_stale_result_rolls_back_whole_batch() -> None:
    connection = duckdb.connect()
    warcs = _warc_seeds()
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            warcs,
            _source_seeds(),
        )
        connection.execute(
            "UPDATE warc_inventory SET object_bytes = 100, attempts = 1 WHERE warc_index = 0"
        )

        with pytest.raises(WarcSizeCheckpointError, match="stale"):
            checkpoint_warc_size_batch(
                connection,
                (
                    _warc_size_success(warcs[0], 200),
                    _warc_size_success(warcs[1], 300),
                ),
            )

        assert connection.execute(
            "SELECT object_bytes, attempts FROM warc_inventory ORDER BY warc_index"
        ).fetchall() == [(100, 1), (None, 0)]
    finally:
        connection.close()


def test_warc_size_checkpoint_rejects_duplicate_indexes_before_transaction() -> None:
    connection = duckdb.connect()
    warc = _warc_seeds()[0]
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            _warc_seeds(),
            _source_seeds(),
        )

        with pytest.raises(ValueError, match="duplicate index"):
            checkpoint_warc_size_batch(
                connection,
                (
                    _warc_size_success(warc, 100),
                    _warc_size_success(warc, 200),
                ),
            )

        assert connection.execute(
            "SELECT count(*) FROM warc_inventory WHERE object_bytes IS NOT NULL"
        ).fetchone() == (0,)
    finally:
        connection.close()


def test_final_warc_size_batch_stores_ordered_hash_atomically() -> None:
    connection = duckdb.connect()
    warcs = _warc_seeds()
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            warcs,
            _source_seeds(),
        )

        result = checkpoint_warc_size_batch(
            connection,
            (
                _warc_size_success(warcs[1], 2000),
                _warc_size_success(warcs[0], 1000),
            ),
        )

        expected_hash = warc_inventory_sha256(
            (
                (0, warcs[0].warc_filename, 1000),
                (1, warcs[1].warc_filename, 2000),
            )
        )
        assert result.inventory_sha256 == expected_hash
        assert result.objects_pending == 0
        assert connection.execute(
            "SELECT warc_inventory_sha256 FROM build_identity"
        ).fetchone() == (expected_hash,)
        assert verify_or_finalize_warc_inventory(connection) == expected_hash
    finally:
        connection.close()


def test_final_warc_size_hash_failure_rolls_back_size_updates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = duckdb.connect()
    warcs = _warc_seeds()
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            warcs,
            _source_seeds(),
        )

        def fail_hash(_inventory) -> str:
            raise RuntimeError("simulated hash failure")

        monkeypatch.setattr(catalog, "warc_inventory_sha256", fail_hash)
        with pytest.raises(RuntimeError, match="simulated hash failure"):
            checkpoint_warc_size_batch(
                connection,
                (
                    _warc_size_success(warcs[0], 1000),
                    _warc_size_success(warcs[1], 2000),
                ),
            )

        assert connection.execute(
            "SELECT object_bytes, attempts FROM warc_inventory ORDER BY warc_index"
        ).fetchall() == [(None, 0), (None, 0)]
        assert connection.execute(
            "SELECT warc_inventory_sha256 FROM build_identity"
        ).fetchone() == (None,)
    finally:
        connection.close()


def test_warc_size_checkpoint_survives_reopen_and_resumes_only_missing(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.duckdb"
    warcs = _warc_seeds()
    connection = duckdb.connect(str(state_path))
    initialize_build_state(
        connection,
        _build_identity(),
        warcs,
        _source_seeds(),
    )
    checkpoint_warc_size_batch(
        connection,
        (_warc_size_success(warcs[0], 1000),),
    )
    connection.close()

    connection = duckdb.connect(str(state_path))
    try:
        resume = prepare_warc_size_resume(connection)
        assert resume.reused_objects == 1
        assert resume.pending == (warcs[1],)
        assert connection.execute(
            "SELECT object_bytes, attempts FROM warc_inventory ORDER BY warc_index"
        ).fetchall() == [(1000, 1), (None, 0)]
    finally:
        connection.close()


def test_warc_size_checkpoint_survives_process_exit_without_connection_close(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "state.duckdb"
    warcs = _warc_seeds()
    connection = duckdb.connect(str(state_path))
    initialize_build_state(
        connection,
        _build_identity(),
        warcs,
        _source_seeds(),
    )
    connection.close()

    script = """
import os
import sys

import duckdb

from warc_index_builder.catalog import checkpoint_warc_size_batch
from warc_index_builder.manifests import WarcObject
from warc_index_builder.object_sizes import ProbeMetrics, WarcSizeSuccess

connection = duckdb.connect(sys.argv[1])
warc = WarcObject(0, sys.argv[2])
checkpoint_warc_size_batch(
    connection,
    (
        WarcSizeSuccess(
            warc=warc,
            object_bytes=1000,
            attempts=1,
            retries=0,
            metrics=ProbeMetrics(head_requests=1),
        ),
    ),
)
os._exit(0)
"""
    subprocess.run(
        [sys.executable, "-c", script, str(state_path), warcs[0].warc_filename],
        check=True,
        cwd=Path(__file__).parents[1],
    )

    connection = duckdb.connect(str(state_path))
    try:
        resume = prepare_warc_size_resume(connection)
        assert resume.reused_objects == 1
        assert resume.pending == (warcs[1],)
        assert connection.execute(
            "SELECT object_bytes, attempts FROM warc_inventory ORDER BY warc_index"
        ).fetchall() == [(1000, 1), (None, 0)]
    finally:
        connection.close()


def test_warc_size_build_with_no_pending_rows_performs_no_http_and_sets_hash() -> None:
    connection = duckdb.connect()
    warcs = _warc_seeds()
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            warcs,
            _source_seeds(),
        )
        connection.execute(
            """
            UPDATE warc_inventory
            SET object_bytes = CASE warc_index WHEN 0 THEN 1000 ELSE 2000 END
            """
        )

        def reject_http(_request: httpx.Request) -> httpx.Response:
            raise AssertionError("completed WARC sizes must not be probed")

        with httpx.Client(transport=httpx.MockTransport(reject_http)) as client:
            result = build_warc_sizes(
                connection,
                client,
                crawl_id="CC-MAIN-2026-25",
                selection_name="pages25",
                concurrency=2,
                http_attempts=2,
            )

        assert result.reused_objects == 2
        assert result.sized_objects == 0
        assert result.batches == 0
        assert connection.execute(
            "SELECT warc_inventory_sha256 IS NOT NULL FROM build_identity"
        ).fetchone() == (True,)
    finally:
        connection.close()


def test_warc_size_build_emits_only_one_event_per_committed_batch() -> None:
    connection = duckdb.connect()
    stream = io.StringIO()
    warcs = _warc_seeds()

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.method == "HEAD"
        size = "1000" if request.url.path.endswith("a.warc.gz") else "2000"
        return httpx.Response(200, headers={"Content-Length": size})

    try:
        initialize_build_state(
            connection,
            _build_identity(),
            warcs,
            _source_seeds(),
        )
        with httpx.Client(transport=httpx.MockTransport(respond)) as client:
            result = build_warc_sizes(
                connection,
                client,
                crawl_id="CC-MAIN-2026-25",
                selection_name="pages25",
                concurrency=2,
                http_attempts=2,
                checkpoint_batch_size=1,
                stream=stream,
            )

        events = [json.loads(line) for line in stream.getvalue().splitlines()]
        assert result.sized_objects == 2
        assert result.batches == 2
        assert result.attempts == 2
        assert result.http_requests == 2
        assert len(events) == 2
        assert [event["msg"] for event in events] == [
            "WARC size batch ready",
            "WARC size batch ready",
        ]
        assert [event["batch_objects"] for event in events] == [1, 1]
        assert [event["objects_completed"] for event in events] == [1, 2]
        assert events[-1]["final"] is True
        assert events[-1]["http_attempts"] == 1
        assert events[-1]["object_size"].endswith("KiB")
    finally:
        connection.close()


def test_warc_size_build_checkpoints_drained_success_before_failing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = duckdb.connect()
    stream = io.StringIO()
    warcs = _warc_seeds()

    def outcomes(*_args, **_kwargs):
        yield _warc_size_failure(warcs[0])
        yield _warc_size_success(warcs[1], 2000)

    monkeypatch.setattr(command, "iter_warc_size_outcomes", outcomes)
    try:
        initialize_build_state(
            connection,
            _build_identity(),
            warcs,
            _source_seeds(),
        )
        with httpx.Client(
            transport=httpx.MockTransport(
                lambda _request: (_ for _ in ()).throw(AssertionError("unused"))
            )
        ) as client:
            with pytest.raises(WarcSizeBuildError, match="warc_index=0"):
                build_warc_sizes(
                    connection,
                    client,
                    crawl_id="CC-MAIN-2026-25",
                    selection_name="pages25",
                    concurrency=2,
                    http_attempts=2,
                    stream=stream,
                )

        assert connection.execute(
            "SELECT object_bytes, attempts, last_error FROM warc_inventory ORDER BY warc_index"
        ).fetchall() == [
            (None, 1, "missing WARC"),
            (2000, 1, None),
        ]
        assert connection.execute(
            "SELECT warc_inventory_sha256 FROM build_identity"
        ).fetchone() == (None,)
        event = json.loads(stream.getvalue())
        assert event["level"] == "WARN"
        assert event["successful_objects"] == 1
        assert event["failed_objects"] == 1
    finally:
        connection.close()
