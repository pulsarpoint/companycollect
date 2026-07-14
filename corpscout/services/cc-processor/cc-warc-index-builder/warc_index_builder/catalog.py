"""Build local candidates and one pruned WARC-oriented DuckDB catalog."""

import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import duckdb

from .manifests import IndexSource, WarcObject, WarcSize
from .selection import SELECTION_VERSION, candidate_query, global_order_clause


CANDIDATE_SCHEMA = (
    ("selection_version", "USMALLINT"),
    ("source_index", "UINTEGER"),
    ("root_domain", "VARCHAR"),
    ("url", "VARCHAR"),
    ("content_languages", "VARCHAR"),
    ("warc_filename", "VARCHAR"),
    ("warc_record_offset", "UBIGINT"),
    ("warc_record_length", "UBIGINT"),
    ("rank_main_site", "UTINYINT"),
    ("rank_homepage", "UTINYINT"),
    ("rank_priority_path", "UTINYINT"),
    ("rank_path_depth", "UBIGINT"),
    ("rank_path_length", "UBIGINT"),
    ("rank_apex", "UTINYINT"),
)


class CandidateBuildError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CandidateResult:
    source_index: int
    path: Path
    rows: int
    byte_count: int
    elapsed_seconds: float
    attempts: int
    reused: bool


@dataclass(frozen=True, slots=True)
class CatalogResult:
    path: Path
    warc_count: int
    selected_warc_count: int
    selected_page_count: int
    selected_bytes: int
    estimated_average_warc_bytes: float
    reused: bool


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def parquet_source(path_or_url: str | Path) -> str:
    return f"read_parquet({sql_string(path_or_url)})"


def complete_parquet(path: Path) -> bool:
    if path.is_symlink() or not path.is_file() or path.stat().st_size < 12:
        return False
    with path.open("rb") as source:
        if source.read(4) != b"PAR1":
            return False
        source.seek(-4, os.SEEK_END)
        return source.read(4) == b"PAR1"


def open_duckdb(
    path: Path | None,
    temp_directory: Path,
    *,
    threads: int | None,
    memory_limit: str | None,
) -> duckdb.DuckDBPyConnection:
    temp_directory.mkdir(parents=True, exist_ok=True)
    config = {
        "temp_directory": str(temp_directory),
        "preserve_insertion_order": "false",
    }
    if threads is not None:
        config["threads"] = str(threads)
    if memory_limit is not None:
        config["memory_limit"] = memory_limit
    return duckdb.connect(":memory:" if path is None else str(path), config=config)


def inspect_source_columns(
    connection: duckdb.DuckDBPyConnection,
    source: IndexSource,
) -> set[str]:
    rows = connection.execute(
        f"DESCRIBE SELECT * FROM {parquet_source(source.url)}"
    ).fetchall()
    columns = {str(row[0]) for row in rows}
    required = {
        "url",
        "url_host_name",
        "url_host_registered_domain",
        "url_path",
        "fetch_status",
        "content_mime_type",
        "warc_filename",
        "warc_record_offset",
        "warc_record_length",
    }
    if missing := sorted(required - columns):
        raise ValueError(
            f"URL-index source {source.path} is missing columns: {', '.join(missing)}"
        )
    return columns


def _candidate_rows(
    connection: duckdb.DuckDBPyConnection,
    path: Path,
    source_index: int,
) -> int:
    described = connection.execute(
        f"DESCRIBE SELECT * FROM {parquet_source(path)}"
    ).fetchall()
    schema = tuple((str(row[0]), str(row[1]).upper()) for row in described)
    if schema != CANDIDATE_SCHEMA:
        raise ValueError(f"candidate has an incompatible schema: {path}")
    rows, invalid_rows = connection.execute(
        f"""
        SELECT count(*), count(*) FILTER (
            WHERE selection_version IS DISTINCT FROM {SELECTION_VERSION}
               OR source_index IS DISTINCT FROM {source_index}
        )
        FROM {parquet_source(path)}
        """
    ).fetchone()
    if invalid_rows:
        raise ValueError(
            f"candidate contains a different selection_version or source_index: {path}"
        )
    return int(rows)


def build_candidate(
    connection: duckdb.DuckDBPyConnection,
    source: IndexSource,
    output_path: Path,
    *,
    pages_per_domain: int,
    attempts: int,
) -> CandidateResult:
    if attempts < 1:
        raise ValueError("attempts must be positive")
    if complete_parquet(output_path):
        try:
            rows = _candidate_rows(connection, output_path, source.source_index)
            return CandidateResult(
                source.source_index,
                output_path,
                rows,
                output_path.stat().st_size,
                0.0,
                0,
                True,
            )
        except duckdb.Error, ValueError:
            output_path.unlink()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial = output_path.with_name(f"{output_path.name}.partial")
    last_error: Exception | None = None
    started = time.monotonic()
    for attempt in range(1, attempts + 1):
        partial.unlink(missing_ok=True)
        try:
            columns = inspect_source_columns(connection, source)
            candidate = candidate_query(
                parquet_source(source.url),
                source.source_index,
                pages_per_domain,
                "content_mime_detected" in columns,
                "content_languages" in columns,
            )
            query = (
                f"SELECT CAST({SELECTION_VERSION} AS USMALLINT) "
                f"AS selection_version, candidate.* FROM ({candidate}) candidate"
            )
            connection.execute(
                f"COPY ({query}) TO {sql_string(partial)} "
                "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
            )
            if not complete_parquet(partial):
                raise OSError("candidate output is not a complete Parquet file")
            rows = _candidate_rows(connection, partial, source.source_index)
            os.replace(partial, output_path)
            return CandidateResult(
                source.source_index,
                output_path,
                rows,
                output_path.stat().st_size,
                time.monotonic() - started,
                attempt,
                False,
            )
        except ValueError:
            partial.unlink(missing_ok=True)
            raise
        except (duckdb.Error, OSError) as error:
            last_error = error
            partial.unlink(missing_ok=True)
            if attempt < attempts:
                time.sleep(min(2 ** (attempt - 1), 30))
        except BaseException:
            partial.unlink(missing_ok=True)
            raise
    raise CandidateBuildError(
        f"query URL-index source {source.source_index} failed after "
        f"{attempts} attempts: {last_error}"
    ) from last_error


def _candidate_list(paths: Sequence[Path]) -> str:
    if not paths:
        raise ValueError("candidate paths must not be empty")
    return "[" + ",".join(sql_string(path) for path in paths) + "]"


def read_catalog(
    path: Path,
    *,
    expected_crawl: str | None = None,
    expected_pages_per_domain: int | None = None,
) -> CatalogResult | None:
    if path.is_symlink() or not path.is_file():
        return None
    try:
        connection = duckdb.connect(str(path), read_only=True)
    except duckdb.Error:
        return None
    try:
        row = connection.execute(
            """
            SELECT crawl_id, pages_per_domain, selection_version,
                   warc_count, selected_warc_count, selected_page_count,
                   selected_bytes, estimated_average_warc_bytes,
                   (SELECT count(*) FROM warcs),
                   (SELECT count(*) FROM pages),
                   (SELECT count(*) FROM warc_stats)
            FROM metadata
            """
        ).fetchone()
    except duckdb.Error:
        return None
    finally:
        connection.close()
    if row is None:
        return None
    crawl, pages_per_domain, version = str(row[0]), int(row[1]), int(row[2])
    warc_count, selected_warc_count, selected_page_count, selected_bytes = (
        int(value) for value in row[3:7]
    )
    if (
        version != SELECTION_VERSION
        or (expected_crawl is not None and crawl != expected_crawl)
        or (
            expected_pages_per_domain is not None
            and pages_per_domain != expected_pages_per_domain
        )
        or int(row[8]) != warc_count
        or int(row[9]) != selected_page_count
        or int(row[10]) != warc_count
    ):
        return None
    return CatalogResult(
        path,
        warc_count,
        selected_warc_count,
        selected_page_count,
        selected_bytes,
        float(row[7]),
        True,
    )


def build_catalog(
    catalog_path: Path,
    candidate_paths: Sequence[Path],
    warcs: Sequence[WarcObject],
    warc_sizes: Sequence[WarcSize],
    *,
    crawl: str,
    pages_per_domain: int,
    index_manifest_sha256: str,
    warc_manifest_sha256: str,
    temp_directory: Path,
    threads: int | None,
    memory_limit: str | None,
) -> CatalogResult:
    if not warcs:
        raise ValueError("WARC inventory must not be empty")
    if not warc_sizes:
        raise ValueError("WARC size sample must not be empty")

    catalog_path.parent.mkdir(parents=True, exist_ok=True)
    partial = catalog_path.with_name(f"{catalog_path.name}.partial")
    partial.unlink(missing_ok=True)
    Path(f"{partial}.wal").unlink(missing_ok=True)
    average_warc_bytes = sum(size.object_bytes for size in warc_sizes) / len(warc_sizes)
    candidates = _candidate_list(candidate_paths)
    order = global_order_clause(pages_per_domain)

    connection = open_duckdb(
        partial,
        temp_directory,
        threads=threads,
        memory_limit=memory_limit,
    )
    active_error: BaseException | None = None
    try:
        connection.execute(
            """
            CREATE TABLE warcs (
                warc_index UINTEGER PRIMARY KEY,
                warc_filename VARCHAR NOT NULL UNIQUE
            );
            CREATE TABLE warc_size_sample (
                warc_index UINTEGER PRIMARY KEY,
                object_bytes UBIGINT NOT NULL
            );
            """
        )
        connection.executemany(
            "INSERT INTO warcs VALUES (?, ?)",
            [(warc.warc_index, warc.warc_filename) for warc in warcs],
        )
        connection.executemany(
            "INSERT INTO warc_size_sample VALUES (?, ?)",
            [(size.warc_index, size.object_bytes) for size in warc_sizes],
        )
        connection.execute(
            f"""
            CREATE TABLE pages AS
            WITH deduplicated AS (
                SELECT *
                FROM read_parquet({candidates}, union_by_name=true)
                QUALIFY row_number() OVER (
                    PARTITION BY warc_filename, warc_record_offset,
                                 warc_record_length
                    ORDER BY root_domain ASC NULLS LAST, {order},
                             source_index ASC NULLS LAST,
                             content_languages ASC NULLS LAST
                ) = 1
            ),
            ranked AS (
                SELECT *, row_number() OVER (
                    PARTITION BY root_domain ORDER BY {order}
                ) AS domain_page_rank
                FROM deduplicated
            )
            SELECT warcs.warc_index, ranked.root_domain, ranked.url,
                   CAST(ranked.domain_page_rank AS USMALLINT) AS domain_page_rank,
                   ranked.content_languages, ranked.warc_record_offset,
                   ranked.warc_record_length
            FROM ranked
            LEFT JOIN warcs USING (warc_filename)
            WHERE ranked.domain_page_rank <= {pages_per_domain}
            ORDER BY warcs.warc_index, ranked.warc_record_offset;

            CREATE TABLE warc_stats AS
            SELECT warcs.warc_index, warcs.warc_filename,
                   count(pages.warc_index)::UBIGINT AS selected_pages,
                   coalesce(sum(pages.warc_record_length), 0)::HUGEINT
                       AS selected_bytes,
                   {average_warc_bytes}::DOUBLE AS estimated_warc_bytes,
                   100.0 * coalesce(sum(pages.warc_record_length), 0)
                       / {average_warc_bytes}::DOUBLE
                       AS estimated_utilization_percent
            FROM warcs
            LEFT JOIN pages USING (warc_index)
            GROUP BY warcs.warc_index, warcs.warc_filename
            ORDER BY selected_bytes DESC, warcs.warc_index;
            """
        )
        warc_count, selected_warc_count, selected_page_count, selected_bytes = (
            connection.execute(
                """
                SELECT (SELECT count(*) FROM warcs),
                       (SELECT count(*) FROM warc_stats WHERE selected_pages > 0),
                       (SELECT count(*) FROM pages),
                       (SELECT coalesce(sum(warc_record_length), 0) FROM pages)
                """
            ).fetchone()
        )
        invalid_domains = int(
            connection.execute(
                f"""
                SELECT count(*) FROM (
                    SELECT root_domain, min(domain_page_rank) AS first_rank,
                           max(domain_page_rank) AS last_rank, count(*) AS pages
                    FROM pages GROUP BY root_domain
                    HAVING first_rank <> 1 OR last_rank <> pages
                       OR last_rank > {pages_per_domain}
                )
                """
            ).fetchone()[0]
        )
        if invalid_domains:
            raise ValueError(
                f"catalog contains {invalid_domains} invalid domain rankings"
            )
        missing_warcs = int(
            connection.execute(
                "SELECT count(*) FROM pages WHERE warc_index IS NULL"
            ).fetchone()[0]
        )
        if missing_warcs:
            raise ValueError(
                f"catalog contains {missing_warcs} pages absent from the WARC manifest"
            )
        if int(warc_count) != len(warcs):
            raise ValueError("catalog WARC count differs from manifest")

        connection.execute(
            """
            CREATE TABLE metadata (
                singleton BOOLEAN PRIMARY KEY CHECK (singleton),
                crawl_id VARCHAR NOT NULL,
                pages_per_domain USMALLINT NOT NULL,
                selection_version USMALLINT NOT NULL,
                index_manifest_sha256 VARCHAR NOT NULL,
                warc_manifest_sha256 VARCHAR NOT NULL,
                index_shard_count UINTEGER NOT NULL,
                warc_count UINTEGER NOT NULL,
                selected_warc_count UINTEGER NOT NULL,
                selected_page_count UBIGINT NOT NULL,
                selected_bytes HUGEINT NOT NULL,
                warc_sample_count UINTEGER NOT NULL,
                estimated_average_warc_bytes DOUBLE NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO metadata VALUES (
                true, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp::TIMESTAMP
            )
            """,
            [
                crawl,
                pages_per_domain,
                SELECTION_VERSION,
                index_manifest_sha256,
                warc_manifest_sha256,
                len(candidate_paths),
                int(warc_count),
                int(selected_warc_count),
                int(selected_page_count),
                int(selected_bytes),
                len(warc_sizes),
                average_warc_bytes,
            ],
        )
        connection.execute("FORCE CHECKPOINT")
    except BaseException as error:
        active_error = error
        raise
    finally:
        try:
            connection.close()
        except Exception as close_error:
            if active_error is None:
                raise
            active_error.add_note(f"also failed to close DuckDB: {close_error}")

    if Path(f"{partial}.wal").exists():
        raise RuntimeError(f"catalog WAL remains after checkpoint: {partial}.wal")
    os.replace(partial, catalog_path)
    shutil.rmtree(temp_directory, ignore_errors=True)
    return CatalogResult(
        catalog_path,
        int(warc_count),
        int(selected_warc_count),
        int(selected_page_count),
        int(selected_bytes),
        average_warc_bytes,
        False,
    )
