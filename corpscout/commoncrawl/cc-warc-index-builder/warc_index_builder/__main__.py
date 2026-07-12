"""Command-line entry point for the WARC index builder."""

import argparse
import os
import re
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

import duckdb
import httpx

from .catalog import (
    CandidateShardResult,
    WarcSizeBatchResult,
    build_candidate_shard,
    catalog_build_lock,
    checkpoint_warc_size_batch,
    prepare_build_directory,
    prepare_warc_size_resume,
    require_path_within,
    verify_or_finalize_warc_inventory,
)
from .events import binary_size, emit_event
from .manifests import IndexSource, SourceSchema
from .object_sizes import (
    WarcSizeFailure,
    WarcSizeOutcome,
    iter_warc_size_outcomes,
)


_CRAWL_ID = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}")
_WARC_SIZE_CHECKPOINT_BATCH = 256


class WarcSizeBuildError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class CommandOptions:
    base: Path
    crawl: str
    pages_per_domain: int
    threads: int | None
    memory_limit: str | None
    temp_dir: Path | None
    warc_size_concurrency: int
    http_attempts: int
    rebuild: bool
    check: bool

    @property
    def selection_name(self) -> str:
        return f"pages{self.pages_per_domain}"

    @property
    def catalog_directory(self) -> Path:
        return (self.base / self.crawl / "catalog" / self.selection_name).resolve()

    @property
    def catalog_path(self) -> Path:
        return self.catalog_directory / "catalog.duckdb"


@dataclass(frozen=True, slots=True)
class WarcSizePhaseResult:
    inventory_sha256: str
    total_objects: int
    reused_objects: int
    sized_objects: int
    batches: int
    attempts: int
    retries: int
    head_requests: int
    range_requests: int
    http_429: int
    http_503: int

    @property
    def http_requests(self) -> int:
        return self.head_requests + self.range_requests


def _crawl_id(value: str) -> str:
    if _CRAWL_ID.fullmatch(value) is None:
        raise argparse.ArgumentTypeError("must match CC-MAIN-YYYY-NN")
    return value


def _positive_integer(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if number <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def _pages_per_domain(value: str) -> int:
    number = _positive_integer(value)
    if number > 65_535:
        raise argparse.ArgumentTypeError("must be between 1 and 65535")
    return number


def _nonempty(value: str) -> str:
    value = value.strip()
    if not value:
        raise argparse.ArgumentTypeError("must not be empty")
    return value


def parse_options(argv: Sequence[str] | None = None) -> CommandOptions:
    default_base = os.environ.get("OUT_BASE_DIR") or "data"
    parser = argparse.ArgumentParser(
        prog="cc-warc-index-builder",
        description="Build a WARC-oriented catalog from the Common Crawl Parquet URL Index.",
    )
    parser.add_argument("--base", type=_nonempty, default=default_base)
    parser.add_argument("--crawl", type=_crawl_id, required=True)
    parser.add_argument("--pages-per-domain", type=_pages_per_domain, default=25)
    parser.add_argument("--threads", type=_positive_integer)
    parser.add_argument("--memory-limit", type=_nonempty)
    parser.add_argument("--temp-dir", type=_nonempty)
    parser.add_argument("--warc-size-concurrency", type=_positive_integer, default=64)
    parser.add_argument("--http-attempts", type=_positive_integer, default=5)
    operation = parser.add_mutually_exclusive_group()
    operation.add_argument("--rebuild", action="store_true")
    operation.add_argument("--check", action="store_true")

    values = parser.parse_args(argv)
    base = Path(values.base).expanduser().resolve()
    temp_dir = Path(values.temp_dir).expanduser().resolve() if values.temp_dir else None
    options = CommandOptions(
        base=base,
        crawl=values.crawl,
        pages_per_domain=values.pages_per_domain,
        threads=values.threads,
        memory_limit=values.memory_limit,
        temp_dir=temp_dir,
        warc_size_concurrency=values.warc_size_concurrency,
        http_attempts=values.http_attempts,
        rebuild=values.rebuild,
        check=values.check,
    )
    try:
        require_path_within(base, options.catalog_directory)
    except ValueError:
        parser.error("catalog path escapes --base")
    return options


def build_candidate_shards(
    state_connection: duckdb.DuckDBPyConnection,
    query_connection: duckdb.DuckDBPyConnection,
    build_directory: Path,
    sources: Sequence[IndexSource],
    schemas: Sequence[SourceSchema],
    *,
    crawl_id: str,
    pages_per_domain: int,
    http_attempts: int,
    stream: TextIO | None = None,
) -> tuple[CandidateShardResult, ...]:
    """Build candidates sequentially and report one completion per source."""
    if len(sources) != len(schemas):
        raise ValueError("index sources and source schemas must have the same length")
    results: list[CandidateShardResult] = []
    sources_total = len(sources)
    for sources_completed, (source, schema) in enumerate(
        zip(sources, schemas), start=1
    ):
        result = build_candidate_shard(
            state_connection,
            query_connection,
            build_directory,
            source,
            schema,
            pages_per_domain,
            http_attempts,
        )
        results.append(result)
        emit_event(
            "candidate shard ready",
            stream=stream,
            crawl=crawl_id,
            selection=f"pages{pages_per_domain}",
            source_index=result.source_index,
            sources_completed=sources_completed,
            sources_total=sources_total,
            candidate_rows=result.rows,
            candidate_bytes=result.byte_count,
            candidate_size=binary_size(result.byte_count),
            elapsed_seconds=result.elapsed_seconds,
            rows_per_second=(
                None
                if result.reused or result.elapsed_seconds <= 0
                else result.rows / result.elapsed_seconds
            ),
            attempts=result.attempts_this_run,
            attempts_total=result.attempts_total,
            retries=result.retries,
            http_429=result.http_429,
            http_503=result.http_503,
            reused=result.reused,
        )
    return tuple(results)


def _emit_warc_size_batch(
    result: WarcSizeBatchResult,
    *,
    crawl_id: str,
    selection_name: str,
    batch_index: int,
    total_objects: int,
    reused_objects: int,
    elapsed_seconds: float,
    stream: TextIO | None,
) -> None:
    emit_event(
        "WARC size batch ready",
        level="WARN" if result.failures else "INFO",
        stream=stream,
        crawl=crawl_id,
        selection=selection_name,
        batch=batch_index,
        batch_objects=result.objects,
        successful_objects=result.successes,
        failed_objects=result.failures,
        objects_completed=result.objects_completed,
        objects_pending=result.objects_pending,
        objects_total=total_objects,
        reused_objects=reused_objects,
        object_bytes=result.object_bytes,
        object_size=binary_size(result.object_bytes),
        known_bytes=result.known_bytes,
        known_size=binary_size(result.known_bytes),
        elapsed_seconds=elapsed_seconds,
        objects_per_second=(
            None if elapsed_seconds <= 0 else result.objects / elapsed_seconds
        ),
        attempts=result.attempts,
        attempts_total=result.attempts_total,
        retries=result.retries,
        http_attempts=result.http_requests,
        head_requests=result.head_requests,
        range_requests=result.range_requests,
        http_429=result.http_429,
        http_503=result.http_503,
        final=result.objects_pending == 0,
    )


def build_warc_sizes(
    state_connection: duckdb.DuckDBPyConnection,
    client: httpx.Client,
    *,
    crawl_id: str,
    selection_name: str,
    concurrency: int,
    http_attempts: int,
    checkpoint_batch_size: int = _WARC_SIZE_CHECKPOINT_BATCH,
    timeout: httpx.Timeout | float = 30.0,
    stream: TextIO | None = None,
) -> WarcSizePhaseResult:
    """Probe only missing sizes and durably commit completion-ordered batches."""
    if checkpoint_batch_size <= 0:
        raise ValueError("checkpoint_batch_size must be positive")
    resume = prepare_warc_size_resume(state_connection)
    if not resume.pending:
        inventory_hash = verify_or_finalize_warc_inventory(state_connection)
        return WarcSizePhaseResult(
            inventory_sha256=inventory_hash,
            total_objects=resume.total_objects,
            reused_objects=resume.reused_objects,
            sized_objects=0,
            batches=0,
            attempts=0,
            retries=0,
            head_requests=0,
            range_requests=0,
            http_429=0,
            http_503=0,
        )

    outcomes = iter_warc_size_outcomes(
        client,
        resume.pending,
        concurrency=concurrency,
        max_attempts=http_attempts,
        timeout=timeout,
    )
    batch: list[WarcSizeOutcome] = []
    failures: list[WarcSizeFailure] = []
    batches: list[WarcSizeBatchResult] = []
    batch_started = time.monotonic()
    try:
        while True:
            try:
                outcome = next(outcomes)
            except StopIteration:
                break
            except BaseException:
                if batch:
                    result = checkpoint_warc_size_batch(state_connection, batch)
                    _emit_warc_size_batch(
                        result,
                        crawl_id=crawl_id,
                        selection_name=selection_name,
                        batch_index=len(batches),
                        total_objects=resume.total_objects,
                        reused_objects=resume.reused_objects,
                        elapsed_seconds=time.monotonic() - batch_started,
                        stream=stream,
                    )
                    batches.append(result)
                    batch = []
                raise

            batch.append(outcome)
            if isinstance(outcome, WarcSizeFailure):
                failures.append(outcome)
            if len(batch) < checkpoint_batch_size:
                continue

            result = checkpoint_warc_size_batch(state_connection, batch)
            _emit_warc_size_batch(
                result,
                crawl_id=crawl_id,
                selection_name=selection_name,
                batch_index=len(batches),
                total_objects=resume.total_objects,
                reused_objects=resume.reused_objects,
                elapsed_seconds=time.monotonic() - batch_started,
                stream=stream,
            )
            batches.append(result)
            batch = []
            batch_started = time.monotonic()
    finally:
        outcomes.close()

    if batch:
        result = checkpoint_warc_size_batch(state_connection, batch)
        _emit_warc_size_batch(
            result,
            crawl_id=crawl_id,
            selection_name=selection_name,
            batch_index=len(batches),
            total_objects=resume.total_objects,
            reused_objects=resume.reused_objects,
            elapsed_seconds=time.monotonic() - batch_started,
            stream=stream,
        )
        batches.append(result)

    if failures:
        first = failures[0]
        raise WarcSizeBuildError(
            f"{len(failures)} WARC size probe(s) failed; first "
            f"warc_index={first.warc.warc_index}: {first.error}"
        ) from first.error

    inventory_hash = verify_or_finalize_warc_inventory(state_connection)
    return WarcSizePhaseResult(
        inventory_sha256=inventory_hash,
        total_objects=resume.total_objects,
        reused_objects=resume.reused_objects,
        sized_objects=sum(batch_result.successes for batch_result in batches),
        batches=len(batches),
        attempts=sum(batch_result.attempts for batch_result in batches),
        retries=sum(batch_result.retries for batch_result in batches),
        head_requests=sum(batch_result.head_requests for batch_result in batches),
        range_requests=sum(batch_result.range_requests for batch_result in batches),
        http_429=sum(batch_result.http_429 for batch_result in batches),
        http_503=sum(batch_result.http_503 for batch_result in batches),
    )


def run(options: CommandOptions) -> int:
    if options.check:
        return 0
    catalog_directory = options.catalog_directory
    require_path_within(options.base, catalog_directory)
    with catalog_build_lock(catalog_directory):
        prepare_build_directory(options.base, catalog_directory, rebuild=options.rebuild)
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    options = parse_options(argv)
    try:
        return run(options)
    except Exception as error:
        emit_event(
            "catalog build failed",
            level="ERROR",
            stream=sys.stderr,
            crawl=options.crawl,
            selection=options.selection_name,
            error_type=type(error).__name__,
            error=str(error),
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
