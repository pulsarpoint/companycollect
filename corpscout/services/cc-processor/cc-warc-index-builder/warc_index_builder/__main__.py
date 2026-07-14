"""Command-line entry point for the WARC-oriented index builder."""

import argparse
import fcntl
import hashlib
import os
import shutil
import sys
from pathlib import Path

import httpx

from .catalog import (
    CandidateBuildError,
    build_candidate,
    build_catalog,
    open_duckdb,
    read_catalog,
)
from .events import binary_size, emit_event
from .manifests import (
    read_index_sources,
    read_warc_inventory,
    sample_warc_sizes,
    sync_manifests,
)
from .publication import destination_from_environment, publish_catalog
from .selection import SELECTION_VERSION


WARC_SIZE_SAMPLE_COUNT = 256
WARC_HEAD_CONCURRENCY = 32


def positive_integer(value: str) -> int:
    try:
        number = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if number < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return number


def parse_options(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="cc-warc-index-builder",
        description=(
            "Query Common Crawl URL-index Parquets per shard and build one "
            "pruned WARC-oriented DuckDB catalog."
        ),
    )
    parser.add_argument("--crawl", required=True)
    parser.add_argument(
        "--base",
        type=Path,
        default=Path(os.environ.get("OUT_BASE_DIR", "data")),
    )
    parser.add_argument("--pages-per-domain", type=positive_integer, default=25)
    parser.add_argument("--attempts", type=positive_integer, default=5)
    parser.add_argument("--threads", type=positive_integer)
    parser.add_argument("--memory-limit")
    parser.add_argument("--rebuild-catalog", action="store_true")
    parser.add_argument("--cleanup-candidates", action="store_true")
    options = parser.parse_args(argv)
    if options.pages_per_domain > 0xFFFF:
        parser.error("--pages-per-domain must not exceed 65535")
    options.base = options.base.expanduser().resolve()
    return options


def file_sha256(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def run(options: argparse.Namespace) -> int:
    selection_directory = (
        options.base / options.crawl / "warc-index" / f"pages{options.pages_per_domain}"
    )
    selection_directory.mkdir(parents=True, exist_ok=True)
    lock_path = selection_directory / ".build.lock"
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise RuntimeError(
                f"another index build is active for {options.crawl} "
                f"with {options.pages_per_domain} pages per domain"
            ) from error
        return _run_locked(options)


def _run_locked(options: argparse.Namespace) -> int:
    crawl_directory = options.base / options.crawl / "warc-index"
    manifest_directory = crawl_directory / "manifests"
    selection = f"pages{options.pages_per_domain}"
    selection_directory = crawl_directory / selection
    candidate_root = selection_directory / "candidates"
    temp_directory = selection_directory / "temp"
    catalog_path = selection_directory / "catalog.duckdb"
    catalog_destination = destination_from_environment(os.environ)

    emit_event(
        "WARC index build started",
        crawl=options.crawl,
        pages_per_domain=options.pages_per_domain,
        output=str(catalog_path),
    )
    if (
        not options.rebuild_catalog
        and (
            existing := read_catalog(
                catalog_path,
                expected_crawl=options.crawl,
                expected_pages_per_domain=options.pages_per_domain,
            )
        )
        is not None
    ):
        emit_event(
            "catalog ready",
            crawl=options.crawl,
            pages_per_domain=options.pages_per_domain,
            catalog=str(existing.path),
            warc_count=existing.warc_count,
            selected_warc_count=existing.selected_warc_count,
            selected_page_count=existing.selected_page_count,
            selected_bytes=existing.selected_bytes,
            selected_size=binary_size(existing.selected_bytes),
            estimated_average_warc_bytes=existing.estimated_average_warc_bytes,
            estimated_average_warc_size=binary_size(
                round(existing.estimated_average_warc_bytes)
            ),
            reused=True,
        )
        publication = publish_catalog(
            catalog_destination,
            crawl=options.crawl,
            selection=selection,
            catalog_path=existing.path,
        )
        emit_event(
            "RustFS catalog ready",
            crawl=options.crawl,
            selection=selection,
            bucket=publication.bucket,
            ready_key=publication.ready_key,
            catalog_key=publication.catalog.key,
            catalog_bytes=publication.catalog.size_bytes,
            catalog_size=binary_size(publication.catalog.size_bytes),
            catalog_sha256=publication.catalog.sha256,
        )
        if options.cleanup_candidates and candidate_root.exists():
            shutil.rmtree(candidate_root)
        return 0
    if not options.rebuild_catalog and (
        catalog_path.exists() or catalog_path.is_symlink()
    ):
        raise RuntimeError(
            f"catalog exists but failed identity or health validation: {catalog_path}; "
            "use --rebuild-catalog to replace it"
        )

    with httpx.Client(
        timeout=httpx.Timeout(60.0, connect=30.0),
        headers={"User-Agent": "cc-warc-index-builder/0.1"},
    ) as client:
        manifests = sync_manifests(
            client,
            options.crawl,
            manifest_directory,
            attempts=options.attempts,
        )
        sources = read_index_sources(manifests.index_path, options.crawl)
        warcs = read_warc_inventory(manifests.warc_path, options.crawl)
        index_manifest_sha256 = file_sha256(manifests.index_path)
        warc_manifest_sha256 = file_sha256(manifests.warc_path)
        candidate_directory = (
            candidate_root / f"v{SELECTION_VERSION}-{index_manifest_sha256}"
        )
        candidate_directory.mkdir(parents=True, exist_ok=True)
        emit_event(
            "source manifests ready",
            crawl=options.crawl,
            index_sources=len(sources),
            warc_objects=len(warcs),
        )

        shutil.rmtree(temp_directory, ignore_errors=True)
        connection = open_duckdb(
            None,
            temp_directory / "candidates",
            threads=options.threads,
            memory_limit=options.memory_limit,
        )
        try:
            ready = []
            failures = []
            for source in sources:
                output_path = (
                    candidate_directory / f"part-{source.source_index:05d}.parquet"
                )
                try:
                    result = build_candidate(
                        connection,
                        source,
                        output_path,
                        pages_per_domain=options.pages_per_domain,
                        attempts=options.attempts,
                    )
                    ready.append(result)
                    emit_event(
                        "candidate shard ready",
                        crawl=options.crawl,
                        source_index=source.source_index,
                        sources_ready=len(ready),
                        sources_total=len(sources),
                        candidate_rows=result.rows,
                        candidate_bytes=result.byte_count,
                        candidate_size=binary_size(result.byte_count),
                        elapsed_seconds=result.elapsed_seconds,
                        rows_per_second=(
                            None
                            if result.reused or result.elapsed_seconds <= 0
                            else result.rows / result.elapsed_seconds
                        ),
                        attempts=result.attempts,
                        reused=result.reused,
                    )
                except CandidateBuildError as error:
                    failures.append((source, str(error)))
                    emit_event(
                        "candidate shard failed",
                        level="WARN",
                        crawl=options.crawl,
                        source_index=source.source_index,
                        source_path=source.path,
                        attempts=options.attempts,
                        error=str(error),
                    )
        finally:
            connection.close()
            shutil.rmtree(temp_directory, ignore_errors=True)

        if failures:
            raise RuntimeError(
                f"{len(failures)} of {len(sources)} remote candidate queries failed; "
                "rerun to retry only the missing shards"
            )

        sample = sample_warc_sizes(
            client,
            options.crawl,
            warcs,
            crawl_directory / f"warc-size-sample-{WARC_SIZE_SAMPLE_COUNT}.json",
            count=WARC_SIZE_SAMPLE_COUNT,
            workers=WARC_HEAD_CONCURRENCY,
            attempts=options.attempts,
        )
        emit_event(
            "WARC size sample ready",
            crawl=options.crawl,
            sampled_warcs=len(sample.sizes),
            sampled_average_warc_bytes=sample.average_bytes,
            sampled_average_warc_size=binary_size(round(sample.average_bytes)),
            reused=sample.reused,
        )

    result = build_catalog(
        catalog_path,
        [candidate.path for candidate in ready],
        warcs,
        sample.sizes,
        crawl=options.crawl,
        pages_per_domain=options.pages_per_domain,
        index_manifest_sha256=index_manifest_sha256,
        warc_manifest_sha256=warc_manifest_sha256,
        temp_directory=temp_directory / "catalog",
        threads=options.threads,
        memory_limit=options.memory_limit,
    )
    emit_event(
        "catalog ready",
        crawl=options.crawl,
        pages_per_domain=options.pages_per_domain,
        catalog=str(result.path),
        warc_count=result.warc_count,
        selected_warc_count=result.selected_warc_count,
        selected_page_count=result.selected_page_count,
        selected_bytes=result.selected_bytes,
        selected_size=binary_size(result.selected_bytes),
        estimated_average_warc_bytes=result.estimated_average_warc_bytes,
        estimated_average_warc_size=binary_size(
            round(result.estimated_average_warc_bytes)
        ),
        reused=result.reused,
    )
    publication = publish_catalog(
        catalog_destination,
        crawl=options.crawl,
        selection=selection,
        catalog_path=result.path,
    )
    emit_event(
        "RustFS catalog ready",
        crawl=options.crawl,
        selection=selection,
        bucket=publication.bucket,
        ready_key=publication.ready_key,
        catalog_key=publication.catalog.key,
        catalog_bytes=publication.catalog.size_bytes,
        catalog_size=binary_size(publication.catalog.size_bytes),
        catalog_sha256=publication.catalog.sha256,
    )
    if options.cleanup_candidates:
        shutil.rmtree(candidate_root)
        emit_event(
            "candidate shards removed",
            crawl=options.crawl,
            path=str(candidate_root),
        )
    return 0


def main(argv: list[str] | None = None) -> int:
    options = parse_options(argv)
    try:
        return run(options)
    except Exception as error:
        emit_event(
            "WARC index build failed",
            level="ERROR",
            stream=sys.stderr,
            crawl=options.crawl,
            error_type=type(error).__name__,
            error=str(error),
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
