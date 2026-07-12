"""Command-line entry point for the WARC index builder."""

import argparse
import os
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from .events import emit_event


_CRAWL_ID = re.compile(r"CC-MAIN-[0-9]{4}-[0-9]{2}")


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
        options.catalog_directory.relative_to(base)
    except ValueError:
        parser.error("catalog path escapes --base")
    return options


def run(_options: CommandOptions) -> int:
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
