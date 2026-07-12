"""Command-line entry point for the WARC index builder."""

import argparse
from collections.abc import Sequence


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="cc-warc-index-builder",
        description="Build a WARC-oriented catalog from the Common Crawl Parquet URL Index.",
    )
    parser.parse_args(argv)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
