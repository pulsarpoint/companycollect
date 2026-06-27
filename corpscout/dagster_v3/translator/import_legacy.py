"""One-time CLI: import completed translations from an old DuckDB queue into ClickHouse.

Reuses the same queue / flush / registry / clickhouse helpers as the live translator so the
operator can drain a previous-run DuckDB queue without re-translating anything.

Usage example
-------------
    translator-import-legacy-queue \\
        --duckdb data/norway_brreg_translation_queue.duckdb \\
        --source norway_brreg \\
        --env-file .env

Add --dry-run to see counts without writing.
"""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from translator.clickhouse import clickhouse_client_from_env
from translator.flush import flush_translations
from translator.queue import FlushTranslationRow, TranslationQueue
from translator.registry import get_source_config
from dotenv import load_dotenv


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="translator-import-legacy-queue",
        description=(
            "Import completed translations from an old DuckDB queue file into "
            "corpscout.text_translations, so the new translator never re-translates them."
        ),
    )
    parser.add_argument(
        "--duckdb",
        required=True,
        help="Path to the old queue DuckDB file.",
    )
    parser.add_argument(
        "--source",
        default="norway_brreg",
        help="Registry source slug (default: norway_brreg).",
    )
    parser.add_argument(
        "--provider",
        default="legacy-import",
        help="Provider label stored on imported rows (default: legacy-import).",
    )
    parser.add_argument(
        "--model",
        default="legacy",
        help="Model label stored on imported rows (default: legacy).",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Path to a .env file to load before connecting (default: .env).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50_000,
        help="Rows per ClickHouse flush (bounds the Memory staging table; default: 50000).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and report counts without writing anything to ClickHouse.",
    )
    args = parser.parse_args(argv)

    load_dotenv(args.env_file, override=False)

    duckdb_path = Path(args.duckdb)
    if not duckdb_path.exists():
        print(f"ERROR: DuckDB file not found: {duckdb_path}")
        return 1

    try:
        config = get_source_config(args.source)
    except KeyError:
        print(f"ERROR: Unknown source slug: {args.source!r}")
        return 1

    # The DuckDB queue records each location's source_field as the *_original column name, which is
    # exactly the cache's source_column. Classify by the registry's columns; no remap needed.
    dynamic_columns = {f.original_col for f in config.fields if f.static_map is None}
    static_columns = {f.original_col for f in config.fields if f.static_map is not None}

    queue = TranslationQueue(duckdb_path)
    queue.initialize()
    all_rows = queue.completed_results_for_flush()
    total_in_queue = len(all_rows)

    import_rows: list[FlushTranslationRow] = []
    imported_per_field: dict[str, int] = {}
    skipped_static: dict[str, int] = {}
    skipped_unknown: dict[str, int] = {}

    for row in all_rows:
        col = row.source_column
        if col in dynamic_columns:
            if row.translated_text:  # drop empty translations (flush does this too, but count accurately)
                import_rows.append(row)
                imported_per_field[col] = imported_per_field.get(col, 0) + 1
        elif col in static_columns:
            skipped_static[col] = skipped_static.get(col, 0) + 1
        else:
            skipped_unknown[col] = skipped_unknown.get(col, 0) + 1

    # Print summary.
    print(f"Legacy queue: {duckdb_path}")
    print(f"  Total completed rows in queue : {total_in_queue}")
    print(f"  Source                        : {args.source}")
    print(f"  Dynamic columns               : {sorted(dynamic_columns)}")
    print()

    if imported_per_field:
        print("  Rows to import (dynamic columns):")
        for field in sorted(imported_per_field):
            print(f"    {field}: {imported_per_field[field]}")
    if skipped_static:
        print("  Rows skipped (static field):")
        for field in sorted(skipped_static):
            print(f"    {field}: {skipped_static[field]}")
    if skipped_unknown:
        print("  Rows skipped (unknown field — not in registry):")
        for field in sorted(skipped_unknown):
            print(f"    {field}: {skipped_unknown[field]}")

    total_import = len(import_rows)
    total_skipped = sum(skipped_static.values()) + sum(skipped_unknown.values())
    print()
    print(f"  Total to import : {total_import}")
    print(f"  Total skipped   : {total_skipped}")

    if args.dry_run:
        print()
        print("DRY RUN — no rows written to ClickHouse.")
        return 0

    if not import_rows:
        print()
        print("Nothing to import.")
        return 0

    client = clickhouse_client_from_env()
    version = int(time.time())
    batch_size = max(1, args.batch_size)
    written = 0
    for idx in range(0, len(import_rows), batch_size):
        chunk = import_rows[idx : idx + batch_size]
        written += flush_translations(
            client,
            config,
            chunk,
            provider=args.provider,
            model=args.model,
            version=version,
            run_id=f"legacy-import-{idx // batch_size}",
        )
        print(f"  flushed {written}/{len(import_rows)} ...")
    print()
    print(f"Written {written} rows to ClickHouse (corpscout.text_translations).")
    return 0
