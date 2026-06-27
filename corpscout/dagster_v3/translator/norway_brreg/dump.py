"""Queue → ClickHouse dump for Norway Brreg translations.

Reads completed results from the DuckDB queue and writes them to
``corpscout.text_translations`` in batched staging-table inserts (reusing
``translator.flush.flush_translations``).  Self-contained — no shared dump core.
"""
from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from translator.flush import flush_translations
from translator.config import SourceConfig
from translator.queue import TranslationQueue


def dump_to_clickhouse(
    queue_duckdb_path: str | Path,
    ch_client: Any,
    config: SourceConfig,
    *,
    provider: str,
    model: str,
    batch_size: int = 50_000,
    heartbeat_fn: Callable[[str], None] | None = None,
) -> int:
    """Write completed queue results to ``corpscout.text_translations``.

    Reads all completed items from the DuckDB queue, chunks them into batches
    of ``batch_size``, and writes each chunk via a ClickHouse staging table
    (``flush_translations`` pattern: CREATE Memory table → INSERT → INSERT INTO
    text_translations → DROP).  Calls ``heartbeat_fn`` after each batch.

    Returns the total number of rows written (empty translations are skipped).
    """
    rows = TranslationQueue(queue_duckdb_path).completed_results_for_flush()
    if not rows:
        return 0

    version = int(time.time())
    written = 0

    for batch_idx, start in enumerate(range(0, len(rows), batch_size)):
        chunk = rows[start : start + batch_size]
        written += flush_translations(
            ch_client,
            config,
            chunk,
            provider=provider,
            model=model,
            version=version,
            run_id=f"dump-batch-{batch_idx}",
        )
        if heartbeat_fn is not None:
            heartbeat_fn(f"dumped batch {batch_idx}, total_written={written}")

    return written
