"""Shared run tags.

``HEAVY_BULK_RUN_TAGS`` marks jobs that download/process multi-GB source
snapshots. dagster.yaml's ``run_queue.tag_concurrency_limits`` caps how many
such runs execute concurrently, so a synchronized schedule storm can't
saturate ``max_concurrent_runs`` (and the shared Postgres/ClickHouse) with
bulk loads all at once.
"""

# dagster/max_runtime overrides dagster.yaml's run_monitoring
# max_runtime_seconds (6h) per run. Heavy bulk chains legitimately exceed
# 6h of WALL CLOCK when their steps queue on a shared per-source DuckDB
# pool behind another run (2026-07-21: the sweden weekly + yearly
# parallel test interleaved perfectly on the pool, then BOTH runs were
# killed at 6h -- the weekly's exports were still waiting their turn).
HEAVY_BULK_RUN_TAGS = {
    "corpscout/workload": "heavy-bulk",
    "dagster/max_runtime": "86400",
}
