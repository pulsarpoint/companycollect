"""Shared run tags.

``HEAVY_BULK_RUN_TAGS`` marks jobs that download/process multi-GB source
snapshots. dagster.yaml's ``run_queue.tag_concurrency_limits`` caps how many
such runs execute concurrently, so a synchronized schedule storm can't
saturate ``max_concurrent_runs`` (and the shared Postgres/ClickHouse) with
bulk loads all at once.
"""

HEAVY_BULK_RUN_TAGS = {"corpscout/workload": "heavy-bulk"}
