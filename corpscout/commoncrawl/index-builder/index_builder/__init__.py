"""Standalone builder of per-domain worklists from the CommonCrawl columnar URL index.

No dagster dependency — only duckdb + pyarrow. The worklist is the small "what to fetch"
list (one row per domain: WARC file + byte offset/length) consumed by the Go cc-enrich-worker.

CLI: `python -m index_builder --help` (or the `cc-index-builder` console script).
"""

from .worklist import build_worklist, run_worklist, worklist_query

__all__ = ["build_worklist", "run_worklist", "worklist_query"]
