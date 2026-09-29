"""Bounded cleanup of delivered SQLite payloads and expired status events."""

import shutil
from pathlib import Path

from crawler_service.company_lookup_store import LookupStore
from crawler_service.crawl_history import CrawlHistory


def prune_sqlite(root: Path, before: str) -> dict[str, int]:
    # Each connection is opened and closed in the maintenance worker thread.
    # Pending delivery and restart receipts are protected by the store queries.
    lookups = LookupStore(root / "company-lookups.sqlite3")
    try:
        publications = lookups.prune(before, limit=1000)
    finally:
        lookups.close()
    history = CrawlHistory(root / "crawl-history.sqlite3")
    try:
        events = history.prune(
            before, lookup_path=root / "company-lookups.sqlite3", limit=1000
        )
    finally:
        history.close()
    return {
        "publications": publications["publications"],
        "events": events["events"],
        "reusable_bytes": publications["reusable_bytes"] + events["reusable_bytes"],
        "free_bytes": shutil.disk_usage(root).free,
    }
