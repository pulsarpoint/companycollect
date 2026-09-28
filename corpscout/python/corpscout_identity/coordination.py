"""Coordinate identity registration with final inventory replacement."""

import os
from contextlib import closing, contextmanager

import psycopg2

# All publishers must use the same PostgreSQL database and this key. A transaction
# lock is safe through transaction-pooling PgBouncer; a session lock is not.
INVENTORY_LOCK_KEY = (1129533523, 1)

DOMAIN_COLUMNS = (
    "root_domain",
    "sources",
    "first_seen_at",
    "last_seen_at",
    "source_run_id",
)
PAGE_COLUMNS = (
    "root_domain",
    "website_origin",
    "page_url",
    "sources",
    "first_seen_at",
    "last_seen_at",
    "last_observed_at",
    "last_successful_fetch_at",
    "source_run_id",
)
WEBSITE_COLUMNS = tuple(column for column in PAGE_COLUMNS if column != "page_url")


@contextmanager
def inventory_publication_lock(*, processing_url: str | None = None):
    """Fail closed if coordination is unavailable; never fall back to an unlocked write.

    Open a dedicated transaction, never a task's existing state transaction.
    Build inventory staging outside this guard. Register parents or catch up and swap
    inside it. PROCESSING_PG_URL must identify the same DB in every service.
    """
    url = processing_url or os.environ.get("PROCESSING_PG_URL")
    if not url:
        raise ValueError("PROCESSING_PG_URL is required for inventory publication")
    timeout_ms = int(os.environ.get("INVENTORY_LOCK_TIMEOUT_MS", "30000"))
    if not 1 <= timeout_ms <= 300000:
        raise ValueError("INVENTORY_LOCK_TIMEOUT_MS must be between 1 and 300000")
    with closing(
        psycopg2.connect(
            url, connect_timeout=10, application_name="identity_publication"
        )
    ) as connection:
        with connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT set_config('lock_timeout', %s, true)", (f"{timeout_ms}ms",)
            )
            cursor.execute("SELECT pg_advisory_xact_lock(%s, %s)", INVENTORY_LOCK_KEY)
            yield
