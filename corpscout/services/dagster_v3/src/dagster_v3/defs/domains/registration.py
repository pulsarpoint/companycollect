"""Register domain parents before country association rows."""

import re
from collections.abc import Mapping, Sequence
from typing import Any

from corpscout_identity.coordination import DOMAIN_COLUMNS, inventory_publication_lock

PUBLISH_POOL = "domains_publish"
ROOT_PATTERN = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:[.][a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?)+$")


def register_swedish_domains(client: Any, rows: Sequence[Mapping[str, Any]]) -> None:
    """The shared PostgreSQL lock serializes parent registration and final inventory exchange.

    A failure stops before the company write. Retrying reuses existing parents and
    does not write the derived contribution index.
    """
    if not rows:
        return
    roots: dict[str, dict[str, Any]] = {}
    for row in rows:
        root = row["root_domain"]
        if len(root) > 253 or ROOT_PATTERN.fullmatch(root) is None or re.fullmatch(r"[0-9]+(?:[.][0-9]+){3}", root):
            raise ValueError(f"Invalid canonical domain: {root!r}")
        before = roots.get(root)
        roots[root] = {
            "root_domain": root, "sources": ["se_company_domain"],
            "first_seen_at": min(before["first_seen_at"], row["first_seen_at"]) if before else row["first_seen_at"],
            "last_seen_at": max(before["last_seen_at"], row["last_seen_at"]) if before else row["last_seen_at"],
            "source_run_id": row["source_run_id"],
        }
    with inventory_publication_lock():
        existing = {root for (root,) in client.execute(
            "SELECT root_domain FROM corpscout.domains WHERE root_domain IN %(roots)s",
            {"roots": tuple(roots)},
        )}
        missing = [tuple(row[column] for column in DOMAIN_COLUMNS)
                   for root, row in roots.items() if root not in existing]
        if missing:
            client.execute(f"INSERT INTO corpscout.domains ({','.join(DOMAIN_COLUMNS)}) VALUES", missing,
                           settings={"async_insert": 0})
