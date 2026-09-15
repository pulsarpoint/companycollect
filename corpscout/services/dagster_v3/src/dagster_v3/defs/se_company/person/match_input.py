"""Current per-company LLM inputs, maintained after normalized blocks are committed.

All normalization writers and the repair asset share the normalization pool. Reading
the full company here is essential: a page of changed suggestions is only a delta.
"""

import hashlib
import json
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from dagster_v3.defs.se_company.basic_info.extract import SCAN_QUERY_SETTINGS, scope_pages
from dagster_v3.defs.se_company.person import tables
from dagster_v3.defs.se_company.person.batch import NORMALIZED_SELECT_COLUMNS, normalized_row_from_row
from dagster_v3.defs.se_company.person.fold import NormalizedRow
from dagster_v3.defs.se_company.person.candidates import (
    Candidate, build_candidates, in_scope, ordered_candidates, prompt_payload,
)

HASH_VERSION = 1
INPUT_QUERY_SETTINGS = {"max_query_size": 4_194_304, "max_execution_time": 1800}


def json_text(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def input_metadata(candidates: Sequence[Candidate]) -> dict[str, str]:
    ordered = ordered_candidates(candidates)
    return {
        "data_hash": digest(prompt_payload(ordered)),
        "bindings_hash": digest(json_text([(c.id, c.members) for c in ordered])),
        "input_snapshot": json_text({
            "version": HASH_VERSION, "candidates": [asdict(c) for c in ordered],
        }),
    }


def snapshot_candidates(snapshot: str) -> tuple[Candidate, ...]:
    payload = json.loads(snapshot)
    if payload["version"] != HASH_VERSION:
        raise ValueError("Unsupported person input snapshot version; refresh match inputs")
    return tuple(
        Candidate(**{**c, "roles": tuple(tuple(role) for role in c["roles"]),
                     "members": tuple(c["members"])})
        for c in payload["candidates"]
    )


def stale_scope_sql() -> str:
    """Repair missing snapshots and writes interrupted after normalized data landed.

    Includes tombstones and companies that lost every eligible source. A missing
    normalized company uses the epoch, so an old eligible snapshot is cleared too.
    """
    return (
        "SELECT company_id FROM (\n"
        "    SELECT company_id, max(normalized_at) AS normalized_at\n"
        f"    FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL GROUP BY company_id\n"
        ") AS n\n"
        f"LEFT ANTI JOIN {tables.QUALIFIED_MATCH_INPUT_TABLE} AS i FINAL USING (company_id)\n"
        "UNION DISTINCT\n"
        "SELECT i.company_id AS company_id\n"
        f"FROM {tables.QUALIFIED_MATCH_INPUT_TABLE} AS i FINAL\n"
        "LEFT JOIN (\n"
        "    SELECT company_id, max(normalized_at) AS latest, count() AS rows\n"
        f"    FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL GROUP BY company_id\n"
        ") AS n ON n.company_id = i.company_id\n"
        f"WHERE i.hash_version != {HASH_VERSION}\n"
        "    OR i.normalized_at != ifNull(n.latest, toDateTime64(0, 3, 'UTC'))\n"
        "    OR (ifNull(n.rows, 0) = 0 AND i.eligible)"
    )


def normalized_inputs_sql() -> str:
    return (
        f"SELECT {', '.join(NORMALIZED_SELECT_COLUMNS)}, normalized_at\n"
        f"FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s ORDER BY company_id, source, slot"
    )


def current_inputs_sql() -> str:
    return (
        "SELECT company_id, data_hash, bindings_hash, input_snapshot, eligible, hash_version\n"
        f"FROM {tables.QUALIFIED_MATCH_INPUT_TABLE} FINAL\n"
        "WHERE company_id IN %(company_ids)s"
    )


def refresh_inputs(client: Any, company_ids: Sequence[str]) -> int:
    if not company_ids:
        return 0
    # The read-start stamp prevents a delayed read from superseding a later read.
    # Dagster also serializes all normalized writers in the normalization pool.
    computed_at = datetime.now(UTC)
    params = {"company_ids": sorted(set(company_ids))}
    grouped: dict[str, list[NormalizedRow]] = defaultdict(list)
    watermarks: dict[str, datetime] = {}
    for raw in client.execute(normalized_inputs_sql(), params, settings=INPUT_QUERY_SETTINGS):
        normalized = normalized_row_from_row(raw[:-1])
        grouped[normalized.company_id].append(normalized)
        watermarks[normalized.company_id] = max(
            watermarks.get(normalized.company_id, raw[-1]), raw[-1],
        )
    rows = []
    for company_id in params["company_ids"]:
        candidates = build_candidates(grouped[company_id])
        values = {
            "company_id": company_id, **input_metadata(candidates),
            "eligible": in_scope(candidates), "hash_version": HASH_VERSION,
            "normalized_at": watermarks.get(company_id, datetime(1970, 1, 1, tzinfo=UTC)),
            "computed_at": computed_at,
        }
        rows.append(tuple(values[column] for column in tables.MATCH_INPUT_COLUMNS))
    client.execute(
        f"INSERT INTO {tables.QUALIFIED_MATCH_INPUT_TABLE} "
        f"({', '.join(tables.MATCH_INPUT_COLUMNS)}) VALUES", rows,
    )
    return len(rows)


def refresh_all_inputs(client: Any, *, changed_only: bool, page_size: int, log: Any) -> int:
    scope_sql = stale_scope_sql() if changed_only else (
        f"SELECT DISTINCT company_id FROM {tables.QUALIFIED_NORMALIZED_TABLE} FINAL\n"
        f"UNION DISTINCT SELECT company_id FROM {tables.QUALIFIED_MATCH_INPUT_TABLE} FINAL"
    )
    count = 0
    for page in scope_pages(
        client, scope_sql=scope_sql, params={}, page_size=page_size,
        settings=SCAN_QUERY_SETTINGS, prefix=tables.SCRATCH_SCOPE_PREFIX,
    ):
        count += refresh_inputs(client, page)
        log.info("Refreshed person input snapshots: %d companies", count)
    return count
