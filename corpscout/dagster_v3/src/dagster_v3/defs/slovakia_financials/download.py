"""Forward sweep of the RÚZ API into raw S3 statement batches.

Fetch-only: stores statements/entities/reports exactly as returned (including
deleted statements and non-public report stubs); decoding and filtering happen
downstream in metrics.build_metrics_from_batches. A statement whose fetch
fails is counted and skipped — mirroring the previous inline behaviour, later
statements still advance last_id, so failed ids are not retried.
"""

import time
from collections.abc import Callable
from typing import Any

from dagster_v3.defs.slovakia_financials import raw_store, tables
from dagster_v3.defs.slovakia_financials.client import RuzClient


def fetch_statement_bundle(
    client: RuzClient,
    statement_id: int,
    *,
    entity_cache: dict[int, dict[str, Any]],
) -> dict[str, Any]:
    statement = client.statement(statement_id)
    if statement.get("stav") == "ZMAZANÉ":
        return {"statement_id": statement_id, "statement": statement, "entity": None, "reports": []}
    entity = None
    entity_id = statement.get("idUJ")
    if entity_id is not None:
        entity = entity_cache.get(entity_id)
        if entity is None:
            entity = client.entity(entity_id)
            entity_cache[entity_id] = entity
    reports = [client.report(report_id) for report_id in statement.get("idUctovnychVykazov") or []]
    return {"statement_id": statement_id, "statement": statement, "entity": entity, "reports": reports}


def sweep_statements_to_s3(
    *,
    object_store: Any,
    source_run_id: str,
    after_id: int,
    max_statements: int,
    changed_since: str = tables.RUZ_CHANGED_SINCE,
    client: RuzClient | None = None,
    request_delay_seconds: float = 0.05,
    log: Callable[..., object] | None = None,
) -> dict[str, Any]:
    """Fetch up to `max_statements` raw bundles after `after_id`, store one S3 batch."""
    ruz = client or RuzClient()
    statement_ids, _ = ruz.statement_ids(
        changed_since=changed_since, after_id=after_id, max_records=max_statements
    )
    entity_cache: dict[int, dict[str, Any]] = {}
    seen_template_ids: set[int] = set()
    bundles: list[dict[str, Any]] = []
    fetch_failed = 0
    templates_stored = 0
    last_id = after_id
    for index, statement_id in enumerate(statement_ids):
        if index and request_delay_seconds:
            time.sleep(request_delay_seconds)
        try:
            bundle = fetch_statement_bundle(ruz, statement_id, entity_cache=entity_cache)
            templates_stored += _store_missing_templates(
                ruz, object_store, bundle["reports"], seen_template_ids
            )
        except Exception as exc:  # noqa: BLE001 - count, don't hide
            fetch_failed += 1
            if log is not None:
                log("RÚZ statement %s fetch failed: %s", statement_id, exc)
            continue
        last_id = statement_id
        bundles.append(bundle)

    batch_key = None
    if bundles:
        batch_key = raw_store.write_statement_batch(
            object_store,
            after_id=after_id,
            last_id=last_id,
            bundles=bundles,
            manifest={
                "source_run_id": source_run_id,
                "after_id": after_id,
                "last_id": last_id,
                "statement_count": len(bundles),
                "fetch_failed": fetch_failed,
                "source_url": tables.RUZ_BASE_URL,
            },
        )
    counts: dict[str, Any] = {
        "fetched_ids": len(statement_ids),
        "stored_statements": len(bundles),
        "fetch_failed": fetch_failed,
        "templates_stored": templates_stored,
        "last_id": last_id,
        "batch_key": batch_key,
    }
    if log is not None:
        log(
            "Stored Slovak RÚZ raw batch: ids=%s stored=%s failed=%s templates=%s "
            "last_id=%s key=%s",
            len(statement_ids), len(bundles), fetch_failed, templates_stored,
            last_id, batch_key,
        )
    return counts


def _store_missing_templates(
    client: RuzClient,
    object_store: Any,
    reports: list[dict[str, Any]],
    seen_template_ids: set[int],
) -> int:
    stored = 0
    for report in reports:
        if report.get("pristupnostDat") != "Verejné":
            continue
        if "tabulky" not in (report.get("obsah") or {}):
            continue
        template_id = report.get("idSablony")
        if template_id is None or int(template_id) in seen_template_ids:
            continue
        template_id = int(template_id)
        if not raw_store.template_exists(object_store, template_id):
            raw_store.write_template(object_store, template_id, client.template(template_id))
            stored += 1
        seen_template_ids.add(template_id)
    return stored
