from __future__ import annotations

from typing import Any

ENTITY_CHANGE_TYPE_SNAPSHOT = "snapshot"
ENTITY_CHANGE_TYPE_NEW = "new"
ENTITY_CHANGE_TYPE_CHANGED = "changed"
ENTITY_CHANGE_TYPE_REMOVED = "removed"

_CHANGE_TYPE_BY_SOURCE = {
    "Ny": ENTITY_CHANGE_TYPE_NEW,
    "Endring": ENTITY_CHANGE_TYPE_CHANGED,
    "Fjernet": ENTITY_CHANGE_TYPE_REMOVED,
    "Slettet": ENTITY_CHANGE_TYPE_REMOVED,
}


def snapshot_entity_record(entity: dict[str, Any]) -> dict[str, Any]:
    return {
        "org_number": _string(entity.get("organisasjonsnummer")),
        "change_type": ENTITY_CHANGE_TYPE_SNAPSHOT,
        "source_change_type": ENTITY_CHANGE_TYPE_SNAPSHOT,
        "updated_at": None,
        "update_id": None,
        "entity_url": _entity_url_from_entity(entity),
        "entity": entity,
        "raw_update": None,
    }


def updated_entity_record(
    update: dict[str, Any],
    *,
    entity: dict[str, Any] | None,
    change_type_override: str | None = None,
) -> dict[str, Any]:
    source_change_type = _string(update.get("endringstype"))
    change_type = change_type_override or _CHANGE_TYPE_BY_SOURCE.get(
        source_change_type,
        source_change_type.lower(),
    )
    return {
        "org_number": _string(update.get("organisasjonsnummer")),
        "change_type": change_type,
        "source_change_type": source_change_type,
        "updated_at": _string(update.get("dato")) or None,
        "update_id": update.get("oppdateringsid"),
        "entity_url": _entity_url_from_update(update) or _entity_url_from_entity(entity or {}),
        "entity": entity,
        "raw_update": update,
    }


def entity_update_requires_hydration(update: dict[str, Any]) -> bool:
    source_change_type = _string(update.get("endringstype"))
    return _CHANGE_TYPE_BY_SOURCE.get(source_change_type, source_change_type) != ENTITY_CHANGE_TYPE_REMOVED


def entity_url_from_update(update: dict[str, Any]) -> str:
    return _entity_url_from_update(update)


def entity_updates_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    embedded = payload.get("_embedded")
    if not isinstance(embedded, dict):
        return []
    updates = embedded.get("oppdaterteEnheter")
    return updates if isinstance(updates, list) else []


def update_payload_has_next_page(payload: dict[str, Any], *, current_page: int) -> bool:
    page = payload.get("page")
    if not isinstance(page, dict):
        return False
    total_pages = page.get("totalPages")
    return isinstance(total_pages, int) and current_page + 1 < total_pages


def _entity_url_from_entity(entity: dict[str, Any]) -> str:
    links = entity.get("_links")
    if not isinstance(links, dict):
        return ""
    self_link = links.get("self")
    return _string(self_link.get("href")) if isinstance(self_link, dict) else ""


def _entity_url_from_update(update: dict[str, Any]) -> str:
    links = update.get("_links")
    if not isinstance(links, dict):
        return ""
    entity_link = links.get("enhet")
    return _string(entity_link.get("href")) if isinstance(entity_link, dict) else ""


def _string(value: Any) -> str:
    return "" if value is None else str(value)
