"""Validate curated synonyms against the merged catalog before publication."""

import hashlib
import json
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class TechnologyAlias:
    alias: str
    alias_key: str
    technology: str
    reviewed_by: str
    reviewed_at: date
    review_note: str
    source_references: tuple[str, ...]


def normalize_technology_name(name: str) -> str:
    """Case-insensitive equality without erasing C++/C# or other punctuation."""
    return " ".join(unicodedata.normalize("NFKC", name).casefold().split())


def load_technology_aliases(
    custom_dir: Path, catalog_names: set[str]
) -> tuple[list[TechnologyAlias], str]:
    """A missing/malformed file fails; an explicit empty list is valid.

    Only accepted synonyms belong here. Candidates remain in review artifacts.
    Alias targets are exact catalog identities, never fuzzy or transitive aliases.
    """
    payload = (custom_dir / "technology_aliases.json").read_bytes()
    document = json.loads(payload)
    return parse_technology_aliases(document, catalog_names), hashlib.sha256(
        payload
    ).hexdigest()


def parse_technology_aliases(
    document: object, catalog_names: set[str]
) -> list[TechnologyAlias]:
    if (
        not isinstance(document, dict)
        or set(document) != {"schema_version", "aliases"}
        or document["schema_version"] != "1.0"
        or not isinstance(document["aliases"], list)
    ):
        raise ValueError(
            "technology_aliases.json requires schema_version 1.0 and an aliases list"
        )

    aliases = []
    catalog_keys = {normalize_technology_name(name) for name in catalog_names}
    seen: set[str] = set()
    text_fields = {"alias", "technology", "reviewed_by", "reviewed_at", "review_note"}
    for index, entry in enumerate(document["aliases"]):
        if not isinstance(entry, dict) or set(entry) != text_fields | {
            "source_references"
        }:
            raise ValueError(f"Alias entry {index} has missing or unsupported fields")
        if any(
            not isinstance(entry[field], str) or not entry[field].strip()
            for field in text_fields
        ):
            raise ValueError(f"Alias entry {index} requires nonempty text fields")
        refs = entry["source_references"]
        if (
            not isinstance(refs, list)
            or not refs
            or any(not isinstance(ref, str) or not ref.strip() for ref in refs)
        ):
            raise ValueError(f"Alias entry {index} requires source references")
        for ref in refs:
            parsed = urlsplit(ref)
            if (
                parsed.scheme not in {"https", "http"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
            ):
                raise ValueError(
                    f"Alias entry {index} requires HTTP(S) source URLs without credentials"
                )

        key = normalize_technology_name(entry["alias"])
        if entry["technology"] not in catalog_names:
            raise ValueError(
                f"Alias target is absent from the merged catalog: {entry['technology']!r}"
            )
        if key in catalog_keys:
            raise ValueError(
                f"Alias duplicates or overrides a normalized catalog name: {entry['alias']!r}"
            )
        if key in seen:
            raise ValueError(
                f"Duplicate or conflicting normalized alias: {entry['alias']!r}"
            )
        reviewed_at = date.fromisoformat(entry["reviewed_at"])
        if reviewed_at.isoformat() != entry["reviewed_at"]:
            raise ValueError(f"Alias entry {index} reviewed_at must use YYYY-MM-DD")
        seen.add(key)
        aliases.append(
            TechnologyAlias(
                alias=entry["alias"],
                alias_key=key,
                technology=entry["technology"],
                reviewed_by=entry["reviewed_by"],
                reviewed_at=reviewed_at,
                review_note=entry["review_note"],
                source_references=tuple(refs),
            )
        )
    return sorted(aliases, key=lambda item: item.alias_key)


def build_alias_rows(
    aliases: list[TechnologyAlias],
    *,
    source_version: str,
    source: str = "custom",
    source_run_id: str,
    updated_at: datetime,
) -> list[tuple]:
    """Rows in TECHNOLOGY_ALIASES_COLUMNS order, owned by migration 000388."""
    return [
        (
            item.alias,
            item.alias_key,
            item.technology,
            "case_insensitive",
            "accepted",
            item.reviewed_by,
            item.reviewed_at,
            item.review_note,
            list(item.source_references),
            source,
            source_version,
            source_run_id,
            updated_at,
        )
        for item in aliases
    ]
