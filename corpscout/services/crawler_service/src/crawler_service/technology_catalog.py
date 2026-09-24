"""A pinned local catalog for model tool calls and deterministic identity checks."""

import hashlib
import json
import re
import unicodedata
from difflib import SequenceMatcher
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field

from crawler_service.models import (
    ModelTechnologyMatch,
    StrictModel,
    validate_specific_technology_name,
)
from crawler_service.storage import utc_now, write_json


def technology_key(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).casefold().split())


class CatalogEntry(StrictModel):
    technology: str
    description: str = Field(min_length=0)
    website: str = Field(min_length=0)
    category_ids: list[int]
    categories: list[str]
    groups: list[str]


class CatalogAlias(StrictModel):
    alias: str
    alias_key: str
    technology: str


class CatalogSnapshot(StrictModel):
    schema_version: str
    version: str
    synced_at: str
    entries: list[CatalogEntry]
    aliases: list[CatalogAlias]


def snapshot_version(entries: list[dict], aliases: list[dict]) -> str:
    payload = json.dumps(
        {"entries": entries, "aliases": aliases},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


class TechnologyCatalog:
    def __init__(self, snapshot: CatalogSnapshot):
        if snapshot.schema_version != "1.0" or not snapshot.entries:
            raise ValueError(
                "Technology catalog requires a nonempty schema 1.0 snapshot"
            )
        if snapshot.version != snapshot_version(
            [entry.model_dump() for entry in snapshot.entries],
            [alias.model_dump() for alias in snapshot.aliases],
        ):
            raise ValueError("Technology catalog content hash does not match")
        self.snapshot = snapshot
        self.entries = {entry.technology: entry for entry in snapshot.entries}
        if len(self.entries) != len(snapshot.entries):
            raise ValueError("Duplicate canonical technology identities")
        self.normalized: dict[str, list[str]] = {}
        for name in self.entries:
            self.normalized.setdefault(technology_key(name), []).append(name)
        self.aliases: dict[str, str] = {}
        for alias in snapshot.aliases:
            if (
                alias.alias_key != technology_key(alias.alias)
                or alias.technology not in self.entries
                or alias.alias_key in self.normalized
                or alias.alias_key in self.aliases
            ):
                raise ValueError("Invalid, conflicting, or dangling catalog alias")
            self.aliases[alias.alias_key] = alias.technology

    @classmethod
    def read(cls, path: Path) -> "TechnologyCatalog":
        return cls(CatalogSnapshot.model_validate_json(path.read_bytes()))

    def resolve(self, name: str) -> tuple[str | None, str]:
        if name in self.entries:
            return name, "exact"
        key = technology_key(name)
        names = self.normalized.get(key, [])
        if len(names) == 1:
            return names[0], "normalized"
        if len(names) > 1:
            return None, "ambiguous"
        if key in self.aliases:
            return self.aliases[key], "alias"
        return None, "not_found"

    def search(self, query: str, context: str = "") -> dict:
        if not isinstance(query, str) or not query.strip() or len(query) > 200:
            raise ValueError("Technology search requires 1–200 characters")
        key = technology_key(query)
        if not isinstance(context, str) or len(context) > 4000:
            raise ValueError("Search context must be text of at most 4000 characters")
        context_terms = set(re.findall(r"[\w+#.-]{3,}", technology_key(context))) - {
            "the",
            "and",
            "for",
            "with",
            "from",
            "that",
            "this",
            "tool",
            "tools",
            "technology",
            "used",
            "uses",
            "using",
            "company",
        }
        whole_name = re.compile(r"(?<![\w+#.])" + re.escape(key) + r"(?![\w+#])")
        canonical, method = self.resolve(query)
        scores = []
        for name, entry in self.entries.items():
            normalized = technology_key(name)
            description = technology_key(entry.description)
            candidate_terms = set(
                re.findall(
                    r"[\w+#.-]{3,}",
                    " ".join(
                        [normalized, description, *entry.categories, *entry.groups]
                    ).casefold(),
                )
            )
            context_overlap = len(context_terms & candidate_terms)
            if len(key) <= 3 and name != canonical and key != normalized:
                # Short names must not match inside words (ADS in Spreadsheets)
                # or through edit distance. Context narrows genuine acronym hits.
                if not whole_name.search(normalized) and not whole_name.search(
                    description
                ):
                    continue
                if context_terms and context_overlap < min(2, len(context_terms)):
                    continue
            score = (
                1.0
                if name == canonical
                else SequenceMatcher(None, key, normalized).ratio()
            )
            if key == normalized:
                score = 1.0
            elif whole_name.search(normalized) or len(key) > 3 and key in normalized:
                score = max(score, 0.8)
            elif whole_name.search(description) or len(key) > 3 and key in description:
                score = max(score, 0.6)
            if score >= 0.55:
                scores.append((score, context_overlap, name))
        names = [
            name
            for _, _, name in sorted(
                scores, key=lambda row: (-row[0], -row[1], row[2])
            )[:10]
        ]
        return {
            "query": query,
            "context": context,
            "catalog_version": self.snapshot.version,
            "catalog_synced_at": self.snapshot.synced_at,
            "canonical_technology": canonical,
            "match_method": method,
            "candidates": [self.entries[name].model_dump() for name in names],
            "note": "Candidates are suggestions, not proof of identity. Preserve source spelling.",
        }

    def category_options(self) -> dict:
        """Expose category IDs with consistent published labels; omit conflicting pairs."""
        labels: dict[int, set[str]] = {}
        for entry in self.entries.values():
            if len(entry.category_ids) != len(entry.categories):
                continue
            for identifier, label in zip(
                entry.category_ids, entry.categories, strict=True
            ):
                labels.setdefault(identifier, set()).add(label)
        return {
            "catalog_version": self.snapshot.version,
            "categories": [
                {"id": identifier, "name": next(iter(names))}
                for identifier, names in sorted(labels.items())
                if len(names) == 1
            ],
            "ambiguous_category_ids": sorted(
                identifier for identifier, names in labels.items() if len(names) != 1
            ),
            "note": "Published category labels only. If none fits, supply a descriptive category_suggestion rather than inventing an ID.",
        }


def sync_catalog(client, path: Path) -> TechnologyCatalog:
    """Read a completed publication; never replace a good cache with a failed sync.

    The caller supplies a read-only native ClickHouse client. Checking the publish
    ledger and row run IDs detects a catalog publication between these reads.
    """
    for _ in range(3):
        before = client.execute(
            "SELECT source_run_id FROM corpscout.technology_catalog_publish_log "
            "ORDER BY published_at DESC LIMIT 1"
        )
        rows = client.execute(
            "SELECT technology, description, website, category_ids, categories, groups, source_run_id "
            "FROM corpscout.technology_catalog FINAL ORDER BY technology LIMIT 100001"
        )
        alias_rows = client.execute(
            "SELECT alias, alias_key, technology, source_run_id FROM corpscout.technology_aliases "
            "WHERE review_status = 'accepted' ORDER BY alias_key LIMIT 100001"
        )
        after = client.execute(
            "SELECT source_run_id FROM corpscout.technology_catalog_publish_log "
            "ORDER BY published_at DESC LIMIT 1"
        )
        if (
            not before
            or before != after
            or any(row[-1] != before[0][0] for row in [*rows, *alias_rows])
        ):
            continue
        if len(rows) > 100000 or len(alias_rows) > 100000:
            raise ValueError("Catalog exceeded the complete snapshot limit")
        entries = [
            dict(zip(CatalogEntry.model_fields, row[:-1], strict=True)) for row in rows
        ]
        aliases = [
            dict(zip(CatalogAlias.model_fields, row[:-1], strict=True))
            for row in alias_rows
        ]
        snapshot = CatalogSnapshot.model_validate(
            {
                "schema_version": "1.0",
                "synced_at": utc_now(),
                "version": snapshot_version(entries, aliases),
                "entries": entries,
                "aliases": aliases,
            }
        )
        catalog = TechnologyCatalog(snapshot)
        write_json(path, snapshot.model_dump())
        return catalog
    raise ValueError(
        "Technology publication is incomplete or changed during catalog sync"
    )


SEARCH_TECHNOLOGIES_TOOL = {
    "type": "function",
    "function": {
        "name": "search_technologies",
        "description": "Search the complete local technology catalog and accepted aliases. Search every observed technology before matching or proposing it. Batch up to 20 names; search alternative spellings when needed.",
        "parameters": {
            "type": "object",
            "properties": {
                "queries": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "maxItems": 20,
                }
            },
            "context": {
                "type": "string",
                "maxLength": 4000,
                "description": "Source context such as RF circuit simulation, to distinguish short names. Optional.",
            },
            "required": ["queries"],
            "additionalProperties": False,
        },
    },
}

LIST_TECHNOLOGY_CATEGORIES_TOOL = {
    "type": "function",
    "function": {
        "name": "list_technology_categories",
        "description": "List published category IDs and names for new-technology proposals. Use a category_suggestion if none fits.",
        "parameters": {
            "type": "object",
            "properties": {},
            "additionalProperties": False,
        },
    },
}


def validate_technology_match(
    observed_name: str,
    claimed: dict | None,
    catalog: TechnologyCatalog | None,
    searches: list[dict],
) -> dict:
    """Model choices cannot invent catalog identities or bypass successful lookup."""
    validate_specific_technology_name(observed_name)
    result = {
        "status": "proposed",
        "canonical_technology": None,
        "proposed_technology": None,
        "proposal_id": None,
        "catalog_version": catalog.snapshot.version if catalog is not None else None,
        "match_method": "no_accepted_match",
        "reason": "No accepted catalog match",
        "searches": [
            {
                "query": search["query"],
                "context": search.get("context", ""),
                "candidates": [
                    {"technology": entry["technology"]}
                    for entry in search["candidates"]
                ],
            }
            for search in searches
        ],
    }
    if catalog is None:
        raise ValueError("Technology matching requires a local catalog")
    canonical, method = catalog.resolve(observed_name)
    if canonical is not None:
        return result | {
            "status": "matched",
            "canonical_technology": canonical,
            "match_method": method,
            "reason": "Resolved against the pinned catalog",
        }
    queried = {technology_key(search["query"]) for search in searches}
    if claimed is None or technology_key(observed_name) not in queried:
        raise ValueError("A successful search of the observed name is required")
    claimed = ModelTechnologyMatch.model_validate(claimed).model_dump()
    result["reason"] = claimed["reason"]
    if claimed["status"] == "matched":
        candidate_names = {
            entry["technology"] for search in searches for entry in search["candidates"]
        }
        if claimed["canonical_technology"] in candidate_names:
            return result | {
                "status": "matched",
                "canonical_technology": claimed["canonical_technology"],
                "match_method": "llm_candidate",
            }
        raise ValueError(
            "The model selected an identity absent from its search results"
        )
    proposed = dict(claimed["proposed_technology"])
    proposed["name"] = proposed["name"].strip()
    if proposed["website"] is not None:
        proposed["website"] = proposed["website"].strip()
    existing, match = catalog.resolve(proposed["name"])
    if existing is not None:
        raise ValueError("Proposed identity already exists; resolve it first")
    known_categories = {
        identifier
        for entry in catalog.entries.values()
        for identifier in entry.category_ids
    }
    if not set(proposed["category_ids"]).issubset(known_categories):
        raise ValueError("Proposed category IDs are absent from the local catalog")
    website = proposed["website"]
    if website is not None:
        parsed = urlsplit(website)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
        ):
            raise ValueError(
                "Proposed website must be an HTTP(S) URL without credentials"
            )
    identity = json.dumps(
        [technology_key(proposed["name"]), website or ""],
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return result | {
        "status": "proposed",
        "proposed_technology": proposed,
        "proposal_id": hashlib.sha256(identity.encode()).hexdigest(),
        "match_method": "no_accepted_match",
    }
