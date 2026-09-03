"""Deterministic candidate lists handed to the LLM for page selection."""

import json
import logging
from collections.abc import Collection, Mapping, Sequence
from pathlib import Path
from typing import Literal

from pydantic import Field, ValidationError

from ex1.models import StrictModel
from ex3.models import DiscoveredUrl, ScoredUrl
from ex3.seeding import HeadMetadata
from ex3.selection import apply_head_metadata, rank_urls
from ex3.urls import url_key

LOGGER = logging.getLogger(__name__)


class PageCandidate(StrictModel):
    url: str
    score: float
    reasons: list[str] = Field(default_factory=list)
    title: str | None = None
    language: str | None = None
    labels: list[str] = Field(default_factory=list)
    occurrences: int = Field(default=1, ge=1)
    source: Literal["inventory", "base_page", "discovered"]


def candidate_shortlist(
    *,
    inventory: Sequence[str],
    base_url: str,
    base_page_links: Sequence[str],
    preferred_languages: Collection[str],
    limit: int,
) -> list[ScoredUrl]:
    """Rank base URL, inventory and base-page links; keep the top ``limit``."""
    unique_links = list(dict.fromkeys(base_page_links))
    eligible, _ = rank_urls(
        [base_url, *inventory, *unique_links],
        base_url=base_url,
        linked_from_base=unique_links,
        preferred_languages=preferred_languages,
    )
    return eligible[:limit]


def build_selection_candidates(
    *,
    inventory: Sequence[str],
    base_url: str,
    base_page_links: Sequence[str],
    heads: Mapping[str, HeadMetadata],
    preferred_languages: Collection[str],
    limit: int,
) -> list[PageCandidate]:
    """Pass-one candidates: capped shortlist with head metadata attached."""
    link_keys = {url_key(link) for link in base_page_links}
    candidates: list[PageCandidate] = []
    for scored in candidate_shortlist(
        inventory=inventory,
        base_url=base_url,
        base_page_links=base_page_links,
        preferred_languages=preferred_languages,
        limit=limit,
    ):
        head = heads.get(scored.url)
        refined = (
            apply_head_metadata(
                scored,
                language=head.language,
                title=head.title,
                description=head.description,
                preferred_languages=preferred_languages,
            )
            if head is not None
            else scored
        )
        candidates.append(
            PageCandidate(
                url=refined.url,
                score=refined.score,
                reasons=refined.reasons,
                title=refined.title,
                language=refined.language,
                source="base_page"
                if url_key(refined.url) in link_keys
                else "inventory",
            )
        )
    return _ordered(candidates)[:limit]


def build_followup_candidates(
    *,
    discovered_urls: Sequence[DiscoveredUrl],
    inventory_eligible: Sequence[ScoredUrl],
    processed_urls: Collection[str],
    base_url: str,
    preferred_languages: Collection[str],
    limit: int,
) -> list[PageCandidate]:
    """Pass-two candidates: unprocessed internal links plus inventory leftovers."""
    processed_keys = {url_key(url) for url in processed_urls}
    discovered_by_key = {
        url_key(item.url): item
        for item in discovered_urls
        if item.link_type == "internal" and url_key(item.url) not in processed_keys
    }
    leftovers = [
        scored
        for scored in inventory_eligible
        if url_key(scored.url) not in processed_keys
        and url_key(scored.url) not in discovered_by_key
    ]
    eligible, _ = rank_urls(
        [item.url for item in discovered_by_key.values()],
        base_url=base_url,
        preferred_languages=preferred_languages,
    )
    candidates = [
        PageCandidate(
            url=scored.url,
            score=scored.score,
            reasons=scored.reasons,
            labels=discovered_by_key[url_key(scored.url)].labels[:5],
            occurrences=discovered_by_key[url_key(scored.url)].occurrences,
            source="discovered",
        )
        for scored in eligible
    ]
    candidates.extend(
        PageCandidate(
            url=scored.url,
            score=scored.score,
            reasons=scored.reasons,
            title=scored.title,
            language=scored.language,
            source="inventory",
        )
        for scored in leftovers
    )
    return _ordered(candidates)[:limit]


def load_inventory_eligible(path: Path | None) -> list[ScoredUrl]:
    """Read the eligible URLs recorded in ``url-inventory.json``; [] if absent."""
    if path is None or not path.is_file():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return [ScoredUrl.model_validate(item) for item in payload.get("eligible", [])]
    except (json.JSONDecodeError, ValidationError, AttributeError, OSError) as error:
        LOGGER.warning("Ignoring unreadable inventory %s: %s", path, error)
        return []


def _ordered(candidates: list[PageCandidate]) -> list[PageCandidate]:
    unique: dict[str, PageCandidate] = {}
    for candidate in candidates:
        unique.setdefault(url_key(candidate.url), candidate)
    return sorted(
        unique.values(),
        key=lambda item: (-item.score, -item.occurrences, item.url),
    )
