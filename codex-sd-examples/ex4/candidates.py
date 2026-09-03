"""Production-format candidate lists for the lab, built by the ex3 code."""

import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path

from ex1.models import StrictModel
from ex3.candidates import PageCandidate, build_selection_candidates, dedupe_by_url_key
from ex3.crawler import _preferred_languages
from ex3.seeding import fetch_head_metadata, seed_sitemap_urls
from ex3.selection import rank_urls
from ex3.urls import normalize_start_url
from ex4.sites import Site

LOGGER = logging.getLogger(__name__)
HEAD_FETCH_CONCURRENCY = 8


class CandidateSet(StrictModel):
    domain: str
    base_url: str
    preferred_languages: list[str]
    built_at: str
    inventory_urls: int
    eligible_urls: int
    excluded_urls: int
    candidates: list[PageCandidate]


async def build_site_candidates(
    site: Site, *, limit: int, accept_language: str, seed_max_urls: int
) -> CandidateSet:
    """Seed, rank, dedupe, cap and head-check exactly as the ex3 LLM selector does."""
    base_url = normalize_start_url(site.start_url)
    outcome = await seed_sitemap_urls(
        base_url,
        source="sitemap",
        max_urls=seed_max_urls,
        accept_language=accept_language,
        proxy=None,
    )
    preferred = _preferred_languages(site.base_language)
    eligible, excluded = rank_urls(
        [base_url, *outcome.urls], base_url=base_url, preferred_languages=preferred
    )
    shortlist = dedupe_by_url_key(eligible)[:limit]
    heads = await fetch_head_metadata(
        [scored.url for scored in shortlist],
        accept_language=accept_language,
        proxy=None,
        concurrency=HEAD_FETCH_CONCURRENCY,
    )
    candidates = build_selection_candidates(
        shortlist,
        heads=heads,
        base_page_links=[],
        preferred_languages=preferred,
        limit=limit,
    )
    LOGGER.info(
        "%s: %d inventory, %d eligible, %d excluded, %d candidates",
        site.domain,
        len(outcome.urls) + 1,
        len(eligible),
        len(excluded),
        len(candidates),
    )
    return CandidateSet(
        domain=site.domain,
        base_url=base_url,
        preferred_languages=sorted(preferred),
        built_at=datetime.now(UTC).isoformat(timespec="seconds"),
        inventory_urls=len(outcome.urls) + 1,
        eligible_urls=len(eligible),
        excluded_urls=len(excluded),
        candidates=candidates,
    )


def candidates_payload(candidate_set: CandidateSet) -> list[dict[str, object]]:
    """The candidate JSON exactly as the production prompt embeds it."""
    return [
        {
            "url": c.url,
            "title": c.title,
            "language": c.language,
            "anchor_text": c.labels,
            "source": c.source,
        }
        for c in candidate_set.candidates
    ]


def candidates_hash(candidate_set: CandidateSet) -> str:
    payload = json.dumps(
        candidates_payload(candidate_set), sort_keys=True, ensure_ascii=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def load_candidate_set(path: Path) -> CandidateSet:
    return CandidateSet.model_validate_json(path.read_text(encoding="utf-8"))
