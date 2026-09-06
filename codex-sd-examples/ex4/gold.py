"""Gold sets: which pages a good selector must pick, and how picks are matched."""

import re
from collections.abc import Sequence
from pathlib import Path
from urllib.parse import urlsplit

from pydantic import Field

from ex1.models import StrictModel
from ex3.selection import NEGATIVE_CATEGORIES, POSITIVE_CATEGORIES
from ex3.urls import url_key
from ex4.candidates import CandidateSet

GOLD_FIELDS = (
    "home",
    "about",
    "contact",
    "management",
    "careers",
    "products_services",
    "group_structure",
    "legal_identity",
)
FIELD_VOCABULARY: dict[str, frozenset[str]] = {
    "about": POSITIVE_CATEGORIES["about"][1],
    "contact": POSITIVE_CATEGORIES["contact"][1],
    "management": POSITIVE_CATEGORIES["people"][1],
    "careers": POSITIVE_CATEGORIES["careers"][1],
    "products_services": POSITIVE_CATEGORIES["offering"][1],
    "group_structure": frozenset(
        {
            "subsidiaries",
            "subsidiary",
            "group",
            "group-structure",
            "koncern",
            "dotterbolag",
            "our-companies",
            "brands",
        }
    ),
    "legal_identity": frozenset(
        {
            "lei",
            "imprint",
            "impressum",
            "legal-notice",
            "mentions-legales",
            "company-information",
            "organisation-number",
            "about-the-company",
        }
    ),
}
JUNK_VOCABULARY: frozenset[str] = frozenset().union(
    *(words for _, words in NEGATIVE_CATEGORIES.values())
)
TOKEN_PATTERN = re.compile(r"[^a-z0-9]+")


class GoldSet(StrictModel):
    domain: str
    base_url: str
    must_have: dict[str, list[str]]
    junk: list[str] = Field(default_factory=list)
    notes: str = ""


def load_gold(path: Path) -> GoldSet:
    return GoldSet.model_validate_json(path.read_text(encoding="utf-8"))


def draft_gold(candidate_set: CandidateSet, *, per_field: int = 5) -> GoldSet:
    """A first draft from URL slugs and titles; the user corrects it by hand."""
    ranked = sorted(candidate_set.candidates, key=lambda c: -c.score)
    must_have: dict[str, list[str]] = {"home": [candidate_set.base_url]}
    for field in GOLD_FIELDS[1:]:
        vocabulary = FIELD_VOCABULARY[field]
        must_have[field] = [
            c.url for c in ranked if _terms(c.url, c.title) & vocabulary
        ][:per_field]
    junk = [c.url for c in ranked if _terms(c.url, c.title) & JUNK_VOCABULARY]
    return GoldSet(
        domain=candidate_set.domain,
        base_url=candidate_set.base_url,
        must_have=must_have,
        junk=junk,
        notes="Draft generated from URL slugs and titles; correct by hand.",
    )


def field_coverage(
    gold: GoldSet, pick_urls: Sequence[str]
) -> tuple[list[str], list[str]]:
    """Return (covered, applicable) gold fields for a set of picked URLs."""
    picked = {url_key(url) for url in pick_urls}
    applicable = [f for f in GOLD_FIELDS if gold.must_have.get(f)]
    covered = [
        f for f in applicable if any(url_key(u) in picked for u in gold.must_have[f])
    ]
    return covered, applicable


def junk_hits(gold: GoldSet, pick_urls: Sequence[str]) -> list[str]:
    junk_keys = {url_key(url) for url in gold.junk}
    return [url for url in pick_urls if url_key(url) in junk_keys]


def _terms(url: str, title: str | None) -> set[str]:
    path = urlsplit(url).path.casefold()
    segments = [s for s in path.split("/") if s]
    terms = set(segments)
    for segment in segments:
        terms.update(t for t in TOKEN_PATTERN.split(segment) if t)
    if title:
        terms.update(t for t in TOKEN_PATTERN.split(title.casefold()) if t)
    return terms
