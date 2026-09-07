"""Bounded sitemap discovery and an objective-balanced queue of public page links."""

import gzip
import io
import re
from collections import Counter, deque
from dataclasses import dataclass, field
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import httpx
import tldextract
from defusedxml import ElementTree

from company_research.models import (
    OBJECTIVES,
    CandidateAssessment,
    Objective,
    ResearchConfig,
)


def normalize_url(value: str, base_url: str | None = None) -> str:
    value = urljoin(base_url, value.strip()) if base_url else value.strip()
    if "://" not in value and base_url is None:
        if re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*:", value) and not re.match(
            r"^[^/]+:\d+(?:/|$)", value
        ):
            raise ValueError("Expected an HTTP(S) website URL")
        value = "https://" + value
    parts = urlsplit(value)
    if (
        parts.scheme not in {"http", "https"}
        or not parts.hostname
        or parts.username
        or parts.password
    ):
        raise ValueError("Expected an HTTP(S) website URL without credentials")
    linkedin = (parts.hostname or "").endswith(
        ".linkedin.com"
    ) or parts.hostname == "linkedin.com"
    query = [
        (k, v)
        for k, v in parse_qsl(parts.query, keep_blank_values=True)
        if not k.lower().startswith("utm_")
        and k.lower() not in {"gclid", "fbclid"}
        and not (linkedin and k.lower() in {"trk", "trackingid", "refid"})
    ]
    return urlunsplit(
        (
            parts.scheme.lower(),
            "www.linkedin.com" if linkedin else parts.netloc.lower(),
            parts.path or "/",
            urlencode(query),
            "",
        )
    )


def crawlable_url(url: str) -> bool:
    path = urlsplit(url).path.casefold()
    if re.search(
        r"\.(?:pdf|zip|gz|xml|json|jpg|jpeg|png|svg|gif|webp|mp4|mp3|css|js|ico|xlsx?|docx?)$",
        path,
    ):
        return False
    return (
        re.search(
            r"(?:^|/)(?:login|logon|logout|signin|sign-in|signup|sign-up|register|account|my-account|mitt-konto|cart|checkout|unsubscribe)(?:/|$)",
            path,
        )
        is None
    )


def social_destination(url: str) -> bool:
    """Keep social contacts in extracted records, reserving crawling for source pages.

    Individual LinkedIn job ads remain eligible; company feeds and platform links
    must not expand into an unrelated platform-wide crawl.
    """
    host = (urlsplit(url).hostname or "").removeprefix("www.")
    if host == "linkedin.com":
        return re.match(r"^/jobs/view/[^/]+", urlsplit(url).path) is None
    return any(
        host == name or host.endswith("." + name)
        for name in (
            "facebook.com",
            "instagram.com",
            "twitter.com",
            "x.com",
            "youtube.com",
            "youtu.be",
        )
    )


def navigation_objectives(url: str, labels: list[str]) -> set[Objective]:
    """Metadata hints for queue ordering/exploration, never extracted source facts."""
    text = " ".join([urlsplit(url).path, *labels]).casefold()
    patterns: dict[Objective, str] = {
        "company_profile": r"\b(?:about|company|who.we.are|our.story)\b",
        "company_contacts": r"\b(?:contact|contacts|get.in.touch)\b",
        "locations": r"\b(?:offices|locations|contact)\b",
        "products_services": r"\b(?:services|products|solutions)\b",
        "people": r"\b(?:team|leadership|management|founders|board)\b",
        "company_relationships": r"\b(?:ownership|shareholders|subsidiaries|group|acquisition|investors)\b",
        "jobs": r"\b(?:career|careers|jobs|vacancies|open.positions|openings|join.us|opportunities)\b",
        "technology_signals": r"\b(?:career|careers|jobs|vacancies|engineering|technology)\b",
        "certifications_compliance": r"\b(?:certifications|certificates|quality|compliance|trust)\b",
        "document_links": r"\b(?:annual.report|financial|reports|investor|investors|disclosures|certificates)\b",
    }
    return {
        objective for objective, pattern in patterns.items() if re.search(pattern, text)
    }


async def sitemap_urls(
    client: httpx.AsyncClient, start_url: str, config: ResearchConfig
) -> dict:
    origin = urlunsplit((*urlsplit(start_url)[:2], "/", "", ""))
    pending = deque([urljoin(origin, "sitemap.xml")])
    visited, urls, seen_urls, errors = set(), [], set(), []
    url_sources = {}
    if config.max_sitemap_urls == 0 or config.max_sitemap_files == 0:
        return {
            "status": "disabled",
            "urls": [],
            "url_sources": {},
            "files": [],
            "errors": [],
        }
    try:
        robots = await client.get(
            urljoin(origin, "robots.txt"), timeout=15, follow_redirects=True
        )
        if robots.status_code == 200:
            pending.extend(re.findall(r"(?im)^sitemap:\s*(\S+)", robots.text))
    except httpx.HTTPError as error:
        errors.append(
            {"url": urljoin(origin, "robots.txt"), "error": type(error).__name__}
        )
    while (
        pending
        and len(visited) < config.max_sitemap_files
        and len(urls) < config.max_sitemap_urls
    ):
        url = pending.popleft()
        if url in visited:
            continue
        visited.add(url)
        try:
            url = normalize_url(url, origin)
            # Sitemap locations must stay on the input host, including www aliases.
            if (urlsplit(url).hostname or "").removeprefix("www.") != (
                urlsplit(origin).hostname or ""
            ).removeprefix("www."):
                errors.append({"url": url, "error": "external_sitemap_not_followed"})
                continue
            async with client.stream(
                "GET", url, timeout=20, follow_redirects=True
            ) as response:
                response.raise_for_status()
                body = bytearray()
                async for piece in response.aiter_bytes():
                    body.extend(piece)
                    if len(body) > 8_000_000:
                        raise ValueError("sitemap_size_limit")
            raw = bytes(body)
            if raw.startswith(b"\x1f\x8b"):
                with gzip.GzipFile(fileobj=io.BytesIO(raw)) as compressed:
                    raw = compressed.read(8_000_001)
                if len(raw) > 8_000_000:
                    raise ValueError("sitemap_size_limit")
            root = ElementTree.fromstring(raw)
            kind = root.tag.rsplit("}", 1)[-1]
            entry_tag = "sitemap" if kind == "sitemapindex" else "url"
            locations = [
                node.text.strip()
                for entry in root
                if entry.tag.rsplit("}", 1)[-1] == entry_tag
                for node in entry
                if node.tag.rsplit("}", 1)[-1] == "loc" and node.text
            ]
            if kind == "sitemapindex":
                pending.extend(locations)
            elif kind == "urlset":
                for location in locations:
                    normalized = normalize_url(location, url)
                    if normalized not in seen_urls:
                        urls.append(normalized)
                        seen_urls.add(normalized)
                        url_sources[normalized] = url
                    if len(urls) >= config.max_sitemap_urls:
                        break
            else:
                errors.append({"url": url, "error": "not_a_sitemap"})
        except (
            Exception
        ) as error:  # Sitemap failures must not prevent link discovery from pages.
            errors.append(
                {"url": url, "error": f"{type(error).__name__}: {str(error)[:200]}"}
            )
    return {
        "status": "limit_reached"
        if pending or len(urls) >= config.max_sitemap_urls
        else "found"
        if urls
        else "not_found",
        "urls": urls,
        "url_sources": url_sources,
        "files": sorted(visited),
        "errors": errors,
    }


@dataclass
class Candidate:
    candidate_id: str
    url: str
    external: bool
    title: str | None = None
    anchor_text: list[str] = field(default_factory=list)
    source: list[str] = field(default_factory=list)
    assessed: bool = False
    assessment_attempts: int = 0
    assessment: CandidateAssessment | None = None

    def prompt_data(self) -> dict:
        return {
            "candidate_id": self.candidate_id,
            "url": self.url,
            "title": self.title,
            "anchor_text": self.anchor_text,
            "source": self.source,
            "external": self.external,
        }


class CrawlQueue:
    """DeepSeek supplies potential; this state chooses pages and enforces scope/budget."""

    def __init__(self, site_url: str, config: ResearchConfig):
        self.site_url, self.config = site_url, config
        self.domains = tldextract.TLDExtract(suffix_list_urls=())
        self.site_domain = self.domain(site_url)
        self.candidates: dict[str, Candidate] = {}
        self.visited: set[str] = set()
        self.external_domains: set[str] = set()
        self.focus_visits: Counter = Counter()
        self.navigation_visits: Counter = Counter()
        self.external_pages = 0
        self.explored = 0
        self.excluded: Counter = Counter()
        self.site_profile: dict | None = None
        self.coverage: dict = {}
        self.objective_order: list[Objective] = list(OBJECTIVES)
        self.document_candidates: dict[str, dict] = {}

    def domain(self, url: str) -> str:
        parsed = self.domains(url)
        return parsed.top_domain_under_public_suffix or urlsplit(url).hostname or ""

    def add(
        self,
        url: str,
        *,
        source: str,
        title: str | None = None,
        label: str | None = None,
    ) -> None:
        try:
            url = normalize_url(url, source)
        except ValueError:
            self.excluded["invalid_url"] += 1
            return
        if re.search(r"\.(?:pdf|xlsx?|docx?)(?:$)", urlsplit(url).path, re.IGNORECASE):
            if (
                len(self.document_candidates) < self.config.max_candidates
                or url in self.document_candidates
            ):
                document = self.document_candidates.setdefault(
                    url,
                    {
                        "url": url,
                        "source_urls": [],
                        "labels": [],
                        "content_examined": False,
                    },
                )
                if (
                    source not in document["source_urls"]
                    and len(document["source_urls"]) < 5
                ):
                    document["source_urls"].append(source)
                if (
                    label
                    and label not in document["labels"]
                    and len(document["labels"]) < 5
                ):
                    document["labels"].append(label[:500])
            else:
                self.excluded["document_candidate_budget"] += 1
            return
        if not crawlable_url(url):
            self.excluded["non_page_or_account"] += 1
            return
        external = self.domain(url) != self.site_domain
        if external and social_destination(url):
            self.excluded["social_destination"] += 1
            return
        source_domain = self.domain(source)
        if (
            external
            and source_domain != self.site_domain
            and self.domain(url) not in self.external_domains
        ):
            self.excluded["outside_approved_scope"] += 1
            return
        if url not in self.candidates:
            if len(self.candidates) >= self.config.max_candidates:
                self.excluded["candidate_budget"] += 1
                return
            self.candidates[url] = Candidate(
                f"c{len(self.candidates) + 1:05}", url, external
            )
        candidate = self.candidates[url]
        if (label and not candidate.anchor_text) or (title and not candidate.title):
            # A link label can clarify a URL previously assessed from a bare sitemap entry.
            candidate.assessed = False
            candidate.assessment_attempts = 0
        if title:
            candidate.title = title[:500]
        if (
            label
            and label not in candidate.anchor_text
            and len(candidate.anchor_text) < 5
        ):
            candidate.anchor_text.append(label[:500])
        if source not in candidate.source and len(candidate.source) < 5:
            candidate.source.append(source)

    def assessment_batch(self) -> list[Candidate]:
        candidates = sorted(
            [c for c in self.available() if not c.assessed],
            key=lambda c: (
                c.assessment_attempts,
                c.external,
                not bool(c.anchor_text),
                urlsplit(c.url).path.count("/"),
                c.url,
            ),
        )
        # Let every objective nominate a candidate before filling with URL order.
        # Otherwise a long menu can starve Careers or Contact before a small crawl ends.
        chosen = []
        for objective in sorted(
            self.objective_order,
            key=lambda o: (
                self.focus_visits[o],
                self.coverage.get(o, {}).get("source_matched_count", 0) > 0,
            ),
        ):
            for candidate in candidates:
                if candidate not in chosen and objective in navigation_objectives(
                    candidate.url, candidate.anchor_text
                ):
                    chosen.append(candidate)
                    break
        return [*chosen, *(c for c in candidates if c not in chosen)][
            : self.config.selection_batch_size
        ]

    def exploration_candidate(
        self, candidates: list[Candidate], objective: Objective
    ) -> Candidate | None:
        if self.explored >= self.config.exploration_pages:
            return None
        choices = [
            c
            for c in candidates
            if c.assessment is None
            and not c.external
            and objective in navigation_objectives(c.url, c.anchor_text)
        ]
        return (
            min(choices, key=lambda c: (not bool(c.anchor_text), len(c.url), c.url))
            if choices
            else None
        )

    def available(self) -> list[Candidate]:
        return [
            c
            for c in self.candidates.values()
            if c.url not in self.visited
            and (not c.external or self.external_pages < self.config.max_external_pages)
        ]

    def pick(self, found_counts: dict[Objective, int]) -> tuple[Candidate, str] | None:
        available = self.available()
        for objective in sorted(
            self.objective_order,
            key=lambda o: (self.focus_visits[o], found_counts[o] > 0),
        ):
            uncertain = self.exploration_candidate(available, objective)
            if (
                found_counts[objective] == 0
                and uncertain is not None
                and uncertain.assessment_attempts > 0
            ):
                self.explored += 1
                self.focus_visits[objective] += 1
                return uncertain, f"exploration:{objective}"
            useful = []
            for candidate in available:
                if candidate.assessment is None:
                    continue
                potential = getattr(candidate.assessment.objectives, objective)
                if (
                    potential.potential in {"high", "medium"}
                    and potential.role != "none"
                ):
                    useful.append((candidate, potential))
            if useful:
                candidate, potential = min(
                    useful,
                    key=lambda item: (
                        item[1].potential != "high",
                        item[1].role
                        != (
                            "navigation"
                            if self.navigation_visits[objective] == 0
                            else "direct"
                        ),
                        item[0].external,
                        item[0].url,
                    ),
                )
                self.focus_visits[objective] += 1
                if potential.role == "navigation":
                    self.navigation_visits[objective] += 1
                if candidate.external:
                    self.external_pages += 1
                    self.external_domains.add(self.domain(candidate.url))
                return candidate, objective
        if self.explored < self.config.exploration_pages:
            uncertain = [
                c
                for c in available
                if not c.external
                and (
                    c.assessment is None
                    or any(
                        getattr(c.assessment.objectives, o).potential == "unknown"
                        for o in OBJECTIVES
                    )
                )
            ]
            if uncertain:
                self.explored += 1
                return min(
                    uncertain,
                    key=lambda c: (not bool(c.anchor_text), len(c.url), c.url),
                ), "exploration"
        return None

    def promising_remaining(self, objective: str) -> int:
        return sum(
            c.assessment is not None
            and getattr(c.assessment.objectives, objective).potential
            in {"high", "medium"}
            and getattr(c.assessment.objectives, objective).role != "none"
            for c in self.available()
        )

    def snapshot(self) -> dict:
        return {
            "site_domain": self.site_domain,
            "excluded": dict(self.excluded),
            "candidate_count": len(self.candidates),
            "visited_count": len(self.visited),
            "document_candidates": list(self.document_candidates.values()),
            "unassessed_count": sum(
                c.assessment is None and c.url not in self.visited
                for c in self.candidates.values()
            ),
            "assessment_failed_count": sum(
                c.assessment_attempts > 0
                and c.assessment is None
                and c.url not in self.visited
                for c in self.candidates.values()
            ),
            "candidates": [
                {
                    **c.prompt_data(),
                    "assessed": c.assessed,
                    "assessment_attempts": c.assessment_attempts,
                    "visited": c.url in self.visited,
                    "assessment": c.assessment.model_dump() if c.assessment else None,
                }
                for c in self.candidates.values()
            ],
        }
