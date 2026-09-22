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

from company_research.content import normalize
from company_research.models import (
    OBJECTIVES,
    CandidateAssessment,
    Finding,
    Findings,
    Objective,
    Page,
    RequestedContentAssessment,
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
    path = urlsplit(url).path.rstrip("/").casefold()
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
        "jobs": r"\b(?:career|careers|job|jobs|vacancies|open.positions|openings|join.us|opportunities)\b",
        "technology_signals": r"\b(?:career|careers|job|jobs|vacancies|engineering|technology)\b",
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
    link_contexts: list[dict] = field(default_factory=list)
    job_record_ids: list[str] = field(default_factory=list)
    observed_relevance: str | None = None
    assessed: bool = False
    assessment_attempts: int = 0
    assessment: CandidateAssessment | RequestedContentAssessment | None = None
    navigation_root: str | None = None
    navigation_depth: int = 0

    def prompt_data(self) -> dict:
        return {
            "candidate_id": self.candidate_id,
            "url": self.url,
            "title": self.title,
            "anchor_text": self.anchor_text,
            "source": self.source,
            "link_contexts": self.link_contexts,
            "external": self.external,
            "job_record_ids": self.job_record_ids,
            "observed_relevance": self.observed_relevance,
            "navigation_root": self.navigation_root,
            "navigation_depth": self.navigation_depth,
        }


class CrawlQueue:
    """DeepSeek supplies potential; this state chooses pages and enforces scope/budget."""

    def __init__(
        self,
        site_url: str,
        config: ResearchConfig,
        *,
        instructions: str | None = None,
        allowed_urls: set[str] | None = None,
    ):
        self.site_url, self.config = site_url, config
        self.instructions = instructions
        self.allowed_urls = allowed_urls
        self.domains = tldextract.TLDExtract(suffix_list_urls=())
        self.site_domain = self.domain(site_url)
        self.candidates: dict[str, Candidate] = {}
        self.visited: set[str] = set()
        self.target_names: set[str] = set()
        self.job_detail_attempts: set[str] = set()
        self.job_details_fetched: set[str] = set()
        self.engineering_attempts: set[str] = set()
        self.page_yields: dict[str, dict] = {}
        self.observed_record_ids: set[str] = set()
        self.focus_visits: Counter = Counter()
        self.navigation_visits: Counter = Counter()
        self.external_pages = 0
        self.explored = 0
        self.excluded: Counter = Counter()
        self.site_profile: dict | None = None
        self.coverage: dict = {}
        self.objective_order: list[Objective] = list(OBJECTIVES)
        self.document_candidates: dict[str, dict] = {}
        self.navigation_sources: dict[str, dict] = {}
        self.source_pages: Counter = Counter()
        self.redirects: dict[str, str] = {}

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
        context: dict | None = None,
        from_search: bool = False,
    ) -> None:
        try:
            url = normalize_url(url, source)
        except ValueError:
            self.excluded["invalid_url"] += 1
            return
        if self.allowed_urls is not None and url not in self.allowed_urls:
            self.excluded["outside_page_list"] += 1
            return
        if re.search(
            r"\.(?:pdf|xlsx?|docx?)$", urlsplit(url).path.rstrip("/"), re.IGNORECASE
        ):
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
                        "link_contexts": [],
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
                if (
                    context
                    and context not in document["link_contexts"]
                    and len(document["link_contexts"]) < 5
                ):
                    document["link_contexts"].append(context | {"source_url": source})
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
        parent = self.candidates.get(self.redirects.get(source, source))
        if parent is not None and parent.navigation_root is not None and external:
            if self.domain(url) != self.domain(parent.navigation_root):
                self.excluded["outside_source_domain"] += 1
                return
            if parent.navigation_depth >= self.config.max_source_depth:
                self.excluded["source_depth_budget"] += 1
                return
        if (
            external
            and source_domain != self.site_domain
            and url not in self.candidates
            and not from_search
            and not self.permits_external_navigation(source)
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
        if external and parent is not None and parent.navigation_root is not None:
            depth = parent.navigation_depth + 1
            if candidate.navigation_root is None or depth < candidate.navigation_depth:
                candidate.navigation_root = parent.navigation_root
                candidate.navigation_depth = depth
        if (
            context
            and context not in candidate.link_contexts
            and len(candidate.link_contexts) < 5
        ):
            candidate.link_contexts.append(context)
            if candidate.url not in self.visited:
                candidate.assessed = False
                candidate.assessment = None
                candidate.assessment_attempts = 0
        if candidate.url not in self.visited and (
            (label and not candidate.anchor_text) or (title and not candidate.title)
        ):
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

    def permits_external_navigation(self, source: str) -> bool:
        parent = self.candidates.get(self.redirects.get(source, source))
        if (
            parent is not None
            and parent.navigation_root is not None
            and isinstance(parent.assessment, RequestedContentAssessment)
            and parent.assessment.follow_scope == "source_navigation"
        ):
            return parent.navigation_depth < self.config.max_source_depth
        return bool(
            parent is not None
            and parent.assessment is not None
            and parent.assessment.target_relevance in {"target", "target_evidence"}
            and parent.assessment.follow_scope == "target_navigation"
            and (
                parent.observed_relevance == "target"
                or (
                    self.instructions is not None
                    and isinstance(parent.assessment, RequestedContentAssessment)
                    and parent.assessment.requested_content.potential
                    in {"high", "medium"}
                    and parent.assessment.requested_content.role
                    in {"direct", "navigation"}
                )
            )
        )

    def navigation_source_error(
        self, candidate: Candidate, assessment: RequestedContentAssessment
    ) -> str | None:
        if assessment.follow_scope != "source_navigation":
            return None
        if not candidate.external or assessment.target_relevance == "unrelated":
            return "Source navigation requires a relevant external source"
        if candidate.navigation_root is not None:
            return None
        source = assessment.navigation_source
        if source is None:
            return "New source navigation requires explicit source evidence"
        quote = normalize(source.evidence)
        if not quote.strip():
            return "Navigation evidence must contain source text"
        for context in candidate.link_contexts:
            if (
                source.kind == "parent_company"
                and self.domain(context.get("source_url", "")) != self.site_domain
            ):
                continue
            if any(
                quote in normalize(str(context.get(field) or ""))
                for field in (
                    "surrounding_text",
                    "section_heading",
                    "anchor_text",
                    "title",
                )
            ):
                return None
        return "Navigation evidence must quote supplied context; parent evidence must come from the target website"

    def is_target(self, name: str | None) -> bool:
        names = self.target_names | {
            normalize((self.site_profile or {}).get("operator_name") or "")
        }
        return bool(name and normalize(name) in names)

    def observe_findings(
        self, page: Page, jobs: list[Finding], facts: list[Finding]
    ) -> None:
        for fact in facts:
            review = fact.data.get("interpretation_review", {})
            if (
                fact.evidence_status == "source_matched"
                and review.get("supported") is True
                and fact.data.get("field") in {"legal_name", "trading_name"}
                and self.is_target(fact.data.get("company"))
            ):
                self.target_names.add(normalize(fact.data["value"]))
        for job in jobs:
            if job.evidence_status != "source_matched" or not self.is_target(
                job.data.get("employer")
            ):
                continue
            job_url = job.data.get("job_url")
            if not job_url:
                continue
            job_url = normalize_url(job_url)
            candidate = self.candidates.get(job_url)
            # Only a URL actually discovered in source HTML can become a follow-up.
            if candidate is None:
                continue
            if job.record_id not in candidate.job_record_ids:
                candidate.job_record_ids.append(job.record_id)
            self.navigation_visits["jobs"] = max(1, self.navigation_visits["jobs"])
        candidate = self.candidates.get(page.requested_url)
        if candidate and candidate.job_record_ids and page.fetch_status == "fetched":
            if any(
                self.is_target(job.data.get("employer"))
                and job.evidence_status == "source_matched"
                and any(source.page_id == page.page_id for source in job.sources)
                for job in jobs
            ):
                self.job_details_fetched.add(page.source_url)
        if candidate and candidate.external:
            on_page = [
                fact
                for fact in [*jobs, *facts]
                if fact.evidence_status == "source_matched"
                and any(source.page_id == page.page_id for source in fact.sources)
            ]
            candidate.observed_relevance = (
                "target"
                if any(
                    self.is_target(
                        fact.data.get("company") or fact.data.get("employer")
                    )
                    for fact in on_page
                )
                else "unconfirmed"
            )

    def assessment_batch(self) -> list[Candidate]:
        candidates = sorted(
            [c for c in self.available() if not c.assessed],
            key=lambda c: (
                not bool(c.job_record_ids),
                c.assessment_attempts,
                c.external,
                not bool(c.anchor_text),
                urlsplit(c.url).path.count("/"),
                c.url,
            ),
        )
        # Let every objective nominate a candidate before filling with URL order.
        # Otherwise a long menu can starve Careers or Contact before a small crawl ends.
        chosen = [
            c
            for c in candidates
            if c.navigation_root is not None
            or any(
                context.get("extraction_method") == "web_search"
                for context in c.link_contexts
            )
        ][: max(1, self.config.selection_batch_size // 2)]
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
            and (
                not c.external
                or self.domain(c.url) not in self.navigation_sources
                or self.source_pages[self.domain(c.url)]
                < self.config.max_source_pages_per_domain
            )
        ]

    def page_kind(self, candidate: Candidate) -> str:
        if (
            isinstance(candidate.assessment, CandidateAssessment)
            and candidate.assessment.page_kind != "unknown"
        ):
            return candidate.assessment.page_kind
        path = urlsplit(candidate.url).path.casefold()
        if re.search(r"/(?:news|blog|case-studies|press)(?:/|$)", path):
            return "news"
        if re.search(
            r"engineering|design|simulation",
            " ".join([path, *candidate.anchor_text]),
            re.I,
        ):
            return "service_detail"
        return "unknown"

    def observe_page_yield(self, page: Page, records: Findings) -> None:
        candidate = self.candidates.get(page.requested_url)
        report = self.page_yields.setdefault(
            page.page_id,
            {
                "url": page.source_url,
                "page_kind": self.page_kind(candidate) if candidate else "unknown",
                "new_record_counts": dict.fromkeys(OBJECTIVES, 0),
            },
        )
        report["completed_objectives"] = (
            list(page.objectives_examined)
            if page.extraction_status == "complete"
            else []
        )
        for objective in OBJECTIVES:
            identifiers = {
                finding.record_id
                for finding in getattr(records, objective)
                if finding.evidence_status == "source_matched"
                and any(
                    source.page_id == page.page_id
                    and source.evidence_status == "source_matched"
                    for source in finding.sources
                )
            }
            report["new_record_counts"][objective] += len(
                identifiers - self.observed_record_ids
            )
            self.observed_record_ids.update(identifiers)

    def repetition_penalty(self, candidate: Candidate, objective: Objective) -> int:
        kind = self.page_kind(candidate)
        if kind not in {"news", "navigation", "job_list"}:
            return 0
        return sum(
            report["page_kind"] == kind
            and objective in report.get("completed_objectives", [])
            and report["new_record_counts"][objective] == 0
            for report in self.page_yields.values()
        )

    def pick(self, found_counts: dict[Objective, int]) -> tuple[Candidate, str] | None:
        if self.instructions is not None:
            return self.pick_for_instructions()
        available = self.available()
        followups = [c for c in available if c.job_record_ids]
        if followups and len(self.job_detail_attempts) < self.config.job_detail_reserve:
            candidate = min(
                followups,
                key=lambda c: (
                    not bool(
                        re.search(
                            r"engineer|developer|software|firmware|embedded|dsp|data",
                            " ".join([c.url, *c.anchor_text]),
                            re.I,
                        )
                    ),
                    c.url,
                ),
            )
            self.job_detail_attempts.add(candidate.url)
            self.focus_visits["jobs"] += 1
            if candidate.external:
                self.external_pages += 1
            return candidate, "job_detail_followup"
        engineering = [
            candidate
            for candidate in available
            if not candidate.external
            and isinstance(candidate.assessment, CandidateAssessment)
            and candidate.assessment.target_relevance == "target"
            and self.page_kind(candidate) == "service_detail"
            and candidate.assessment.objectives.products_services.role == "direct"
            and candidate.assessment.objectives.technology_signals.role == "direct"
            and candidate.assessment.objectives.technology_signals.potential
            in {"high", "medium"}
        ]
        if (
            engineering
            and len(self.engineering_attempts) < self.config.engineering_page_reserve
        ):
            candidate = min(
                engineering,
                key=lambda c: (
                    not bool(
                        re.search(
                            r"engineering|simulation",
                            " ".join([c.url, *c.anchor_text]),
                            re.I,
                        )
                    ),
                    not isinstance(c.assessment, CandidateAssessment)
                    or c.assessment.objectives.technology_signals.potential != "high",
                    c.url,
                ),
            )
            self.engineering_attempts.add(candidate.url)
            self.focus_visits["technology_signals"] += 1
            return candidate, "engineering_followup"
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
                if not isinstance(candidate.assessment, CandidateAssessment):
                    continue
                if candidate.assessment.target_relevance in {
                    "related_company",
                    "unrelated",
                }:
                    continue
                if candidate.external and candidate.assessment.target_relevance not in {
                    "target",
                    "target_evidence",
                }:
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
                        self.repetition_penalty(item[0], objective),
                        item[1].role
                        != (
                            "navigation"
                            if self.navigation_visits[objective] == 0
                            else "direct"
                        ),
                        item[1].potential != "high",
                        item[0].external,
                        item[0].url,
                    ),
                )
                self.focus_visits[objective] += 1
                if potential.role == "navigation":
                    self.navigation_visits[objective] += 1
                if candidate.external:
                    self.external_pages += 1

                return candidate, objective
        if self.explored < self.config.exploration_pages:
            uncertain = [
                c
                for c in available
                if not c.external
                and (
                    c.assessment is None
                    or isinstance(c.assessment, CandidateAssessment)
                    and any(
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

    def pick_for_instructions(self) -> tuple[Candidate, str] | None:
        """Use the requested content as the selection criterion; keep scope budgets."""
        useful = [
            (
                candidate,
                candidate.assessment.requested_content,
                candidate.assessment.priority,
            )
            for candidate in self.available()
            if isinstance(candidate.assessment, RequestedContentAssessment)
            and candidate.assessment.target_relevance != "unrelated"
            and (
                candidate.assessment.target_relevance != "related_company"
                or candidate.assessment.follow_scope == "source_navigation"
            )
            and (
                not candidate.external
                or candidate.assessment.target_relevance
                in {"target", "target_evidence"}
                or candidate.assessment.follow_scope == "source_navigation"
            )
            and (
                candidate.assessment.follow_scope != "source_navigation"
                or (
                    self.navigation_source_error(candidate, candidate.assessment)
                    is None
                    and (
                        self.domain(candidate.url) in self.navigation_sources
                        or len(self.navigation_sources) < self.config.max_source_domains
                    )
                )
            )
            and candidate.assessment.requested_content.potential in {"high", "medium"}
            and candidate.assessment.requested_content.role in {"direct", "navigation"}
        ]
        if not useful:
            return None
        candidate, _, _ = min(
            useful,
            key=lambda item: (
                item[1].role != "direct",
                item[1].potential != "high",
                -item[2],
                item[0].external,
                item[0].url,
            ),
        )
        if candidate.external:
            self.external_pages += 1
            self.source_pages[self.domain(candidate.url)] += 1
            if (
                isinstance(candidate.assessment, RequestedContentAssessment)
                and candidate.assessment.follow_scope == "source_navigation"
            ):
                candidate.navigation_root = candidate.navigation_root or candidate.url
                self.navigation_sources.setdefault(
                    self.domain(candidate.url),
                    {
                        "url": candidate.navigation_root,
                        "evidence": candidate.assessment.navigation_source.model_dump()
                        if candidate.assessment.navigation_source is not None
                        else None,
                        "basis": "source_matched_navigation_hypothesis",
                        "independently_verified": False,
                    },
                )
        return candidate, "requested_content"

    def source_budget_reason(self) -> str | None:
        """Distinguish useful source navigation blocked by limits from exhaustion."""
        for candidate in self.candidates.values():
            assessment = candidate.assessment
            if (
                candidate.url in self.visited
                or not isinstance(assessment, RequestedContentAssessment)
                or assessment.follow_scope != "source_navigation"
                or self.navigation_source_error(candidate, assessment) is not None
            ):
                continue
            domain = self.domain(candidate.url)
            if domain in self.navigation_sources:
                if self.source_pages[domain] >= self.config.max_source_pages_per_domain:
                    return "source_page_budget"
            elif len(self.navigation_sources) >= self.config.max_source_domains:
                return "source_domain_budget"
        return None

    def promising_remaining(self, objective: str) -> int:
        return sum(
            isinstance(c.assessment, CandidateAssessment)
            and getattr(c.assessment.objectives, objective).potential
            in {"high", "medium"}
            and getattr(c.assessment.objectives, objective).role != "none"
            for c in self.available()
        )

    def snapshot(self) -> dict:
        return {
            "selection_instructions": self.instructions,
            "allowed_urls": sorted(self.allowed_urls)
            if self.allowed_urls is not None
            else None,
            "site_domain": self.site_domain,
            "target_names": sorted(
                self.target_names
                | {normalize((self.site_profile or {}).get("operator_name") or "")}
                - {""}
            ),
            "job_coverage": {
                "listing_urls_discovered": sum(
                    bool(c.job_record_ids) for c in self.candidates.values()
                ),
                "detail_attempts": sorted(self.job_detail_attempts),
                "details_fetched_with_target_jobs": sorted(self.job_details_fetched),
                "unvisited_job_urls": sorted(
                    c.url
                    for c in self.candidates.values()
                    if c.job_record_ids and c.url not in self.visited
                ),
            },
            "engineering_coverage": {
                "detail_attempts": sorted(self.engineering_attempts)
            },
            "page_yields": self.page_yields,
            "excluded": dict(self.excluded),
            "candidate_count": len(self.candidates),
            "visited_count": len(self.visited),
            "document_candidates": list(self.document_candidates.values()),
            "navigation_sources": self.navigation_sources,
            "source_pages": dict(self.source_pages),
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
