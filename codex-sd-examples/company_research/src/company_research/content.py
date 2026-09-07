"""Lossless HTML windows and source checks for independently attributable records."""

import json
import re
import unicodedata
from dataclasses import dataclass, field
from html import unescape
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from company_research.models import EvidenceFragment, Finding, Source
from company_research.storage import content_hash


@dataclass
class HtmlSpan:
    tag: str
    start: int
    end: int
    children: list["HtmlSpan"] = field(default_factory=list)


class HtmlSourceSpans(HTMLParser):
    """Track original character offsets without serializing or editing the DOM."""

    def __init__(self, html: str) -> None:
        super().__init__(convert_charrefs=False)
        self.html = html
        self.line_starts = [0] + [m.end() for m in re.finditer("\n", html)]
        self.root = HtmlSpan("document", 0, len(html))
        self.stack = [self.root]
        self.feed(html)
        self.close()

    def source_offset(self) -> int:
        line, column = self.getpos()
        return self.line_starts[line - 1] + column

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        start = self.source_offset()
        node = HtmlSpan(tag, start, len(self.html))
        self.stack[-1].children.append(node)
        if tag in {
            "area",
            "base",
            "br",
            "col",
            "embed",
            "hr",
            "img",
            "input",
            "link",
            "meta",
            "param",
            "source",
            "track",
            "wbr",
        }:
            tag_text = self.get_starttag_text()
            assert tag_text is not None
            node.end = start + len(tag_text)
        else:
            self.stack.append(node)

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        self.handle_starttag(tag, attrs)
        if self.stack[-1].start == self.source_offset() and self.stack[-1].tag == tag:
            tag_text = self.get_starttag_text()
            assert tag_text is not None
            self.stack.pop().end = self.source_offset() + len(tag_text)

    def handle_endtag(self, tag: str) -> None:
        end = self.html.find(">", self.source_offset()) + 1
        for index in range(len(self.stack) - 1, 0, -1):
            if self.stack[index].tag == tag:
                for node in self.stack[index:]:
                    node.end = end
                del self.stack[index:]
                break


def source_atoms(node: HtmlSpan, max_chars: int) -> list[tuple[int, int]]:
    if node.end - node.start <= max_chars or node.tag in {
        "a",
        "tr",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
    }:
        return [(node.start, node.end)]
    spans = []
    cursor = node.start
    for child in node.children:
        if child.start < cursor or child.end > node.end:
            raise ValueError("Overlapping HTML source spans; cannot safely segment")
        if cursor < child.start:
            spans.append((cursor, child.start))
        spans.extend(source_atoms(child, max_chars))
        cursor = child.end
    if cursor < node.end:
        spans.append((cursor, node.end))
    return spans


@dataclass(frozen=True)
class HtmlWindow:
    start: int
    end: int
    content: str


def split_html(html: str, *, max_chars: int, overlap_chars: int) -> list[HtmlWindow]:
    """Keep DOM units together where possible; every source character is included.

    Oversized units are sliced with overlap to enforce the input-size ceiling.
    Windows can be HTML fragments; the original snapshot is never reserialized.
    """
    if not 0 <= overlap_chars < max_chars:
        raise ValueError("Require 0 <= overlap_chars < max_chars")
    if not html:
        return []
    parsed = HtmlSourceSpans(html)
    try:
        atoms = source_atoms(parsed.root, max(1, (max_chars - overlap_chars) // 2))
    except (ValueError, RecursionError):
        # Malformed or excessively nested markup still receives lossless bounded windows.
        atoms = [(0, len(html))]
    boundaries = sorted({end for _, end in atoms})
    windows = []
    start = 0
    while start < len(html):
        ceiling = min(start + max_chars, len(html))
        eligible = [b for b in boundaries if start + overlap_chars < b <= ceiling]
        end = max(eligible) if eligible else ceiling
        windows.append(HtmlWindow(start, end, html[start:end]))
        if end == len(html):
            break
        start = end - overlap_chars
    return windows


def normalize(text: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", unescape(text)).casefold().split())


def normalize_evidence(text: str) -> str:
    # Joining inline DOM nodes can insert a space before punctuation, e.g. <b>Yocto</b>.
    return re.sub(r"\s+([,.;:!?\)\]\}])", r"\1", normalize(text))


def source_finding(
    objective: str, record: dict, *, page: dict, window: HtmlWindow
) -> Finding:
    """Check source presence, not semantic truth. Keep failures explicitly reviewable."""
    soup = BeautifulSoup(window.content, "html.parser")
    text, html = (
        normalize_evidence(soup.get_text(" ", strip=True)),
        normalize_evidence(window.content),
    )
    fragments, issues = [], []
    for fragment in record["evidence"]:
        needle = normalize_evidence(fragment)
        matched = (
            "text"
            if needle and needle in text
            else "html"
            if needle and needle in html
            else "not_found"
        )
        fragments.append(EvidenceFragment(text=fragment, matched_in=matched))
        if matched == "not_found":
            issues.append("evidence_fragment_absent")
    data = {k: v for k, v in record.items() if k != "evidence"}
    urls = {page["source_url"]}
    urls.update(
        urljoin(page["source_url"], str(a["href"])) for a in soup.find_all(href=True)
    )
    for key in ("url", "profile_url", "job_url", "document_url"):
        if data.get(key) is not None:
            data[key] = urljoin(page["source_url"], data[key])
            if urlsplit(data[key]).scheme not in {"http", "https"}:
                issues.append(f"{key}_not_http")
            if data[key] not in urls:
                issues.append(f"{key}_absent")
    if objective == "company_contacts" and data["type"] in {"form", "social"}:
        data["value"] = urljoin(page["source_url"], data["value"])
        if data["value"] not in urls:
            issues.append("contact_url_absent")
    anchor_fields = {
        "people": ["name"],
        "jobs": ["title"],
        "products_services": ["name"],
        "company_relationships": ["subject", "object"],
        "locations": ["address"],
        "company_contacts": ["value"],
        "certifications_compliance": ["standard_name"],
        "site_classification": ["operator_name"]
        if data.get("operator_name") is not None
        else [],
    }.get(objective, [])
    if objective == "company_contacts" and data["type"] in {"form", "social"}:
        # These values are checked as source hrefs above, rather than visible text.
        anchor_fields = []
    if objective == "company_profile" and data["field"] not in {
        "description",
        "industry",
    }:
        anchor_fields = ["value"]
    quoted = normalize_evidence(
        " ".join(
            BeautifulSoup(fragment, "html.parser").get_text(" ", strip=True)
            for fragment in record["evidence"]
        )
    )
    attribution_fields = {
        "people": ("company", "role"),
        "jobs": ("employer",),
        "products_services": ("company",),
        "locations": ("company",),
        "company_contacts": ("owner",),
        "company_profile": ("company",),
    }.get(objective, ())
    anchor_fields.extend(key for key in attribution_fields if data.get(key) is not None)
    if objective == "document_links":
        document_label = re.sub(r"[^a-z]", "", (data.get("label") or "").casefold())
        document_path = re.sub(
            r"[^a-z]", "", urlsplit(data["document_url"]).path.casefold()
        )
        if any(
            term in document_label or term in document_path
            for term in (
                "purchaseconditions",
                "purchasingconditions",
                "privacypolicy",
                "cookiepolicy",
                "termsandconditions",
            )
        ):
            issues.append("routine_policy_not_company_report")
        anchor_fields.extend(
            key
            for key in ("label", "company", "reporting_period")
            if data.get(key) is not None
        )
        data["content_examined"] = False
        data["metadata_basis"] = "link_and_surrounding_html"
    if (
        objective == "company_relationships"
        and data.get("ownership_percentage") is not None
    ):
        percentages = re.findall(
            r"(?<![\d.,])(\d+(?:[.,]\d+)?)\s*(?:%|percent\b|per cent\b)", quoted
        )
        if data["ownership_percentage"] not in [
            float(value.replace(",", ".")) for value in percentages
        ]:
            issues.append("ownership_percentage_not_in_evidence")
        if data.get("as_of") is not None:
            anchor_fields.append("as_of")
    if objective == "certifications_compliance":
        anchor_fields.extend(
            key
            for key in (
                "subject_name",
                "certificate_or_report_id",
                "issuer_or_assessor",
                "issued_on",
                "valid_until",
            )
            if data.get(key) is not None
        )
        data["verification_level"] = "website_claim"
        data["document_examined"] = False
    for key in anchor_fields:
        value = normalize_evidence(data[key])
        present = value in quoted
        if key in {"role", "address"}:
            # Composite roles may use commas where source markup uses <br>.
            # Each quotation still requires an exact contiguous match above.
            role_words = " ".join(re.findall(r"\w+", value))
            present = role_words in " ".join(re.findall(r"\w+", quoted))
        if objective == "company_contacts" and data["type"] == "phone":
            digits = re.sub(r"\D", "", value)
            present = bool(digits) and digits in re.sub(r"\D", "", quoted)
        if not present:
            issues.append(f"{key}_not_in_evidence")
    if objective == "technology_signals":
        # Short names such as R, C and Go must not match inside another word or C++/C#.
        technology_pattern = (
            r"(?<![\w+#.])" + re.escape(normalize(data["technology"])) + r"(?![\w+#])"
        )
        if re.search(technology_pattern, quoted) is None:
            issues.append("technology_not_in_evidence")
        for key in ("company", "job_employer", "job_title", "alternative_group"):
            if data[key] is not None and normalize_evidence(data[key]) not in quoted:
                issues.append(f"{key}_not_in_evidence")
    status = "needs_review" if issues else "source_matched"
    source = Source(
        url=page["source_url"],
        page_id=page["page_id"],
        fetched_at=page["fetched_at"],
        html_sha256=page["html_sha256"],
        chunk_start=window.start,
        chunk_end=window.end,
        evidence=fragments,
        evidence_status=status,
        issues=sorted(set(issues)),
    )
    identity = {
        key: normalize(value)
        if isinstance(value, str) and not value.startswith(("http://", "https://"))
        else value
        for key, value in data.items()
    }
    record_id = content_hash(objective + json.dumps(identity, sort_keys=True))[:24]
    return Finding(
        record_id=record_id, data=data, sources=[source], evidence_status=status
    )


def merge_finding(records: list[Finding], finding: Finding) -> None:
    for existing in records:
        if existing.record_id == finding.record_id:
            if (
                finding.data.get("catalog_match") is not None
                and existing.data.get("catalog_match") is None
            ):
                existing.data["catalog_match"] = finding.data["catalog_match"]
                existing.data.pop("catalog_error", None)
            for source in finding.sources:
                if source not in existing.sources:
                    existing.sources.append(source)
                    if source.evidence_status == "source_matched":
                        existing.data.pop("interpretation_review", None)
            if finding.evidence_status == "source_matched":
                if (
                    existing.data.get("interpretation_review", {}).get("supported")
                    is not False
                ):
                    existing.evidence_status = "source_matched"
            return
    records.append(finding)
