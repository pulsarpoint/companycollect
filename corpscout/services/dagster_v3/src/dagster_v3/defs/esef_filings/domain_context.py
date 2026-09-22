"""Persist bounded report sections around domain occurrences, without inference."""

import re
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path

from lxml import etree

from dagster_v3.defs.esef_filings.website_candidates import (
    EsefWebsiteCandidate,
    EsefWebsiteEvidence,
    _is_visible,
    _local_name,
)

CONTEXT_VERSION = "esef-domain-context-v1"
SECTION_CHARS = 12_000
LOCAL_CHARS = 2_000
_PAGE_ID = re.compile(r"(?:pf[0-9a-f]+|page[-_]?\d+)", re.IGNORECASE)


def context_text(element: etree._Element) -> str:
    """Preserve inline word fragments, separating block and table-cell boundaries."""
    parts: list[str] = []
    boundaries = {"p", "div", "li", "tr", "td", "th", "section", "article", "br",
                  "h1", "h2", "h3", "h4", "h5", "h6", "caption"}

    def visit(node: etree._Element) -> None:
        if not isinstance(node.tag, str) or not _is_visible(node):
            return
        boundary = _local_name(node.tag) in boundaries
        if boundary:
            parts.append(" ")
        if node.text:
            parts.append(node.text)
        for child in node:
            visit(child)
            if child.tail:
                parts.append(child.tail)
        if boundary:
            parts.append(" ")

    visit(element)
    return " ".join("".join(parts).split())


def bounded_excerpt(text: str, anchor: str, limit: int) -> tuple[str, bool]:
    """Keep the occurrence in a long excerpt, and expose every truncation."""
    if len(text) <= limit:
        return text, False
    position = text.find(anchor) if anchor else 0
    start = max(0, position - limit // 3)
    start = min(start, len(text) - limit)
    return text[start : start + limit], True


def source_context(tree: etree._ElementTree, evidence: EsefWebsiteEvidence) -> dict[str, object]:
    namespaces = {prefix: uri for prefix, uri in tree.getroot().nsmap.items() if prefix}
    try:
        nodes = tree.xpath(evidence.xpath, namespaces=namespaces) if evidence.xpath else []
    except etree.XPathError:
        # A legacy locator may use a namespace declared only in a nested subtree.
        # Keep the original occurrence, explicitly marked as unlocated.
        nodes = []
    if len(nodes) != 1 or not isinstance(nodes[0], etree._Element):
        return {
            "version": CONTEXT_VERSION, "scope": "unlocated",
            "text": evidence.surrounding_text, "local_text": evidence.surrounding_text,
            "headings": [], "table_headers": [], "xpath": evidence.xpath,
            "truncated": False, "local_truncated": False,
            "reading_order": "unavailable",
        }
    node = nodes[0]
    ancestors = (node, *node.iterancestors())
    page = next((ancestor for ancestor in ancestors if (
        _PAGE_ID.fullmatch(ancestor.get("id", ""))
        or "pf" in ancestor.get("class", "").split()
        or ancestor.get("data-page-number") is not None
    )), None)
    scope = None
    scope_kind = "local_container"
    # Never cross the containing page while looking for a semantic section.
    for ancestor in ancestors:
        if _local_name(ancestor.tag) in {"section", "article"}:
            scope, scope_kind = ancestor, "section"
            break
        if ancestor is page:
            scope, scope_kind = page, "page"
            break
    if scope is None:
        scope = node
        for ancestor in ancestors:
            if _local_name(ancestor.tag) in {"body", "html"}:
                break
            scope = ancestor
            # An ordinary paragraph's parent usually supplies the section. Stop
            # climbing before copying an entire unpaginated annual report.
            if len(context_text(ancestor)) >= LOCAL_CHARS:
                break
    local_node = next((ancestor for ancestor in ancestors
                       if _local_name(ancestor.tag) in {"p", "li", "tr", "address"}), node)
    local_text = context_text(local_node)
    text = context_text(scope)
    anchor = evidence.raw_value if evidence.raw_value in text else local_text[:120]
    text, truncated = bounded_excerpt(text, anchor, SECTION_CHARS)
    local_text, local_truncated = bounded_excerpt(local_text, evidence.raw_value, LOCAL_CHARS)
    headings = []
    for element in scope.iter():
        if element is node:
            break
        if _local_name(element.tag) in {"h1", "h2", "h3", "h4", "h5", "h6"} and _is_visible(element):
            heading = context_text(element)
            if heading:
                headings.append(heading[:500])
    table = next((ancestor for ancestor in ancestors if _local_name(ancestor.tag) == "table"), None)
    headers = [] if table is None else [
        context_text(element)[:500] for element in table.iter()
        if _local_name(element.tag) in {"th", "caption"} and _is_visible(element)
    ]
    return {
        "version": CONTEXT_VERSION, "scope": scope_kind, "xpath": tree.getpath(scope),
        "text": text, "local_text": local_text, "headings": headings[-6:],
        "table_headers": headers[:30], "truncated": truncated,
        "local_truncated": local_truncated,
        # XHTML can use absolute positioning and multiple columns. DOM order is
        # not a claim about visual reading order or which entity a heading names.
        "reading_order": "document_order_unverified",
    }


def enrich_domain_context(
    candidates: list[EsefWebsiteCandidate], report_paths: Mapping[str, Path],
) -> list[EsefWebsiteCandidate]:
    """Parse each referenced member once; keep all original occurrence locators."""
    contexts: dict[tuple[str, str, str], dict[str, object]] = {}
    for member, path in sorted(report_paths.items()):
        evidence = [item for candidate in candidates for item in candidate.evidence
                    if item.report_member == member]
        if not evidence:
            continue
        tree = etree.parse(path, etree.XMLParser(
            load_dtd=False, no_network=True, resolve_entities=False, huge_tree=True,
        ))
        for item in evidence:
            key = member, item.xpath, item.raw_value
            if key not in contexts:
                contexts[key] = source_context(tree, item)
    return [replace(candidate, evidence=[
        replace(item, source_context=contexts.get((item.report_member, item.xpath, item.raw_value)))
        for item in candidate.evidence
    ]) for candidate in candidates]
