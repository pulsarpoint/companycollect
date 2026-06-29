"""Lightweight inline-XBRL (iXBRL) fact extractor.

Targets the practical need — pull a known set of financial concepts out of filing
documents (UK Companies House FRC taxonomy, Finland, …) — WITHOUT loading the full
taxonomy DTS. Full taxonomy loading is too heavy/slow to run over millions of
filings; for extracting a fixed concept set we only need the inline facts plus
their contexts. Parses `ix:nonFraction`/`ix:nonNumeric` facts and `xbrli:context`
periods/dimensions, applying `scale`/`sign`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation

from lxml import etree

IX_NS = "http://www.xbrl.org/2013/inlineXBRL"
XBRLI_NS = "http://www.xbrl.org/2003/instance"
XBRLDI_NS = "http://xbrl.org/2006/xbrldi"
LINK_NS = "http://www.xbrl.org/2003/linkbase"
# Namespaces whose elements are structure, not facts (plain-XBRL extraction).
_NON_FACT_NS = frozenset({XBRLI_NS, XBRLDI_NS, LINK_NS})

_NUM_CLEAN_RE = re.compile(r"[^0-9.\-]")


@dataclass
class Context:
    id: str
    entity_id: str | None
    instant: date | None
    start: date | None
    end: date | None
    dimensions: dict[str, str] = field(default_factory=dict)

    @property
    def period_end(self) -> date | None:
        return self.instant or self.end

    @property
    def has_dimensions(self) -> bool:
        return bool(self.dimensions)


@dataclass
class Fact:
    concept: str  # local name, e.g. "NetAssetsLiabilities" / "md103"
    namespace: str  # namespace URI
    context_ref: str
    value: Decimal | None
    unit_ref: str | None
    prefix: str | None = None  # source prefix (plain XBRL), e.g. "fi_met"

    @property
    def qname(self) -> str:
        """Prefixed name as it appeared in the source (e.g. 'fi_met:md103')."""
        return f"{self.prefix}:{self.concept}" if self.prefix else self.concept


@dataclass
class XbrlDocument:
    contexts: dict[str, Context]
    facts: list[Fact]

    @property
    def entity_id(self) -> str | None:
        for ctx in self.contexts.values():
            if ctx.entity_id:
                return ctx.entity_id
        return None

    @property
    def reporting_period_end(self) -> date | None:
        ends = [c.period_end for c in self.contexts.values() if c.period_end]
        return max(ends) if ends else None

    def metric(
        self,
        local_names: set[str] | tuple[str, ...],
        *,
        period_end: date | None = None,
        prefer_undimensioned: bool = True,
    ) -> Decimal | None:
        """Value for the first matching concept in the target period.

        period_end defaults to the document's reporting period end (current year).
        Undimensioned facts are preferred (the headline figure, not a breakdown).
        """
        names = set(local_names)
        target = period_end or self.reporting_period_end
        candidates: list[tuple[bool, Decimal]] = []
        for fact in self.facts:
            if fact.concept not in names or fact.value is None:
                continue
            ctx = self.contexts.get(fact.context_ref)
            if ctx is None:
                continue
            if target is not None and ctx.period_end != target:
                continue
            candidates.append((ctx.has_dimensions, fact.value))
        if not candidates:
            return None
        if prefer_undimensioned:
            undimensioned = [v for dim, v in candidates if not dim]
            if undimensioned:
                return undimensioned[0]
        return candidates[0][1]


def _parse_date(text: str | None) -> date | None:
    if not text:
        return None
    text = text.strip()[:10]
    try:
        y, m, d = (int(p) for p in text.split("-"))
        return date(y, m, d)
    except (ValueError, TypeError):
        return None


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _split_qname(qname: str, nsmap: dict) -> tuple[str, str]:
    """Resolve a `prefix:local` concept name to (namespace_uri, local)."""
    if ":" in qname:
        prefix, local = qname.split(":", 1)
    else:
        prefix, local = None, qname
    return nsmap.get(prefix, ""), local


def _parse_number(text: str, *, scale: str | None, sign: str | None) -> Decimal | None:
    raw = (text or "").strip()
    if not raw:
        return None
    negative = sign == "-" or raw.startswith("(")
    cleaned = _NUM_CLEAN_RE.sub("", raw.replace(",", ""))
    cleaned = cleaned.replace("-", "")  # sign handled separately
    if cleaned in ("", "."):
        return None
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None
    if scale:
        try:
            value *= Decimal(10) ** int(scale)
        except (ValueError, InvalidOperation):
            pass
    return -value if negative else value


def _parse_context(el: etree._Element) -> Context:
    ctx_id = el.get("id", "")
    entity_id = None
    instant = start = end = None
    dimensions: dict[str, str] = {}
    for child in el.iter():
        tag = _local(child.tag)
        if tag == "identifier" and entity_id is None:
            entity_id = (child.text or "").strip() or None
        elif tag == "instant":
            instant = _parse_date(child.text)
        elif tag == "startDate":
            start = _parse_date(child.text)
        elif tag == "endDate":
            end = _parse_date(child.text)
        elif tag == "explicitMember":
            dim = child.get("dimension")
            member = (child.text or "").strip()
            if dim and member:
                dimensions[dim] = member
    return Context(
        id=ctx_id,
        entity_id=entity_id,
        instant=instant,
        start=start,
        end=end,
        dimensions=dimensions,
    )


def _parse_root(content: bytes | str) -> etree._Element:
    if isinstance(content, str):
        content = content.encode("utf-8", errors="replace")
    parser = etree.XMLParser(recover=True, huge_tree=True, resolve_entities=False)
    root = etree.fromstring(content, parser=parser)
    if root is None:
        raise ValueError("could not parse XBRL document")
    return root


def parse_xbrl(content: bytes | str) -> XbrlDocument:
    """Parse a *plain* XBRL instance (facts are concept-tagged elements).

    Unlike iXBRL, each fact is its own element whose tag is the concept and whose
    text is the value (e.g. `<fi_met:md103 contextRef="c" unitRef="u">123</...>`).
    The line item is often identified by concept + the context's dimension member
    (the Finnish CRR taxonomy), so contexts carry their explicitMember dimensions.
    """
    root = _parse_root(content)
    contexts: dict[str, Context] = {}
    facts: list[Fact] = []
    for el in root.iter():
        tag = el.tag
        if not isinstance(tag, str):
            continue
        if tag == f"{{{XBRLI_NS}}}context":
            ctx = _parse_context(el)
            contexts[ctx.id] = ctx
            continue
        if el.get("contextRef") is None:
            continue
        qname = etree.QName(el)
        if qname.namespace in _NON_FACT_NS:
            continue
        facts.append(
            Fact(
                concept=qname.localname,
                namespace=qname.namespace or "",
                context_ref=el.get("contextRef", ""),
                value=_parse_number("".join(el.itertext()), scale=None, sign=None),
                unit_ref=el.get("unitRef"),
                prefix=el.prefix,
            )
        )
    return XbrlDocument(contexts=contexts, facts=facts)


def parse_ixbrl(content: bytes | str) -> XbrlDocument:
    """Parse an iXBRL document into contexts + numeric facts."""
    root = _parse_root(content)
    if root is None:
        raise ValueError("could not parse iXBRL document")

    contexts: dict[str, Context] = {}
    facts: list[Fact] = []
    for el in root.iter():
        tag = el.tag
        if not isinstance(tag, str):
            continue  # comments / PIs
        if tag == f"{{{XBRLI_NS}}}context":
            ctx = _parse_context(el)
            contexts[ctx.id] = ctx
        elif tag == f"{{{IX_NS}}}nonFraction":
            name = el.get("name")
            if not name:
                continue
            namespace, local = _split_qname(name, el.nsmap)
            value = _parse_number(
                "".join(el.itertext()), scale=el.get("scale"), sign=el.get("sign")
            )
            facts.append(
                Fact(
                    concept=local,
                    namespace=namespace,
                    context_ref=el.get("contextRef", ""),
                    value=value,
                    unit_ref=el.get("unitRef"),
                )
            )
    return XbrlDocument(contexts=contexts, facts=facts)
