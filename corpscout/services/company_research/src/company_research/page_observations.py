"""Deterministic observations from captured HTML; no requests or company inference."""

import json
import re
from collections.abc import Iterator
from urllib.parse import unquote, urlsplit

import phonenumbers
import trafilatura
from bs4 import BeautifulSoup, Tag

from company_research.external_links import (
    attribute_tokens,
    link_context,
    resolved_http_url,
)
from company_research.identifiers import (
    IDENTIFIER_PROPERTIES,
    LEI_TOKEN,
    TRACKER_PATTERNS,
    VAT_TOKEN,
    identifier_validation,
)
from company_research.storage import content_hash

EMAIL = re.compile(r"[\w.+-]+@(?:[A-Za-z0-9-]+\.)+[A-Za-z]{2,24}", re.ASCII)
ASSET_EXTENSIONS = set(
    "png jpg jpeg gif svg webp avif ico bmp css js mjs json map woff woff2 ttf otf "
    "eot mp4 webm mov mp3 wav pdf zip gz html htm php".split()
)
SOCIAL_HOSTS = {
    "facebook.com": "facebook",
    "fb.com": "facebook",
    "twitter.com": "twitter",
    "x.com": "twitter",
    "linkedin.com": "linkedin",
    "instagram.com": "instagram",
    "youtube.com": "youtube",
    "youtu.be": "youtube",
    "tiktok.com": "tiktok",
    "pinterest.com": "pinterest",
    "github.com": "github",
}
RESPONSE_HEADERS = set(
    "content-type content-language server x-powered-by x-generator via "
    "strict-transport-security content-security-policy content-security-policy-report-only "
    "x-frame-options x-content-type-options referrer-policy permissions-policy "
    "cross-origin-opener-policy cross-origin-embedder-policy cross-origin-resource-policy "
    "cache-control expires last-modified etag x-robots-tag link".split()
)
FINANCIAL_LINK = re.compile(
    r"\b(?:financials?|annual\s+reports?|investor(?:s|\s+relations)?|"
    r"shareholder(?:s|\s+information)?|sec\s+filings?|"
    r"accounts\s+of\s+subsidiar(?:ies|y)|"
    r"finansij\w*|financij\w*|финансиј\w*|"
    r"godišnj\w*\s+izvje?št\w*|godisnj\w*\s+izvje?st\w*|"
    r"годишњ\w*\s+извешт\w*)\b",
    re.IGNORECASE,
)


def public_response_headers(headers: dict | None) -> dict[str, str]:
    """Keep content, technology and security signals without cookies/session headers."""
    return {
        name.lower(): ", ".join(map(str, value))
        if isinstance(value, list)
        else str(value)
        for name, value in (headers or {}).items()
        if name.lower() in RESPONSE_HEADERS
    }


def observation_hash(observations: dict) -> str:
    return content_hash(
        json.dumps(observations, ensure_ascii=False, sort_keys=True, allow_nan=False)
    )


def json_nodes(value: object) -> Iterator[tuple[str, dict]]:
    """Iterative RFC 6901 paths, excluding vocabulary definitions from entity discovery."""
    pending = [("", value)]
    while pending:
        path, node = pending.pop()
        if isinstance(node, dict):
            yield path, node
            pending.extend(
                (path + "/" + key.replace("~", "~0").replace("/", "~1"), node[key])
                for key in sorted(node, reverse=True)
                if key != "@context"
            )
        elif isinstance(node, list):
            pending.extend((f"{path}/{i}", node[i]) for i in reversed(range(len(node))))


def string_values(value: object) -> Iterator[str]:
    if isinstance(value, str):
        if value.strip():
            yield value.strip()
    elif isinstance(value, list):
        for item in value:
            yield from string_values(item)
    elif isinstance(value, dict):
        for key in ("@value", "value", "name", "url", "@id"):
            if isinstance(value.get(key), str):
                yield from string_values(value[key])
                break


def microdata_items(soup: BeautifulSoup, base: str) -> list[dict]:
    """Retain every scope and its properties; nested entities stay addressable by index."""
    scopes = soup.find_all(True, attrs={"itemscope": True})
    indexes = {id(tag): index for index, tag in enumerate(scopes)}
    items = []
    for index, scope in enumerate(scopes):
        properties: dict[str, list] = {}
        pending = list(reversed(list(scope.children)))
        for reference in reversed(attribute_tokens(scope, "itemref")):
            referenced = soup.find(id=reference)
            if referenced is not None:
                pending.append(referenced)
        visited = {id(scope)}
        while pending:
            tag = pending.pop()
            if not isinstance(tag, Tag) or id(tag) in visited:
                continue
            visited.add(id(tag))
            names = attribute_tokens(tag, "itemprop")
            if tag.has_attr("itemscope"):
                value: object = {"entity_index": indexes[id(tag)]}
            else:
                url_attribute = (
                    "href"
                    if tag.name in {"a", "area", "link"}
                    else "src"
                    if tag.name
                    in {"audio", "embed", "iframe", "img", "source", "track", "video"}
                    else "data"
                    if tag.name == "object"
                    else None
                )
                if url_attribute is not None:
                    raw = str(tag.get(url_attribute, ""))
                    value = resolved_http_url(raw, base) or raw
                elif tag.name == "meta":
                    value = str(tag.get("content", ""))
                elif tag.name in {"data", "meter"}:
                    value = str(tag.get("value", ""))
                elif tag.name == "time" and tag.has_attr("datetime"):
                    value = str(tag["datetime"])
                else:
                    value = tag.get_text(" ", strip=True)
                pending.extend(reversed(list(tag.children)))
            for name in names:
                properties.setdefault(name, []).append(value)
        raw_id = str(scope.get("itemid", ""))
        items.append(
            {
                "entity_index": index,
                "types": attribute_tokens(scope, "itemtype"),
                "id": (resolved_http_url(raw_id, base) or raw_id) if raw_id else None,
                "properties": properties,
            }
        )
    return items


def collect_technology_observations(
    html: str, base: str
) -> tuple[list[dict], list[dict]]:
    """Preserved for offline processing; deliberately not called during collection."""
    trackers = []
    for kind, pattern in TRACKER_PATTERNS.items():
        for match in re.finditer(pattern, html):
            trackers.append(
                {
                    "type": kind,
                    "value": match[1],
                    "source": "html",
                    "start": match.start(1),
                    "end": match.end(1),
                    "validation": "format",
                }
            )
    resources = []
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup.find_all(["script", "iframe", "link"]):
        attribute = "href" if tag.name == "link" else "src"
        if tag.name == "link" and "stylesheet" not in attribute_tokens(tag, "rel"):
            continue
        raw = str(tag.get(attribute, ""))
        url = resolved_http_url(raw, base) if raw else None
        if url is not None:
            resources.append(
                {"kind": "stylesheet" if tag.name == "link" else tag.name, "url": url}
            )
    return trackers, resources


def collect_page_observations(
    html: str,
    *,
    page_id: str,
    source_url: str,
    representation: str,
    response_headers: dict | None = None,
) -> dict:
    """Extract the full capture before cleanup can remove metadata and structured data."""
    soup = BeautifulSoup(html, "html.parser")
    base_tag = soup.find("base", href=True)
    base = resolved_http_url(str(base_tag["href"]), source_url) if base_tag else None
    base = base or source_url
    metadata: dict = {
        "title": soup.title.get_text(" ", strip=True) if soup.title else None,
        "language": str(soup.html.get("lang") or soup.html.get("xml:lang") or "")
        if soup.html
        else None,
        "charset": None,
        "meta": {},
        "meta_entries": [],
        "canonical_url": None,
        "alternate_languages": [],
        "links": [],
    }
    for tag in soup.find_all("meta"):
        if tag.get("charset") and metadata["charset"] is None:
            metadata["charset"] = str(tag["charset"]).strip().lower()
        key = (
            str(tag.get("name") or tag.get("property") or tag.get("http-equiv") or "")
            .strip()
            .lower()
        )
        if key and tag.has_attr("content"):
            value = str(tag["content"])
            metadata["meta"].setdefault(key, value)
            metadata["meta_entries"].append({"name": key, "content": value})
    for tag in soup.find_all("link", href=True):
        rel = [part.lower() for part in attribute_tokens(tag, "rel")]
        url = resolved_http_url(str(tag["href"]), base)
        entry = {
            "rel": rel,
            "url": url,
            "raw_href": str(tag["href"]),
            "hreflang": tag.get("hreflang"),
            "type": tag.get("type"),
        }
        metadata["links"].append(entry)
        if "canonical" in rel and metadata["canonical_url"] is None:
            metadata["canonical_url"] = url
        if "alternate" in rel and tag.get("hreflang"):
            metadata["alternate_languages"].append(entry)

    errors: list[dict] = []
    blocks, entities = [], []
    scripts = [
        tag
        for tag in soup.find_all("script")
        if str(tag.get("type", "")).split(";")[0].strip().lower()
        == "application/ld+json"
    ]
    for index, tag in enumerate(scripts):
        raw = tag.string if tag.string is not None else tag.get_text()
        try:
            data = json.loads(str(raw))
            # Python accepts NaN/Infinity in input JSON; portable results must remain strict JSON.
            json.dumps(data, allow_nan=False)
        except (ValueError, RecursionError) as error:
            blocks.append({"script_index": index, "status": "invalid", "raw": str(raw)})
            errors.append(
                {
                    "section": "jsonld",
                    "script_index": index,
                    "error": type(error).__name__,
                }
            )
            continue
        blocks.append({"script_index": index, "status": "parsed", "data": data})
        for path, node in json_nodes(data):
            types = sorted(set(string_values(node.get("@type"))))
            if types or isinstance(node.get("@id"), str):
                entities.append(
                    {
                        "script_index": index,
                        "entity_path": path,
                        "types": types,
                        "id": node.get("@id"),
                        "data": node,
                    }
                )
    microdata = microdata_items(soup, base)
    contacts, identifiers = [], []
    seen_contacts: set[tuple] = set()
    hostname = urlsplit(source_url).hostname or ""
    tld = hostname.rsplit(".", 1)[-1].upper()
    region = (
        "GB" if tld == "UK" else tld if tld in phonenumbers.SUPPORTED_REGIONS else None
    )

    def add_contact(kind: str, raw: str, source: str, locator: object) -> None:
        value = raw.strip()
        details: dict = {}
        if kind == "email":
            value = re.sub(r"^mailto:", "", value, flags=re.I).split("?", 1)[0]
        elif kind == "phone":
            value = re.sub(r"^tel:", "", value, flags=re.I)
            try:
                number = phonenumbers.parse(value, region)
            except phonenumbers.NumberParseException:
                number = None
            valid = number is not None and phonenumbers.is_valid_number(number)
            details = {
                "valid": valid,
                "validation": "numbering_plan",
                "extension": number.extension if number is not None else None,
            }
            if valid:
                value = phonenumbers.format_number(
                    number, phonenumbers.PhoneNumberFormat.E164
                )
        elif kind == "profile":
            url = resolved_http_url(value, base)
            if url is None:
                return
            value = url
            host = urlsplit(url).hostname or ""
            details["platform"] = next(
                (
                    platform
                    for domain, platform in SOCIAL_HOSTS.items()
                    if host == domain or host.endswith("." + domain)
                ),
                None,
            )
        key = (kind, value, source, str(locator), details.get("extension"))
        if value and key not in seen_contacts:
            seen_contacts.add(key)
            contacts.append(
                {
                    "type": kind,
                    "value": value,
                    "raw_value": raw,
                    "source": source,
                    "locator": locator,
                    **details,
                }
            )

    for source, nodes in (
        (
            "jsonld",
            (
                (node, {"script_index": index, "entity_path": path})
                for index, block in enumerate(blocks)
                if block["status"] == "parsed"
                for path, node in json_nodes(block["data"])
            ),
        ),
        (
            "microdata",
            (
                (item["properties"], {"entity_index": item["entity_index"]})
                for item in microdata
            ),
        ),
    ):
        for node, locator in nodes:
            for field, kind in (
                ("email", "email"),
                ("telephone", "phone"),
                ("faxNumber", "phone"),
                ("sameAs", "profile"),
            ):
                for raw in string_values(node.get(field)):
                    add_contact(kind, raw, source, {**locator, "property": field})
            for field, kind in IDENTIFIER_PROPERTIES.items():
                for raw in string_values(node.get(field)):
                    value = raw.strip().upper()
                    if kind in {"lei", "vat"}:
                        value = value.replace(" ", "")
                    identifiers.append(
                        {
                            "type": kind,
                            "value": value,
                            "raw_value": raw,
                            "source": source,
                            "locator": {**locator, "property": field},
                            **identifier_validation(kind, value),
                        }
                    )

    for match in EMAIL.finditer(html):
        if match[0].rsplit(".", 1)[-1].lower() not in ASSET_EXTENSIONS:
            add_contact(
                "email", match[0], "html", {"start": match.start(), "end": match.end()}
            )
    document_links, financial_links = [], []
    for index, tag in enumerate(soup.find_all("a", href=True)):
        href = str(tag["href"])
        if href.lower().startswith("mailto:"):
            for match in EMAIL.finditer(unquote(href.split("?", 1)[0])):
                add_contact("email", match[0], "link", {"anchor_index": index})
        elif href.lower().startswith("tel:"):
            add_contact("phone", unquote(href), "link", {"anchor_index": index})
        url = resolved_http_url(href, base)
        if url is not None:
            link = {
                "url": url,
                "text": tag.get_text(" ", strip=True),
                "type": tag.get("type"),
                "anchor_index": index,
                "source_url": source_url,
            }
            context = link_context(tag)
            row_text = ""
            for parent in tag.parents:
                if parent.name in {
                    "body",
                    "html",
                    "[document]",
                    "main",
                    "nav",
                    "header",
                    "footer",
                }:
                    break
                if len(parent.find_all("a", href=True, limit=2)) > 1:
                    break
                text = parent.get_text(" ", strip=True)
                if text != link["text"]:
                    row_text = text[:1600]
                    break
            link["context"] = {
                "section_heading": context["section_heading"],
                "page_title": metadata["title"],
                "surrounding_text": row_text or link["text"],
                "context_truncated": bool(row_text and len(text) > 1600),
                "page_region": context["page_region"],
            }
            label = " ".join(
                [unquote(urlsplit(url).path), link["text"], str(tag.get("title", ""))]
            )
            direct_match = FINANCIAL_LINK.search(re.sub(r"[\W_]+", " ", label))
            context_match = FINANCIAL_LINK.search(
                re.sub(
                    r"[\W_]+",
                    " ",
                    " ".join([row_text, context["section_heading"] or ""]),
                )
            )
            is_document = urlsplit(url).path.rstrip("/").rsplit(".", 1)[-1].lower() in {
                "pdf",
                "doc",
                "docx",
                "xls",
                "xlsx",
            } or tag.has_attr("download")
            page_match = is_document and FINANCIAL_LINK.search(metadata["title"] or "")
            if direct_match or context_match or page_match:
                financial_links.append(
                    link
                    | {
                        "basis": "link_text_or_url"
                        if direct_match
                        else "surrounding_context"
                        if context_match
                        else "document_page_title",
                        "content_examined": False,
                    }
                )
            host = urlsplit(url).hostname or ""
            if any(
                host == domain or host.endswith("." + domain) for domain in SOCIAL_HOSTS
            ):
                add_contact("profile", url, "link", {"anchor_index": index})
            extension = urlsplit(url).path.rstrip("/").rsplit(".", 1)[-1].lower()
            if tag.has_attr("download") or extension in {
                "pdf",
                "doc",
                "docx",
                "xls",
                "xlsx",
                "csv",
                "ppt",
                "pptx",
                "odt",
                "rtf",
            }:
                document_links.append(link)
    # Deferred until offline processing: collect_technology_observations(html, base).
    # Keep raw HTML, metadata and headers as evidence, without a technology inventory.
    for tag in soup.find_all(["script", "style", "template", "head"]):
        tag.decompose()
    visible_text = soup.get_text(" ", strip=True)
    for kind, pattern in (("lei", LEI_TOKEN), ("vat", VAT_TOKEN)):
        for match in pattern.finditer(visible_text):
            validation = identifier_validation(kind, match[0])
            if validation["valid"]:
                identifiers.append(
                    {
                        "type": kind,
                        "value": match[0],
                        "raw_value": match[0],
                        "source": "visible_text",
                        "locator": {"start": match.start(), "end": match.end()},
                        **validation,
                    }
                )
    for match in phonenumbers.PhoneNumberMatcher(visible_text, region):
        add_contact(
            "phone",
            match.raw_string,
            "visible_text",
            {"start": match.start, "end": match.end},
        )
    try:
        main_text = trafilatura.extract(html, url=source_url, include_comments=False)
    except Exception as error:
        # A third-party text-parser failure must not discard a successful capture.
        main_text = None
        errors.append({"section": "main_text", "error": type(error).__name__})
    return {
        "schema_version": "company-page-observations/1.1",
        "page_id": page_id,
        "source_url": source_url,
        "representation": representation,
        "html_sha256": content_hash(html),
        "attribution": "page_observations; company_ownership_not_established",
        "metadata": metadata,
        "response_headers": public_response_headers(response_headers),
        "response_headers_status": "unavailable"
        if response_headers is None
        else "selected_headers",
        "text": {
            "visible": visible_text,
            "main": main_text or visible_text,
            "main_method": "trafilatura" if main_text else "visible_text_fallback",
        },
        "structured_data": {
            "jsonld_blocks": blocks,
            "jsonld_entities": entities,
            "jsonld_types": sorted(
                {kind for entity in entities for kind in entity["types"]}
            ),
            "microdata": microdata,
        },
        "contacts": contacts,
        "identifiers": identifiers,
        "technology_analysis": "deferred",
        "trackers": None,
        "resources": None,
        "document_links": document_links,
        "financial_links": financial_links,
        "errors": errors,
        "status": "partial" if errors else "collected",
    }
