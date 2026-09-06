"""Wikimedia HTTP boundary and deterministic article-to-row normalization."""

import re
import time
from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import quote, urljoin, urlsplit

import dagster as dg
from dlt.sources.helpers.requests import Client
from lxml import html
from pydantic import Field, field_validator
from requests import Response

ARTICLE_TABLE = "wikidata_company_wikipedia_articles"
ARTICLE_BUCKET = "source-wikipedia-articles-weekly"
ARTICLE_COLUMNS = (
    "wikidata_id",
    "site_id",
    "language_code",
    "wikipedia_page_id",
    "wikipedia_revision_id",
    "wikipedia_revision_at",
    "article_title",
    "article_url",
    "article_revision_url",
    "article_lead_text",
    "article_text",
    "content_format",
    "license_name",
    "license_url",
    "source_system",
    "source_run_id",
    "source_record_id",
    "retrieved_at",
    "resolved_at",
)
MAX_PAGE_BYTES = 32 * 1024 * 1024


class WikipediaSnapshotConfig(dg.Config):
    source_run_id: str = Field(
        description="Completed Wikidata source snapshot date, YYYY-MM-DD."
    )
    request_delay_seconds: float = Field(default=0.2, ge=0.2)
    request_timeout_seconds: int = Field(default=60, ge=1, le=300)
    user_agent: str = Field(
        default="CorpscoutWikipedia/1.0 (https://corpscout.com; company research)",
        min_length=20,
    )

    @field_validator("source_run_id")
    @classmethod
    def validate_source_run_id(cls, value: str) -> str:
        if date.fromisoformat(value).isoformat() != value:
            raise ValueError("source_run_id must be a YYYY-MM-DD snapshot date")
        return value


def wikipedia_host(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not re.fullmatch(
        r"[a-z0-9-]+\.wikipedia\.org", parsed.netloc
    ):
        raise ValueError(f"Not a canonical HTTPS Wikipedia URL: {url}")
    return parsed.netloc


def wikipedia_sitelinks(
    wikidata_id: str, entity: dict[str, Any]
) -> list[dict[str, str]]:
    targets = []
    for site_id, link in sorted(entity.get("sitelinks", {}).items()):
        if not site_id.endswith("wiki") or site_id in {
            "commonswiki",
            "specieswiki",
            "metawiki",
            "mediawikiwiki",
            "wikidatawiki",
            "incubatorwiki",
        }:
            continue
        host = wikipedia_host(link["url"])
        if not link["title"]:
            raise ValueError(f"Empty Wikipedia title for {wikidata_id}:{site_id}")
        targets.append(
            {
                "wikidata_id": wikidata_id,
                "site_id": site_id,
                "language_code": host.removesuffix(".wikipedia.org"),
                "article_title": link["title"],
                "article_url": link["url"],
            }
        )
    return targets


def _mediawiki_retry(response: Response | None, error: BaseException | None) -> bool:
    if response is None or response.status_code != 200:
        return False
    if "json" not in response.headers.get("Content-Type", ""):
        return False
    payload = response.json()
    api_error = payload.get("error") if isinstance(payload, dict) else None
    return isinstance(api_error, dict) and api_error.get("code") in {
        "maxlag",
        "ratelimited",
        "readonly",
        "internal_api_error_DBQueryError",
    }


class WikipediaClient:
    """Serial, rate-limited requests; dlt retries the complete non-streaming response read."""

    def __init__(self, config: WikipediaSnapshotConfig) -> None:
        self.delay = config.request_delay_seconds
        self.http = Client(
            request_timeout=config.request_timeout_seconds,
            request_max_attempts=5,
            raise_for_status=False,
            retry_condition=_mediawiki_retry,
            session_attrs={"headers": {"User-Agent": config.user_agent}},
        )

    def entities(self, qids: list[str]) -> dict[str, Any]:
        if (
            not qids
            or len(qids) > 50
            or any(re.fullmatch(r"Q[1-9][0-9]*", qid) is None for qid in qids)
        ):
            raise ValueError("Wikidata entity requests require 1–50 valid QIDs")
        time.sleep(self.delay)
        response = self.http.get(
            "https://www.wikidata.org/w/api.php",
            params={
                "action": "wbgetentities",
                "ids": "|".join(qids),
                "props": "sitelinks/urls",
                "format": "json",
                "formatversion": 2,
                "maxlag": 5,
            },
            allow_redirects=False,
        )
        response.raise_for_status()
        payload = response.json()
        if "error" in payload or set(payload.get("entities", {})) != set(qids):
            raise ValueError("Incomplete or unsuccessful Wikidata sitelink response")
        return payload

    def article(self, target: dict[str, str]) -> dict[str, Any]:
        host = wikipedia_host(target["article_url"])
        url = f"https://{host}/w/rest.php/v1/page/{quote(target['article_title'], safe='')}/with_html"
        for _ in range(6):
            time.sleep(self.delay)
            response = self.http.get(url, allow_redirects=False)
            if response.status_code in {301, 302, 303, 307, 308}:
                url = urljoin(url, response.headers["Location"])
                if wikipedia_host(url) != host:
                    raise ValueError(
                        "Wikipedia article redirect changed language edition"
                    )
                continue
            if response.status_code in {404, 410}:
                error = response.json()
                if error.get("errorKey") != "rest-nonexistent-title":
                    raise ValueError(
                        f"Wikipedia endpoint failed without a confirmed missing page: {url}"
                    )
                return {
                    "target": target,
                    "status": "missing",
                    "http_status": response.status_code,
                    "error": error,
                    "retrieved_at": datetime.now(UTC).isoformat(),
                }
            response.raise_for_status()
            if len(response.content) > MAX_PAGE_BYTES:
                raise ValueError(
                    f"Wikipedia response exceeds {MAX_PAGE_BYTES} bytes: {url}"
                )
            payload = response.json()
            record = {
                "target": target,
                "status": "ok",
                "page": payload,
                "retrieved_at": datetime.now(UTC).isoformat(),
            }
            # Reject malformed or empty success payloads before checkpointing them.
            article_row(
                record, source_run_id="1970-01-01", resolved_at=datetime.now(UTC)
            )
            return record
        raise ValueError(f"Too many Wikipedia redirects: {target['article_url']}")


def _text(node: html.HtmlElement) -> str:
    return " ".join(node.text_content().split())


def _article_blocks(node: html.HtmlElement) -> Iterator[str]:
    if node.tag == "table":
        for row in node.xpath(".//tr"):
            cells = [_text(cell) for cell in row.xpath("./th | ./td")]
            if any(cells):
                yield " | ".join(cells)
    elif node.tag in {"p", "h1", "h2", "h3", "h4", "h5", "h6", "li", "dt", "dd", "pre"}:
        text = _text(node)
        if text:
            yield text
    else:
        if node.text and node.text.strip():
            yield " ".join(node.text.split())
        for child in node:
            yield from _article_blocks(child)
            if child.tail and child.tail.strip():
                yield " ".join(child.tail.split())


def extract_article_text(raw_html: str) -> tuple[str, str]:
    root = html.document_fromstring(raw_html)
    excluded_classes = {
        "navbox",
        "vertical-navbox",
        "sidebar",
        "hatnote",
        "reflist",
        "references",
        "reference",
        "mw-editsection",
        "toc",
        "metadata",
        "mw-empty-elt",
    }
    for element in list(root.iter()):
        if element.tag in {
            "script",
            "style",
            "noscript",
            "nav",
            "footer",
            "figure",
            "link",
            "meta",
        } or excluded_classes.intersection(element.get("class", "").split()):
            if element.getparent() is not None:
                element.drop_tree()
    body = root.find("body")
    if body is None:
        raise ValueError("Wikipedia HTML has no body")
    lead_sections = body.xpath('.//section[@data-mw-section-id="0"]')
    lead_root = lead_sections[0] if lead_sections else body
    lead_paragraphs = []
    for element in lead_root.iter():
        if element.tag == "h2":
            break
        if element.tag == "p" and not element.xpath("ancestor::table"):
            text = _text(element)
            if text:
                lead_paragraphs.append(text)
    full_text = "\n\n".join(_article_blocks(body))
    if not full_text:
        raise ValueError("Wikipedia article produced empty text")
    return "\n\n".join(lead_paragraphs), full_text


def article_row(
    record: dict[str, Any], *, source_run_id: str, resolved_at: datetime
) -> dict[str, Any]:
    target, page = record["target"], record["page"]
    host = wikipedia_host(target["article_url"])
    if not page["title"] or int(page["id"]) <= 0 or int(page["latest"]["id"]) <= 0:
        raise ValueError("Wikipedia page/revision identity is missing")
    if not page["license"]["title"] or not page["license"]["url"]:
        raise ValueError("Wikipedia article license is missing")
    lead, text = extract_article_text(page["html"])
    return {
        "wikidata_id": target["wikidata_id"],
        "site_id": target["site_id"],
        "language_code": target["language_code"],
        "wikipedia_page_id": int(page["id"]),
        "wikipedia_revision_id": int(page["latest"]["id"]),
        "wikipedia_revision_at": datetime.fromisoformat(page["latest"]["timestamp"]),
        "article_title": page["title"],
        "article_url": f"https://{host}/wiki/{quote(page['title'].replace(' ', '_'), safe='')}",
        "article_revision_url": f"https://{host}/w/index.php?oldid={page['latest']['id']}",
        "article_lead_text": lead,
        "article_text": text,
        "content_format": "text/plain",
        "license_name": page["license"]["title"],
        "license_url": page["license"]["url"],
        "source_system": "wikipedia",
        "source_run_id": source_run_id,
        "source_record_id": f"{target['wikidata_id']}:{target['site_id']}",
        "retrieved_at": datetime.fromisoformat(record["retrieved_at"]),
        "resolved_at": resolved_at,
    }
