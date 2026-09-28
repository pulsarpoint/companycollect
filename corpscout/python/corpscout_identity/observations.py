"""Register actual requested/visited resources, never arbitrary extracted links."""

import json
from datetime import datetime
from urllib.parse import urljoin, urlsplit

from corpscout_identity.registration import (
    MAX_REGISTRATION_BATCH,
    WebsiteObservation,
    identify_website,
    register_websites,
)
from corpscout_identity.urls import website_reference


def timestamp(value) -> datetime | None:
    if value is None or value == "":
        return None
    result = (
        value
        if isinstance(value, datetime)
        else datetime.fromisoformat(value.replace("Z", "+00:00"))
    )
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError("Crawl observation timestamp requires a timezone")
    return result


def crawl_observations(
    result: dict, *, requested_url: str, website_id: str, discovered_at: datetime
) -> list[WebsiteObservation]:
    """Return identities for one durable attempt without changing its requested site.

    Failed requested pages are still valid identities. Successful-fetch timestamps
    only come from recorded fetched pages; canonical tags and outgoing links are
    not visited resources. Nested basic/full captures retain their own pages.
    """
    website_reference(requested_url, website_id)
    observations = [
        WebsiteObservation(identify_website(requested_url), discovered_at, None, None)
    ]
    captures = [result]
    captures.extend(
        result[key]
        for key in ("site_info_result", "crawl_result")
        if isinstance(result.get(key), dict)
    )
    for capture in captures:
        if capture.get("website_id") is not None:
            website_reference(requested_url, capture["website_id"])
        crawl = capture.get("crawl") or capture
        if isinstance(crawl, str):
            crawl = json.loads(crawl)
        if not isinstance(crawl, dict):
            raise ValueError("Crawl data must be an object")
        pages = crawl.get("pages") or []
        if isinstance(pages, str):
            pages = json.loads(pages)
        if not isinstance(pages, list):
            raise ValueError("Crawl pages must be an array")
        details = capture.get("page_observations") or []
        if isinstance(details, str):
            details = json.loads(details)
        documents = capture.get("documents") or []
        if isinstance(documents, str):
            documents = json.loads(documents)
        if not isinstance(details, list) or not isinstance(documents, list):
            raise ValueError("Crawl observations and documents must be arrays")
        details = [
            *details,
            *(
                (document.get("input") or {}).get("observations", {})
                for document in documents
                if isinstance(document, dict)
            ),
        ]
        observed_urls = {
            item.get("page_id"): item.get("source_url")
            for item in details
            if isinstance(item, dict)
        }
        for page in pages:
            if not isinstance(page, dict):
                raise ValueError("Crawl page must be an object")
            fetched = timestamp(page.get("fetched_at"))
            visited = (
                page.get("source_url")
                or page.get("url")
                or observed_urls.get(page.get("page_id"))
            )
            requested = page.get("requested_url")
            if requested:
                observations.append(
                    WebsiteObservation(
                        identify_website(requested), discovered_at, None, None
                    )
                )
            if visited:
                observations.append(
                    WebsiteObservation(
                        identify_website(visited),
                        discovered_at,
                        fetched,
                        fetched if page.get("fetch_status") == "fetched" else None,
                    )
                )
            for redirect in page.get("redirects") or []:
                if redirect.get("url"):
                    observations.append(
                        WebsiteObservation(
                            identify_website(redirect["url"]), discovered_at, None, None
                        )
                    )
                # Register only recorded navigation targets, not links on the page.
                if redirect.get("location") and redirect.get("url"):
                    target = urljoin(redirect["url"], redirect["location"])
                    if urlsplit(target).scheme in {"http", "https"}:
                        observations.append(
                            WebsiteObservation(
                                identify_website(target), discovered_at, None, None
                            )
                        )
    return observations


def register_crawl_results(
    client,
    results: list[dict],
    *,
    source: str,
    run_id: str,
    processing_url: str | None = None,
) -> None:
    """Validate the complete batch before publishing any child results."""
    observations = []
    for result in results:
        if not result.get("website_id"):
            raise ValueError("Crawl result requires a registered website reference")
        finished = timestamp(result["finished_at"])
        if finished is None:
            raise ValueError("Crawl result requires a completion timestamp")
        observations.extend(
            crawl_observations(
                result,
                requested_url=result["website_url"],
                website_id=result["website_id"],
                discovered_at=finished,
            )
        )
    for start in range(0, len(observations), MAX_REGISTRATION_BATCH):
        register_websites(
            client,
            observations[start : start + MAX_REGISTRATION_BATCH],
            source=source,
            run_id=run_id,
            processing_url=processing_url,
        )
