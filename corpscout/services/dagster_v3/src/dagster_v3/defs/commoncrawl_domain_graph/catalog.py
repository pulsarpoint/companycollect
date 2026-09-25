"""Discover official releases and bulk-file capabilities without downloading data."""

from dataclasses import dataclass
from datetime import date, datetime
from html.parser import HTMLParser
from urllib.parse import urljoin, urlsplit

from requests.exceptions import HTTPError

from dagster_v3.defs.commoncrawl_domain_graph.source import (
    BASE_URL,
    validate_graph_release,
)

GRAPH_CATALOG_URL = "https://index.commoncrawl.org/graphinfo.json"
CRAWL_CATALOG_URL = "https://index.commoncrawl.org/collinfo.json"
ARTIFACT_SUFFIXES = {
    "nodes": "-domain-vertices.txt.gz",
    "edges": "-domain-edges.txt.gz",
    "ranks": "-domain-ranks.txt.gz",
}


@dataclass(frozen=True)
class ReleaseFile:
    artifact_kind: str
    source_url: str | None
    source_etag: str | None
    source_bytes: int | None
    expected_rows: int | None
    availability: str


@dataclass(frozen=True)
class GraphRelease:
    graph_release: str
    source_index_url: str
    crawl_ids: list[str]
    coverage_start: date | None
    coverage_end: date | None
    files: list[ReleaseFile]


class DownloadLinks(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag, attrs) -> None:
        if tag == "a":
            self.links.extend(
                value for key, value in attrs if key == "href" and value is not None
            )


def trusted_release_url(url: str, release: str, base_url: str = BASE_URL) -> str:
    """Source HTML may select files only within the requested official release."""
    parsed = urlsplit(url)
    base = urlsplit(base_url)
    prefix = base.path.rstrip("/") + "/" + release + "/"
    if (
        (parsed.scheme, parsed.netloc) != (base.scheme, base.netloc)
        or not parsed.path.startswith(prefix)
        or any(part in (".", "..") for part in parsed.path.split("/"))
        or "%" in parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"Graph URL is outside the selected release: {url}")
    return url


def crawl_coverage(
    crawl_ids: list[str], crawls: dict[str, dict]
) -> tuple[date | None, date | None]:
    # Older crawl metadata can omit dates. Do not turn partial coverage into a date claim.
    entries = [crawls.get(crawl_id) for crawl_id in crawl_ids]
    if not entries or any(
        entry is None or not entry.get("from") or not entry.get("to")
        for entry in entries
    ):
        return None, None
    starts = [datetime.fromisoformat(entry["from"]).date() for entry in entries]
    ends = [datetime.fromisoformat(entry["to"]).date() for entry in entries]
    if any(end < start for start, end in zip(starts, ends, strict=True)):
        raise ValueError("Crawl coverage ends before it starts")
    return min(starts), max(ends)


def discover_releases(
    session,
    graph_catalog_url: str = GRAPH_CATALOG_URL,
    crawl_catalog_url: str = CRAWL_CATALOG_URL,
    base_url: str = BASE_URL,
) -> list[GraphRelease]:
    """Resolve a complete metadata refresh before committing any catalog changes."""
    response = session.get(graph_catalog_url, timeout=60)
    response.raise_for_status()
    entries = response.json()
    if not isinstance(entries, list) or not entries:
        raise ValueError("Graph catalog must be a nonempty list")
    response = session.get(crawl_catalog_url, timeout=60)
    response.raise_for_status()
    crawl_entries = response.json()
    if not isinstance(crawl_entries, list) or not crawl_entries:
        raise ValueError("Crawl catalog must be a nonempty list")
    crawls = {entry["id"]: entry for entry in crawl_entries}
    releases = []
    seen = set()
    for entry in entries:
        release = validate_graph_release(entry["id"])
        if release in seen:
            raise ValueError(f"Duplicate graph release: {release}")
        seen.add(release)
        index_url = trusted_release_url(entry["index"], release, base_url)
        crawl_ids = entry["crawls"]
        if not isinstance(crawl_ids, list) or not all(
            isinstance(value, str) for value in crawl_ids
        ):
            raise ValueError(f"Invalid crawl list for {release}")
        start, end = crawl_coverage(crawl_ids, crawls)
        counts = entry.get("stats", {}).get("domain", {})
        nodes, edges = counts.get("nodes"), counts.get("arcs")
        if any(
            value is not None and (type(value) is not int or value < 0)
            for value in (nodes, edges)
        ):
            raise ValueError(f"Invalid domain graph counts for {release}")
        if nodes == 0:
            files = [
                ReleaseFile(kind, None, None, None, 0, "unsupported")
                for kind in ARTIFACT_SUFFIXES
            ]
            releases.append(
                GraphRelease(release, index_url, crawl_ids, start, end, files)
            )
            continue
        response = session.get(index_url, timeout=60, allow_redirects=False)
        response.raise_for_status()
        if response.status_code != 200:
            raise ValueError(f"Unexpected graph index response for {release}")
        parser = DownloadLinks()
        parser.feed(response.text)
        files = []
        for kind, suffix in ARTIFACT_SUFFIXES.items():
            urls = {
                trusted_release_url(urljoin(index_url, link), release, base_url)
                for link in parser.links
                if urlsplit(link).path.endswith(suffix)
            }
            if len(urls) > 1:
                raise ValueError(f"Ambiguous {kind} downloads for {release}")
            expected = edges if kind == "edges" else nodes
            if not urls:
                files.append(
                    ReleaseFile(kind, None, None, None, expected, "unavailable")
                )
                continue
            url = urls.pop()
            try:
                response = session.head(url, timeout=60, allow_redirects=False)
            except HTTPError as error:
                if error.response is None or error.response.status_code != 404:
                    raise
                response = error.response
            if response.status_code == 404:
                files.append(
                    ReleaseFile(kind, url, None, None, expected, "unavailable")
                )
                continue
            response.raise_for_status()
            if response.status_code != 200:
                raise ValueError(f"Unexpected {kind} file response for {release}")
            size = int(response.headers.get("Content-Length", "0"))
            etag = response.headers.get("ETag")
            if size <= 0 or not etag:
                raise ValueError(f"Missing HTTP validators for {release} {kind}")
            files.append(ReleaseFile(kind, url, etag, size, expected, "available"))
        releases.append(GraphRelease(release, index_url, crawl_ids, start, end, files))
    return releases
