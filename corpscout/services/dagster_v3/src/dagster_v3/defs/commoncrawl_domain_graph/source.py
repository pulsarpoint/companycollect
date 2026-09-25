"""Resolve an immutable Common Crawl domain graph and its published row counts."""

import re
from dataclasses import dataclass

from dlt.sources.helpers import requests

BASE_URL = "https://data.commoncrawl.org/projects/hyperlinkgraph"


def validate_graph_release(value: str) -> str:
    if not re.fullmatch(r"cc-main-\d{4}(?:-\d{2})?-[a-z]+(?:-[a-z]+)*", value):
        raise ValueError(
            "Expected a Common Crawl release such as cc-main-2026-jun-jul-aug"
        )
    return value


@dataclass(frozen=True)
class GraphSource:
    graph_release: str
    nodes: int
    edges: int
    loops: int
    vertices_url: str
    edges_url: str
    vertices_etag: str
    edges_etag: str
    vertices_bytes: int
    edges_bytes: int
    source_index_url: str


def parse_graph_stats(text: str) -> dict[str, int]:
    fields = dict(line.split("=", 1) for line in text.splitlines() if "=" in line)
    try:
        result = {
            "nodes": int(fields["nodes"]),
            "edges": int(fields["arcs"]),
            "loops": int(fields["loops"]),
        }
    except (KeyError, ValueError) as error:
        raise ValueError(
            "Graph statistics must contain integer nodes, arcs and loops"
        ) from error
    if (
        not 0 < result["nodes"] <= 2**32
        or result["edges"] <= 0
        or not 0 <= result["loops"] <= result["edges"]
    ):
        raise ValueError("Empty, invalid or unsupported graph statistics")
    return result


def resolve_graph_source(release: str) -> GraphSource:
    validate_graph_release(release)
    prefix = f"{BASE_URL}/{release}"
    index_url = f"{prefix}/index.html"
    vertices_url = f"{prefix}/domain/{release}-domain-vertices.txt.gz"
    edges_url = f"{prefix}/domain/{release}-domain-edges.txt.gz"
    with requests.Session() as session:
        index = session.get(index_url, timeout=60)
        index.raise_for_status()
        for url in (vertices_url, edges_url):
            if url.rsplit("/", 1)[-1] not in index.text:
                raise ValueError(
                    f"Requested graph file is absent from release index: {url}"
                )
        stats = session.get(f"{prefix}/domain/{release}-domain.stats", timeout=60)
        stats.raise_for_status()
        counts = parse_graph_stats(stats.text)
        headers = []
        for url in (vertices_url, edges_url):
            response = session.head(url, timeout=60)
            response.raise_for_status()
            size = int(response.headers.get("Content-Length", "0"))
            etag = response.headers.get("ETag", "")
            if size <= 0 or not etag:
                raise ValueError(
                    f"Graph file must have a positive length and ETag: {url}"
                )
            headers.append((size, etag))
    return GraphSource(
        release,
        counts["nodes"],
        counts["edges"],
        counts["loops"],
        vertices_url,
        edges_url,
        headers[0][1],
        headers[1][1],
        headers[0][0],
        headers[1][0],
        index_url,
    )
