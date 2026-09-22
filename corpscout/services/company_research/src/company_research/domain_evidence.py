"""Project saved link occurrences for relationship analysis without recrawling."""

from urllib.parse import urlsplit

import tldextract

EXTRACTION_VERSION = "website-domain-context-v1"
EXTRACT = tldextract.TLDExtract(suffix_list_urls=(), include_psl_private_domains=True)


def external_domain_evidence(payload: dict) -> list[dict] | None:
    observations = payload.get("external_links")
    if observations is not None and not isinstance(observations, list):
        raise ValueError("external_links must be an array or null")
    observations = list(observations or [])
    has_inventory = payload.get("external_links") is not None
    for document in payload.get("documents", []):
        captured = document.get("input", {})
        if "links" not in captured:
            continue
        has_inventory = True
        page = captured["page"]
        for link in captured["links"]:
            observations.append({
                **link, **link.get("context", {}),
                "link_id": f"{page['page_id']}:{link['link_id']}",
                "source_page_id": page["page_id"], "source_url": page["source_url"],
                "fetched_at": page.get("fetched_at", ""),
                "html_sha256": page.get("link_html_sha256") or page.get("html_sha256", ""),
                "extraction_method": link.get("inventory_source", ""),
                "page_title": captured.get("observations", {}).get("metadata", {}).get("title", ""),
            })
    output = []
    for item in observations:
        source = urlsplit(item.get("source_url") or "")
        destination = urlsplit(item.get("url") or "")
        if (source.scheme not in {"http", "https"} or destination.scheme not in {"http", "https"}
                or source.username or destination.username or not source.hostname or not destination.hostname):
            continue
        source_host = source.hostname.rstrip(".").encode("idna").decode().lower().removeprefix("www.")
        target_host = destination.hostname.rstrip(".").encode("idna").decode().lower()
        source_domain = EXTRACT(source_host).top_domain_under_public_suffix
        destination_domain = EXTRACT(target_host).top_domain_under_public_suffix
        if not source_domain or not destination_domain or source_domain == destination_domain:
            continue
        output.append({**item, "source_host": source_host, "source_domain": source_domain,
                       "destination_host": target_host, "destination_domain": destination_domain,
                       "context_version": EXTRACTION_VERSION})
    return output if has_inventory else None
