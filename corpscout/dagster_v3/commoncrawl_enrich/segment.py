"""Per-file CommonCrawl segment processors.

Two independent functions — one for WET, one for WARC — each takes a single file
(local path or `data.commoncrawl.org` URL), processes it fully, and writes ONE
Parquet of per-record results. They run as separate apps/loops at their own speed
(WARC is slower); their outputs join downstream by `url` (WET↔WARC map 1:1).

- WET (plaintext): url, domain, subdomain, emails, + industry (optional LLM).
- WARC (full HTML+headers): url, domain, subdomain, emails (incl. mailto), socials, technologies.

A thin download→process→remove wrapper over a file list is intentionally left to the
caller (the per-file functions are the reusable unit).
"""

import logging
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import requests
import tldextract
from warcio.archiveiterator import ArchiveIterator

from commoncrawl_enrich import extract, tech
from commoncrawl_enrich.llm import LLMArm

LOGGER = logging.getLogger(__name__)
USER_AGENT = "corpscout-commoncrawl-enrich/0.1 (goran.raovic@gmail.com)"
DATA_HOST = "https://data.commoncrawl.org"
COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"  # all crawls, newest first
_TE = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)  # bundled PSL, no network


def wet_url_to_warc(url: str) -> str:
    """Map a WET file URL to its 1:1 corresponding WARC file URL (same segment+name)."""
    return url.replace("/wet/", "/warc/").replace(".warc.wet.gz", ".warc.gz")


def latest_crawl(session: requests.Session | None = None) -> str:
    """Id of the newest CommonCrawl crawl (first entry of collinfo.json), e.g. 'CC-MAIN-2026-25'."""
    http = session or requests.Session()
    resp = http.get(COLLINFO_URL, headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    return resp.json()[0]["id"]


def first_wet_url(crawl: str | None = None, segment_index: int = 0,
                  session: requests.Session | None = None) -> str:
    """The Nth WET file URL of a crawl (latest crawl when `crawl` is None), from wet.paths.gz."""
    import gzip

    http = session or requests.Session()
    if not crawl:
        crawl = latest_crawl(http)
    resp = http.get(f"{DATA_HOST}/crawl-data/{crawl}/wet.paths.gz",
                    headers={"User-Agent": USER_AGENT}, timeout=60)
    resp.raise_for_status()
    paths = gzip.decompress(resp.content).decode().splitlines()
    return f"{DATA_HOST}/{paths[segment_index]}"


def _open_stream(source: str, session: requests.Session | None):
    """Binary stream for a local path or an http(s) URL (gzip handled by warcio)."""
    if str(source).startswith(("http://", "https://")):
        http = session or requests.Session()
        resp = http.get(str(source), stream=True, headers={"User-Agent": USER_AGENT}, timeout=300)
        resp.raise_for_status()
        resp.raw.decode_content = False
        return resp.raw
    return open(source, "rb")


def _host(uri: str) -> tuple[str, str]:
    parts = _TE(uri)
    return parts.registered_domain, parts.subdomain


_WET_SCHEMA = pa.schema([
    ("url", pa.string()), ("root_domain", pa.string()), ("subdomain", pa.string()),
    ("emails", pa.list_(pa.string())), ("email_count", pa.int32()),
    ("industry_label", pa.string()), ("industry_nace_hint", pa.string()),
    ("industry_confidence", pa.int32()),
])

_WARC_SCHEMA = pa.schema([
    ("url", pa.string()), ("root_domain", pa.string()), ("subdomain", pa.string()),
    ("emails", pa.list_(pa.string())), ("social_platforms", pa.list_(pa.string())),
    ("technologies", pa.list_(pa.string())),
])


def _is_homepage(url: str) -> bool:
    """True if the URL points at a site root (path '' or '/') — the per-domain 'main page'."""
    try:
        return urlparse(url).path in ("", "/")
    except ValueError:  # garbage URLs (invalid NFKC) -> not a homepage
        return False


def process_wet_file(
    source: str,
    out_path: str | Path,
    *,
    llm: LLMArm | None = None,
    industry_workers: int = 32,
    limit: int | None = None,
    session: requests.Session | None = None,
    homepages_only: bool = True,
) -> dict:
    """Process one WET file → one Parquet.

    Per record: url/root_domain/subdomain/emails + optional LLM industry. With
    `homepages_only` (default), only the per-domain main page (path '/') is kept —
    industry is a domain property, so this avoids classifying every page. WARC
    handles per-page tech/socials/contacts across all pages.
    """
    stream = _open_stream(source, session)
    rows: list[dict] = []
    industry_inputs: list[tuple[int, str]] = []
    try:
        for record in ArchiveIterator(stream):
            if record.rec_type != "conversion":
                continue
            uri = record.rec_headers.get_header("WARC-Target-URI") or ""
            if homepages_only and not _is_homepage(uri):
                continue
            text = record.content_stream().read().decode("utf-8", "replace")
            root, sub = _host(uri)
            emails = [e.email for e in extract.extract_emails(text)]
            rows.append({
                "url": uri, "root_domain": root, "subdomain": sub,
                "emails": emails, "email_count": len(emails),
                "industry_label": "", "industry_nace_hint": "", "industry_confidence": 0,
            })
            if llm is not None:
                industry_inputs.append((len(rows) - 1, text))
            if limit and len(rows) >= limit:
                break
    finally:
        stream.close()

    if llm is not None and industry_inputs:
        def _classify(item: tuple[int, str]) -> None:
            index, text = item
            guess = llm.classify_industry(text)
            rows[index].update(
                industry_label=guess.label, industry_nace_hint=guess.nace_hint,
                industry_confidence=guess.confidence,
            )
        with ThreadPoolExecutor(max_workers=industry_workers) as pool:
            list(pool.map(_classify, industry_inputs))

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=_WET_SCHEMA), out_path)
    return {
        "records": len(rows),
        "with_email": sum(1 for r in rows if r["emails"]),
        "with_industry": sum(1 for r in rows if r["industry_label"]),
        "out": str(out_path),
    }


def process_warc_file(
    source: str,
    out_path: str | Path,
    *,
    limit: int | None = None,
    session: requests.Session | None = None,
) -> dict:
    """Process one WARC file → one Parquet (url/domain/emails/socials/technologies)."""
    stream = _open_stream(source, session)
    rows: list[dict] = []
    try:
        for record in ArchiveIterator(stream):
            if record.rec_type != "response":
                continue
            http = record.http_headers
            content_type = (http.get_header("Content-Type") or "").lower() if http else ""
            if "html" not in content_type:
                continue
            html = record.content_stream().read().decode("utf-8", "replace")
            uri = record.rec_headers.get_header("WARC-Target-URI") or ""
            root, sub = _host(uri)
            parsed = extract.parse_html(html)
            headers = {k: v for k, v in (http.headers if http else [])}
            rows.append({
                "url": uri, "root_domain": root, "subdomain": sub,
                "emails": [e.email for e in extract.extract_emails(html)],  # raw html catches mailto:
                "social_platforms": sorted({s.platform for s in extract.extract_socials(parsed.links)}),
                "technologies": [t.technology for t in tech.detect_technologies(html, headers)],
            })
            if limit and len(rows) >= limit:
                break
    finally:
        stream.close()

    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows, schema=_WARC_SCHEMA), out_path)
    return {
        "records": len(rows),
        "with_social": sum(1 for r in rows if r["social_platforms"]),
        "with_tech": sum(1 for r in rows if r["technologies"]),
        "out": str(out_path),
    }
