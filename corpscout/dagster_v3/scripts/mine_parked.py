#!/usr/bin/env python
"""Mine parked / placeholder / for-sale page exemplars from a WET file (plaintext).

The classify path embeds WET plaintext, so junk prototypes must come from the SAME
modality (not HTML). This scans a WET file, keyword-matches parking signals in the
extracted text, dedupes by domain, and writes exemplar texts to a parquet. A later
step embeds them into a `junk` PrototypeSet that `nace_embed` consults to label a
page `parked -> Unknown` instead of forcing a wrong NACE code.

This is the high-precision SEED phase. Expansion (nearest-neighbour retrieval of
templates the keywords miss) and tail-clustering come later.

Run:
    export EMBED_BASE_URL=http://100.77.62.33:8000/v1
    uv run python scripts/mine_parked.py --scan 40000 --out data/commoncrawl/parked_exemplars.parquet
"""
import argparse
import re
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from warcio.archiveiterator import ArchiveIterator

from commoncrawl_enrich import segment

# (label, pattern) — case-insensitive. Label groups the signal for distribution stats
# and per-class prototypes. High-precision phrases only (seed phase).
# HIGH-PRECISION signals only — these exemplars become ground-truth prototypes, so a
# false positive poisons the junk centroid. Dropped the generic phrases that hit real
# sites in the first run ("courtesy of" -> a weather page, "make an offer"/"for sale"
# -> an art gallery, bare "coming soon"). Recall is recovered later by embedding-NN
# expansion, so the seed favours precision over yield.
_SIGNALS: list[tuple[str, str]] = [
    ("for_sale", r"\bbuy this domain\b"),
    ("for_sale", r"\bpurchase this domain\b"),
    ("for_sale", r"this domain (?:is|may be|could be) for sale"),
    ("for_sale", r"\bdomain(?: name)? is for sale\b"),
    ("for_sale", r"the domain .{0,60}? is for sale"),
    ("for_sale", r"\bthis domain has expired\b"),
    ("for_sale", r"\binterested in this domain\b"),
    ("parking_provider", r"\bsedoparking\b"),
    ("parking_provider", r"\bparkingcrew\b"),
    ("parking_provider", r"\bafternic\b"),
    ("parking_provider", r"\bhugedomains\b"),
    ("parking_provider", r"\bcashparking\b"),
    ("parking_provider", r"this (?:web ?)?page is parked"),
    ("parking_provider", r"\bthis domain (?:is )?parked\b"),
    ("parking_provider", r"\bparked free\b"),
    ("parking_provider", r"\bdomain parking\b"),
    ("parking_provider", r"\bparked by (?:the )?(?:domain|registrant|owner)\b"),
    ("placeholder", r"\bunder construction\b"),
    ("placeholder", r"\bsite not configured\b"),
    ("placeholder", r"apache2?.{0,20}default page"),
    ("placeholder", r"\bwelcome to nginx\b"),
    ("placeholder", r"\byour new website is ready\b"),
    ("placeholder", r"\bwebsite coming soon\b"),
    # directory listings: legit infra, but not a business homepage -> own class,
    # require the corroborating listing structure to avoid matching prose.
    ("directory_listing", r"\bindex of /\S*.{0,120}(?:parent directory|last modified)"),
]
_COMPILED = [(label, re.compile(pat, re.I | re.S)) for label, pat in _SIGNALS]
_MAX_CHARS = 8000


def match_signal(text: str) -> str | None:
    """First matching junk-signal label, or None. Only scan the head (parking text is up top)."""
    head = text[:2000]
    for label, rx in _COMPILED:
        if rx.search(head):
            return label
    return None


def mine_wet(source: str, *, scan_limit: int, max_exemplars: int = 4000,
             homepages_only: bool = False, session=None) -> list[dict]:
    """Scan up to `scan_limit` WET records; return junk exemplars, deduped by root_domain."""
    stream = segment._open_stream(source, session)
    seen_domains: set[str] = set()
    rows: list[dict] = []
    scanned = 0
    try:
        for record in ArchiveIterator(stream):
            if record.rec_type != "conversion":
                continue
            scanned += 1
            uri = record.rec_headers.get_header("WARC-Target-URI") or ""
            if homepages_only and not segment._is_homepage(uri):
                continue
            text = record.content_stream().read().decode("utf-8", "replace")
            label = match_signal(text)
            if not label:
                continue
            root, _ = segment._host(uri)
            if root in seen_domains:  # one exemplar per domain -> template diversity
                continue
            seen_domains.add(root)
            rows.append({"url": uri, "root_domain": root, "signal": label,
                         "text": text[:_MAX_CHARS].strip()})
            if len(rows) >= max_exemplars:
                break
            if scanned >= scan_limit:
                break
    finally:
        stream.close()
    return rows


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--source", default=None, help="WET path/URL (default: first WET of latest crawl)")
    ap.add_argument("--scan", type=int, default=40000, help="max WET records to scan")
    ap.add_argument("--max-exemplars", type=int, default=4000)
    ap.add_argument("--homepages-only", action="store_true")
    ap.add_argument("--out", default="data/commoncrawl/parked_exemplars.parquet")
    args = ap.parse_args()

    source = args.source or segment.first_wet_url()
    print(f"mining parked exemplars from {source}")
    rows = mine_wet(source, scan_limit=args.scan, max_exemplars=args.max_exemplars,
                    homepages_only=args.homepages_only)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), args.out)

    from collections import Counter
    dist = Counter(r["signal"] for r in rows)
    print(f"exemplars: {len(rows)} (deduped by domain) -> {args.out}")
    print(f"signal distribution: {dict(dist)}")
    for r in rows[:8]:
        snippet = " ".join(r["text"].split())[:90]
        print(f"  [{r['signal']:16}] {r['root_domain'][:28]:28} {snippet}")


if __name__ == "__main__":
    main()
