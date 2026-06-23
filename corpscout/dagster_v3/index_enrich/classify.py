"""Turn one fetched record into a per-domain industry row (+ homepage tech rows)."""
from datetime import datetime, timezone

import tldextract

from commoncrawl_enrich import extract

_TE = tldextract.TLDExtract(suffix_list_urls=(), cache_dir=None)


def _subdomain(url: str) -> str:
    try:
        return _TE(url).subdomain
    except Exception:  # noqa: BLE001 - garbage URL -> no subdomain
        return ""


def enrich_domain(html: str, headers: dict, *, root_domain: str, url: str, crawl_id: str,
                  classifier, wappalyzer=None, source_run_id: str = "",
                  resolved_at: datetime | None = None) -> tuple[tuple, list[tuple]]:
    """Build (domain_row, tech_rows) from one fetched homepage record."""
    resolved_at = resolved_at or datetime.now(timezone.utc)
    sub = _subdomain(url)
    parsed = extract.parse_html(html)
    emails = [e.email for e in extract.extract_emails(html)]
    res = classifier.classify([parsed.text or html])[0]
    domain_row = (
        crawl_id, url, root_domain, sub, emails, len(emails),
        res.page_type, float(res.page_type_score),
        res.nace_code, res.nace_label, res.nace_division,
        int(res.nace_confident), float(res.nace_margin), float(res.nace_score), res.method,
        res.nace_top3, res.nace_top3_labels, [float(s) for s in res.nace_top3_scores],
        url, source_run_id, resolved_at,
    )
    tech_rows: list[tuple] = []
    if wappalyzer is not None:
        hmap = {k: [v] for k, v in (headers or {}).items()}
        for techs in wappalyzer.analyze_batch([(url, hmap, html)]).values():
            for t in techs:
                tech_rows.append((crawl_id, url, root_domain, sub, t.technology, t.category,
                                  t.version, int(t.confidence), url, source_run_id, resolved_at))
    return domain_row, tech_rows
