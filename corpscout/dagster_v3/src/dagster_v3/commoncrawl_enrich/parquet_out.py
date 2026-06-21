from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_v3.commoncrawl_enrich.models import DomainEnrichment


def _spine_rows(enrichments: list[DomainEnrichment]) -> list[dict]:
    rows = []
    for e in enrichments:
        ind = e.industry
        rows.append({
            "root_domain": e.target.root_domain, "source_rank": e.target.source_rank,
            "open_page_rank": e.target.open_page_rank,
            "tld": e.target.root_domain.rsplit(".", 1)[-1] if "." in e.target.root_domain else "",
            "homepage_url": e.page.final_url if e.page else "",
            "capture_date": e.page.capture_date if e.page else "",
            "http_status": e.page.http_status if e.page else 0,
            "content_language": e.content_language, "title": e.title,
            "meta_description": e.meta_description, "ico": e.ico, "dic": e.dic,
            "ico_checksum_valid": int(e.ico_checksum_valid),
            "industry_label": ind.label if ind else "",
            "industry_nace_hint": ind.nace_hint if ind else "",
            "industry_confidence": ind.confidence if ind else 0,
            "industry_method": ind.method if ind else "none",
            "email_count": len(e.emails), "phone_count": len(e.phones),
            "social_count": len(e.socials), "technology_count": len(e.technologies),
            "fetch_status": e.fetch_status,
        })
    return rows


def write_parquet(enrichments: list[DomainEnrichment], out_dir: str | Path) -> dict[str, str]:
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    emails = [{"root_domain": e.target.root_domain, "email": m.email,
               "is_role": int(m.is_role), "source_method": m.source_method}
              for e in enrichments for m in e.emails]
    phones = [{"root_domain": e.target.root_domain, "phone_raw": p.phone_raw,
               "phone_e164": p.phone_e164, "source_method": p.source_method}
              for e in enrichments for p in e.phones]
    socials = [{"root_domain": e.target.root_domain, "platform": s.platform,
                "url": s.url, "handle": s.handle}
               for e in enrichments for s in e.socials]
    techs = [{"root_domain": e.target.root_domain, "technology": t.technology,
              "category": t.category, "version": t.version, "confidence": t.confidence}
             for e in enrichments for t in e.technologies]

    tables = {
        "domain_enrichment": _spine_rows(enrichments),
        "domain_emails": emails, "domain_phones": phones,
        "domain_socials": socials, "domain_technologies": techs,
    }
    paths: dict[str, str] = {}
    for name, rows in tables.items():
        path = out / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path)
        paths[name] = str(path)
    return paths
