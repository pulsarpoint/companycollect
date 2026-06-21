from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from dagster_v3.commoncrawl_enrich.models import DomainEnrichment

# Explicit schemas so that empty tables still carry their columns.
# pa.Table.from_pylist([]) would produce a 0-column table that DuckDB cannot read back.

SCHEMA_ENRICHMENT = pa.schema([
    ("root_domain", pa.string()),
    ("source_rank", pa.int64()),
    ("open_page_rank", pa.float64()),
    ("tld", pa.string()),
    ("homepage_url", pa.string()),
    ("capture_date", pa.string()),
    ("http_status", pa.int64()),
    ("content_language", pa.string()),
    ("title", pa.string()),
    ("meta_description", pa.string()),
    ("ico", pa.string()),
    ("dic", pa.string()),
    ("ico_checksum_valid", pa.int64()),
    ("industry_label", pa.string()),
    ("industry_nace_hint", pa.string()),
    ("industry_confidence", pa.int64()),
    ("industry_method", pa.string()),
    ("email_count", pa.int64()),
    ("phone_count", pa.int64()),
    ("social_count", pa.int64()),
    ("technology_count", pa.int64()),
    ("fetch_status", pa.string()),
])

SCHEMA_EMAILS = pa.schema([
    ("root_domain", pa.string()),
    ("email", pa.string()),
    ("is_role", pa.int64()),
    ("source_method", pa.string()),
])

SCHEMA_PHONES = pa.schema([
    ("root_domain", pa.string()),
    ("phone_raw", pa.string()),
    ("phone_e164", pa.string()),
    ("source_method", pa.string()),
])

SCHEMA_SOCIALS = pa.schema([
    ("root_domain", pa.string()),
    ("platform", pa.string()),
    ("url", pa.string()),
    ("handle", pa.string()),
])

SCHEMA_TECHNOLOGIES = pa.schema([
    ("root_domain", pa.string()),
    ("technology", pa.string()),
    ("category", pa.string()),
    ("version", pa.string()),
    ("confidence", pa.int64()),
])


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

    tables: list[tuple[str, list[dict], pa.Schema]] = [
        ("domain_enrichment", _spine_rows(enrichments), SCHEMA_ENRICHMENT),
        ("domain_emails", emails, SCHEMA_EMAILS),
        ("domain_phones", phones, SCHEMA_PHONES),
        ("domain_socials", socials, SCHEMA_SOCIALS),
        ("domain_technologies", techs, SCHEMA_TECHNOLOGIES),
    ]
    paths: dict[str, str] = {}
    for name, rows, schema in tables:
        path = out / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
        paths[name] = str(path)
    return paths
