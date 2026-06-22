import statistics

from commoncrawl_enrich.models import DomainEnrichment


def build_report(enrichments: list[DomainEnrichment], *, wall_clock_seconds: float) -> dict:
    total = len(enrichments)
    found = [e for e in enrichments if e.fetch_status != "not_in_index"]
    ok = [e for e in enrichments if e.fetch_status == "ok"]
    with_email = [e for e in ok if e.emails]
    # uplift = domains whose ONLY contacts came from the LLM
    email_llm_uplift = sum(
        1 for e in ok if e.emails and all(m.source_method == "llm" for m in e.emails)
    )
    industry_classified = sum(1 for e in ok if e.industry and e.industry.method == "llm")
    dps = round(total / wall_clock_seconds, 4) if wall_clock_seconds > 0 else 0.0
    median_fetch_ms = round(statistics.median(e.fetch_ms for e in ok), 2) if ok else 0.0
    median_llm_ms = round(statistics.median(e.llm_ms for e in ok), 2) if ok else 0.0
    return {
        "total": total,
        "found_in_index": len(found),
        "fetched_ok": len(ok),
        "with_valid_ico": sum(1 for e in ok if e.ico_checksum_valid),
        "with_email": len(with_email),
        "with_phone": sum(1 for e in ok if e.phones),
        "with_social": sum(1 for e in ok if e.socials),
        "with_technology": sum(1 for e in ok if e.technologies),
        "email_llm_uplift": email_llm_uplift,
        "industry_classified": industry_classified,
        "wall_clock_seconds": round(wall_clock_seconds, 2),
        "domains_per_second": dps,
        "projected_100k_hours": round(100_000 / dps / 3600, 2) if dps else None,
        "projected_10m_hours": round(10_000_000 / dps / 3600, 2) if dps else None,
        "median_fetch_ms": median_fetch_ms,
        "median_llm_ms": median_llm_ms,
    }
