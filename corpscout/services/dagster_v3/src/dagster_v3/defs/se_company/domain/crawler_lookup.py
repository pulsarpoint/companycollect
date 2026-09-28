"""Fold saved website/company matching decisions without browser or model calls."""

import dagster as dg

from dagster_v3.defs.se_company.domain.suggestions import (
    SELECT_COLUMNS,
    define_domain_source,
)


def crawler_lookup_live_sql(*, scoped: bool) -> str:
    scope = "AND r.company_id IN %(company_ids)s" if scoped else ""
    stored_scope = "AND stored.company_id IN %(company_ids)s" if scoped else ""
    return f"""SELECT {", ".join(SELECT_COLUMNS)} FROM (
        SELECT r.company_id AS company_id, 'crawler_lookup' AS source,
            r.website_id AS slot, d.root_domain AS root_domain,
            r.website_url AS website_url, domain(r.website_url) AS website_host,
            'connected' AS association, toUInt8(0) AS is_primary,
            assumeNotNull(r.confidence) AS confidence, concat('crawler_lookup:', r.basis) AS confidence_basis,
            concat(r.request_id, ':', toString(r.attempt)) AS source_record_id,
            r.website_url AS source_url,
            toJSONString(map('request_id', r.request_id, 'attempt', toString(r.attempt),
                'result_path', r.result_path, 'reasons', toJSONString(r.reasons),
                'basis', r.basis, 'model', r.model, 'site_type', r.site_type)) AS evidence,
            toDateTime64(r.finished_at, 3, 'UTC') AS observed_at,
            toUInt8(0) AS removed, '' AS decided_by, '' AS note
        FROM corpscout.website_company_lookup_results_latest AS r
        LEFT JOIN corpscout.websites AS w ON w.website_id=r.website_id
        LEFT JOIN corpscout.domains AS d ON d.domain_id=w.domain_id
        INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS companies
            ON companies.company_id=r.company_id
        WHERE r.country='SE' AND r.status='matched' AND r.found {scope}
    )
    UNION ALL
    -- A skip or operational failure is not a new ownership decision. It neither
    -- creates a claim nor withdraws an existing one. Explicit not_found does.
    SELECT {", ".join("stored." + column for column in SELECT_COLUMNS)}
    FROM corpscout.se_company_domain_sources_resolved AS stored FINAL
    INNER JOIN corpscout.website_company_lookup_results_latest AS r
        ON r.country='SE' AND r.website_id=stored.slot
    WHERE stored.source='crawler_lookup' AND stored.removed=0
        AND r.status IN ('already_mapped','failed','cancelled') {stored_scope}"""


se_company_domain_suggestions_crawler_lookup = define_domain_source(
    source="crawler_lookup",
    live_sql=crawler_lookup_live_sql,
    deps=[
        dg.AssetKey("website_company_lookup_results"),
        dg.AssetKey("se_company_basic_info_fold"),
    ],
    description="Sync saved Swedish company matches into source evidence, preserving website/attempt references. No network or model calls. A failed or skipped attempt preserves existing evidence; not_found withdraws it.",
)
