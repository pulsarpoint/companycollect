"""Filing domain observations retain their original evidence and document identity."""

import dagster as dg

from dagster_v3.defs.se_company.domain.suggestions import SELECT_COLUMNS, define_domain_source


def esef_live_sql(*, scoped: bool) -> str:
    scope = "AND domains.company_id IN %(company_ids)s" if scoped else ""
    stored_scope = "AND stored.company_id IN %(company_ids)s" if scoped else ""
    return f"""SELECT {', '.join(SELECT_COLUMNS)} FROM (
    SELECT domains.company_id AS company_id, 'esef_filing' AS source,
        concat(domains.source_document_id, ':', domains.registrable_domain) AS slot,
        domains.registrable_domain AS root_domain,
        concat('https://', domains.registrable_domain) AS website_url,
        domains.registrable_domain AS website_host,
        if(has(JSONExtract(domains.roles_json, 'Array(String)'), 'company_website'), 'connected', 'uncertain') AS association,
        toUInt8(has(JSONExtract(domains.roles_json, 'Array(String)'), 'company_website')) AS is_primary,
        toFloat64(multiIf(has(JSONExtract(domains.roles_json, 'Array(String)'), 'company_website'), 0.95,
            domains.corroborated = 1 OR domains.evidence_count >= 2, 0.90, 0.50)) AS confidence,
        multiIf(has(JSONExtract(domains.roles_json, 'Array(String)'), 'company_website'), 'explicit_company_website',
            domains.corroborated = 1 OR domains.evidence_count >= 2, 'repeated_filing_website', 'filing_website_mention') AS confidence_basis,
        domains.source_document_id AS source_record_id,
        coalesce(nullIf(filings.viewer_url, ''), nullIf(filings.report_url, ''), nullIf(filings.package_url, ''), '') AS source_url,
        domains.evidence_json AS evidence,
        toDateTime64(domains.extracted_at, 3, 'UTC') AS observed_at,
        toUInt8(0) AS removed, '' AS decided_by, '' AS note
    FROM corpscout.se_esef_domains AS domains
    INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS companies
        ON companies.company_id = domains.company_id
    LEFT ANY JOIN corpscout.se_esef_filings AS filings ON filings.fxo_id = domains.source_document_id
    WHERE domains.extraction_status = 'ok' AND domains.registrable_domain != ''
        AND NOT arrayAll(role -> role IN ('auditor', 'social_media', 'external_reference'), JSONExtract(domains.roles_json, 'Array(String)'))
        {scope}
)
UNION ALL
-- A failed document extraction is not a source withdrawal. Retain that document's
-- last good observations until it has a successful replacement, including an empty one.
SELECT {', '.join('stored.' + column for column in SELECT_COLUMNS)}
FROM corpscout.se_company_domain_suggestion AS stored FINAL
INNER JOIN (
    SELECT DISTINCT company_id, source_document_id FROM corpscout.se_esef_domains
    WHERE extraction_status != 'ok'
) AS failed ON failed.company_id = stored.company_id AND failed.source_document_id = stored.source_record_id
INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS companies
    ON companies.company_id = stored.company_id
WHERE stored.source = 'esef_filing' AND stored.removed = 0 {stored_scope}"""


se_company_domain_suggestions_esef_filing = define_domain_source(
    source="esef_filing", live_sql=esef_live_sql,
    deps=[dg.AssetKey("esef_domains_clickhouse"), dg.AssetKey("se_company_basic_info_fold")],
    description="Sync ESEF domain evidence, excluding references only to auditors, social networks or third parties.",
)
