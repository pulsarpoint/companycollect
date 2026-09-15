"""Common Crawl identity candidates; discovery run IDs never invalidate content hashes."""

import dagster as dg

from dagster_v3.defs.se_company.domain.suggestions import SELECT_COLUMNS, define_domain_source


def common_crawl_live_sql(*, scoped: bool) -> str:
    scope = "AND company_id IN %(company_ids)s" if scoped else ""
    return f"""WITH evidence AS (
    SELECT company_id, root_domain,
        arraySort(groupUniqArray(tuple(signal_type, source_field, company_value, domain_value, toFloat64(score_contribution), raw.source_url))) AS signals,
        min(raw.source_url) AS source_url
    FROM corpscout.company_domain_suggestion_evidence_active AS raw
    WHERE country_iso2 = 'SE' {scope}
    GROUP BY company_id, root_domain
)
SELECT {', '.join(SELECT_COLUMNS)} FROM (
    SELECT suggestions.company_id AS company_id, 'common_crawl_identity' AS source,
        suggestions.root_domain AS slot, suggestions.root_domain AS root_domain,
        concat('https://', suggestions.root_domain) AS website_url, suggestions.root_domain AS website_host,
        'uncertain' AS association, toUInt8(suggestions.rank = 1) AS is_primary,
        toFloat64(least(greatest(suggestions.total_score / 100, 0), 1)) AS confidence,
        concat(suggestions.scoring_version, ':', arrayStringConcat(arraySort(suggestions.candidate_sources), '+')) AS confidence_basis,
        concat(suggestions.company_id, ':', suggestions.root_domain) AS source_record_id,
        ifNull(evidence.source_url, '') AS source_url,
        toJSONString(evidence.signals) AS evidence,
        toDateTime64(suggestions.suggested_at, 3, 'UTC') AS observed_at,
        toUInt8(0) AS removed, '' AS decided_by, '' AS note
    FROM (SELECT * FROM corpscout.company_domain_suggestions_active WHERE country_iso2 = 'SE' {scope}) AS suggestions
    INNER JOIN (SELECT company_id FROM corpscout.se_company_basic_info FINAL) AS companies
        ON companies.company_id = suggestions.company_id
    LEFT JOIN evidence ON evidence.company_id = suggestions.company_id AND evidence.root_domain = suggestions.root_domain
    WHERE suggestions.root_domain != ''
)"""


se_company_domain_suggestions_common_crawl_identity = define_domain_source(
    source="common_crawl_identity", live_sql=common_crawl_live_sql,
    deps=[dg.AssetKey("se_company_basic_info_fold"), dg.AssetKey("sweden_company_domain_suggestions_dbt_run")],
    description="Sync existing Common Crawl company-domain candidates and their compared identity evidence. No crawling.",
)
