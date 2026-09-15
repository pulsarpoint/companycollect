"""Official website claims linked through Swedish identifiers and verified LEIs."""

import dagster as dg

from dagster_v3.defs.se_company.domain.suggestions import SELECT_COLUMNS, define_domain_source
from dagster_v3.defs.se_company.person.wikidata import wikidata_links_cte_sql


def wikidata_live_sql(*, scoped: bool) -> str:
    # The shared link CTE reads raw identifiers, avoiding a cycle through serving.
    return f"""{wikidata_links_cte_sql(scoped=scoped)}
SELECT {', '.join(SELECT_COLUMNS)} FROM (
    SELECT links.company_id AS company_id, 'wikidata' AS source,
        concat(domains.registry_id, ':', domains.domain) AS slot,
        lowerUTF8(domains.domain) AS root_domain,
        argMax(domains.website_url, tuple(domains.is_primary, domains.website_url)) AS website_url,
        argMax(domains.website_host, tuple(domains.is_primary, domains.website_url)) AS website_host,
        'connected' AS association, toUInt8(max(domains.is_primary)) AS is_primary,
        toFloat64(max(domains.confidence)) AS confidence, 'official_website_claim' AS confidence_basis,
        concat(domains.registry_id, ':', domains.domain) AS source_record_id,
        concat('https://www.wikidata.org/wiki/', domains.registry_id) AS source_url,
        toJSONString(map('claim', 'official website', 'wikidata_id', domains.registry_id)) AS evidence,
        max(toDateTime64(domains.resolved_at, 3, 'UTC')) AS observed_at,
        toUInt8(0) AS removed, '' AS decided_by, '' AS note
    FROM links
    INNER JOIN corpscout.wikidata_company_domains AS domains FINAL ON domains.registry_id = links.wikidata_id
    WHERE domains.is_current = 1 AND domains.domain != ''
    GROUP BY links.company_id, domains.registry_id, domains.domain
)"""


se_company_domain_suggestions_wikidata = define_domain_source(
    source="wikidata", live_sql=wikidata_live_sql,
    deps=[dg.AssetKey("se_company_basic_info_fold"), dg.AssetKey("wikidata_clickhouse_canonical_contacts"), dg.AssetKey("wikidata_company_identifiers_clickhouse")],
    description="Sync Wikidata official website claims linked by Swedish company ID or LEI; withdraw removed claims.",
)
