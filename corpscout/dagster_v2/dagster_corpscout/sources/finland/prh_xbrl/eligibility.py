"""Which companies' statements are worth downloading.

The pipeline exists to correlate technology signals with financials, so only
companies that are alive and have a web presence are pulled:
lifecycle_status = 'active' AND website != '' in the PRH YTJ explorer cache
(~119k of ~459k active companies as of 2026-06).

Statements of ineligible companies are recorded as skipped in the window
listing — never silently dropped — and re-materializing a window picks up
companies that became eligible since the window was last pulled.
"""

from dagster_corpscout.resources.clickhouse import ClickHouseResource

COMPANY_CACHE_TABLE = "fi_prhytj_company_explorer_cache"
COMPANY_CACHE_ASSET_KEY = ["sources", "finland", "prh_ytj", "company_explorer_cache"]

ELIGIBLE_BUSINESS_IDS_QUERY = f"""
SELECT business_id
FROM {COMPANY_CACHE_TABLE}
WHERE lifecycle_status = 'active' AND website != ''
"""


def fetch_eligible_business_ids(clickhouse: ClickHouseResource) -> set[str]:
    result = clickhouse.client().query(ELIGIBLE_BUSINESS_IDS_QUERY)
    return {row[0] for row in result.result_rows}
