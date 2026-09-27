"""Company matching tables produced as optional outputs of ordinary crawls."""

import dagster as dg

TABLES = (
    "website_company_lookup_results",
    "website_company_lookup_candidates",
    "website_company_lookup_evidence",
    "website_company_lookup_searches",
)
defs = dg.Definitions(
    assets=[
        dg.AssetSpec(
            table,
            group_name="website_crawl",
            kinds={"clickhouse"},
            deps=["website_site_info_results", "website_full_crawl_results"],
            metadata={"dagster/table_name": "corpscout." + table},
        )
        for table in TABLES
    ]
)
