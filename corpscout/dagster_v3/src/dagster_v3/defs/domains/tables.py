DOMAINS_TABLE = "domains"
COMPANY_WEBSITE_DOMAINS_TABLE = "company_website_domains"

DOMAIN_TABLES = (
    DOMAINS_TABLE,
    COMPANY_WEBSITE_DOMAINS_TABLE,
)

DOMAINS_COLUMNS = (
    "root_domain",
    "company_count",
    "website_count",
    "source_slug_count",
    "country_count",
    "resolved_at",
)

COMPANY_WEBSITE_DOMAINS_COLUMNS = (
    "source_website_table",
    "source_website_id",
    "country_iso2",
    "source_slug",
    "company_id_type",
    "company_id",
    "website_url",
    "website_normalized_url",
    "website_host",
    "root_domain",
    "domain_source",
    "is_current",
    "is_primary",
    "resolved_at",
)
