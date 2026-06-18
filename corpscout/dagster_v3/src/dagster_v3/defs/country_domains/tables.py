COUNTRY_DOMAINS_TABLE = "country_domains"
COMPANY_WEBSITE_DOMAINS_TABLE = "company_website_domains"

COUNTRY_DOMAIN_TABLES = (
    COUNTRY_DOMAINS_TABLE,
    COMPANY_WEBSITE_DOMAINS_TABLE,
)

COUNTRY_DOMAINS_COLUMNS = (
    "country_iso2",
    "root_domain",
    "company_count",
    "website_count",
    "source_slug_count",
    "resolved_at",
)

COMPANY_WEBSITE_DOMAINS_COLUMNS = (
    "country_iso2",
    "source_slug",
    "company_id_type",
    "company_id",
    "website_url",
    "website_normalized_url",
    "website_host",
    "root_domain",
    "is_current",
    "is_primary",
    "resolved_at",
)
