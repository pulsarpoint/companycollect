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

# Phase E: the graph reads ONLY the seven canonical <src>_company_domains
# tables (spec decision 7). slug/id-type are config literals, not data
# columns — ee_company_contacts carries a legacy slug outlier and the
# graph's provenance values must stay stable regardless of source data.
CANONICAL_DOMAIN_SOURCES: tuple[dict[str, str], ...] = (
    {"table": "cz_company_domains", "registry_id_type": "ico", "source_slug": "czech_ares"},
    {"table": "lv_company_domains", "registry_id_type": "regcode", "source_slug": "latvia_ur"},
    {"table": "ee_company_domains", "registry_id_type": "reg_code", "source_slug": "estonia_ar"},
    {"table": "br_company_domains", "registry_id_type": "cnpj_basico", "source_slug": "brazil_rfb"},
    {"table": "no_company_domains", "registry_id_type": "org_number", "source_slug": "norway_brreg"},
    {"table": "fi_company_domains", "registry_id_type": "business_id", "source_slug": "finland_ytj"},
    {
        "table": "wikidata_company_domains",
        "registry_id_type": "wikidata_id",
        "source_slug": "wikidata",
    },
)
