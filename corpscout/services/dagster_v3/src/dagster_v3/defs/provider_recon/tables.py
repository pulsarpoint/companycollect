"""ClickHouse objects owned by migration 000460 (provider-recon)."""

DOCUMENTS_S3_TABLE = "provider_recon_documents_s3"
SERVICES_TABLE = "provider_services"
IP_RANGES_TABLE = "provider_ip_ranges"
RULES_TABLE = "provider_rules"
CURRENT_VIEW = "provider_ip_ranges_current"

ALL_TABLES = (DOCUMENTS_S3_TABLE, SERVICES_TABLE, IP_RANGES_TABLE, RULES_TABLE, CURRENT_VIEW)
