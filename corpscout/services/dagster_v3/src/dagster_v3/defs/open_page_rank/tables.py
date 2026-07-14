OPEN_PAGE_RANK_DOMAINS_TABLE = "open_page_rank_domains"

OPEN_PAGE_RANK_TABLES = (OPEN_PAGE_RANK_DOMAINS_TABLE,)

OPEN_PAGE_RANK_DOMAINS_COLUMNS = (
    "source_system",
    "source_list_name",
    "source_run_id",
    "source_record_id",
    "source_rank",
    "domain",
    "root_domain",
    "domain_extension",
    "open_page_rank",
    "source_url",
    "retrieved_date",
    "retrieved_at",
    "resolved_at",
)

OPEN_PAGE_RANK_TABLE_COLUMNS = {
    OPEN_PAGE_RANK_DOMAINS_TABLE: OPEN_PAGE_RANK_DOMAINS_COLUMNS,
}
