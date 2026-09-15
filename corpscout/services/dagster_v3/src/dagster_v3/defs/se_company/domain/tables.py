"""Swedish company-domain entity; column order is owned by migration 000408."""

DATABASE = "corpscout"
GROUP_NAME = "se_company_domain"
SUGGESTION_TABLE = "se_company_domain_suggestion"
MAIN_TABLE = "se_company_domain"
HISTORY_TABLE = "se_company_domain_history"
PRECEDENCE_TABLE = "se_company_domain_precedence"
RULE_TABLE = "se_company_domain_rule"
VERIFICATION_TABLE = "se_company_domain_verification"
TABLES = (SUGGESTION_TABLE, MAIN_TABLE, HISTORY_TABLE, PRECEDENCE_TABLE, RULE_TABLE, VERIFICATION_TABLE)
SOURCES = ("wikidata", "esef_filing", "common_crawl_identity", "reviewer", "reviewer_draft")
EXTRACTOR_SOURCES = SOURCES[:3]
EXTRACTOR_ASSETS = tuple(f"se_company_domain_suggestions_{source}" for source in EXTRACTOR_SOURCES)
FOLDED_FIELDS = ("website", "association", "primary")
SUGGESTION_COLUMNS = (
    "company_id", "source", "slot", "root_domain", "website_url", "website_host",
    "association", "is_primary", "confidence", "confidence_basis", "source_record_id",
    "source_url", "evidence", "observed_at", "removed", "decided_by", "note", "suggestion_id",
    "suggested_at", "source_run_id", "extractor_version",
)
MAIN_COLUMNS = (
    "company_id", "root_domain", "website_url", "website_host", "website_source",
    "association", "association_source", "is_primary", "primary_source", "confidence",
    "sources", "source_confidences", "source_record_ids", "source_urls", "confidence_bases",
    "evidence_hash", "verification_status", "verification_reason", "verification_input_hash",
    "review_status", "review_note", "reviewed_by", "reviewed_at", "reviewed_evidence_hash",
    "active", "inactive_reason", "first_seen_at", "last_seen_at", "folded_at",
    "fold_version", "fold_input_hash", "source_run_id",
)
HISTORY_COLUMNS = (*MAIN_COLUMNS, "changed_fields", "changed_at", "change_kind", "fold_run_id")
PRECEDENCE_COLUMNS = ("company_id", "root_domain", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at")
RULE_COLUMNS = ("company_id", "root_domain", "action", "removed", "decided_by", "note", "evidence_hash", "decided_at")
VERIFICATION_COLUMNS = (
    "company_id", "root_domain", "input_hash", "data_hash", "prompt_hash", "model_hash",
    "input_json", "system_prompt", "status", "verdict", "confidence", "reason", "evidence_ids", "provider", "model",
    "prompt_version", "prompt_tokens", "completion_tokens", "raw_response", "error",
    "verified_at", "source_run_id",
)
