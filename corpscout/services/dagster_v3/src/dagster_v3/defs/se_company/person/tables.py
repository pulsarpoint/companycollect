"""Table names and column tuples of the person entity, pinned against migration 000395.

The main table is se_company_person_v2 for slices 0 to 3; slice 4 renames it to
se_company_person, which is why MAIN_TABLE is the one place the name appears.
"""

DATABASE = "corpscout"
SUGGESTION_TABLE = "se_company_person_suggestion"
NORMALIZED_TABLE = "se_company_person_normalized"
MAIN_TABLE = "se_company_person_v2"
HISTORY_TABLE = "se_company_person_history"
RULE_TABLE = "se_company_person_rule"
PRECEDENCE_TABLE = "se_company_person_precedence"
# Kept from the retired model: the 25-code role catalog the normalizer maps into, seeded for
# Sweden (000290/000294) and Serbia (000319).
ROLE_TYPE_TABLE = "company_person_role_type"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_NORMALIZED_TABLE = f"{DATABASE}.{NORMALIZED_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_RULE_TABLE = f"{DATABASE}.{RULE_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"
QUALIFIED_ROLE_TYPE_TABLE = f"{DATABASE}.{ROLE_TYPE_TABLE}"

SOURCES: tuple[str, ...] = ("bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft")
PARSE_STATUSES: tuple[str, ...] = ("ok", "partial", "no_person")
RULE_KINDS: tuple[str, ...] = ("hide", "merge", "split")
INACTIVE_REASONS: tuple[str, ...] = ("", "hidden", "withdrawn")
CHANGE_KINDS: tuple[str, ...] = ("created", "updated", "hidden", "withdrawn", "reactivated")

SUGGESTION_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "suggested_at", "source_record_id",
    "full_name", "first_name", "last_name", "birth_year", "wikidata_id", "role_original",
    "role_key", "fiscal_year", "role_from", "role_to", "document_ref", "data",
)
NORMALIZED_COLUMNS: tuple[str, ...] = (
    "company_id", "source", "slot", "suggestion_id", "normalized_id", "normalizer_version",
    "parse_status", "parse_notes", "first_tokens", "middle_tokens", "last_tokens",
    "display_first", "display_last", "display_name", "birth_year", "wikidata_id",
    "role_code", "role_key", "role_year", "role_from", "role_to", "data", "normalized_at",
)
MEMBER_COLUMNS: tuple[str, ...] = (
    "member_sources", "member_slots", "member_names", "member_birth_years",
    "member_wikidata_ids", "member_data",
)
ROLE_BLOCK_COLUMNS: tuple[str, ...] = (
    "role_codes", "role_years", "role_sources", "current_roles", "first_year", "last_year",
)
MAIN_COLUMNS: tuple[str, ...] = (
    "company_id", "person_key", "display_name", "first_name", "last_name", "birth_year",
    "wikidata_id", "sources", "slots", "normalized_ids",
    *MEMBER_COLUMNS,
    *ROLE_BLOCK_COLUMNS,
    "text_source", "data", "active", "inactive_reason", "folded_at", "fold_version",
    "source_run_id",
)
HISTORY_COLUMNS: tuple[str, ...] = (*MAIN_COLUMNS, "changed_at", "change_kind", "fold_run_id")
RULE_COLUMNS: tuple[str, ...] = (
    "company_id", "rule_id", "kind", "person_keys", "slots", "active", "note",
    "created_at", "created_by",
)
PRECEDENCE_COLUMNS: tuple[str, ...] = (
    "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
)
