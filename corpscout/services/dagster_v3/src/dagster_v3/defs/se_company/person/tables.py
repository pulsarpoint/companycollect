"""Table names and column tuples of the person entity, pinned against migration 000396.

The main table is se_company_person since migration 000398 (slice 4). It was BUILT as
se_company_person_v2, because the 2026-08-19 table held the final name until slice 0 dropped
it, and 000396's DDL still declares it under that build name -- the rename is a RENAME TABLE
on the deployed database, and MAIN_TABLE is the one place this package spells it.
"""

DATABASE = "corpscout"
SUGGESTION_TABLE = "se_company_person_suggestion"
NORMALIZED_TABLE = "se_company_person_normalized"
MAIN_TABLE = "se_company_person"
HISTORY_TABLE = "se_company_person_history"
RULE_TABLE = "se_company_person_rule"
PRECEDENCE_TABLE = "se_company_person_precedence"
# Kept from the retired model: the 25-code role catalog the normalizer maps into, seeded for
# Sweden (000290/000294) and Serbia (000319).
ROLE_TYPE_TABLE = "company_person_role_type"
# The LLM matching phase (migration 000399): the scored pairs and one state row per matched
# company. Both names have MAIN_TABLE as a prefix, like the five older siblings, so every
# string match on a table name compares whole names.
MATCH_TABLE = "se_company_person_match"
MATCH_STATE_TABLE = "se_company_person_match_state"

# This entity's own scratch-table prefix (basic_info/extract.py:scope_pages), so a person
# scan's scratch table can never collide with a basic-info or an address one. Shared by
# normalize.py's company scan and suggestions.py's extractor scan.
SCRATCH_SCOPE_PREFIX = "corpscout._tmp_person_scope_"

QUALIFIED_SUGGESTION_TABLE = f"{DATABASE}.{SUGGESTION_TABLE}"
QUALIFIED_NORMALIZED_TABLE = f"{DATABASE}.{NORMALIZED_TABLE}"
QUALIFIED_MAIN_TABLE = f"{DATABASE}.{MAIN_TABLE}"
QUALIFIED_HISTORY_TABLE = f"{DATABASE}.{HISTORY_TABLE}"
QUALIFIED_RULE_TABLE = f"{DATABASE}.{RULE_TABLE}"
QUALIFIED_PRECEDENCE_TABLE = f"{DATABASE}.{PRECEDENCE_TABLE}"
QUALIFIED_ROLE_TYPE_TABLE = f"{DATABASE}.{ROLE_TYPE_TABLE}"
QUALIFIED_MATCH_TABLE = f"{DATABASE}.{MATCH_TABLE}"
QUALIFIED_MATCH_STATE_TABLE = f"{DATABASE}.{MATCH_STATE_TABLE}"

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
MATCH_COLUMNS: tuple[str, ...] = (
    "company_id", "candidate_a", "candidate_b", "members_a", "members_b",
    "source_a", "source_b", "name_a", "name_b", "confidence", "reason",
    "model", "prompt_version", "input_hash", "matched_at",
)
MATCH_STATE_COLUMNS: tuple[str, ...] = (
    "company_id", "input_hash", "candidates", "sources", "pairs", "model",
    "prompt_version", "prompt_tokens", "completion_tokens", "raw_response", "error",
    "source_run_id", "matched_at",
)
