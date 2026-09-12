"""Table names and column tuples of the person entity, pinned against migration 000396.

The main table is se_company_person since migration 000398 (slice 4). It was BUILT as
se_company_person_v2, because the 2026-08-19 table held the final name until slice 0 dropped
it, and 000396's DDL still declares it under that build name -- the rename is a RENAME TABLE
on the deployed database, and MAIN_TABLE is the one place this package spells it. Slice 5
added the derived role view (ROLE_VIEW, build_se_company_person_role_sql), created by
migration 000402.
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

# Slice 5 (spec section 11): roles as ROWS. A refreshable materialized view over the main
# table and the normalized rows -- one row per published person and role observation --
# rebuilt hourly at :20. It is DERIVED: nothing in this package writes it, and the fold
# never reads it. THE NAME IS REUSED: corpscout.se_company_person_role was the 2026-08-19
# model's role table, dropped by hand in slice 0 on 2026-09-09, the same freed-name story
# migration 000398 played out for the main table.
ROLE_VIEW = "se_company_person_role"

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
QUALIFIED_ROLE_VIEW = f"{DATABASE}.{ROLE_VIEW}"

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

# The view's columns in DDL order, and its sort key. `is_current` is the only derived
# column; every other one is a main-table or a normalized-table column carried through.
ROLE_VIEW_COLUMNS: tuple[str, ...] = (
    "company_id", "person_key", "display_name", "birth_year", "role_code", "role_year",
    "role_from", "role_to", "source", "slot", "normalized_id", "is_current", "folded_at",
)
# ORDER BY holds no Nullable column (allow_nullable_key is off), which is why the SELECT
# below unwraps role_code and role_year and nothing else.
ROLE_VIEW_ORDER_BY: tuple[str, ...] = (
    "company_id", "person_key", "role_year", "role_code", "source", "slot",
)


def build_se_company_person_role_sql() -> str:
    """The SELECT behind `corpscout.se_company_person_role` (spec section 11).

    One row per published ACTIVE person and per role-carrying observation the fold built
    them from: `ARRAY JOIN` over the main row's `normalized_ids` -- the normalized
    versions the CURRENT published row was folded from -- back to the normalized table,
    keeping the rows that carry a role code. A person with no role at all (160,279 of
    them on prod) gets no row; an observation re-normalized since the last fold drops out
    until the next one, because its `normalized_id` is no longer the one the person row
    names.

    Two expressions differ from the spec's prose SELECT, and both are forced by the sort
    key: `role_code` and `role_year` are Nullable on the normalized row and `ORDER BY`
    cannot hold a Nullable column. `assumeNotNull(n.role_code)` is exact -- the WHERE has
    already dropped every NULL -- and `ifNull(n.role_year, 0)` publishes a role that
    carries no FISCAL year under year 0. Wikidata delivers a role's span in
    `role_from`/`role_to` and never a fiscal year, so every Wikidata role lands under 0
    here; `role_year` 0 means "no fiscal year", not "no year at all" -- the fold's own
    `role_years` array says it differently, expanding that span into the real years the
    role was held.

    Ends with the same SETTINGS block every serving refresh has carried since 000347/000391:
    grace_hash spill joins, external group-by/sort, and a 12 GiB memory cap -- an unbounded
    refresh here would face the same shared-server ceiling that block already exists to
    avoid.

    THE VIEW IS DERIVED AND NOTHING WRITES IT. It lags a fold by at most an hour; the
    person row's role arrays remain the fold's own summary.
    """
    return f"""SELECT
  p.company_id AS company_id,
  p.person_key AS person_key,
  p.display_name AS display_name,
  p.birth_year AS birth_year,
  assumeNotNull(n.role_code) AS role_code,
  ifNull(n.role_year, 0) AS role_year,
  n.role_from AS role_from,
  n.role_to AS role_to,
  n.source AS source,
  n.slot AS slot,
  n.normalized_id AS normalized_id,
  has(p.current_roles, assumeNotNull(n.role_code)) AS is_current,
  p.folded_at AS folded_at
FROM {QUALIFIED_MAIN_TABLE} AS p FINAL
ARRAY JOIN p.normalized_ids AS member_id
INNER JOIN {QUALIFIED_NORMALIZED_TABLE} AS n FINAL
  ON n.company_id = p.company_id AND n.normalized_id = member_id
WHERE p.active = 1 AND n.role_code IS NOT NULL
SETTINGS join_algorithm = 'grace_hash,hash',
    grace_hash_join_initial_buckets = 16,
    max_bytes_before_external_group_by = 8589934592,
    max_bytes_before_external_sort = 8589934592,
    max_memory_usage = 12884901888"""
