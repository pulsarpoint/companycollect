"""The six person-entity tables (spec 2026-09-09 section 3), pinned against the migration
DDL through tests/se_company_ddl.py so tables.py and the deployed schema cannot drift."""

from dagster_v3.defs.se_company.person import tables
from tests.se_company_ddl import declared_columns, table_block

COMPANY_ID_CHECK = "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')"
# `data` is a String holding a JSON object, not the native JSON type (spec 3, amended
# 2026-09-09): clickhouse-driver reads and writes it like any other String, and this
# constraint is what stops a hand-written row from putting an array or a scalar in it.
DATA_CHECK = "CONSTRAINT valid_data CHECK JSONType(data) = 'Object'"
# Migration 000396 declares the main table under the build name it was created with; 000398
# renames the DEPLOYED table and, under the ledger policy, does not touch that file.
MAIN_DDL_TABLE = "se_company_person_v2"


def test_suggestion_table_is_one_current_row_per_company_source_and_slot() -> None:
    block = table_block("se_company_person_suggestion")
    assert declared_columns("se_company_person_suggestion") == list(tables.SUGGESTION_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(suggested_at)" in block
    assert "ORDER BY (company_id, source, slot)" in block
    assert COMPANY_ID_CHECK in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    full_name Nullable(String)," in block
    assert "    first_name Nullable(String)," in block
    assert "    last_name Nullable(String)," in block
    assert "    birth_year Nullable(UInt16)," in block
    assert "    role_original Nullable(String)," in block
    assert "    role_key Nullable(String)," in block
    assert "    role_from Nullable(Date)," in block
    assert "    data String DEFAULT '{}'," in block
    assert DATA_CHECK in block
    assert "MATERIALIZED" not in block


def test_normalized_table_has_the_same_key_and_its_own_version() -> None:
    block = table_block("se_company_person_normalized")
    assert declared_columns("se_company_person_normalized") == list(tables.NORMALIZED_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(normalized_at)" in block
    assert "ORDER BY (company_id, source, slot)" in block
    assert COMPANY_ID_CHECK in block
    assert "    normalized_id FixedString(64)," in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    normalizer_version LowCardinality(String)," in block
    assert "    parse_status LowCardinality(String)," in block
    assert "    parse_notes Array(String)," in block
    for column in ("first_tokens", "middle_tokens", "last_tokens"):
        assert f"    {column} Array(String)," in block, column
    assert "    role_key Nullable(String)," in block
    assert "    data String DEFAULT '{}'," in block
    assert DATA_CHECK in block
    # The normalized row carries no suggested_at: suggestion_id already names the raw
    # version it was computed from, and that is what the change scan compares.
    assert "suggested_at" not in block


def test_main_table_is_one_row_per_company_and_person() -> None:
    block = table_block(MAIN_DDL_TABLE)
    assert declared_columns(MAIN_DDL_TABLE) == list(tables.MAIN_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(folded_at)" in block
    assert "ORDER BY (company_id, person_key)" in block
    assert COMPANY_ID_CHECK in block
    assert "    person_key FixedString(64)," in block
    assert "    sources Array(LowCardinality(String))," in block
    assert "    normalized_ids Array(FixedString(64))," in block
    assert "    member_birth_years Array(Nullable(UInt16))," in block
    assert "    member_data Array(String)," in block
    assert "    role_sources Array(Array(String))," in block
    assert "    active UInt8," in block
    assert "    data String DEFAULT '{}'," in block
    assert DATA_CHECK in block
    for column in tables.MEMBER_COLUMNS:
        assert f"    {column} Array(" in block, column


def test_history_is_the_main_row_plus_the_change_block() -> None:
    block = table_block("se_company_person_history")
    assert declared_columns("se_company_person_history") == list(tables.HISTORY_COLUMNS)
    assert tables.HISTORY_COLUMNS == (*tables.MAIN_COLUMNS, "changed_at", "change_kind", "fold_run_id")
    # Append-only, written only by the fold from rows the main table already validated:
    # neither constraint is repeated here.
    assert "CONSTRAINT" not in block
    assert "    data String DEFAULT '{}'," in block
    assert "ENGINE = MergeTree" in block
    assert "ORDER BY (company_id, person_key, changed_at)" in block
    assert "    change_kind LowCardinality(String)," in block


def test_rule_table_is_a_per_company_reviewer_decision() -> None:
    block = table_block("se_company_person_rule")
    assert declared_columns("se_company_person_rule") == list(tables.RULE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(created_at)" in block
    assert "ORDER BY (company_id, rule_id)" in block
    assert COMPANY_ID_CHECK in block
    assert "    kind LowCardinality(String)," in block
    assert "    person_keys Array(FixedString(64))," in block
    assert "    slots Array(String)," in block
    assert "    active UInt8," in block


def test_precedence_table_has_the_basic_info_shape() -> None:
    block = table_block("se_company_person_precedence")
    assert declared_columns("se_company_person_precedence") == list(tables.PRECEDENCE_COLUMNS)
    assert tables.PRECEDENCE_COLUMNS == (
        "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
    )
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, field, source)" in block
    assert "CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block


def test_column_tuples_agree_with_each_other() -> None:
    assert tables.QUALIFIED_SUGGESTION_TABLE == "corpscout.se_company_person_suggestion"
    assert tables.QUALIFIED_NORMALIZED_TABLE == "corpscout.se_company_person_normalized"
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_person"
    # The five sibling tables keep names the main one is a PREFIX of, which is why every
    # string match on it elsewhere carries the alias or the token that follows it.
    assert tables.QUALIFIED_SUGGESTION_TABLE.startswith(tables.QUALIFIED_MAIN_TABLE)
    assert tables.QUALIFIED_HISTORY_TABLE == "corpscout.se_company_person_history"
    assert tables.QUALIFIED_RULE_TABLE == "corpscout.se_company_person_rule"
    assert tables.QUALIFIED_PRECEDENCE_TABLE == "corpscout.se_company_person_precedence"
    assert tables.SOURCES == ("bolagsverket", "esef", "wikidata", "ratsit", "reviewer", "reviewer_draft")
    assert tables.PARSE_STATUSES == ("ok", "partial", "no_person")
    assert tables.RULE_KINDS == ("hide", "merge", "split")
    assert tables.INACTIVE_REASONS == ("", "hidden", "withdrawn")
    assert tables.CHANGE_KINDS == ("created", "updated", "hidden", "withdrawn", "reactivated")
    assert tables.ROLE_TYPE_TABLE == "company_person_role_type"
    for column in tables.MEMBER_COLUMNS:
        assert column in tables.MAIN_COLUMNS and column.startswith("member_")
    for column in tables.ROLE_BLOCK_COLUMNS:
        assert column in tables.MAIN_COLUMNS


def test_the_entity_name_is_a_prefix_of_five_siblings() -> None:
    """Whole-name matching, everywhere. Since migration 000398 the main table is
    se_company_person, and that name prefixes all five of the tables below."""
    siblings = (
        tables.SUGGESTION_TABLE, tables.NORMALIZED_TABLE, tables.HISTORY_TABLE,
        tables.RULE_TABLE, tables.PRECEDENCE_TABLE,
    )
    for name in siblings:
        assert name.startswith(f"{tables.MAIN_TABLE}_")
        assert name != tables.MAIN_TABLE


def test_the_role_view_constants_describe_the_slice_5_view() -> None:
    """Migration 000402's derived view (spec section 11). It is the SIXTH name with the
    entity's prefix and the first that is not a table, so whole-name matching applies to
    it exactly as it does to the five sibling tables above."""
    assert tables.ROLE_VIEW == "se_company_person_role"
    assert tables.QUALIFIED_ROLE_VIEW == "corpscout.se_company_person_role"
    assert tables.ROLE_VIEW.startswith(f"{tables.MAIN_TABLE}_")
    assert tables.ROLE_VIEW != tables.MAIN_TABLE
    assert tables.ROLE_VIEW_COLUMNS == (
        "company_id", "person_key", "display_name", "birth_year", "role_code", "role_year",
        "role_from", "role_to", "source", "slot", "normalized_id", "is_current", "folded_at",
    )
    assert tables.ROLE_VIEW_ORDER_BY == (
        "company_id", "person_key", "role_year", "role_code", "source", "slot",
    )
    # Every key column is one the view publishes, and every column it publishes comes from
    # one of the two tables it reads -- except is_current, which the SELECT derives.
    for column in tables.ROLE_VIEW_ORDER_BY:
        assert column in tables.ROLE_VIEW_COLUMNS, column
    for column in tables.ROLE_VIEW_COLUMNS:
        assert (
            column in tables.MAIN_COLUMNS
            or column in tables.NORMALIZED_COLUMNS
            or column == "is_current"
        ), column


def test_the_match_gap_view_constants_describe_the_derived_view() -> None:
    """Migration 000406's derived view. Like the role view it carries the
    entity's prefix and is not a table, so whole-name matching applies to it -- and it is a
    prefix collision waiting to happen with se_company_person_match, which is why nothing
    ever matches the pair table's name without the token that follows it."""
    assert tables.MATCH_GAP_VIEW == "se_company_person_match_gap"
    assert tables.QUALIFIED_MATCH_GAP_VIEW == "corpscout.se_company_person_match_gap"
    assert tables.MATCH_GAP_VIEW.startswith(f"{tables.MATCH_TABLE}_")
    assert tables.MATCH_GAP_VIEW != tables.MATCH_TABLE
    assert tables.MATCH_GAP_VIEW_COLUMNS == (
        "company_id", "call_name_pairs", "double_surname_pairs", "computed_at",
    )
    assert tables.MATCH_GAP_VIEW_ORDER_BY == ("company_id",)
    for column in tables.MATCH_GAP_VIEW_ORDER_BY:
        assert column in tables.MATCH_GAP_VIEW_COLUMNS, column


def test_match_table_is_one_row_per_unordered_pair() -> None:
    block = table_block("se_company_person_match")
    assert declared_columns("se_company_person_match") == list(tables.MATCH_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(matched_at)" in block
    assert "ORDER BY (company_id, candidate_a, candidate_b)" in block
    assert COMPANY_ID_CHECK in block
    assert "    candidate_a FixedString(64)," in block
    assert "    candidate_b FixedString(64)," in block
    assert "    members_a Array(FixedString(64))," in block
    assert "    members_b Array(FixedString(64))," in block
    assert "    source_a LowCardinality(String)," in block
    # Float64, never Float32: the fold admits a pair with `confidence >= MATCH_THRESHOLD`
    # and batch.match_pairs_sql() repeats that comparison in SQL. Float32 would store 0.8 as
    # 0.800000011920929 and a threshold of 0.7 as 0.69999998807907104, so a pair scored at
    # exactly the threshold would be kept or dropped by the storage format.
    assert "    confidence Float64," in block
    assert "    confidence Float32," not in block
    # 000399 DECLARES the three-column key; migration 000406 extends it to
    # (company_id, candidate_a, candidate_b, request_id), which is why the deployed key is
    # asserted in test_se_company_person_match_gap_view.py against the ALTER and here only
    # against this file's own CREATE. input_hash is in NEITHER key, so it versions nothing: a
    # re-match of the same pair under the same request REPLACES its row. What supersedes a
    # previous input is the fold's join on the state row's hash (batch.match_pairs_sql).
    assert "input_hash" not in block.split("ORDER BY", 1)[1]
    assert "request_id" not in block              # added by 000406, not declared here
    assert tables.MATCH_COLUMNS[-1] == "request_id"
    assert "    model LowCardinality(String)," in block
    assert "    prompt_version LowCardinality(String)," in block
    assert "    input_hash FixedString(64)," in block
    # The pair table carries no `data` column, so it carries no valid_data constraint.
    assert "JSONType" not in block


def test_match_state_is_one_row_per_matched_company() -> None:
    block = table_block("se_company_person_match_state")
    assert declared_columns("se_company_person_match_state") == list(tables.MATCH_STATE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(matched_at)" in block
    assert "ORDER BY (company_id)" in block
    assert COMPANY_ID_CHECK in block
    assert "    input_hash FixedString(64)," in block
    assert "    candidates UInt16," in block
    assert "    sources UInt8," in block
    assert "    pairs UInt16," in block
    assert "    prompt_tokens UInt32," in block
    assert "    completion_tokens UInt32," in block
    # error DEFAULT '' so a successful run may omit it; raw_response keeps the model's
    # exact text the way the basic-info observation cache does.
    assert "    error String DEFAULT ''," in block
    assert "    raw_response String," in block
    # 000406 adds request_id here too -- not in the key: one row per company, replaced by
    # whoever certifies it last, and the column records which request did.
    assert "request_id" not in block
    assert tables.MATCH_STATE_COLUMNS[13] == "request_id"


def test_the_match_tables_join_the_entitys_column_tuples() -> None:
    assert tables.QUALIFIED_MATCH_TABLE == "corpscout.se_company_person_match"
    assert tables.QUALIFIED_MATCH_STATE_TABLE == "corpscout.se_company_person_match_state"
    # Whole-name matching: the state table's name has the pair table's as a prefix, so no
    # membership test on a qualified name may ever stand in for equality.
    assert tables.MATCH_STATE_TABLE.startswith(f"{tables.MATCH_TABLE}_")
    assert tables.MATCH_STATE_TABLE != tables.MATCH_TABLE
    assert tables.MATCH_COLUMNS == (
        "company_id", "candidate_a", "candidate_b", "members_a", "members_b",
        "source_a", "source_b", "name_a", "name_b", "confidence", "reason",
        "model", "prompt_version", "input_hash", "matched_at", "request_id",
    )
    assert tables.MATCH_STATE_COLUMNS == (
        "company_id", "input_hash", "candidates", "sources", "pairs", "model",
        "prompt_version", "prompt_tokens", "completion_tokens", "raw_response", "error",
        "source_run_id", "matched_at", "request_id",
        "data_hash", "bindings_hash", "prompt_hash", "model_hash", "input_snapshot", "config_snapshot",
    )


def test_match_input_schema_supports_compact_current_company_comparisons() -> None:
    assert declared_columns(tables.MATCH_INPUT_TABLE) == list(tables.MATCH_INPUT_COLUMNS)
    block = table_block(tables.MATCH_INPUT_TABLE)
    assert "ReplacingMergeTree(computed_at)" in block
    assert "ORDER BY (company_id)" in block
    assert "eligible Bool" in block
