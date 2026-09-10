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
