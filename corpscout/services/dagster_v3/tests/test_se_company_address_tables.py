"""The six address-entity tables (spec 2026-09-06 section 3), pinned against the migration
DDL through tests/se_company_ddl.py so tables.py and the deployed schema cannot drift."""

from dagster_v3.defs.se_company.address import tables
from tests.se_company_ddl import declared_columns, table_block

COMPANY_ID_CHECK = "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')"


def test_suggestion_table_is_one_current_row_per_company_source_and_slot() -> None:
    block = table_block("se_company_address_suggestion")
    assert declared_columns("se_company_address_suggestion") == list(tables.SUGGESTION_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(suggested_at)" in block
    assert "ORDER BY (company_id, source, slot)" in block
    assert COMPANY_ID_CHECK in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    replaces_key Nullable(FixedString(64))," in block
    for column in tables.RAW_ADDRESS_COLUMNS:
        assert f"    {column} Nullable(String)," in block, column
    assert "MATERIALIZED" not in block


def test_normalized_table_has_the_same_key_and_its_own_version() -> None:
    block = table_block("se_company_address_normalized")
    assert declared_columns("se_company_address_normalized") == list(tables.NORMALIZED_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(normalized_at)" in block
    assert "ORDER BY (company_id, source, slot)" in block
    assert COMPANY_ID_CHECK in block
    assert "    normalized_id FixedString(64)," in block
    assert "    suggestion_id FixedString(64)," in block
    assert "    address_key FixedString(64)," in block
    for column in tables.COMPONENT_COLUMNS:
        assert f"    {column} Nullable(String)," in block, column
    assert "    country_code LowCardinality(String)," in block
    assert "    parse_status LowCardinality(String)," in block
    assert "    normalizer_version LowCardinality(String)," in block


def test_main_table_is_one_row_per_company_and_published_address() -> None:
    block = table_block("se_company_address_v2")
    assert declared_columns("se_company_address_v2") == list(tables.MAIN_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(folded_at)" in block
    assert "ORDER BY (company_id, address_key)" in block
    assert COMPANY_ID_CHECK in block
    assert "    normalized_ids Array(FixedString(64))," in block
    assert "    sources Array(LowCardinality(String))," in block
    assert "    active UInt8," in block
    assert "    geocode_policy LowCardinality(String)," in block
    assert "    geocoded_at Nullable(DateTime64(3, 'UTC'))," in block


def test_history_table_is_the_main_row_keyed_by_fold_time() -> None:
    block = table_block("se_company_address_history")
    assert declared_columns("se_company_address_history") == list(tables.HISTORY_COLUMNS)
    assert tables.HISTORY_COLUMNS == tables.MAIN_COLUMNS
    assert "CONSTRAINT valid_company_id" not in block
    assert "ENGINE = MergeTree" in block
    assert "ORDER BY (company_id, address_key, folded_at)" in block


def test_rule_table_is_a_per_company_decision_per_address() -> None:
    block = table_block("se_company_address_rule")
    assert declared_columns("se_company_address_rule") == list(tables.RULE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, address_key, action)" in block
    assert COMPANY_ID_CHECK in block
    assert "    removed UInt8 DEFAULT 0," in block


def test_precedence_table_has_the_basic_info_shape() -> None:
    block = table_block("se_company_address_precedence")
    assert declared_columns("se_company_address_precedence") == list(tables.PRECEDENCE_COLUMNS)
    assert tables.PRECEDENCE_COLUMNS == (
        "company_id", "field", "source", "precedence", "removed", "decided_by", "note", "decided_at",
    )
    assert "ENGINE = ReplacingMergeTree(decided_at)" in block
    assert "ORDER BY (company_id, field, source)" in block
    assert "CHECK company_id = '' OR match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block


def test_column_tuples_agree_with_each_other() -> None:
    assert tables.QUALIFIED_SUGGESTION_TABLE == "corpscout.se_company_address_suggestion"
    assert tables.QUALIFIED_NORMALIZED_TABLE == "corpscout.se_company_address_normalized"
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_address_v2"
    assert tables.QUALIFIED_HISTORY_TABLE == "corpscout.se_company_address_history"
    assert tables.QUALIFIED_RULE_TABLE == "corpscout.se_company_address_rule"
    assert tables.QUALIFIED_PRECEDENCE_TABLE == "corpscout.se_company_address_precedence"
    assert tables.SOURCES == ("scb", "bolagsverket", "ratsit", "reviewer", "reviewer_draft")
    assert tables.PARSE_STATUSES == ("ok", "partial", "no_address", "foreign")
    assert tables.KINDS == ("postal", "visiting", "visiting_or_postal", "registered", "workplace", "unknown")
    for column in tables.COMPONENT_COLUMNS:
        assert column in tables.NORMALIZED_COLUMNS and column in tables.MAIN_COLUMNS
    for column in tables.GEOCODE_COLUMNS:
        assert column in tables.MAIN_COLUMNS
