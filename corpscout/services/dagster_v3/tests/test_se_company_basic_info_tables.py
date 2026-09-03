"""The four basic-info entity tables (spec 3.2-3.5), pinned against the migration DDL
through tests/se_company_ddl.py so tables.py and the deployed schema cannot drift."""

from dagster_v3.defs.se_company.basic_info import tables
from tests.se_company_ddl import declared_columns, table_block


def test_suggestion_table_is_one_current_row_per_company_and_source() -> None:
    block = table_block("se_company_basic_info_suggestion")
    assert declared_columns("se_company_basic_info_suggestion") == list(tables.SUGGESTION_INSERT_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(suggested_at)" in block
    assert "ORDER BY (company_id, source)" in block
    assert "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block
    # NULL means no opinion: every value column is Nullable. There is no content hash
    # (owner decision 2026-09-03): the source's observed_at is the change signal.
    for column in tables.VALUE_COLUMNS:
        assert f"    {column} Nullable(" in block, column
    assert "content_hash" not in block
    assert "MATERIALIZED" not in block
    assert "decided_by Nullable(String)" in block
    assert "note Nullable(String)" in block


def test_main_table_carries_a_source_beside_every_folded_value() -> None:
    block = table_block("se_company_basic_info")
    assert declared_columns("se_company_basic_info") == list(tables.MAIN_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(folded_at)" in block
    assert "ORDER BY company_id" in block
    assert "CONSTRAINT valid_company_id CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block
    for field in tables.FOLDED_FIELDS:
        assert f"    {field}_source LowCardinality(String)" in block, field
    # status is '' when unknown, like the old table -- never NULL.
    assert "    status LowCardinality(String)," in block
    assert "    legal_name String," in block
    for column in ("legal_form_code", "incorporation_date", "lei", "wikidata_id",
                   "description", "description_language", "description_sv"):
        assert f"    {column} Nullable(" in block, column
    assert "description_language_source" not in block
    assert "fold_version LowCardinality(String)" in block
    assert "MATERIALIZED" not in block


def test_history_table_is_the_main_row_plus_changed_fields() -> None:
    block = table_block("se_company_basic_info_history")
    assert declared_columns("se_company_basic_info_history") == list(tables.HISTORY_COLUMNS)
    assert tables.HISTORY_COLUMNS == (*tables.MAIN_COLUMNS, "changed_fields")
    # Append-only history carries no company_id format constraint: it replays whatever
    # the main row held, and the main table already enforces the format on write.
    assert "CONSTRAINT valid_company_id" not in block
    for column in ("legal_form_code", "incorporation_date", "lei", "wikidata_id",
                   "description", "description_language", "description_sv"):
        assert f"    {column} Nullable(" in block, column
    assert "changed_fields Array(String)" in block
    assert "ENGINE = MergeTree" in block
    assert "ORDER BY (company_id, folded_at)" in block
    assert "MATERIALIZED" not in block


def test_precedence_table_is_exported_never_edited() -> None:
    block = table_block("se_company_basic_info_precedence")
    assert declared_columns("se_company_basic_info_precedence") == list(tables.PRECEDENCE_COLUMNS)
    assert "ENGINE = ReplacingMergeTree(exported_at)" in block
    assert "ORDER BY (field, source)" in block
    assert "precedence UInt32" in block


def test_column_tuples_agree_with_each_other() -> None:
    assert tables.VALUE_COLUMNS == (
        "legal_name", "legal_form_code", "status", "incorporation_date", "lei",
        "wikidata_id", "description", "description_language", "description_sv",
    )
    assert tables.FOLDED_FIELDS == tuple(c for c in tables.VALUE_COLUMNS if c != "description_language")
    assert tables.QUALIFIED_SUGGESTION_TABLE == "corpscout.se_company_basic_info_suggestion"
    assert tables.QUALIFIED_MAIN_TABLE == "corpscout.se_company_basic_info"
    assert tables.QUALIFIED_HISTORY_TABLE == "corpscout.se_company_basic_info_history"
    assert tables.QUALIFIED_PRECEDENCE_TABLE == "corpscout.se_company_basic_info_precedence"
