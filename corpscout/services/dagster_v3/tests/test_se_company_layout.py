# tests/test_se_company_layout.py
import pytest

from tests.se_company_ddl import ENVELOPE, FINAL_PROVENANCE, artifact_tables, declared_columns, table_block, MIGRATIONS_DIR


def test_the_pilot_declares_three_artifact_tables() -> None:
    assert artifact_tables() == ["se_company_info_esef", "se_company_info_scb", "se_company_info_wikidata"]


@pytest.mark.parametrize("table", artifact_tables())
def test_artifact_table_starts_with_the_envelope(table: str) -> None:
    columns = declared_columns(table)
    block = table_block(table)

    assert tuple(columns[: len(ENVELOPE)]) == ENVELOPE
    assert len(columns) > len(ENVELOPE)  # a payload exists
    assert "evidence_hash FixedString(64) MATERIALIZED" in block
    assert "ENGINE = ReplacingMergeTree(observed_at)" in block
    assert "ORDER BY (company_id, source_record_uid)" in block
    assert "CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$')" in block


def test_final_table_ends_with_provenance() -> None:
    columns = declared_columns("se_company_info")
    block = table_block("se_company_info")

    assert columns[0] == "company_id"
    for column in ("description_sources", "description_source_record_uids", "description_source_count"):
        assert column in columns
    assert tuple(columns[-len(FINAL_PROVENANCE):]) == FINAL_PROVENANCE
    assert "evidence_set_hash FixedString(64) MATERIALIZED" in block
    assert "arraySort(arrayMap(x -> toString(x), evidence_hashes))" in block
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in block and "ORDER BY (company_id)" in block


def test_ledger_and_observation_tables_twin_the_person_ones() -> None:
    ledger, observation = table_block("se_company_info_correction"), table_block("se_company_info_enrichment_observation")
    for column in ("correction_id", "company_id", "correction_kind", "payload", "evidence_hash",
                   "reason", "decided_by", "supersedes_correction_id", "created_at"):
        assert f"    {column} " in ledger
    assert "subject_person_id" not in ledger
    assert "ORDER BY (company_id, created_at, correction_id)" in ledger
    for column in ("suggestion_id", "company_id", "input_hash", "suggestion", "raw_response",
                   "model_provider", "model_name", "prompt_version", "prompt_tokens",
                   "completion_tokens", "source_run_id", "created_at"):
        assert f"    {column} " in observation
    assert "ORDER BY (company_id, input_hash, created_at)" in observation


def test_writer_grants_are_insert_only() -> None:
    up = (MIGRATIONS_DIR / "000298_corpscout_se_company_info_writer_grants.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000298_corpscout_se_company_info_writer_grants.down.sql").read_text()
    assert "GRANT INSERT ON corpscout.se_company_info_correction\nTO corpscout_person_correction_writer" in up
    assert "GRANT INSERT ON corpscout.se_company_info_enrichment_observation\nTO corpscout_person_correction_writer" in up
    assert "GRANT SELECT" not in up and "GRANT ALL" not in up
    assert "REVOKE INSERT ON corpscout.se_company_info_correction" in down
