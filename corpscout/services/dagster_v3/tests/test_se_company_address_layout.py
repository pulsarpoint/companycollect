"""The address datatype's DDL contract, read from the migration itself.

Mirrors tests/test_se_company_layout.py for the second datatype. What is pinned
here is the ENVELOPE and the final's provenance tail, not every column -- the
per-module tests (Tasks 2, 3, 5) pin each module's own insert list against
declared_columns(), so a column added in a later migration is picked up by the
replay rather than hand-copied into three places.
"""
import pytest

from tests.se_company_ddl import (
    ADDRESS_MIGRATION,
    ENVELOPE,
    MIGRATIONS_DIR,
    address_artifact_tables,
    declared_columns,
    table_block,
)

ADDRESS_FINAL_PROVENANCE = ("sources", "source_record_uids", "evidence_hashes",
                            "evidence_set_hash", "correction_ids", "source_run_id", "resolved_at")


def test_the_datatype_declares_two_artifact_tables() -> None:
    assert address_artifact_tables() == ["se_company_address_bolagsverket", "se_company_address_scb"]


@pytest.mark.parametrize("table", address_artifact_tables())
def test_artifact_table_starts_with_the_envelope(table: str) -> None:
    columns = declared_columns(table)
    block = table_block(table)

    assert tuple(columns[: len(ENVELOPE)]) == ENVELOPE
    assert len(columns) > len(ENVELOPE)  # a payload exists
    assert "evidence_hash FixedString(64) MATERIALIZED" in block
    assert "ENGINE = ReplacingMergeTree(observed_at)" in block
    assert "ORDER BY (company_id, source_record_uid)" in block
    assert "CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in block


@pytest.mark.parametrize("table", address_artifact_tables())
def test_every_artifact_payload_column_is_hashed_into_the_evidence(table: str) -> None:
    """A payload column outside evidence_hash would change silently: the anti-join
    would never append a version for it and no downstream run would ever see it."""
    block = table_block(table)
    payload = [column for column in declared_columns(table)
               if column not in ENVELOPE]
    hashed = block[block.index("MATERIALIZED") : block.index("CONSTRAINT")]
    for column in payload:
        assert column in hashed, f"{table}.{column} is not part of evidence_hash"


def test_the_final_carries_the_geocode_augmentation_and_the_tombstone_flag() -> None:
    columns = declared_columns("se_company_address")
    block = table_block("se_company_address")

    assert columns[:2] == ["company_id", "address_key"]
    for column in ("address_id", "latitude", "longitude", "geocode_status", "geocoded_at", "is_current"):
        assert column in columns
    assert tuple(columns[-len(ADDRESS_FINAL_PROVENANCE):]) == ADDRESS_FINAL_PROVENANCE
    assert "evidence_set_hash FixedString(64) MATERIALIZED" in block
    assert "arraySort(arrayMap(x -> toString(x), evidence_hashes))" in block
    assert "is_current Bool DEFAULT true" in block
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in block
    assert "ORDER BY (company_id, address_key)" in block
    assert "CONSTRAINT has_evidence CHECK notEmpty(source_record_uids)" in block


def test_no_model_columns_exist_anywhere_in_this_datatype() -> None:
    """Nothing here is model-written -- the spec's one hard negative."""
    sql = (MIGRATIONS_DIR / ADDRESS_MIGRATION).read_text(encoding="utf-8")
    for forbidden in ("suggestion_id", "model_provider", "model_name", "prompt_version",
                      "llm_enhanced", "enrichment_observation"):
        assert forbidden not in sql


def test_the_ledger_twins_the_info_one_with_its_own_kinds() -> None:
    ledger = table_block("se_company_address_correction")
    for column in ("correction_id", "company_id", "correction_kind", "payload", "evidence_hash",
                   "reason", "decided_by", "supersedes_correction_id", "created_at"):
        assert f"    {column} " in ledger
    assert "ORDER BY (company_id, created_at, correction_id)" in ledger
    assert "CONSTRAINT valid_payload CHECK isValidJSON(payload)" in ledger


def test_writer_grant_is_insert_only() -> None:
    name = "000308_corpscout_se_company_address_writer_grants"
    up = (MIGRATIONS_DIR / f"{name}.up.sql").read_text(encoding="utf-8")
    down = (MIGRATIONS_DIR / f"{name}.down.sql").read_text(encoding="utf-8")
    assert ("GRANT INSERT ON corpscout.se_company_address_correction\n"
            "TO corpscout_person_correction_writer") in up
    assert "GRANT SELECT" not in up and "GRANT ALL" not in up
    assert "REVOKE INSERT ON corpscout.se_company_address_correction" in down
