import pytest

from tests.se_company_ddl import ENVELOPE, FINAL_PROVENANCE, artifact_tables, declared_columns, table_block, MIGRATIONS_DIR


def test_the_pilot_declares_three_artifact_tables() -> None:
    assert artifact_tables() == ["se_company_info_esef", "se_company_info_scb", "se_company_info_wikidata"]


@pytest.mark.parametrize("table", artifact_tables())
def test_artifact_table_starts_with_the_envelope(table: str) -> None:
    columns = declared_columns(table)
    block = table_block(table)

    assert block.count("CREATE TABLE") == 1 and block.endswith(";")  # one statement, nothing of the next table

    assert tuple(columns[: len(ENVELOPE)]) == ENVELOPE
    assert len(columns) > len(ENVELOPE)  # a payload exists
    assert "evidence_hash FixedString(64) MATERIALIZED" in block
    assert "ENGINE = ReplacingMergeTree(observed_at)" in block
    assert "ORDER BY (company_id, source_record_uid)" in block
    assert "CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$')" in block  # 000297; widened by 000299


def _materialized(sql: str) -> str:
    """The evidence_hash MATERIALIZED expression, whitespace-normalized.

    000297 indents it inside a CREATE TABLE and 000300 inside an ALTER, so the two are
    only comparable once the indentation is collapsed.
    """
    start = sql.index("MATERIALIZED lower(hex(SHA256(concat(")
    return " ".join(sql[start : sql.index("))))", start) + 4].split())


def test_scb_carries_the_english_activity_description_from_000300() -> None:
    """The owner's 2026-08-23 decision: SCB descriptions are published in English, taken
    from the translator's corpscout.text_translations, so the artifact carries the
    translated text beside the Swedish one and its evidence_hash covers it (v2)."""
    up = (MIGRATIONS_DIR / "000300_corpscout_se_company_info_scb_english.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000300_corpscout_se_company_info_scb_english.down.sql").read_text()

    columns = declared_columns("se_company_info_scb")
    assert columns[columns.index("activity_description") + 1] == "activity_description_en"
    assert (
        "ADD COLUMN IF NOT EXISTS activity_description_en String DEFAULT '' AFTER activity_description"
    ) in up

    # v2 is v1's list with the English text hashed right after the Swedish one -- derived
    # here rather than transcribed, so a drifting column cannot pass by being pasted twice.
    v1 = _materialized(table_block("se_company_info_scb"))
    v2 = v1.replace("scb-v1", "scb-v2").replace(
        "ifNull(activity_description, ''), '\\n', primary_sni_code",
        "ifNull(activity_description, ''), '\\n', activity_description_en, '\\n', primary_sni_code",
    )
    assert v2 != v1
    assert _materialized(up) == v2
    # The down file restores 000297's expression verbatim, and does so BEFORE dropping the
    # column the v2 expression reads.
    assert _materialized(down) == v1
    assert down.index("MODIFY COLUMN evidence_hash") < down.index(
        "DROP COLUMN IF EXISTS activity_description_en"
    )


def test_final_table_ends_with_provenance() -> None:
    columns = declared_columns("se_company_info")
    block = table_block("se_company_info")

    assert columns[0] == "company_id"
    after_description_source = columns.index("description_source") + 1
    assert columns[after_description_source : after_description_source + 3] == [
        "description_sources", "description_source_record_uids", "description_source_count"]
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
    assert "REVOKE INSERT ON corpscout.se_company_info_enrichment_observation" in down


def test_sole_traders_are_admitted_by_000299_on_every_company_keyed_table() -> None:
    """The owner's 2026-08-22 decision: 12-digit personnummer-based ids are published too."""
    from dagster_v3.defs.se_company.common import SE_COMPANY_ID_PATTERN

    up = (MIGRATIONS_DIR / "000299_corpscout_se_company_info_sole_traders.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000299_corpscout_se_company_info_sole_traders.down.sql").read_text()
    for table in ("se_company_info_scb", "se_company_info_esef", "se_company_info_wikidata",
                  "se_company_info", "se_company_info_correction"):
        assert f"ALTER TABLE corpscout.{table}\n    DROP CONSTRAINT has_company,\n    ADD CONSTRAINT has_company CHECK match(company_id, '{SE_COMPANY_ID_PATTERN}')" in up
        assert ("ALTER TABLE corpscout." + table + "\n    DROP CONSTRAINT has_company,\n"
                "    ADD CONSTRAINT has_company CHECK match(company_id, '^[0-9]{10}$')") in down
    assert SE_COMPANY_ID_PATTERN == "^([0-9]{10}|[0-9]{12})$"
