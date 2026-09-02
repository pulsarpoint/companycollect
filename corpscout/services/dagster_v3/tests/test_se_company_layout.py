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
    # 000304 replaced the single description_source label with the llm_enhanced flag,
    # in that same slot: the description block still runs language -> flag -> the list
    # of every source that contributed and how many there were.
    after_description_language = columns.index("description_language") + 1
    assert columns[after_description_language : after_description_language + 4] == [
        "llm_enhanced", "description_sources", "description_source_record_uids",
        "description_source_count"]
    assert "description_source" not in columns
    assert tuple(columns[-len(FINAL_PROVENANCE):]) == FINAL_PROVENANCE
    assert "evidence_set_hash FixedString(64) MATERIALIZED" in block
    assert "arraySort(arrayMap(x -> toString(x), evidence_hashes))" in block
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in block and "ORDER BY (company_id)" in block


def test_ledger_and_observation_tables_twin_the_person_ones() -> None:
    """2026-09-01: se_company_info_field_value replaces se_company_info_correction --
    append-only history where the live value per (company_id, field) is the row with the
    greatest (created_at, value_id); no kinds, no ranking, no undo chain."""
    ledger, observation = table_block("se_company_info_field_value"), table_block("se_company_info_enrichment_observation")
    for column in ("value_id", "company_id", "field", "value", "source", "source_ref",
                   "source_at", "decided_by", "note", "created_at"):
        assert f"    {column} " in ledger
    assert "subject_person_id" not in ledger
    assert "ORDER BY (company_id, field, created_at, value_id)" in ledger
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


def test_field_value_writer_grant_is_insert_only() -> None:
    up = (MIGRATIONS_DIR / "000371_corpscout_se_company_info_field_value.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000371_corpscout_se_company_info_field_value.down.sql").read_text()
    assert "GRANT INSERT ON corpscout.se_company_info_field_value\nTO corpscout_person_correction_writer" in up
    assert "GRANT SELECT" not in up and "GRANT ALL" not in up
    assert "REVOKE INSERT ON corpscout.se_company_info_field_value\nFROM corpscout_person_correction_writer" in down


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


def test_the_final_carries_both_description_languages_from_000301() -> None:
    """The owner's 2026-08-23 decision: the final holds both languages natively --
    the model writes the English and the Swedish summary in one call, so there is no
    translation-load asset and no bilingual view to keep in step."""
    up = (MIGRATIONS_DIR / "000301_corpscout_se_company_info_description_sv.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000301_corpscout_se_company_info_description_sv.down.sql").read_text()

    columns = declared_columns("se_company_info")
    assert columns[columns.index("description") + 1] == "description_sv"
    assert "ADD COLUMN IF NOT EXISTS description_sv Nullable(String) AFTER description" in up
    assert "DROP COLUMN IF EXISTS description_sv" in down
    # Nullable, never DEFAULT '': "this company has no Swedish text" (Wikidata/ESEF-only)
    # is a different fact from "its Swedish text is empty", and the merge publishes NULL.
    assert "description_sv String" not in up
    # 000301 touches the final only -- the artifacts keep their own columns.
    assert "se_company_info_scb" not in up and "se_company_info_scb" not in down


def _statements(sql: str) -> str:
    """`sql` with its comment lines removed -- what ClickHouse would actually execute."""
    return "\n".join(line for line in sql.splitlines() if not line.strip().startswith("--"))


def test_the_final_flags_llm_written_text_from_000304() -> None:
    """The owner's 2026-08-23 decision: keep every description source and where it came
    from (description_sources / description_source_record_uids / the artifact rows), keep
    the model's suggestions, and drop the single description_source label in favour of one
    boolean -- did the published text come out of the model or not. Reviewer involvement
    stays visible through correction_ids, so no 'reviewed' label is needed either."""
    up = (MIGRATIONS_DIR / "000304_corpscout_se_company_info_llm_enhanced.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000304_corpscout_se_company_info_llm_enhanced.down.sql").read_text()

    assert "ADD COLUMN IF NOT EXISTS llm_enhanced Bool DEFAULT false AFTER description_language" in up
    assert "DROP COLUMN IF EXISTS description_source" in up
    # Two statements, add before drop: llm_enhanced is positioned against a column the
    # same migration is about to remove, so the order is load-bearing.
    assert up.count("ALTER TABLE corpscout.se_company_info") == 2
    assert up.index("ADD COLUMN IF NOT EXISTS llm_enhanced") < up.index("DROP COLUMN IF EXISTS description_source")

    # The down file is the exact inverse, in the mirrored order.
    assert "ADD COLUMN IF NOT EXISTS description_source LowCardinality(String) AFTER description_language" in down
    assert "DROP COLUMN IF EXISTS llm_enhanced" in down
    assert down.index("ADD COLUMN IF NOT EXISTS description_source") < down.index("DROP COLUMN IF EXISTS llm_enhanced")

    # Asserted against the STATEMENTS, not the files: both carry a long comment block
    # that names the columns and types deliberately left alone.
    statements, down_statements = (_statements(text) for text in (up, down))
    # DEFAULT false, never Nullable: every row answers the question, and the rows written
    # before this migration answer it with "no" until they are resolved again.
    assert "Nullable" not in statements
    # 000304 touches the final only -- the artifacts and the ledger keep their columns.
    for other in ("se_company_info_scb", "se_company_info_esef", "se_company_info_wikidata",
                  "se_company_info_correction", "se_company_info_enrichment_observation"):
        assert other not in statements and other not in down_statements
    # The columns the flag does NOT replace: where each description came from is still
    # recorded, per source and per source record.
    for kept in ("description_sources", "description_source_record_uids", "description_source_count"):
        assert kept not in statements and kept not in down_statements


def test_declared_columns_replays_a_dropped_column() -> None:
    """``declared_columns`` is the tests' picture of the DEPLOYED table, so a migration
    that drops a column has to be replayed as faithfully as one that adds one -- 000304
    does both in one file, and the add is positioned against the column the drop removes."""
    columns = declared_columns("se_company_info")
    assert "llm_enhanced" in columns and "description_source" not in columns
    # The drop is narrow: the three columns whose names merely start the same way stay.
    assert {"description_sources", "description_source_record_uids", "description_source_count"} <= set(columns)


def test_the_curated_label_table_gains_the_official_swedish_name_in_000305() -> None:
    """The labels are a FIXTURE, not a translation job: the Bolagsverket/SCB names are
    curated in-repo and seeded by se_code_labels_clickhouse, so the Swedish name is one
    more column of that dictionary."""
    up = (MIGRATIONS_DIR / "000305_corpscout_se_code_labels_swedish.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000305_corpscout_se_code_labels_swedish.down.sql").read_text()

    assert "ADD COLUMN IF NOT EXISTS label_sv String DEFAULT '' AFTER label_en" in up
    assert "DROP COLUMN IF EXISTS label_sv" in down
    # DEFAULT '', never Nullable: a code either has a curated Swedish name or it has none
    # (the status-reason codes do not), and every consumer reads a label through ifNull.
    assert "Nullable" not in _statements(up)
    # 000305 touches the label dictionary only -- the info tables are 000306's business.
    assert "se_company_info" not in _statements(up) and "se_company_info" not in _statements(down)


def test_scb_and_the_final_carry_both_legal_form_labels_from_000306() -> None:
    """The owner's 2026-08-23 decision: the legal-form label is part of the info merge,
    COPIED from the register like the translated description and never written by the
    model, in both languages -- so the artifact hashes them (v3) and the final holds them
    beside the code they name."""
    up = (MIGRATIONS_DIR / "000306_corpscout_se_company_info_legal_form_label.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000306_corpscout_se_company_info_legal_form_label.down.sql").read_text()

    # Both tables: the pair sits straight after legal_form_code, English then Swedish.
    for table in ("se_company_info_scb", "se_company_info"):
        columns = declared_columns(table)
        at = columns.index("legal_form_code") + 1
        assert columns[at : at + 2] == ["legal_form_label_en", "legal_form_label_sv"], table
    assert up.count("ADD COLUMN IF NOT EXISTS legal_form_label_en String DEFAULT '' AFTER legal_form_code") == 2
    assert up.count(
        "ADD COLUMN IF NOT EXISTS legal_form_label_sv String DEFAULT '' AFTER legal_form_label_en"
    ) == 2

    # v3 is v2's list with the two labels hashed right after the code they name -- derived
    # from 000300's v2 rather than transcribed, so a drifting column cannot pass by being
    # pasted twice. (v2 is itself derived from 000297's v1 by that migration's own test.)
    v2 = _materialized((MIGRATIONS_DIR / "000300_corpscout_se_company_info_scb_english.up.sql").read_text())
    v3 = v2.replace("scb-v2", "scb-v3").replace(
        "ifNull(legal_form_code, ''), '\\n', status",
        "ifNull(legal_form_code, ''), '\\n', legal_form_label_en, '\\n', legal_form_label_sv, '\\n', status",
    )
    assert v3 != v2
    assert _materialized(up) == v3
    # The down file restores 000300's expression verbatim, and does so BEFORE dropping the
    # columns the v3 expression reads.
    assert _materialized(down) == v2
    assert down.index("MODIFY COLUMN evidence_hash") < down.index("DROP COLUMN IF EXISTS legal_form_label_sv")

    # The final needs no expression change: evidence_set_hash covers the artifacts' hashes
    # only, so the two copied columns are plain data there.
    final_alter = up[up.index("ALTER TABLE corpscout.se_company_info\n") :]
    assert "MATERIALIZED" not in final_alter and "evidence_set_hash" not in final_alter


def test_field_tables_from_000373_match_the_positional_column_tuples() -> None:
    """2026-09-02 registry design: one long candidates table, one long resolved table, one
    registry export table. The Python tuples are the positional insert lists (MATERIALIZED
    evidence_hash omitted), pinned to the migration the way INSERT_COLUMNS is pinned."""
    from dagster_v3.defs.se_company.fields.tables import (
        SE_COMPANY_FIELD_CANDIDATE_COLUMNS,
        SE_COMPANY_FIELD_COLUMNS,
        SE_COMPANY_FIELD_REGISTRY_COLUMNS,
    )

    candidate = table_block("se_company_field_candidate")
    assert [c for c in declared_columns("se_company_field_candidate") if c != "evidence_hash"] == list(
        SE_COMPANY_FIELD_CANDIDATE_COLUMNS
    )
    assert "evidence_hash FixedString(64) MATERIALIZED lower(hex(SHA256(concat(" in candidate
    assert "field, '\\n', source, '\\n', source_record_uid, '\\n', value, '\\n', value_json" in candidate
    assert "ENGINE = ReplacingMergeTree(extracted_at)" in candidate
    assert "ORDER BY (company_id, field, source, source_record_uid)" in candidate
    assert "CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in candidate
    assert "CONSTRAINT has_value CHECK trim(value) != ''" in candidate

    resolved = table_block("se_company_field")
    assert declared_columns("se_company_field") == list(SE_COMPANY_FIELD_COLUMNS)
    assert "decision_id Nullable(UUID)" in resolved
    assert "ENGINE = ReplacingMergeTree(resolved_at)" in resolved and "ORDER BY (company_id, field)" in resolved
    assert "CONSTRAINT has_company CHECK match(company_id, '^([0-9]{10}|[0-9]{12})$')" in resolved
    assert "CONSTRAINT has_value CHECK trim(value) != ''" in resolved

    registry = table_block("se_company_field_registry")
    assert declared_columns("se_company_field_registry") == list(SE_COMPANY_FIELD_REGISTRY_COLUMNS)
    assert "sources Array(String)" in registry and "resolve_sql String" in registry
    assert "ENGINE = ReplacingMergeTree(version)" in registry and "ORDER BY (datatype, country, field)" in registry


def test_field_table_writer_grants_are_insert_only() -> None:
    up = (MIGRATIONS_DIR / "000373_corpscout_se_company_field_tables.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000373_corpscout_se_company_field_tables.down.sql").read_text()
    for table in ("se_company_field_candidate", "se_company_field", "se_company_info"):
        assert f"GRANT INSERT ON corpscout.{table}\nTO corpscout_person_correction_writer" in up
        assert f"REVOKE INSERT ON corpscout.{table}\nFROM corpscout_person_correction_writer" in down
    assert "GRANT SELECT" not in up and "GRANT ALL" not in up
    # The registry table is read by both runners and written by Dagster alone.
    assert "GRANT INSERT ON corpscout.se_company_field_registry" not in up
    assert "DROP" not in _statements(up) and "TRUNCATE" not in _statements(up)


def test_the_projection_gains_the_registry_scalars_from_000374() -> None:
    """Spec 8.3: eight new wide columns between primary_sni_code and wikidata_id. The
    provenance tail is untouched, and SE_COMPANY_INFO_COLUMNS -- the list the registry
    projection inserts by -- is the deployed column list minus the MATERIALIZED hash."""
    from dagster_v3.defs.se_company.fields.tables import (
        SE_COMPANY_INFO_COLUMNS,
        SE_COMPANY_INFO_REGISTRY_COLUMNS,
    )

    up = (MIGRATIONS_DIR / "000374_corpscout_se_company_info_field_columns.up.sql").read_text()
    down = (MIGRATIONS_DIR / "000374_corpscout_se_company_info_field_columns.down.sql").read_text()

    columns = declared_columns("se_company_info")
    at = columns.index("primary_sni_code") + 1
    assert tuple(columns[at : at + 8]) == SE_COMPANY_INFO_REGISTRY_COLUMNS
    assert columns[at + 8] == "wikidata_id"
    assert tuple(columns[-len(FINAL_PROVENANCE):]) == FINAL_PROVENANCE
    assert [c for c in columns if c != "evidence_set_hash"] == list(SE_COMPANY_INFO_COLUMNS)

    assert up.count("ADD COLUMN IF NOT EXISTS") == 8 and down.count("DROP COLUMN IF EXISTS") == 8
    # Only the projection moves: the artifacts, the decisions and the long tables keep theirs.
    statements, down_statements = (_statements(text) for text in (up, down))
    assert statements.count("ALTER TABLE corpscout.se_company_info\n") == 1
    for other in ("se_company_info_scb", "se_company_info_field_value", "se_company_field"):
        assert other not in statements and other not in down_statements
