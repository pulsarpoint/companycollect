"""Executes the info artifact SELECTs and the final's scan/load queries against the
migrations' DDL in a disposable clickhouse-local. Proves the SQL runs on the deployed
ClickHouse version -- substring tests cannot.

Two companies: ALPHA carries an SCB row whose ``scb_source_record_uid`` is derived
straight from ``scb_source_payload_hash`` (see migration 000244), plus an ESEF filing
and a Wikidata entity linked by orgnr. BETA carries only an SCB row whose
``scb_source_payload_hash`` is empty, so its ``source_record_uid`` must come from the
SCB SELECT's ``bolagsverket_source_record_uid`` fallback -- both derivation paths are
exercised, not just the common one.

The publish sequence (stage -> validate -> LEFT ANTI JOIN copy -> drop stage) mirrors
``publish_with_stage(..., new_versions_only=True)`` in
``dagster_v3.defs.se_company.common``. common.py has no separate SQL-string builder
for that anti-join -- it is inlined in the function -- so ``_publish_pass`` below
copies the shape verbatim instead of importing a builder that does not exist.

The whole script runs twice, once under default settings and once with
``SET join_use_nulls = 1`` prepended (mirrors ``tests/test_se_company_person_clickhouse_local.py``):
every LEFT JOIN miss in ``build_changed_companies_sql`` is read through ``ifNull``, so
both settings must answer identically.
"""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.esef import SE_COMPANY_INFO_ESEF_COLUMNS, SE_COMPANY_INFO_ESEF_SQL
from dagster_v3.defs.se_company.info import (
    INSERT_COLUMNS,
    build_artifact_rows_sql,
    build_changed_companies_sql,
)
from dagster_v3.defs.se_company.info_rules import evidence_set_hash_for
from dagster_v3.defs.se_company.scb import SE_COMPANY_INFO_SCB_COLUMNS, SE_COMPANY_INFO_SCB_SQL
from dagster_v3.defs.se_company.wikidata import (
    SE_COMPANY_INFO_WIKIDATA_COLUMNS,
    SE_COMPANY_INFO_WIKIDATA_SQL,
)
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal, _render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# Every migration that creates or alters one of NEEDED_TABLES, in migration order.
# Derived by `rg -l <table>` over corpscout/clickhouse/migrations for each table the
# six SQL constants (plus the se_company_info* targets) reference, then keeping only
# the files that actually contain a CREATE TABLE/ALTER TABLE on one of those tables
# (several hits were CREATE VIEW bodies that merely *read* the table, or ALTER TABLE
# statements aimed at other tables entirely -- see task-8-report.md for the full
# per-migration accounting).
MIGRATIONS = (
    "000013_corpscout_wikidata_company_seed.up.sql",
    "000017_corpscout_wikidata_company_country.up.sql",
    "000018_corpscout_wikidata_company_augmentations.up.sql",
    "000084_corpscout_se_company_registry.up.sql",
    "000174_corpscout_company_identifier.up.sql",
    "000243_corpscout_esef_source_documents.up.sql",
    "000244_corpscout_company_source_records.up.sql",
    "000247_corpscout_esef_llm_provenance.up.sql",
    "000248_corpscout_esef_source_record_uid_cast.up.sql",
    "000281_corpscout_se_company_presentation_fields.up.sql",
    "000297_corpscout_se_company_info.up.sql",
    "000299_corpscout_se_company_info_sole_traders.up.sql",
)
NEEDED_TABLES = frozenset(
    {
        "se_companies",
        "se_industries",
        "esef_source_documents",
        "esef_document_company_information",
        "wikidata_companies",
        "wikidata_company_identifiers",
        "company_identifier",
        "se_company_info_scb",
        "se_company_info_esef",
        "se_company_info_wikidata",
        "se_company_info",
        "se_company_info_correction",
        "se_company_info_enrichment_observation",
    }
)
_TABLE_RE = re.compile(r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE)

RUN_ID = "fixture-run-1"
ALPHA = "5565200028"  # SCB path: scb_source_payload_hash drives scb_source_record_uid.
BETA = "5560125220"  # Fallback path: no scb hash -- falls back to bolagsverket_source_record_uid.
GAMMA = "196408233412"  # Sole trader: 12-digit personnummer-based id, admitted by 000299.

T_SEED = _literal(datetime(2026, 8, 1, tzinfo=UTC))
T_ESEF_SOURCE = _literal(datetime(2025, 4, 1, tzinfo=UTC))
T_ESEF_INFO = _literal(datetime(2025, 4, 2, tzinfo=UTC))
T_CHANGED = _literal(datetime(2026, 8, 5, tzinfo=UTC))  # ALPHA's new SCB payload version.
T_RESOLVED = _literal(datetime(2026, 8, 10, tzinfo=UTC))  # ALPHA's final row -- after T_CHANGED.
EVIDENCE_HASHES = ("a" * 64, "b" * 64, "c" * 64)


def _schema_statements() -> list[str]:
    """CREATE/ALTER TABLE statements for NEEDED_TABLES only, in migration order.

    Several of the included migration files also touch tables this pipeline never
    reads (se_company_addresses, se_financial_reports, wikidata_persons, ...);
    applying those blindly (as a whole-file dump would) fails since this harness
    never creates them. Filtered per statement's target table instead.
    """
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


FIXTURE = f"""
INSERT INTO corpscout.se_companies
    (company_id, registration_number, legal_name, legal_name_raw, legal_form_code, status,
     incorporation_date, activity_description, source_run_id,
     bolagsverket_source_record_id, bolagsverket_source_payload_hash,
     scb_source_record_id, scb_source_payload_hash, updated_from_raw_at)
VALUES
    ('{ALPHA}', '{ALPHA}', 'Alpha AB', 'ALPHA AB', 'AB', 'active',
     '2001-02-03', 'IT-konsulter.', 'fixture',
     NULL, NULL,
     'scb-1', 'alpha-scb-payload-hash', {T_SEED}),
    ('{BETA}', '{BETA}', 'Beta AB', 'BETA AB', 'AB', 'active',
     '1998-06-15', 'Handel med datorer.', 'fixture',
     'bv-2', 'beta-bolagsverket-payload-hash',
     NULL, NULL, {T_SEED}),
    ('{GAMMA}', '{GAMMA}', 'Gamma Enskild Firma', 'GAMMA ENSKILD FIRMA', 'E', 'active',
     '2010-01-01', 'Snickeri.', 'fixture',
     'bv-3', 'gamma-bolagsverket-payload-hash',
     NULL, NULL, {T_SEED});

INSERT INTO corpscout.se_industries
    (company_id, sequence, is_primary, sni_code, nace_rev2_class_code, source_field,
     source_run_id, source_record_id, source_payload_hash, updated_from_raw_at)
VALUES
    ('{ALPHA}', 1, 1, '62010', '62.01', 'sni', 'fixture', 'ind-1', 'ind-hash', {T_SEED});

INSERT INTO corpscout.esef_source_documents
    (source_document_id, document_type, lei, entity_name, country_iso2, company_id, period_end,
     fiscal_year, package_url, report_url, viewer_url, package_sha256, package_object_key,
     package_size_bytes, parsed_artifact_object_key, artifact_schema_version, parser_name,
     parser_version, archive_status, extraction_status, fact_count, text_fact_count,
     numeric_fact_count, contact_candidate_count, website_candidate_count,
     validation_error_count, validation_warning_count, source_processed_at, source_run_id,
     extracted_at, resolved_at)
VALUES
    ('doc-1', 'annual', '5493001KJTIIGC8Y1R12', 'Alpha AB', 'SE', '{ALPHA}', '2024-12-31', 2024,
     '', '', '', '', '', 0, '', 1, 'p', '1', 'ok', 'ok', 0, 0, 0, 0, 0, 0, 0,
     '2025-04-01 00:00:00.000', 'fixture', '2025-04-01 00:00:00.000', {T_ESEF_SOURCE});

INSERT INTO corpscout.esef_document_company_information
    (source_document_id, package_sha256, lei, country_iso2, company_id, period_end, fiscal_year,
     extraction_status, company_description, description_language, description_confidence,
     description_evidence_ids_json, people_json, products_and_services_json,
     customer_markets_json, operating_geographies_json, business_segments_json,
     material_group_relationships_json, enrichment_artifact_object_key,
     input_artifact_object_key, model_provider, model_name, prompt_version, prompt_tokens,
     completion_tokens, input_character_count, source_run_id, extracted_at, resolved_at)
VALUES
    ('doc-1', '', '5493001KJTIIGC8Y1R12', 'SE', '{ALPHA}', '2024-12-31', 2024, 'ok',
     'Alpha builds payment software.', 'en', 0.9, '[]', '[]', '[]', '[]', '[]', '[]', '[]',
     '', '', 'deepseek', 'm', 'v', 0, 0, 0, 'fixture', '2025-04-02', {T_ESEF_INFO});

INSERT INTO corpscout.wikidata_companies
    (wikidata_id, wikidata_url, name, name_normalized, company_description, has_current_listing,
     listing_count, source_system, source_run_id, source_record_id, source_payload_hash,
     retrieved_at, resolved_at)
VALUES
    ('Q1', 'https://www.wikidata.org/wiki/Q1', 'Alpha', 'alpha', 'Swedish fintech company', 0, 0,
     'wikidata', 'fixture', 'Q1', repeat('0', 64), {T_SEED}, {T_SEED});

INSERT INTO corpscout.wikidata_company_identifiers
    (wikidata_id, identifier_type, wikidata_property_id, identifier_value, is_primary,
     source_system, source_run_id, source_record_id, source_payload_hash, retrieved_at, resolved_at)
VALUES
    ('Q1', 'se_orgnr', 'P6460', '556520-0028', 1, 'wikidata', 'fixture', 'Q1', repeat('0', 64),
     {T_SEED}, {T_SEED});
""".strip()

# A new SCB version for ALPHA: same scb_source_record_id/payload_hash (so
# scb_source_record_uid, derived from those two, is unchanged) but a different
# legal_name/activity_description, so evidence_hash (materialized from those columns)
# changes. Exercises "same (company_id, source_record_uid), new evidence_hash" -- the
# exact case publish_with_stage's anti-join is built to let through as a new version.
CHANGED_PAYLOAD_SQL = f"""
INSERT INTO corpscout.se_companies
    (company_id, registration_number, legal_name, legal_name_raw, legal_form_code, status,
     incorporation_date, activity_description, source_run_id,
     bolagsverket_source_record_id, bolagsverket_source_payload_hash,
     scb_source_record_id, scb_source_payload_hash, updated_from_raw_at)
VALUES
    ('{ALPHA}', '{ALPHA}', 'Alpha Aktiebolag', 'ALPHA AKTIEBOLAG', 'AB', 'active',
     '2001-02-03', 'IT-konsulter och molntjaenster.', 'fixture-v2',
     NULL, NULL,
     'scb-1', 'alpha-scb-payload-hash', {T_CHANGED});
""".strip()

ARTIFACTS = (
    ("se_company_info_scb", SE_COMPANY_INFO_SCB_COLUMNS, SE_COMPANY_INFO_SCB_SQL),
    ("se_company_info_esef", SE_COMPANY_INFO_ESEF_COLUMNS, SE_COMPANY_INFO_ESEF_SQL),
    ("se_company_info_wikidata", SE_COMPANY_INFO_WIKIDATA_COLUMNS, SE_COMPANY_INFO_WIKIDATA_SQL),
)
COUNTS_SQL = (
    "SELECT 'se_company_info_scb', count() FROM corpscout.se_company_info_scb "
    "UNION ALL SELECT 'se_company_info_esef', count() FROM corpscout.se_company_info_esef "
    "UNION ALL SELECT 'se_company_info_wikidata', count() FROM corpscout.se_company_info_wikidata"
)


def _publish_pass(table: str, columns: tuple[str, ...], select_sql: str, params: dict[str, object]) -> str:
    """Mirrors ``publish_with_stage(..., new_versions_only=True)`` in
    ``dagster_v3.defs.se_company.common``: stage <- SELECT, then copy into the target
    only the rows whose (company_id, source_record_uid, evidence_hash) is not already
    there, via a LEFT ANTI JOIN. common.py has no separate SQL-string builder for this
    shape -- it is inlined inside that function -- so the anti-join text below is
    copied verbatim from it rather than re-derived.
    """
    col_list = ", ".join(columns)
    stage_cols = ", ".join(f"stage.{c}" for c in columns)
    stage = f"corpscout._tmp_{table}"
    anti_join = (
        f"FROM {stage} AS stage\n"
        f"LEFT ANTI JOIN corpscout.{table} AS existing\n"
        "ON existing.company_id = stage.company_id "
        "AND existing.source_record_uid = stage.source_record_uid "
        "AND existing.evidence_hash = stage.evidence_hash"
    )
    rendered_select = _render(select_sql, params)
    return (
        f"CREATE TABLE {stage} AS corpscout.{table};\n"
        f"INSERT INTO {stage} ({col_list})\n{rendered_select};\n"
        f"INSERT INTO corpscout.{table} ({col_list})\nSELECT {stage_cols}\n{anti_join};\n"
        f"DROP TABLE {stage};\n"
    )


def _string_array(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(f"'{v}'" for v in values) + "]"


# One row shaped like a real info.py `_final_row(...)` tuple, in INSERT_COLUMNS order
# (imported, never hand-copied). description_source_count=2, suggestion_id=NULL and
# correction_ids=[] together satisfy info.py's PENDING_MODEL_SQL, and resolved_at
# (T_RESOLVED) is later than every artifact observed_at ALPHA carries (including the
# changed-payload SCB version at T_CHANGED) -- so build_changed_companies_sql must
# drop ALPHA under default settings and re-select it only when include_pending=1.
_FINAL_ROW_VALUES = (
    f"'{ALPHA}', 'Alpha AB', 'AB', 'active', '2001-02-03', "
    "'Alpha builds payment software.', 'en', 'llm', "
    f"{_string_array(('esef', 'wikidata'))}, {_string_array(('doc-esef-uid', 'wikidata:Q1'))}, 2, "
    "'62.01', '62010', 'Q1', '5493001KJTIIGC8Y1R12', "
    f"{_string_array(('scb-uid-1', 'doc-esef-uid', 'wikidata:Q1'))}, {_string_array(EVIDENCE_HASHES)}, "
    f"[], NULL, 'deterministic', 'se-company-info-rules', 'se-company-info-rules-v1', "
    f"'{RUN_ID}', {T_RESOLVED}"
)
FINAL_ROW_SQL = (
    "CREATE TABLE corpscout._tmp_se_company_info AS corpscout.se_company_info;\n"
    f"INSERT INTO corpscout._tmp_se_company_info ({', '.join(INSERT_COLUMNS)})\n"
    f"VALUES ({_FINAL_ROW_VALUES});\n"
    f"INSERT INTO corpscout.se_company_info ({', '.join(INSERT_COLUMNS)})\n"
    f"SELECT {', '.join(INSERT_COLUMNS)} FROM corpscout._tmp_se_company_info;\n"
    "DROP TABLE corpscout._tmp_se_company_info;\n"
)


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query} FORMAT TSV;\n"


def _changed_params(*, pending_model_only: int, include_pending: int) -> dict[str, object]:
    return {
        "all_companies": 1,
        "company_ids": ("",),
        "pending_model_only": pending_model_only,
        "include_pending": include_pending,
        "after_company_id": "",
        "page_size": 10,
    }


def _script(*, join_use_nulls: int) -> str:
    render_params = {"source_run_id": RUN_ID}
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;")
    parts.append(";\n".join(_schema_statements()) + ";")
    parts.append(FIXTURE)

    # Pass 1: every artifact's SELECT, staged then copied through the anti-join.
    for table, columns, sql in ARTIFACTS:
        parts.append(_publish_pass(table, columns, sql, render_params))
    parts.append(_marked("counts", COUNTS_SQL))

    # Nothing published yet: every company with an artifact row is "changed".
    parts.append(
        _marked(
            "changed_empty_final",
            _render(build_changed_companies_sql(), _changed_params(pending_model_only=0, include_pending=0)),
        )
    )

    # Pass 2: identical rerun -- same evidence, so the anti-join lets nothing through.
    for table, columns, sql in ARTIFACTS:
        parts.append(_publish_pass(table, columns, sql, render_params))
    parts.append(_marked("counts_after_rerun", COUNTS_SQL))

    # Pass 3: ALPHA's SCB payload changes (same source_record_uid, new evidence_hash)
    # -- exactly one new version is appended; BETA and the other artifacts are untouched.
    parts.append(CHANGED_PAYLOAD_SQL)
    scb_table, scb_columns, scb_sql = ARTIFACTS[0]
    parts.append(_publish_pass(scb_table, scb_columns, scb_sql, render_params))
    parts.append(_marked("counts_after_changed_payload", COUNTS_SQL))

    # Publish ALPHA's final row, then rescan: ALPHA drops out under default settings
    # (its resolved_at is newer than every artifact) and reappears once include_pending=1
    # picks it up through PENDING_MODEL_SQL. BETA has no final row either way, so it is
    # selected in both scans.
    parts.append(FINAL_ROW_SQL)
    parts.append(
        _marked(
            "changed_after_final_default",
            _render(build_changed_companies_sql(), _changed_params(pending_model_only=0, include_pending=0)),
        )
    )
    parts.append(
        _marked(
            "changed_after_final_pending",
            _render(build_changed_companies_sql(), _changed_params(pending_model_only=0, include_pending=1)),
        )
    )

    rows_sql = _render(build_artifact_rows_sql(), {"company_ids": (ALPHA,)})
    parts.append(
        _marked(
            "rows",
            "SELECT source, company_id, source_record_uid, isValidJSON(payload_json), "
            "JSONType(payload_json, 'incorporation_date'), "
            "JSONExtractString(payload_json, 'incorporation_date'), "
            "JSONExtractString(payload_json, 'dissolution_date'), "
            "JSONExtractString(payload_json, 'primary_sni_code') "
            f"FROM ({rows_sql}) ORDER BY source",
        )
    )
    parts.append(
        _marked(
            "evidence_set_hash",
            f"SELECT toString(evidence_set_hash) FROM corpscout.se_company_info FINAL WHERE company_id = '{ALPHA}'",
        )
    )
    return "\n".join(parts) + "\n"


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command, input=_script(join_use_nulls=request.param), capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in completed.stdout.splitlines():
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        elif current and line.strip():
            result[current].append(line.split("\t"))
    return result


def _counts(rows: list[list[str]]) -> dict[str, int]:
    return {source: int(count) for source, count in rows}


def test_artifact_publishes_append_new_versions_and_are_idempotent(
    sections: dict[str, list[list[str]]],
) -> None:
    """Pins R9: ALPHA's scb_source_record_uid path and BETA's bolagsverket fallback
    both produce a non-empty source_record_uid, and GAMMA (12-digit sole trader) is admitted
    by 000299, so se_company_info_scb gets 3 rows."""
    counts = _counts(sections["counts"])
    assert counts == {"se_company_info_scb": 3, "se_company_info_esef": 1, "se_company_info_wikidata": 1}

    # Second pass, identical evidence: the anti-join lets nothing new through.
    assert _counts(sections["counts_after_rerun"]) == counts

    # Third pass, ALPHA's SCB payload changed: exactly one new version, nothing else.
    after_change = _counts(sections["counts_after_changed_payload"])
    assert after_change["se_company_info_scb"] == counts["se_company_info_scb"] + 1
    assert after_change["se_company_info_esef"] == counts["se_company_info_esef"]
    assert after_change["se_company_info_wikidata"] == counts["se_company_info_wikidata"]


def test_changed_companies_scan_tracks_publication_and_pending_model(
    sections: dict[str, list[list[str]]],
) -> None:
    """Pins build_changed_companies_sql's three states: never published, published and
    settled, and published-but-still-owed-a-description (include_pending)."""
    assert {row[0] for row in sections["changed_empty_final"]} == {ALPHA, BETA, GAMMA}
    assert {row[0] for row in sections["changed_after_final_default"]} == {BETA, GAMMA}
    assert {row[0] for row in sections["changed_after_final_pending"]} == {ALPHA, BETA, GAMMA}


def test_artifact_rows_sql_returns_one_row_per_source_for_alpha(
    sections: dict[str, list[list[str]]],
) -> None:
    rows = sections["rows"]
    assert sorted(row[0] for row in rows) == ["esef", "scb", "wikidata"]
    wikidata_row = next(row for row in rows if row[0] == "wikidata")
    assert wikidata_row[2] == "wikidata:Q1"

    # Ruling R3, pinned by execution rather than by substring: every payload column is
    # stringified INSIDE the ifNull, so the map is String -> String throughout. A
    # Date32 therefore arrives as JSON *text* (not a number or a JSON date), and a NULL
    # arrives as the empty string -- which is exactly what info_rules reads as "missing".
    # The rejected shape, toString(ifNull(col, '')), is a NO_COMMON_TYPE error on 26.5,
    # so this row could not exist at all if the expressions were the other way round.
    scb_row = next(row for row in rows if row[0] == "scb")
    is_valid_json, date_type, incorporation_date, dissolution_date, sni_code = scb_row[3:8]
    assert is_valid_json == "1"
    assert date_type == "String" and incorporation_date == "2001-02-03"  # Nullable(Date32) as text
    assert dissolution_date == ""  # a NULL Date32 renders as '', never as "null"
    assert sni_code == "62010"


def test_final_row_evidence_set_hash_matches_info_rules(sections: dict[str, list[list[str]]]) -> None:
    expected = evidence_set_hash_for(EVIDENCE_HASHES)
    assert sections["evidence_set_hash"] == [[expected]]
