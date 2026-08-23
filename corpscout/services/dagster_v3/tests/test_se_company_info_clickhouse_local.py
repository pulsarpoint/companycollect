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

The script also replays 000300's live upgrade rather than assuming it: the SCB table is
first filled with v1 rows (the SELECT projected down to its pre-000300 columns), then
000300's ALTER lands on them, and the next publish pass appends one v2 version per company
-- so "ADD COLUMN + MODIFY COLUMN of a MATERIALIZED expression, on a table with rows in it"
is executed on the deployed ClickHouse version, not argued about.

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
    # text_translations holds the translator service's English renderings, which the SCB
    # SELECT joins. 000069 is the CREATE TABLE whose columns are the deployed ones; the
    # later 000252 only re-keys the table (ORDER BY gains the language pair) through a
    # build-beside-and-RENAME dance whose INSERT/RENAME statements this per-table filter
    # cannot replay -- and the re-key changes no column and no result here, since the
    # SELECT names one explicit language pair and picks argMax(version) itself.
    "000069_corpscout_text_translations_table_column.up.sql",
    "000084_corpscout_se_company_registry.up.sql",
    "000174_corpscout_company_identifier.up.sql",
    "000243_corpscout_esef_source_documents.up.sql",
    "000244_corpscout_company_source_records.up.sql",
    "000247_corpscout_esef_llm_provenance.up.sql",
    "000248_corpscout_esef_source_record_uid_cast.up.sql",
    "000281_corpscout_se_company_presentation_fields.up.sql",
    "000297_corpscout_se_company_info.up.sql",
    "000299_corpscout_se_company_info_sole_traders.up.sql",
    # 000301 adds se_company_info.description_sv, which INSERT_COLUMNS below names: it is
    # applied with the rest of the DDL rather than late like 000300, because it lands on
    # the final (empty here until FINAL_ROW_SQL) and not on the SCB artifact 000300 alters.
    "000301_corpscout_se_company_info_description_sv.up.sql",
)
# Applied later in the script than the rest, on purpose: on the live host 000300 lands on
# a se_company_info_scb that already holds 3.5M v1 rows, and "ALTER ... MODIFY COLUMN of a
# MATERIALIZED expression, with rows present" is exactly the statement this harness has to
# prove ClickHouse accepts. Applying it with the other DDL would only ever test it against
# an empty table.
ENGLISH_MIGRATION = ("000300_corpscout_se_company_info_scb_english.up.sql",)
NEEDED_TABLES = frozenset(
    {
        "se_companies",
        "se_industries",
        "text_translations",
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
# ALPHA's final row resolves *now*: artifact observed_at is now64 at append time (scb.py),
# so a fixed literal could no longer be "after every artifact this script has produced".
T_RESOLVED = "now64(3, 'UTC')"
# DateTime64(3) has millisecond resolution and consecutive statements can share one, so
# every point where a stamp must be strictly newer than the previous one is separated by a
# real pause. FORMAT Null keeps sleep's own row out of the marked-section stream.
SETTLE = "SELECT sleep(0.05) FORMAT Null;\n"
EVIDENCE_HASHES = ("a" * 64, "b" * 64, "c" * 64)


def _schema_statements(migrations: tuple[str, ...]) -> list[str]:
    """CREATE/ALTER TABLE statements for NEEDED_TABLES only, in migration order.

    Several of the included migration files also touch tables this pipeline never
    reads (se_company_addresses, se_financial_reports, wikidata_persons, ...);
    applying those blindly (as a whole-file dump would) fails since this harness
    never creates them. Filtered per statement's target table instead.
    """
    statements: list[str] = []
    for name in migrations:
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

INSERT INTO corpscout.text_translations
    (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text,
     provider, model, version)
VALUES
    ('corpscout.se_companies', 'activity_description', cityHash64('IT-konsulter.'), 'sv', 'en',
     'IT consultants.', 'translator', 'm', 1);

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

# The pre-000300 state of the table on the live host: rows written by the v1 SELECT, whose
# v1 evidence_hash was computed without the English text. Written by projecting the current
# SELECT down to its v1 columns, so no second copy of the SCB SELECT has to be maintained
# here just to produce them.
V1_SCB_COLUMNS = tuple(c for c in SE_COMPANY_INFO_SCB_COLUMNS if c != "activity_description_en")

# BETA's description is translated only after the artifact already holds its row -- the
# translator service runs outside Dagster, so this is the ordinary case, not an edge one.
BETA_TRANSLATION_SQL = """
INSERT INTO corpscout.text_translations
    (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text,
     provider, model, version)
VALUES
    ('corpscout.se_companies', 'activity_description', cityHash64('Handel med datorer.'), 'sv', 'en',
     'Trade in computers.', 'translator', 'm', 1);
""".strip()

# ALPHA's changed payload is translated only after ALPHA has been published -- the case
# that proves an appended version is visible to the change scan at all (see
# test_a_translation_arriving_after_publication_re_selects_the_company).
ALPHA_TRANSLATION_SQL = """
INSERT INTO corpscout.text_translations
    (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text,
     provider, model, version)
VALUES
    ('corpscout.se_companies', 'activity_description', cityHash64('IT-konsulter och molntjaenster.'),
     'sv', 'en', 'IT consultants and cloud services.', 'translator', 'm', 1);
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


def _v1_scb_rows_sql(params: dict[str, object]) -> str:
    """The v1 artifact rows, inserted straight (no anti-join needed: the table is empty)."""
    columns = ", ".join(V1_SCB_COLUMNS)
    return (
        f"INSERT INTO corpscout.se_company_info_scb ({columns})\n"
        f"SELECT {columns} FROM (\n{_render(SE_COMPANY_INFO_SCB_SQL, params)}\n);\n"
    )


def _string_array(values: tuple[str, ...]) -> str:
    return "[" + ", ".join(f"'{v}'" for v in values) + "]"


# One row shaped like a real info.py `_final_row(...)` tuple, in INSERT_COLUMNS order
# (imported, never hand-copied). description_source_count=2, suggestion_id=NULL and
# correction_ids=[] together satisfy info.py's PENDING_MODEL_SQL, and resolved_at
# (T_RESOLVED = now64, inserted after a SETTLE) is later than every artifact observed_at
# ALPHA carries at that point -- so build_changed_companies_sql must drop ALPHA under
# default settings and re-select it only when include_pending=1, until pass 5 appends a
# genuinely newer artifact version.
_FINAL_ROW_VALUES = (
    f"'{ALPHA}', 'Alpha AB', 'AB', 'active', '2001-02-03', "
    "'Alpha builds payment software.', 'Alpha bygger betalprogramvara.', 'en', 'llm', "
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


def _changed_params(
    *, pending_model_only: int, include_pending: int, resolve_all: int = 0
) -> dict[str, object]:
    return {
        "all_companies": 1,
        "company_ids": ("",),
        "pending_model_only": pending_model_only,
        "include_pending": include_pending,
        "resolve_all": resolve_all,
        "after_company_id": "",
        "page_size": 10,
    }


def _script(*, join_use_nulls: int) -> str:
    render_params = {"source_run_id": RUN_ID}
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;")
    parts.append(";\n".join(_schema_statements(MIGRATIONS)) + ";")
    parts.append(FIXTURE)

    # The live host's starting point: se_company_info_scb already holds v1 rows, and 000300
    # lands on them. Their evidence_hash must survive the ALTER untouched (MODIFY COLUMN of
    # a MATERIALIZED expression rewrites no existing part), and the column added beside them
    # must read as its DEFAULT ''.
    parts.append(_v1_scb_rows_sql(render_params))
    parts.append(
        _marked(
            "v1_rows",
            "SELECT company_id, toString(evidence_hash), toString(observed_at) "
            "FROM corpscout.se_company_info_scb ORDER BY company_id",
        )
    )
    parts.append(SETTLE)
    parts.append(";\n".join(_schema_statements(ENGLISH_MIGRATION)) + ";")
    parts.append(
        _marked(
            "v1_rows_after_migration",
            "SELECT company_id, toString(evidence_hash), activity_description_en "
            "FROM corpscout.se_company_info_scb ORDER BY company_id",
        )
    )

    # Pass 1: every artifact's SELECT, staged then copied through the anti-join.
    for table, columns, sql in ARTIFACTS:
        parts.append(_publish_pass(table, columns, sql, render_params))
    parts.append(_marked("counts", COUNTS_SQL))
    parts.append(
        _marked(
            "scb_versions",
            "SELECT company_id, toString(evidence_hash), activity_description_en, "
            "toString(observed_at) FROM corpscout.se_company_info_scb "
            "ORDER BY company_id, observed_at, evidence_hash",
        )
    )
    parts.append(
        _marked(
            "scb_final",
            "SELECT company_id, ifNull(activity_description, ''), activity_description_en "
            "FROM corpscout.se_company_info_scb FINAL ORDER BY company_id",
        )
    )

    # Nothing published yet: every company with an artifact row is "changed".
    parts.append(
        _marked(
            "changed_empty_final",
            _render(build_changed_companies_sql(), _changed_params(pending_model_only=0, include_pending=0)),
        )
    )

    # Pass 2: identical rerun -- same evidence, so the anti-join lets nothing through. The
    # SETTLE guarantees the rerun's now64 differs from pass 1's, so an unchanged row keeping
    # its stamp is a real result: the anti-join keys on the hash, and rows it skips are never
    # rewritten (observed_at advancing on its own would re-select the company forever).
    parts.append(SETTLE)
    for table, columns, sql in ARTIFACTS:
        parts.append(_publish_pass(table, columns, sql, render_params))
    parts.append(_marked("counts_after_rerun", COUNTS_SQL))
    parts.append(
        _marked(
            "scb_versions_after_rerun",
            "SELECT company_id, toString(evidence_hash), activity_description_en, "
            "toString(observed_at) FROM corpscout.se_company_info_scb "
            "ORDER BY company_id, observed_at, evidence_hash",
        )
    )

    # Pass 3: ALPHA's SCB payload changes (same source_record_uid, new evidence_hash)
    # -- exactly one new version is appended; BETA and the other artifacts are untouched.
    parts.append(CHANGED_PAYLOAD_SQL)
    parts.append(SETTLE)
    scb_table, scb_columns, scb_sql = ARTIFACTS[0]
    parts.append(_publish_pass(scb_table, scb_columns, scb_sql, render_params))
    parts.append(_marked("counts_after_changed_payload", COUNTS_SQL))

    # Pass 4: BETA's description gets translated (by a service outside this pipeline), so
    # only BETA's evidence changes -- exactly one new version is appended, carrying the
    # English text the review page and the model will read.
    parts.append(BETA_TRANSLATION_SQL)
    parts.append(SETTLE)
    parts.append(_publish_pass(scb_table, scb_columns, scb_sql, render_params))
    parts.append(_marked("counts_after_beta_translation", COUNTS_SQL))
    parts.append(
        _marked(
            "beta_english",
            "SELECT activity_description_en FROM corpscout.se_company_info_scb FINAL "
            f"WHERE company_id = '{BETA}'",
        )
    )

    # Publish ALPHA's final row, then rescan: ALPHA drops out under default settings
    # (its resolved_at is newer than every artifact) and reappears once include_pending=1
    # picks it up through PENDING_MODEL_SQL. BETA has no final row either way, so it is
    # selected in both scans.
    parts.append(SETTLE)
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
    # resolve_all re-selects a settled company although nothing about its evidence moved.
    parts.append(
        _marked(
            "changed_after_final_resolve_all",
            _render(
                build_changed_companies_sql(),
                _changed_params(pending_model_only=0, include_pending=0, resolve_all=1),
            ),
        )
    )

    # Pass 5 -- the regression this harness exists to catch: a translation arrives for a
    # company that is already published and settled. The appended version is stamped when it
    # is appended, so it is newer than the final row's resolved_at and the change scan picks
    # ALPHA up again. Stamped from the register's own updated_from_raw_at (one constant per
    # bulk load, older than every resolved_at) it never would, and the English text would
    # never reach se_company_info.
    parts.append(ALPHA_TRANSLATION_SQL)
    parts.append(SETTLE)
    parts.append(_publish_pass(scb_table, scb_columns, scb_sql, render_params))
    parts.append(_marked("counts_after_alpha_translation", COUNTS_SQL))
    parts.append(
        _marked(
            "changed_after_alpha_translation",
            _render(build_changed_companies_sql(), _changed_params(pending_model_only=0, include_pending=0)),
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
            "JSONExtractString(payload_json, 'primary_sni_code'), "
            "JSONExtractString(payload_json, 'activity_description_en') "
            f"FROM ({rows_sql}) ORDER BY source",
        )
    )
    parts.append(
        _marked(
            "final_description",
            "SELECT ifNull(description, ''), ifNull(description_sv, '') "
            f"FROM corpscout.se_company_info FINAL WHERE company_id = '{ALPHA}'",
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


def test_000300_alters_a_table_that_already_holds_v1_rows(
    sections: dict[str, list[list[str]]],
) -> None:
    """The live upgrade path: 000300 lands on 3.5M existing rows. The script would have
    failed outright had ClickHouse 26.5 rejected ADD COLUMN + MODIFY COLUMN of a
    MATERIALIZED expression in one ALTER on a non-empty table, so reaching these rows is
    itself the acceptance proof; what is asserted here is that the rows came through
    unchanged -- old parts keep their v1 hash, and the new column reads as DEFAULT ''."""
    before = {row[0]: row[1] for row in sections["v1_rows"]}
    after = {row[0]: row[1] for row in sections["v1_rows_after_migration"]}
    assert set(before) == {ALPHA, BETA, GAMMA}
    assert after == before
    assert [row[2] for row in sections["v1_rows_after_migration"]] == ["", "", ""]


def test_the_next_run_appends_v2_versions_carrying_the_english_description(
    sections: dict[str, list[list[str]]],
) -> None:
    """v2 hashes the English text, so every company is re-appended once -- with a strictly
    newer observed_at than the v1 row it supersedes, which is what makes FINAL keep it (and
    what makes the change scan see it). No ReplacingMergeTree version tie is involved."""
    v1 = {row[0]: (row[1], row[2]) for row in sections["v1_rows"]}
    versions: dict[str, list[tuple[str, str, str]]] = {}
    for company, evidence_hash, english, observed_at in sections["scb_versions"]:
        versions.setdefault(company, []).append((evidence_hash, english, observed_at))
    assert set(versions) == {ALPHA, BETA, GAMMA}
    for company, rows in versions.items():
        assert len(rows) == 2, company
        v1_hash, v1_stamp = v1[company]
        (old_hash, _, old_stamp), (new_hash, _, new_stamp) = rows  # ordered by observed_at
        assert (old_hash, old_stamp) == (v1_hash, v1_stamp)  # the pre-000300 row, untouched
        assert new_hash != v1_hash  # the appended version hashes differently (v2)
        assert new_stamp > v1_stamp  # ... and is observed later, so it wins by version

    final = {row[0]: (row[1], row[2]) for row in sections["scb_final"]}
    assert final[ALPHA] == ("IT-konsulter.", "IT consultants.")
    assert final[BETA] == ("Handel med datorer.", "")  # not translated yet at this point
    assert final[GAMMA] == ("Snickeri.", "")


def test_a_late_translation_appends_exactly_one_new_version(
    sections: dict[str, list[list[str]]],
) -> None:
    """The translator service writes text_translations outside this pipeline, so a
    description translated between two runs must show up as a new artifact version."""
    before = _counts(sections["counts_after_changed_payload"])
    after = _counts(sections["counts_after_beta_translation"])
    assert after["se_company_info_scb"] == before["se_company_info_scb"] + 1
    assert after["se_company_info_esef"] == before["se_company_info_esef"]
    assert after["se_company_info_wikidata"] == before["se_company_info_wikidata"]
    assert sections["beta_english"] == [["Trade in computers."]]


def test_artifact_publishes_append_new_versions_and_are_idempotent(
    sections: dict[str, list[list[str]]],
) -> None:
    """Pins R9: ALPHA's scb_source_record_uid path and BETA's bolagsverket fallback
    both produce a non-empty source_record_uid, and GAMMA (12-digit sole trader) is admitted
    by 000299, so all three companies publish -- 6 SCB rows, since each one's pre-000300 v1
    row is still on disk beside the v2 version this pass appended."""
    counts = _counts(sections["counts"])
    assert counts == {"se_company_info_scb": 6, "se_company_info_esef": 1, "se_company_info_wikidata": 1}

    # Second pass, identical evidence: the anti-join lets nothing new through -- and the
    # rows it skips keep the observed_at they were appended with, although the rerun's
    # now64 is a later instant. A restamped row would look newer than the final that
    # published it and re-select the company on every single run.
    assert _counts(sections["counts_after_rerun"]) == counts
    assert sections["scb_versions_after_rerun"] == sections["scb_versions"]

    # Third pass, ALPHA's SCB payload changed: exactly one new version, nothing else.
    after_change = _counts(sections["counts_after_changed_payload"])
    assert after_change["se_company_info_scb"] == counts["se_company_info_scb"] + 1
    assert after_change["se_company_info_esef"] == counts["se_company_info_esef"]
    assert after_change["se_company_info_wikidata"] == counts["se_company_info_wikidata"]


def test_a_translation_arriving_after_publication_re_selects_the_company(
    sections: dict[str, list[list[str]]],
) -> None:
    """The regression 000300 would otherwise have shipped: the translator writes
    text_translations between two runs, the SCB artifact appends the changed row -- and the
    change scan has to see it. It only can because observed_at is the moment the version was
    appended; se_companies.updated_from_raw_at is one constant per bulk load, older than
    every resolved_at, so a version stamped with it is invisible to this scan forever."""
    before = _counts(sections["counts_after_beta_translation"])
    after = _counts(sections["counts_after_alpha_translation"])
    assert after["se_company_info_scb"] == before["se_company_info_scb"] + 1
    # ALPHA was settled (dropped by the default scan two sections earlier) and is back.
    assert {row[0] for row in sections["changed_after_final_default"]} == {BETA, GAMMA}
    assert {row[0] for row in sections["changed_after_alpha_translation"]} == {ALPHA, BETA, GAMMA}


def test_resolve_all_re_selects_settled_companies(sections: dict[str, list[list[str]]]) -> None:
    """For a rules-only change -- new merge logic, a new artifact column -- nothing about a
    company's evidence moved, so only resolve_all can bring it back."""
    assert {row[0] for row in sections["changed_after_final_default"]} == {BETA, GAMMA}
    assert {row[0] for row in sections["changed_after_final_resolve_all"]} == {ALPHA, BETA, GAMMA}


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
    is_valid_json, date_type, incorporation_date, dissolution_date, sni_code, english = scb_row[3:9]
    # info_rules reads this key and prefers it over the Swedish text: ALPHA's newest SCB
    # version is the changed payload, and its own translation is what the payload carries.
    assert english == "IT consultants and cloud services."
    assert is_valid_json == "1"
    assert date_type == "String" and incorporation_date == "2001-02-03"  # Nullable(Date32) as text
    assert dissolution_date == ""  # a NULL Date32 renders as '', never as "null"
    assert sni_code == "62010"


def test_final_row_evidence_set_hash_matches_info_rules(sections: dict[str, list[list[str]]]) -> None:
    expected = evidence_set_hash_for(EVIDENCE_HASHES)
    assert sections["evidence_set_hash"] == [[expected]]


def test_the_final_row_carries_both_description_languages(sections: dict[str, list[list[str]]]) -> None:
    """000301's Nullable(String) column takes a value through the same positional
    INSERT_COLUMNS list info.py binds to -- if description_sv were declared in a different
    place than INSERT_COLUMNS names, the two texts would come back transposed."""
    assert sections["final_description"] == [
        ["Alpha builds payment software.", "Alpha bygger betalprogramvara."]
    ]
