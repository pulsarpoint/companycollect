"""Executes every candidate extractor's scope and candidates SQL against the migrations'
DDL in a disposable clickhouse-local, then publishes the rows exactly the way
publish_candidates does (stage -> anti-join on five columns -> insert) and reads them back.
Substring tests cannot prove the SQL runs on ClickHouse 26.5; this file does.

Two companies. HB (Handelsbanken's orgnr and LEI, fixture content) has rows in every source
table: registry rows from both registers, an SCB artifact with its legal facts and Swedish and English text, an
ESEF filing text and ESEF metrics, a Wikidata entity with a website, a Ratsit report with
an industry and two financial periods, two domain candidates and two Bolagsverket financial
years -- so every extractor produces its documented rows for it. SOLO has SCB text only
(Swedish, untranslated), two registry rows whose statuses disagree, and a Bolagsverket legal
name that is a placeholder -- the single-source company the LLM gate must skip.

The script runs twice, under join_use_nulls 0 and 1: every LEFT JOIN miss in the
extractors is read through ifNull, so both settings must answer identically.

The publish mirror below copies publish_with_stage(new_versions_only=True,
anti_join_columns=CANDIDATE_ANTI_JOIN_COLUMNS) as publish_candidates calls it: the
function inlines its SQL, so the shape is repeated here rather than imported.
"""

import hashlib
import json
import re
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

import pytest

from dagster_v3.defs.se_company.common import EPOCH
from dagster_v3.defs.se_company.fields.candidates.common import (
    CANDIDATE_ANTI_JOIN_COLUMNS,
    CANDIDATE_SELECT_COLUMNS,
)
from dagster_v3.defs.se_company.fields.candidates import bolagsverket as bolagsverket_candidates
from dagster_v3.defs.se_company.fields.candidates import domains as domains_candidates
from dagster_v3.defs.se_company.fields.candidates import esef as esef_candidates
from dagster_v3.defs.se_company.fields.candidates import llm as llm_candidates
from dagster_v3.defs.se_company.fields.candidates import ratsit as ratsit_candidates
from dagster_v3.defs.se_company.fields.candidates import scb as scb_candidates
from dagster_v3.defs.se_company.fields.candidates import wikidata as wikidata_candidates
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_FIELD_CANDIDATE_COLUMNS
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal, _render

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
# Every migration that creates, alters, renames or (re)defines one of NEEDED_TABLES /
# NEEDED_VIEWS, in ledger order. 000373 is plan 1's candidate-table migration, located by
# glob so its exact name is not repeated here.
MIGRATIONS = (
    "000001_reference_nace_categories.up.sql",
    "000002_reference_exchange_rates.up.sql",
    "000013_corpscout_wikidata_company_seed.up.sql",
    "000017_corpscout_wikidata_company_country.up.sql",
    "000018_corpscout_wikidata_company_augmentations.up.sql",
    "000084_corpscout_se_company_registry.up.sql",
    # se_bolagsverket_financial_metrics is 000090's se_financial_metrics after 000285's RENAME;
    # the view (000286) reads its 000244 source_record_uid and 000284 observation_kind columns.
    "000090_corpscout_se_financial_tables.up.sql",
    "000149_corpscout_esef_filings.up.sql",
    "000174_corpscout_company_identifier.up.sql",
    "000244_corpscout_company_source_records.up.sql",
    "000257_corpscout_se_company_profile_history.up.sql",
    "000269_corpscout_company_domains.up.sql",
    "000284_corpscout_se_financial_metrics_unified_years.up.sql",
    "000285_corpscout_se_bolagsverket_financial_metrics_rename.up.sql",
    "000286_corpscout_se_financial_source_views.up.sql",
    "000297_corpscout_se_company_info.up.sql",
    "000300_corpscout_se_company_info_scb_english.up.sql",
    "000306_corpscout_se_company_info_legal_form_label.up.sql",
    "000343_corpscout_se_ratsit_normalized_segments.up.sql",
    "000346_corpscout_se_ratsit_normalization_v2.up.sql",
    "000364_corpscout_esef_personnel_expenses.up.sql",
    "000365_corpscout_se_company_info_esef_enrichment.up.sql",
    next(path.name for path in sorted(MIGRATIONS_DIR.glob("000373_*.up.sql"))),
)
NEEDED_TABLES = frozenset({
    "nace_categories", "exchange_rates",
    "wikidata_companies", "wikidata_company_websites",
    "se_industries", "se_financial_metrics", "se_bolagsverket_financial_metrics",
    "esef_filings", "esef_financial_metrics", "company_identifier",
    "se_company_registry_current", "company_domains",
    "se_company_info_scb", "se_company_info_esef", "se_company_info_wikidata",
    "se_ratsit_company", "se_ratsit_company_industry_codes", "se_ratsit_financial_periods",
    "se_company_field_candidate",
})
NEEDED_VIEWS = frozenset({"se_financials_bolagsverket_current", "se_financials_esef_current"})
_OBJECT_RE = re.compile(
    r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE|RENAME TABLE|CREATE (?:OR REPLACE )?VIEW)\s+corpscout\.(\w+)",
    re.IGNORECASE,
)

RUN_ID = "candidates-fixture-run"
HB = "5020077862"
SOLO = "5560125220"
HB_LEI = "NHBDILHZTYCNBV5UYZ31"
HB_QID = "Q1421630"  # fixture value; whether it is the real Handelsbanken item is irrelevant here
HB_PACKAGE_SHA = "e" * 64
HB_RATSIT_SHA = "f" * 64
ZERO_HASH = "0" * 64


def _stamp(moment: datetime) -> tuple[str, str]:
    """(SQL literal, the toString() text ClickHouse prints for it)."""
    return _literal(moment), moment.strftime("%Y-%m-%d %H:%M:%S.000")


T_REG, T_REG_TEXT = _stamp(datetime(2026, 8, 1, tzinfo=UTC))            # both registry rows, SOLO's artifact
T_ART, T_ART_TEXT = _stamp(datetime(2026, 8, 2, tzinfo=UTC))            # HB's SCB artifact
T_ART2, T_ART2_TEXT = _stamp(datetime(2026, 8, 5, tzinfo=UTC))          # HB's changed SCB artifact (Task 3)
T_IND, T_IND_TEXT = _stamp(datetime(2026, 7, 28, tzinfo=UTC))           # se_industries bulk stamp
T_FIN, _ = _stamp(datetime(2026, 8, 3, tzinfo=UTC))                     # Bolagsverket metrics resolved_at
T_ESEF_ART, T_ESEF_ART_TEXT = _stamp(datetime(2025, 4, 2, tzinfo=UTC))  # ESEF artifact (older than SINCE)
T_ESEF_FIN, _ = _stamp(datetime(2026, 8, 4, tzinfo=UTC))                # ESEF metrics resolved_at
T_WD, T_WD_TEXT = _stamp(datetime(2026, 7, 15, tzinfo=UTC))             # Wikidata artifact + entity
T_WEB, T_WEB_TEXT = _stamp(datetime(2026, 7, 16, tzinfo=UTC))           # Wikidata website row
T_RATSIT_TEXT = "2026-08-10 12:00:00.000"
T_RATSIT = "toDateTime64('2026-08-10 12:00:00.000000', 6, 'UTC')"
T_DOM, T_DOM_TEXT = _stamp(datetime(2026, 8, 12, tzinfo=UTC))           # company_domains last_seen/resolved
T_EXTRACT_1, T_EXTRACT_1_TEXT = _stamp(datetime(2026, 9, 1, 10, 0, tzinfo=UTC))
T_EXTRACT_2, T_EXTRACT_2_TEXT = _stamp(datetime(2026, 9, 1, 11, 0, tzinfo=UTC))
T_EXTRACT_3, T_EXTRACT_3_TEXT = _stamp(datetime(2026, 9, 1, 12, 0, tzinfo=UTC))
# The since the *_scope_since sections use: after every registry stamp, before HB's newer
# artifacts and financials, after everything Wikidata carries.
SINCE = "2026-08-01 12:00:00.000"
PERIOD_END_TEXT = "2024-12-31 00:00:00.000"
SETTLE = "SELECT sleep(0.05) FORMAT Null;\n"


def _record_uid(*parts: str) -> str:
    """The company-source-record-v1 uid the tables' DEFAULT expressions compute."""
    return hashlib.sha256("\n".join(parts).encode()).hexdigest()


HB_BV_REG_UID = _record_uid("company-source-record-v1", "structured", "sweden_bolagsverket", "registry_company", "bv-hb", "bv-hb-hash")
SOLO_BV_REG_UID = _record_uid("company-source-record-v1", "structured", "sweden_bolagsverket", "registry_company", "bv-solo", "bv-solo-hash")
HB_IND_UID = _record_uid("company-source-record-v1", "structured", "sweden_scb", "registry_company", "ind-hb", "ind-hb-hash")
HB_BV_FIN_UID = _record_uid("company-source-record-v1", "structured", "sweden_financial", "annual_report_xhtml", "hb-fy2024", "hb-fy2024")
HB_ESEF_FIN_UID = _record_uid("company-source-record-v1", "file", "esef_report_package", HB_PACKAGE_SHA)
HB_WD_UID = f"wikidata:{HB_QID}"
HB_RATSIT_IND_UID = f"ratsit:{HB_RATSIT_SHA}:industry:0"
HB_RATSIT_FIN_UID = f"ratsit:{HB_RATSIT_SHA}:financial:0:1"
HB_DOMAIN_UID = "fp-hb-primary"

# Every source task appends (source, module) here; _script iterates it.
EXTRACTORS: list[tuple[str, ModuleType]] = []
EXTRACTORS.append(("scb", scb_candidates))
EXTRACTORS.append(("bolagsverket", bolagsverket_candidates))
EXTRACTORS.append(("esef", esef_candidates))
EXTRACTORS.append(("wikidata", wikidata_candidates))
EXTRACTORS.append(("ratsit", ratsit_candidates))
EXTRACTORS.append(("domains", domains_candidates))


def _schema_statements() -> list[str]:
    """CREATE/ALTER/RENAME TABLE and CREATE VIEW statements for the needed objects only, in
    migration order. 000269's INSERT ... SELECT and every statement aimed at a table this
    harness never creates are dropped; 000285's RENAME is kept because both of its names
    are needed."""
    statements: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _OBJECT_RE.match(statement)
            if match and (match.group(1) in NEEDED_TABLES or match.group(1) in NEEDED_VIEWS):
                statements.append(statement)
    return statements


FIXTURE = f"""
INSERT INTO corpscout.se_company_registry_current
    (company_id, source, legal_name, legal_form_code, derived_status, incorporation_date,
     source_run_id, source_record_id, source_payload_hash, updated_from_raw_at, has_company,
     state_fingerprint, observation_fingerprint, observed_at)
VALUES
    ('{HB}', 'scb', 'Svenska Handelsbanken AB', '49', 'active', '1971-04-01',
     'fixture', 'scb-hb', 'scb-hb-hash', {T_REG}, 1, '{ZERO_HASH}', '{ZERO_HASH}', {T_REG}),
    ('{HB}', 'bolagsverket', 'Svenska Handelsbanken AB', 'AB-ORGFO', 'active', '1971-04-01',
     'fixture', 'bv-hb', 'bv-hb-hash', {T_REG}, 1, '{ZERO_HASH}', '{ZERO_HASH}', {T_REG}),
    ('{SOLO}', 'scb', 'Beta AB', '42', 'active', '1998-06-15',
     'fixture', 'scb-solo', 'scb-solo-hash', {T_REG}, 1, '{ZERO_HASH}', '{ZERO_HASH}', {T_REG}),
    ('{SOLO}', 'bolagsverket', '-', 'AB-ORGFO', 'inactive', '1998-06-15',
     'fixture', 'bv-solo', 'bv-solo-hash', {T_REG}, 1, '{ZERO_HASH}', '{ZERO_HASH}', {T_REG});

INSERT INTO corpscout.se_company_info_scb
    (company_id, source_record_uid, observed_at, source_run_id, legal_name, legal_form_code, status,
     incorporation_date, activity_description, activity_description_en, primary_sni_code, primary_nace_code)
VALUES
    ('{HB}', 'scb-art-hb', {T_ART}, 'fixture', 'Svenska Handelsbanken AB', 'AB-ORGFO', 'active',
     '1971-04-01', 'Bankverksamhet.', 'Banking operations.', '64190', '64.19'),
    ('{SOLO}', 'scb-art-solo', {T_REG}, 'fixture', 'Beta AB', 'AB-ORGFO', 'active',
     '1998-06-15', 'Handel med datorer.', '', '', '');

INSERT INTO corpscout.se_industries
    (company_id, sequence, is_primary, sni_code, nace_rev2_class_code, source_field,
     source_run_id, source_record_id, source_payload_hash, updated_from_raw_at)
VALUES
    ('{HB}', 1, 1, '64190', '64.19', 'sni', 'fixture', 'ind-hb', 'ind-hb-hash', {T_IND}),
    ('{HB}', 2, 0, '66190', '66.19', 'sni', 'fixture', 'ind-hb-2', 'ind-hb-2-hash', {T_IND});

INSERT INTO corpscout.nace_categories
    (classification_version, code, normalized_code, parent_code, level, section_code, description_en,
     concept_uri, parent_concept_uri, source_scheme_uri, source_url, source_payload_hash, valid_from,
     valid_to, is_current, source_run_id, pulled_at, _dlt_load_id, _dlt_id)
VALUES
    ('NACE_REV_2', '64.19', '6419', '64.1', 'class', 'K', '64.19 Other monetary intermediation',
     'uri:nace2:6419', NULL, 'uri:nace2', 'https://nace', '{ZERO_HASH}', '2008-01-01',
     NULL, 1, 'fixture', {T_REG}, '', ''),
    ('NACE_REV_2_1', '64.19', '6419', '64.1', 'class', 'K', 'Other monetary intermediation',
     'uri:nace21:6419', NULL, 'uri:nace21', 'https://nace', '{ZERO_HASH}', '2025-01-01',
     NULL, 1, 'fixture', {T_REG}, '', '');

INSERT INTO corpscout.se_company_info_esef
    (company_id, source_record_uid, observed_at, source_run_id, source_document_id, lei, entity_name,
     fiscal_year, company_description, description_language, description_confidence,
     products_and_services_json, business_segments_json)
VALUES
    ('{HB}', 'esef-art-hb-2024', {T_ESEF_ART}, 'fixture', 'doc-hb-2024', '{HB_LEI}', '',
     2024, 'Handelsbanken is a Nordic bank.', 'en', 0.9, '[]', '[]');

INSERT INTO corpscout.esef_filings
    (lei, entity_name, fxo_id, country, period_end, date_added, json_url, package_url, report_url,
     viewer_url, package_sha256, error_count, warning_count, inconsistency_count, has_json_facts,
     source_url, source_run_id, resolved_at)
VALUES
    ('{HB_LEI}', 'Svenska Handelsbanken AB', 'HB-2024-1', 'SE', '2024-12-31', '2025-03-01', '', '', '',
     'https://viewer/hb-2024', '{HB_PACKAGE_SHA}', 0, 0, 0, 1,
     'https://filings.xbrl.org/hb-2024', 'fixture', {T_ESEF_FIN});

INSERT INTO corpscout.esef_financial_metrics
    (lei, entity_name, fxo_id, country, scope, fiscal_year, period_start, period_end, currency,
     revenue_amount_original, revenue_amount_usd, employees, mapped_fact_count, source_fact_count,
     mapping_version, fx_rate_to_usd, fx_rate_date, fx_source, viewer_url, source_run_id, resolved_at)
VALUES
    ('{HB_LEI}', 'Svenska Handelsbanken AB', 'HB-2024-1', 'SE', 'consolidated', 2024, '2024-01-01', '2024-12-31', 'SEK',
     48000000000, 4500000000, 12000, 10, 12,
     'v1', 0.09375, '2024-12-31', 'ecb', 'https://viewer/hb-2024', 'fixture', {T_ESEF_FIN});

INSERT INTO corpscout.company_identifier
    (issuer_scheme, issuer_id, country_code, company_id, match_method, match_confidence,
     registration_authority_id, registered_as_raw, company_id_normalized, entity_status,
     registration_status, is_current, successor_issuer_id, first_seen_date, last_seen_date,
     source_run_id, resolved_at)
VALUES
    ('lei', '{HB_LEI}', 'SE', '{HB}', 'exact', 'high', 'RA000544', '{HB}', '{HB}', 'ACTIVE',
     'ISSUED', 1, '', '2020-01-01', '2026-08-01', 'fixture', {T_REG});

INSERT INTO corpscout.se_company_info_wikidata
    (company_id, source_record_uid, observed_at, source_run_id, wikidata_id, wikidata_url, name,
     official_name, company_description, inception_date, industry_label, employee_count)
VALUES
    ('{HB}', '{HB_WD_UID}', {T_WD}, 'fixture', '{HB_QID}', 'https://www.wikidata.org/wiki/{HB_QID}', 'Handelsbanken',
     'Svenska Handelsbanken AB', 'Swedish bank', '1971-04-01', 'banking', 12500);

INSERT INTO corpscout.wikidata_companies
    (wikidata_id, wikidata_url, name, name_normalized, employee_count, employee_count_point_in_time,
     has_current_listing, listing_count, source_system, source_run_id, source_record_id,
     source_payload_hash, retrieved_at, resolved_at)
VALUES
    ('{HB_QID}', 'https://www.wikidata.org/wiki/{HB_QID}', 'Handelsbanken', 'handelsbanken', 12500, '2024-12-31',
     1, 1, 'wikidata', 'fixture', '{HB_QID}', '{ZERO_HASH}', {T_WD}, {T_WD});

INSERT INTO corpscout.wikidata_company_websites
    (wikidata_id, website_url, website_normalized_url, website_host, root_domain, website_path,
     website_kind, confidence, validation_status, is_primary_candidate, source_system, source_run_id,
     source_record_id, source_payload_hash, retrieved_at, resolved_at)
VALUES
    ('{HB_QID}', 'https://www.handelsbanken.se/', 'https://www.handelsbanken.se', 'www.handelsbanken.se',
     'handelsbanken.se', NULL, 'official', 'wikidata', 'unverified', 1, 'wikidata', 'fixture',
     '{HB_QID}', '{ZERO_HASH}', {T_WEB}, {T_WEB});

INSERT INTO corpscout.se_ratsit_company
    (company_id, result_sha256, normalizer_version, schema_version, parser_version, requested_url, source_url,
     result_bucket, result_object_key, name, organization_number, industry_code_count, summary_count,
     responsible_people_count, establishment_count, financial_report_count, financial_period_count,
     people_at_address_count, normalized_at)
VALUES
    ('{HB}', '{HB_RATSIT_SHA}', 'ratsit-normalizer-v2', 1, 'parser-v1', 'https://www.ratsit.se/{HB}',
     'https://www.ratsit.se/{HB}/Svenska_Handelsbanken_AB', 'ratsit-results',
     'sweden_ratsit/pilot/company_id={HB}/report.json', 'Svenska Handelsbanken AB', '{HB}', 1, 0,
     0, 0, 1, 2, 0, {T_RATSIT});

INSERT INTO corpscout.se_ratsit_company_industry_codes
    (company_id, result_sha256, normalizer_version, industry_index, industry_code, industry_description,
     source_industry_code, source_industry_code_set, industry_description_original, nace_revision, nace_code,
     nace_normalized_code, nace_mapping_method, nace_mapping_status, normalized_at)
VALUES
    ('{HB}', '{HB_RATSIT_SHA}', 'ratsit-normalizer-v2', 0, '64190', 'Bankverksamhet',
     '64190', 'SNI_2025', 'Bankverksamhet', 'NACE_REV_2_1', '64.19',
     '6419', 'sni_four_digit_prefix', 'mapped', {T_RATSIT});

INSERT INTO corpscout.se_ratsit_financial_periods
    (company_id, result_sha256, normalizer_version, financial_report_index, period_index, period_kind, scope,
     monetary_unit, fiscal_year, period_start, period_end, period_months, revenue_amount, employee_count, normalized_at)
VALUES
    ('{HB}', '{HB_RATSIT_SHA}', 'ratsit-normalizer-v2', 0, 0, 'financial_and_employment', 'company',
     'TSEK', 2023, '2023-01-01', '2023-12-31', 12, 45000000, 11800, {T_RATSIT}),
    ('{HB}', '{HB_RATSIT_SHA}', 'ratsit-normalizer-v2', 0, 1, 'financial_and_employment', 'company',
     'TSEK', 2024, '2024-01-01', '2024-12-31', 12, 48000000, 11900, {T_RATSIT});

INSERT INTO corpscout.exchange_rates
    (rate_date, base_currency, quote_currency, rate, source, source_url, source_payload_hash, source_run_id,
     pulled_at, _dlt_load_id, _dlt_id)
VALUES
    ('2024-06-30', 'EUR', 'SEK', 11, 'ecb', 'https://ecb', '{ZERO_HASH}', 'fixture', {T_REG}, '', ''),
    ('2024-12-31', 'EUR', 'SEK', 10, 'ecb', 'https://ecb', '{ZERO_HASH}', 'fixture', {T_REG}, '', ''),
    ('2025-01-31', 'EUR', 'SEK', 9, 'ecb', 'https://ecb', '{ZERO_HASH}', 'fixture', {T_REG}, '', ''),
    ('2024-12-31', 'EUR', 'USD', 1.25, 'ecb', 'https://ecb', '{ZERO_HASH}', 'fixture', {T_REG}, '', '');

INSERT INTO corpscout.company_domains
    (country_code, company_id, root_domain, website_url, website_host, source_names, source_confidences,
     source_record_ids, source_urls, confidence_bases, suggested_confidence, suggested_primary,
     evidence_fingerprint, review_status, first_seen_at, last_seen_at, resolved_at)
VALUES
    ('SE', '{HB}', 'handelsbanken.se', 'https://www.handelsbanken.se/', 'www.handelsbanken.se', ['wikidata'], [0.9],
     [''], [''], ['official_website_claim'], 0.9, 1,
     '{HB_DOMAIN_UID}', 'confirmed_primary', {T_REG}, {T_DOM}, {T_DOM}),
    ('SE', '{HB}', 'handelsbanken.com', 'https://www.handelsbanken.com/', 'www.handelsbanken.com', ['common_crawl'], [0.95],
     [''], [''], ['crawl_link'], 0.95, 1,
     'fp-hb-com', 'unreviewed', {T_REG}, {T_DOM}, {T_DOM});

INSERT INTO corpscout.se_bolagsverket_financial_metrics
    (country_iso2, source_slug, source_run_id, source_record_id, statement_key, company_id, report_period_start,
     report_period_end, fiscal_year, currency, revenue_amount_original, revenue_amount_usd, employees,
     source_fact_count, mapped_fact_count, unmapped_numeric_fact_count, metric_warnings, mapping_version,
     fx_rate_to_usd, fx_rate_date, fx_source, source_payload_hash, resolved_at)
VALUES
    ('SE', 'sweden_financial', 'fixture', 'hb-fy2023', 'hb-fy2023', '{HB}', '2023-01-01',
     '2023-12-31', 2023, 'SEK', 45000000000, 4100000000, 11850,
     10, 10, 0, '', 'v1', 0.091, '2023-12-31', 'ecb', '{ZERO_HASH}', {T_FIN}),
    ('SE', 'sweden_financial', 'fixture', 'hb-fy2024', 'hb-fy2024', '{HB}', '2024-01-01',
     '2024-12-31', 2024, 'SEK', 47500000000, 4400000000, 11950,
     10, 10, 0, '', 'v1', 0.0926, '2024-12-31', 'ecb', '{ZERO_HASH}', {T_FIN});
""".strip()

COUNTS_SQL = "SELECT source, count() FROM corpscout.se_company_field_candidate GROUP BY source ORDER BY source"


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query} FORMAT TSV;\n"


def _scope_for(module: ModuleType, since: str) -> str:
    return _render(module.build_scope_sql(), {"after_company_id": "", "page_size": 10, "since": since})


def _candidates_for(module: ModuleType, company_id: str) -> str:
    inner = _render(module.build_candidates_sql(), {"company_ids": (company_id,)})
    return (
        "SELECT field, source_record_uid, toString(observed_at), value, value_json\n"
        f"FROM ({inner}) AS c ({', '.join(CANDIDATE_SELECT_COLUMNS)})\nORDER BY field, source_record_uid"
    )


def _publish_pass(source: str, module: ModuleType, extracted_at_sql: str) -> str:
    """Mirrors publish_candidates -> publish_with_stage(new_versions_only=True,
    anti_join_columns=CANDIDATE_ANTI_JOIN_COLUMNS): stage <- the extractor SELECT wrapped
    into the insert list, then copy only rows whose five-column identity is not there."""
    columns = ", ".join(SE_COMPANY_FIELD_CANDIDATE_COLUMNS)
    stage = "corpscout._tmp_se_company_field_candidate"
    stage_columns = ", ".join(f"stage.{column}" for column in SE_COMPANY_FIELD_CANDIDATE_COLUMNS)
    on_clause = " AND ".join(f"existing.{column} = stage.{column}" for column in CANDIDATE_ANTI_JOIN_COLUMNS)
    projected = ", ".join(CANDIDATE_SELECT_COLUMNS)
    inner = _render(module.build_candidates_sql(), {"company_ids": (HB, SOLO)})
    return (
        f"CREATE TABLE {stage} AS corpscout.se_company_field_candidate;\n"
        f"INSERT INTO {stage} ({columns})\n"
        f"SELECT company_id, field, '{source}', source_record_uid, value, value_json, observed_at, "
        f"{extracted_at_sql}, '{module.EXTRACTOR_VERSION}', '{RUN_ID}'\n"
        f"FROM (SELECT {projected} FROM ({inner}) AS c ({projected}));\n"
        f"INSERT INTO corpscout.se_company_field_candidate ({columns})\n"
        f"SELECT {stage_columns} FROM {stage} AS stage\n"
        f"LEFT ANTI JOIN corpscout.se_company_field_candidate AS existing ON {on_clause};\n"
        f"DROP TABLE {stage};\n"
    )


def _script(*, join_use_nulls: int) -> str:
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;")
    parts.append(";\n".join(_schema_statements()) + ";")
    parts.append(FIXTURE)
    parts.append(_marked(
        "candidate_columns",
        "SELECT name FROM system.columns WHERE database = 'corpscout' "
        "AND table = 'se_company_field_candidate' ORDER BY position"))
    parts.append(_marked(
        "financial_views",
        "SELECT 'bolagsverket', count() FROM corpscout.se_financials_bolagsverket_current "
        "UNION ALL SELECT 'esef', count() FROM corpscout.se_financials_esef_current"))

    # Pass 1: every registered extractor's scan (with and without a since), then its rows.
    for source, module in EXTRACTORS:
        parts.append(_marked(f"{source}_scope_all", _scope_for(module, EPOCH)))
        parts.append(_marked(f"{source}_scope_since", _scope_for(module, SINCE)))
        parts.append(_publish_pass(source, module, T_EXTRACT_1))
        parts.append(_marked(f"{source}_hb", _candidates_for(module, HB)))
        parts.append(_marked(f"{source}_solo", _candidates_for(module, SOLO)))
    parts.append(_marked("counts_after_first_pass", COUNTS_SQL))

    # Pass 2: identical rerun at a later extracted_at -- the anti-join lets nothing through.
    parts.append(SETTLE)
    for source, module in EXTRACTORS:
        parts.append(_publish_pass(source, module, T_EXTRACT_2))
    parts.append(_marked("counts_after_rerun", COUNTS_SQL))
    parts.extend(_late_sections())
    return "\n".join(parts) + "\n"


# A new version of HB's SCB artifact: same source_record_uid, newer observed_at, a changed
# English text. FINAL then reads it, the description candidate's evidence_hash changes, and
# the anti-join must let exactly that one row through -- every other artifact field is
# unchanged, so their candidates keep their first-pass extracted_at.
CHANGED_SCB_ARTIFACT_SQL = f"""
INSERT INTO corpscout.se_company_info_scb
    (company_id, source_record_uid, observed_at, source_run_id, legal_name, legal_form_code, status,
     incorporation_date, activity_description, activity_description_en, primary_sni_code, primary_nace_code)
VALUES
    ('{HB}', 'scb-art-hb', {T_ART2}, 'fixture-v2', 'Svenska Handelsbanken AB', 'AB-ORGFO', 'active',
     '1971-04-01', 'Bankverksamhet.', 'Banking and financial services.', '64190', '64.19');
""".strip()

# A THIRD version of HB's SCB artifact, later still -- the text candidate the LLM gate's
# "newer than every llm row" test needs after a stored llm row has silenced the company.
CHANGED_SCB_ARTIFACT_AGAIN_SQL = f"""
INSERT INTO corpscout.se_company_info_scb
    (company_id, source_record_uid, observed_at, source_run_id, legal_name, legal_form_code, status,
     incorporation_date, activity_description, activity_description_en, primary_sni_code, primary_nace_code)
VALUES
    ('{HB}', 'scb-art-hb', {_literal(datetime(2026, 8, 6, tzinfo=UTC))}, 'fixture-v3', 'Svenska Handelsbanken AB', 'AB-ORGFO', 'active',
     '1971-04-01', 'Bankverksamhet.', 'Banking, financing and insurance.', '64190', '64.19');
""".strip()


def _late_sections() -> list[str]:
    """Sections appended after the rerun: the SCB change pass, then the LLM scan (Task 9)."""
    return [
        CHANGED_SCB_ARTIFACT_SQL, SETTLE, _publish_pass("scb", scb_candidates, T_EXTRACT_3),
        _marked("counts_after_scb_change", COUNTS_SQL),
        _marked("scb_after_change",
                "SELECT field, value, toString(observed_at), toString(extracted_at) "
                f"FROM corpscout.se_company_field_candidate FINAL WHERE company_id = '{HB}' "
                "AND source = 'scb' AND field IN ('description', 'legal_name') ORDER BY field"),
        # The LLM gate over the candidate table: HB has three text sources, SOLO one.
        _marked("llm_scope_after_first_pass", _scope_for(llm_candidates, EPOCH)),
        # A stored llm candidate newer than every text candidate silences HB ...
        f"INSERT INTO corpscout.se_company_field_candidate "
        f"(company_id, field, source, source_record_uid, value, value_json, observed_at, extracted_at, extractor_version, source_run_id) "
        f"VALUES ('{HB}', 'description', 'llm', '{uuid.UUID(int=7)}', 'Handelsbanken is a Nordic bank offering banking operations.', "
        f"'{{\"compare_key\":\"handelsbanken is a nordic bank offering banking operations.\",\"language\":\"en\"}}', "
        f"{T_EXTRACT_3}, {T_EXTRACT_3}, 'llm-candidates-v1', '{RUN_ID}');",
        _marked("llm_scope_after_llm_row", _scope_for(llm_candidates, EPOCH)),
        # ... until a text candidate is extracted after it: a third artifact version, published at 13:00.
        CHANGED_SCB_ARTIFACT_AGAIN_SQL, SETTLE,
        _publish_pass("scb", scb_candidates, _literal(datetime(2026, 9, 1, 13, 0, tzinfo=UTC))),
        _marked("llm_scope_after_newer_text", _scope_for(llm_candidates, EPOCH)),
        _marked("llm_context", "SELECT field, source, value FROM ("
                + _render(llm_candidates.build_context_sql(), {"company_ids": (HB,)})
                + ") ORDER BY field, source"),
    ]


def _text(compare_key: str, **members: str) -> str:
    """value_json exactly as the SQL renders it: sorted keys, compact."""
    return json.dumps({**members, "compare_key": compare_key}, separators=(",", ":"), sort_keys=True)


HB_SCB_ROWS = [
    ["description", "scb-art-hb", T_ART_TEXT, "Banking operations.", _text("banking operations.", language="en")],
    ["description_sv", "scb-art-hb", T_ART_TEXT, "Bankverksamhet.", _text("bankverksamhet.", language="sv")],
    ["incorporation_date", "scb-art-hb", T_ART_TEXT, "1971-04-01", _text("1971-04-01")],
    ["industry_label_en", HB_IND_UID, T_IND_TEXT, "Other monetary intermediation", _text("other monetary intermediation")],
    ["legal_form_code", "scb-art-hb", T_ART_TEXT, "AB-ORGFO", _text("ab-orgfo")],
    ["legal_name", "scb-art-hb", T_ART_TEXT, "Svenska Handelsbanken AB", _text("svenska handelsbanken ab")],
    ["primary_nace_code", HB_IND_UID, T_IND_TEXT, "6419", _text("6419")],
    ["primary_sni_code", HB_IND_UID, T_IND_TEXT, "64190", _text("64190")],
    ["status", "scb-art-hb", T_ART_TEXT, "active", _text("active")],
]
SOLO_SCB_ROWS = [
    ["description", "scb-art-solo", T_REG_TEXT, "Handel med datorer.", _text("handel med datorer.", language="sv")],
    ["description_sv", "scb-art-solo", T_REG_TEXT, "Handel med datorer.", _text("handel med datorer.", language="sv")],
    ["incorporation_date", "scb-art-solo", T_REG_TEXT, "1998-06-15", _text("1998-06-15")],
    ["legal_form_code", "scb-art-solo", T_REG_TEXT, "AB-ORGFO", _text("ab-orgfo")],
    ["legal_name", "scb-art-solo", T_REG_TEXT, "Beta AB", _text("beta ab")],
    ["status", "scb-art-solo", T_REG_TEXT, "active", _text("active")],
]


def test_scb_scope_selects_changed_companies_only(sections: dict[str, list[list[str]]]) -> None:
    assert [row[0] for row in sections["scb_scope_all"]] == [HB, SOLO]
    # SOLO's artifact and industries are stamped before SINCE; HB's artifact is newer.
    assert [row[0] for row in sections["scb_scope_since"]] == [HB]


def test_scb_candidates_carry_the_artifact_uid_and_stamp(sections: dict[str, list[list[str]]]) -> None:
    assert sections["scb_hb"] == HB_SCB_ROWS
    # Untranslated: the Swedish text is the description too, marked sv; no industry rows.
    assert sections["scb_solo"] == SOLO_SCB_ROWS


def test_scb_publish_is_idempotent_and_a_changed_artifact_appends_one_row(
    sections: dict[str, list[list[str]]],
) -> None:
    first = _counts(sections["counts_after_first_pass"])
    assert first["scb"] == len(HB_SCB_ROWS) + len(SOLO_SCB_ROWS)
    assert _counts(sections["counts_after_rerun"])["scb"] == first["scb"]
    assert _counts(sections["counts_after_scb_change"])["scb"] == first["scb"] + 1
    # The changed description is a new version (new observed_at, this pass's extracted_at);
    # the unchanged legal name keeps the first pass's stamps although the artifact version moved.
    assert sections["scb_after_change"] == [
        ["description", "Banking and financial services.", T_ART2_TEXT, T_EXTRACT_3_TEXT],
        ["legal_name", "Svenska Handelsbanken AB", T_ART_TEXT, T_EXTRACT_1_TEXT],
    ]


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(
            command, input=_script(join_use_nulls=request.param), capture_output=True, text=True, timeout=900)
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


def test_the_candidate_table_columns_are_the_insert_list_plus_the_materialized_hash(
    sections: dict[str, list[list[str]]],
) -> None:
    """publish_candidates binds SE_COMPANY_FIELD_CANDIDATE_COLUMNS positionally; here
    ClickHouse itself says what 000373 declared, so a column added out of order fails
    loudly instead of transposing values."""
    names = [row[0] for row in sections["candidate_columns"]]
    assert [name for name in names if name != "evidence_hash"] == list(SE_COMPANY_FIELD_CANDIDATE_COLUMNS)
    assert "evidence_hash" in names


def test_both_financial_views_resolve_the_fixture(sections: dict[str, list[list[str]]]) -> None:
    """The views are read as-is by the bolagsverket and esef extractors; two fiscal years for
    Bolagsverket, one ESEF filing linked through company_identifier."""
    assert _counts(sections["financial_views"]) == {"bolagsverket": 2, "esef": 1}


HB_BV_ROWS = [
    ["employee_count", HB_BV_FIN_UID, PERIOD_END_TEXT, "11950",
     '{"as_of":"2024-12-31","compare_key":"11950","count":11950,"period":"2024"}'],
    ["incorporation_date", HB_BV_REG_UID, T_REG_TEXT, "1971-04-01", _text("1971-04-01")],
    ["latest_revenue", HB_BV_FIN_UID, PERIOD_END_TEXT, "SEK 47500000000 FY2024",
     '{"amount":47500000000,"amount_usd":4400000000,"compare_key":"sek:47500000000:2024",'
     '"currency":"SEK","fiscal_year":2024,"period_end":"2024-12-31"}'],
    ["legal_form_code", HB_BV_REG_UID, T_REG_TEXT, "AB-ORGFO", _text("ab-orgfo")],
    ["legal_name", HB_BV_REG_UID, T_REG_TEXT, "Svenska Handelsbanken AB", _text("svenska handelsbanken ab")],
    ["status", HB_BV_REG_UID, T_REG_TEXT, "active", '{"compare_key":"active","conflict":false}'],
]
# No legal_name: the register wrote the placeholder '-'. status carries the conflict with SCB.
SOLO_BV_ROWS = [
    ["incorporation_date", SOLO_BV_REG_UID, T_REG_TEXT, "1998-06-15", _text("1998-06-15")],
    ["legal_form_code", SOLO_BV_REG_UID, T_REG_TEXT, "AB-ORGFO", _text("ab-orgfo")],
    ["status", SOLO_BV_REG_UID, T_REG_TEXT, "inactive", '{"compare_key":"inactive","conflict":true}'],
]


def test_bolagsverket_scope_and_rows(sections: dict[str, list[list[str]]]) -> None:
    assert [row[0] for row in sections["bolagsverket_scope_all"]] == [HB, SOLO]
    assert [row[0] for row in sections["bolagsverket_scope_since"]] == [HB]  # the metrics row is newer than SINCE
    assert sections["bolagsverket_hb"] == HB_BV_ROWS
    assert sections["bolagsverket_solo"] == SOLO_BV_ROWS
    first = _counts(sections["counts_after_first_pass"])
    assert first["bolagsverket"] == len(HB_BV_ROWS) + len(SOLO_BV_ROWS)
    assert _counts(sections["counts_after_rerun"])["bolagsverket"] == first["bolagsverket"]


HB_ESEF_ROWS = [
    ["description", "esef-art-hb-2024", T_ESEF_ART_TEXT, "Handelsbanken is a Nordic bank.",
     _text("handelsbanken is a nordic bank.", language="en")],
    ["employee_count", HB_ESEF_FIN_UID, PERIOD_END_TEXT, "12000",
     '{"as_of":"2024-12-31","compare_key":"12000","count":12000,"period":"2024"}'],
    ["latest_revenue", HB_ESEF_FIN_UID, PERIOD_END_TEXT, "SEK 48000000000 FY2024",
     '{"amount":48000000000,"amount_usd":4500000000,"compare_key":"sek:48000000000:2024",'
     '"currency":"SEK","fiscal_year":2024,"period_end":"2024-12-31"}'],
]


def test_esef_scope_and_rows(sections: dict[str, list[list[str]]]) -> None:
    assert [row[0] for row in sections["esef_scope_all"]] == [HB]
    # The artifact is from 2025 (older than SINCE) but the metrics row is newer: still selected.
    assert [row[0] for row in sections["esef_scope_since"]] == [HB]
    assert sections["esef_hb"] == HB_ESEF_ROWS
    assert sections["esef_solo"] == []
    first = _counts(sections["counts_after_first_pass"])
    assert first["esef"] == len(HB_ESEF_ROWS)
    assert _counts(sections["counts_after_rerun"])["esef"] == first["esef"]


HB_WD_ROWS = [
    ["description", HB_WD_UID, T_WD_TEXT, "Swedish bank", _text("swedish bank", language="en")],
    ["employee_count", HB_WD_UID, T_WD_TEXT, "12500",
     '{"as_of":"2024-12-31","compare_key":"12500","count":12500,"period":null}'],
    ["incorporation_date", HB_WD_UID, T_WD_TEXT, "1971-04-01", _text("1971-04-01")],
    ["industry_label_en", HB_WD_UID, T_WD_TEXT, "banking", _text("banking")],
    ["legal_name", HB_WD_UID, T_WD_TEXT, "Svenska Handelsbanken AB", _text("svenska handelsbanken ab")],
    ["website", HB_WD_UID, T_WEB_TEXT, "https://www.handelsbanken.se/",
     '{"compare_key":"handelsbanken.se","root_domain":"handelsbanken.se"}'],
]


def test_wikidata_scope_and_rows(sections: dict[str, list[list[str]]]) -> None:
    assert [row[0] for row in sections["wikidata_scope_all"]] == [HB]
    assert sections["wikidata_scope_since"] == []  # every Wikidata stamp is older than SINCE
    assert sections["wikidata_hb"] == HB_WD_ROWS
    assert sections["wikidata_solo"] == []
    first = _counts(sections["counts_after_first_pass"])
    assert first["wikidata"] == len(HB_WD_ROWS)
    assert _counts(sections["counts_after_rerun"])["wikidata"] == first["wikidata"]


HB_RATSIT_ROWS = [
    ["employee_count", HB_RATSIT_FIN_UID, PERIOD_END_TEXT, "11900",
     '{"as_of":"2024-12-31","compare_key":"11900","count":11900,"period":"2024"}'],
    ["industry_label_en", HB_RATSIT_IND_UID, T_RATSIT_TEXT, "Other monetary intermediation", _text("other monetary intermediation")],
    # 48,000,000 TSEK -> 48,000,000,000.00 SEK; / 10 (EUR->SEK on 2024-12-31, not the older 11
    # nor the later 9) * 1.25 (EUR->USD) -> 6,000,000,000.00 USD, exact in float64.
    ["latest_revenue", HB_RATSIT_FIN_UID, PERIOD_END_TEXT, "SEK 48000000000 FY2024",
     '{"amount":48000000000,"amount_usd":6000000000,"compare_key":"sek:48000000000:2024",'
     '"currency":"SEK","fiscal_year":2024,"period_end":"2024-12-31"}'],
    ["primary_nace_code", HB_RATSIT_IND_UID, T_RATSIT_TEXT, "6419", _text("6419")],
    ["primary_sni_code", HB_RATSIT_IND_UID, T_RATSIT_TEXT, "64190", _text("64190")],
]


def test_ratsit_scope_and_rows(sections: dict[str, list[list[str]]]) -> None:
    assert [row[0] for row in sections["ratsit_scope_all"]] == [HB]
    assert [row[0] for row in sections["ratsit_scope_since"]] == [HB]
    assert sections["ratsit_hb"] == HB_RATSIT_ROWS
    assert sections["ratsit_solo"] == []
    first = _counts(sections["counts_after_first_pass"])
    assert first["ratsit"] == len(HB_RATSIT_ROWS)
    assert _counts(sections["counts_after_rerun"])["ratsit"] == first["ratsit"]


HB_DOMAIN_ROWS = [
    # confirmed_primary (0.9) beats the higher-confidence unreviewed suggestion (0.95).
    ["website", HB_DOMAIN_UID, T_DOM_TEXT, "https://www.handelsbanken.se/",
     '{"compare_key":"handelsbanken.se","review_status":"confirmed_primary","root_domain":"handelsbanken.se"}'],
]


def test_domains_scope_and_rows(sections: dict[str, list[list[str]]]) -> None:
    assert [row[0] for row in sections["domains_scope_all"]] == [HB]
    assert [row[0] for row in sections["domains_scope_since"]] == [HB]
    assert sections["domains_hb"] == HB_DOMAIN_ROWS
    assert sections["domains_solo"] == []
    first = _counts(sections["counts_after_first_pass"])
    assert first["domains"] == 1
    assert _counts(sections["counts_after_rerun"])["domains"] == 1


def test_llm_gate_selects_multi_source_companies_with_text_newer_than_their_llm_row(
    sections: dict[str, list[list[str]]],
) -> None:
    assert [row[0] for row in sections["llm_scope_after_first_pass"]] == [HB]   # SOLO: one text source
    assert sections["llm_scope_after_llm_row"] == []                            # silenced by a newer llm row
    assert [row[0] for row in sections["llm_scope_after_newer_text"]] == [HB]  # re-armed by newer SCB text
    context = sections["llm_context"]
    assert [row[:2] for row in context] == [
        ["description", "esef"], ["description", "scb"], ["description", "wikidata"],
        ["description_sv", "scb"], ["legal_name", "bolagsverket"], ["legal_name", "scb"],
        ["legal_name", "wikidata"], ["primary_nace_code", "ratsit"], ["primary_nace_code", "scb"],
    ]
    assert next(row[2] for row in context if row[:2] == ["description", "scb"]) == "Banking, financing and insurance."
