"""The suggestion extractors' SQL on a real ClickHouse (spec 9): the change scan converges,
each source's SELECT produces the expected wide row, and the LLM gate selects the right
companies. Runs under join_use_nulls 0 and 1."""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings import tables as esef_tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql
from dagster_v3.defs.se_company.basic_info import bolagsverket, esef, ratsit, scb, wikidata
from dagster_v3.defs.se_company.basic_info import tables
from dagster_v3.defs.se_company.basic_info.extract import changed_scope_sql, insert_page_sql, since_scope_sql
from dagster_v3.defs.se_company.basic_info.llm import llm_scope_sql
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command
from tests.test_se_company_basic_info_clickhouse_local import _bind

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
MIGRATIONS = (
    "000373_corpscout_se_scb_companies.up.sql",
    "000374_corpscout_se_bolagsverket_companies.up.sql",
    "000376_corpscout_se_company_basic_info_suggestion.up.sql",
    "000390_corpscout_se_source_translated_views.up.sql",
    "000394_corpscout_se_company_basic_info_economic_activity.up.sql",
)
FIXTURE = Path(__file__).resolve().parent / "fixtures" / "se_basic_info_source_tables.sql"


def _schema() -> list[str]:
    tables: list[str] = []
    alters: list[str] = []
    views: list[str] = []
    for name in MIGRATIONS:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(
                line for line in raw.splitlines() if not line.strip().startswith("--")
            ).strip()
            if statement.upper().startswith(("CREATE DATABASE", "CREATE TABLE")):
                tables.append(statement)
            elif statement.upper().startswith("CREATE OR REPLACE VIEW"):
                views.append(statement)
            elif statement.upper().startswith("ALTER TABLE"):
                # 000394 alters the three entity tables; only the suggestion table exists here.
                target = statement.split()[2]
                if any(f"CREATE TABLE IF NOT EXISTS {target}\n" in t for t in tables):
                    alters.append(statement)
    fixture = [s.strip() for s in FIXTURE.read_text(encoding="utf-8").split(";") if s.strip()]
    # The esef extractor now reads se_esef_document_company_information (migration 000395,
    # ESEF slice 1 Task 5) instead of the raw product -- 000395 is not in MIGRATIONS (it would
    # try to create views over other esef_* tables this harness does not have), so its one
    # view this suite needs is rendered from the same builder the migration embeds, over the
    # fixture's esef_document_company_information + esef_entity_registry_map tables.
    esef_company_information_view = build_se_esef_view_sql(
        next(v for v in esef_tables.SE_ESEF_VIEWS if v.table == "esef_document_company_information")
    )
    # Views last: 000390's read se_ratsit_company and text_translations, which the fixture
    # creates. Its INSERT ... SELECT statements are data moves and are not replayed.
    return tables + alters + fixture + [esef_company_information_view] + views


def _run(statements: list[str], *, join_use_nulls: int) -> list[str]:
    script = f"SET join_use_nulls = {join_use_nulls};\n" + ";\n".join(statements) + ";\n"
    completed = subprocess.run(clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900)
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return [line for line in completed.stdout.splitlines() if line.strip()]


def _scope(current_sql: str, source: str, **extra) -> str:
    # The scope SQL is unpaged now (`scope_pages` runs it into a scratch table and pages
    # that), so the harness renders it alone and sorts for a stable printed order.
    return _bind(changed_scope_sql(current_sql=current_sql), source=source, **extra) + "\nORDER BY company_id"


def _insert(select_sql: str, ids: list[str], **extra) -> str:
    return _bind(insert_page_sql(select_sql=select_sql), company_ids=ids, source_run_id="run-1", extractor_version="x-v1", **extra)


def _labelled(sql: str, label: str) -> str:
    """Tag a scope query's rows so several scopes in one script stay tellable apart."""
    return f"SELECT '{label}', company_id FROM ({sql}) ORDER BY company_id"


SCB_ROW = (
    "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, legal_name, legal_form_code, "
    "source_status_code, registration_date, registration_date_raw, source_run_id, source_record_id, source_payload_hash, observed_at) VALUES "
    "('5560000000', '5560000000', ' SCB AB ', '49', '1', toDate32('1990-01-02'), '19900102', 'r', 'rec-1', 'h1', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)
BV_ROW = (
    "INSERT INTO corpscout.se_bolagsverket_companies (company_id, company_id_raw, legal_name, legal_form_code, "
    "registration_date, deregistration_date, activity_description, source_run_id, source_record_id, source_payload_hash, observed_at) VALUES "
    "('5560000000', '5560000000$X', 'Bolag AB', 'AB-ORGFO', toDate32('1990-01-02'), toDate32('2020-05-05'), 'Handel med kaffe', 'r', 'rec-b', 'hb', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)
TRANSLATION_ROW = (
    "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
    "('corpscout.se_bolagsverket_companies', 'activity_description', cityHash64('Handel med kaffe'), 'sv', 'en', 'Coffee trading', 'p', 'm', 1)"
)


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_scb_and_bolagsverket_extract_map_and_converge(join_use_nulls: int) -> None:
    script = _schema() + [SCB_ROW, BV_ROW, TRANSLATION_ROW]
    script += [_scope(scb.scb_current_sql(), "scb")]                       # 1: 5560000000
    script += [_insert(scb.scb_select_sql(), ["5560000000"])]
    script += [_scope(scb.scb_current_sql(), "scb")]                       # 2: nothing (converged)
    script += [_scope(bolagsverket.bolagsverket_current_sql(), "bolagsverket")]  # 3
    script += [_insert(bolagsverket.bolagsverket_select_sql(), ["5560000000"])]
    script += [
        "SELECT source, legal_name, legal_form_code, status, toString(incorporation_date), description, description_language, description_sv, extractor_version "
        f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL WHERE company_id = '5560000000' ORDER BY source",
        # A newer register record re-selects the company.
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, legal_name, legal_form_code, source_status_code, source_run_id, source_record_id, source_payload_hash, observed_at) VALUES "
        "('5560000000', '5560000000', 'SCB AB', '49', '9', 'r', 'rec-1', 'h2', toDateTime64('2026-09-08 00:00:00', 3, 'UTC'))",
        _scope(scb.scb_current_sql(), "scb"),                              # 4: 5560000000 again
    ]
    lines = _run(script, join_use_nulls=join_use_nulls)
    assert lines[0] == "5560000000"
    assert lines[1] == "5560000000"
    # AB-ORGFO lands as SCB code 49, the same vocabulary as the scb row below.
    assert lines[2] == "bolagsverket\tBolag AB\t49\tinactive\t1990-01-02\tCoffee trading\ten\tHandel med kaffe\tx-v1"
    assert lines[3] == "scb\tSCB AB\t49\tactive\t1990-01-02\t\\N\t\\N\t\\N\tx-v1"
    assert lines[4] == "5560000000"
    # lines[0] is the first scb scope, lines[1] is the bolagsverket scope (the second scb
    # scope in between printed nothing -- converged), lines[4] is the final scb scope after
    # the newer register record: count the id lines to be sure.
    assert lines.count("5560000000") == 3


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_bolagsverket_without_translation_keeps_the_swedish_text_as_description(join_use_nulls: int) -> None:
    script = _schema() + [BV_ROW, _insert(bolagsverket.bolagsverket_select_sql(), ["5560000000"]),
                          f"SELECT description, description_language, description_sv FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"]
    assert _run(script, join_use_nulls=join_use_nulls) == ["Handel med kaffe\tsv\tHandel med kaffe"]


def test_a_later_translation_re_selects_bolagsverket_and_flips_the_language() -> None:
    """I2: the register row is not the only input.

    The Swedish text is translated asynchronously by the translation pipeline. With
    observed_at taken from the register alone, a company translated after its last
    extraction kept description_language = 'sv' -- and the Swedish text on an
    English-facing field -- until its register record next changed. observed_at is the
    later of the two stamps, so the change scan visits it again.
    """
    late_translation = (
        "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
        "('corpscout.se_bolagsverket_companies', 'activity_description', cityHash64('Handel med kaffe'), 'sv', 'en', 'Coffee trading', 'p', 'm', "
        "toUnixTimestamp(toDateTime('2026-09-10 00:00:00', 'UTC')))"
    )
    script = _schema() + [
        BV_ROW,
        _insert(bolagsverket.bolagsverket_select_sql(), ["5560000000"]),
        _labelled(_bind(changed_scope_sql(current_sql=bolagsverket.bolagsverket_current_sql()), source="bolagsverket"), "converged"),
        late_translation,
        _labelled(_bind(changed_scope_sql(current_sql=bolagsverket.bolagsverket_current_sql()), source="bolagsverket"), "translated"),
        _insert(bolagsverket.bolagsverket_select_sql(), ["5560000000"]),
        # Two rows now differ only in observed_at; read the newer one without FINAL, whose
        # ReplacingMergeTree version (suggested_at) can tie inside one clickhouse-local run.
        f"SELECT description, description_language, toString(observed_at) FROM {tables.QUALIFIED_SUGGESTION_TABLE} ORDER BY observed_at DESC LIMIT 1",
    ]
    lines = _run(script, join_use_nulls=0)
    # The first scope prints nothing (converged on the register alone); the second selects
    # the company because only its translation is new.
    assert lines == ["translated\t5560000000", "Coffee trading\ten\t2026-09-10 00:00:00.000"]


def test_since_scope_reselects_a_company_newer_than_the_instant() -> None:
    """M7: `since` is the escape hatch out of a converged scan, so it is executed, not
    only pinned as text. The scope SQL is rendered alone -- in the extractor it lives
    inside `scope_pages`' scratch INSERT."""
    register = (
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, legal_name, legal_form_code, "
        "source_status_code, source_run_id, source_record_id, source_payload_hash, observed_at) VALUES "
        "('5560000000', '5560000000', 'SCB AB', '49', '1', 'r', 'rec-1', 'h1', toDateTime64('2026-09-05 00:00:00', 3, 'UTC'))"
    )
    since_sql = since_scope_sql(current_sql=scb.scb_current_sql())
    script = _schema() + [
        register,
        _insert(scb.scb_select_sql(), ["5560000000"]),
        _labelled(_bind(changed_scope_sql(current_sql=scb.scb_current_sql()), source="scb"), "changed"),
        _labelled(_bind(since_sql, since="2026-09-01T00:00:00Z"), "since-before"),
        _labelled(_bind(since_sql, since="2026-09-06T00:00:00Z"), "since-after"),
    ]
    # The change scan has converged and `since-after` is past the register stamp: only the
    # instant before it re-selects the company.
    assert _run(script, join_use_nulls=0) == ["since-before\t5560000000"]


def test_bolagsverket_without_deregistration_date_is_active() -> None:
    row = (
        "INSERT INTO corpscout.se_bolagsverket_companies (company_id, company_id_raw, legal_name, legal_form_code, "
        "registration_date, activity_description, source_run_id, source_record_id, source_payload_hash, observed_at) VALUES "
        "('5561111111', '5561111111$X', 'Open AB', 'AB-ORGFO', toDate32('1990-01-02'), 'Bakning', 'r', 'rec-b2', 'hb2', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
    )
    script = _schema() + [row, _insert(bolagsverket.bolagsverket_select_sql(), ["5561111111"]),
                          f"SELECT status, description_language FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"]
    assert _run(script, join_use_nulls=0) == ["active\tsv"]


def test_bolagsverket_legal_form_maps_trims_and_passes_unknown_tokens_through() -> None:
    rows = (
        "INSERT INTO corpscout.se_bolagsverket_companies (company_id, company_id_raw, legal_name, legal_form_code, "
        "source_run_id, source_record_id, source_payload_hash, observed_at) VALUES "
        "('5560000001', '5560000001$X', 'Trimmed HB', ' HB-ORGFO ', 'r', 'rec-1', 'h1', toDateTime64('2026-09-01 00:00:00', 3, 'UTC')), "
        "('5560000002', '5560000002$X', 'Novel form', 'ZZ-ORGFO', 'r', 'rec-2', 'h2', toDateTime64('2026-09-01 00:00:00', 3, 'UTC')), "
        "('5560000003', '5560000003$X', 'Formless', '', 'r', 'rec-3', 'h3', toDateTime64('2026-09-01 00:00:00', 3, 'UTC')), "
        "('5560000004', '5560000004$X', 'Foundation', 'S-ORGFO', 'r', 'rec-4', 'h4', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
    )
    script = _schema() + [
        rows,
        _insert(bolagsverket.bolagsverket_select_sql(), ["5560000001", "5560000002", "5560000003", "5560000004"]),
        f"SELECT company_id, legal_form_code FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL ORDER BY company_id",
    ]
    assert _run(script, join_use_nulls=0) == [
        "5560000001\t31",
        "5560000002\tZZ-ORGFO",
        "5560000003\t\\N",
        "5560000004\t72",
    ]


def test_esef_takes_the_newest_filing_and_upper_cases_the_lei() -> None:
    # The register-verified link the se_esef_document_company_information view joins
    # through (migration 000395): its own lei must match esef_document_company_information's
    # lei column byte-for-byte (the view's join is `t.lei = m.lei`, no case-folding), so this
    # is inserted in the same lowercase the rows below carry.
    esef_map_row = (
        "INSERT INTO corpscout.esef_entity_registry_map (lei, country_iso2, registry_id_raw, registry_id, match_source, link_status, source_run_id) VALUES "
        "('5493001kjtiigc8y1r12', 'SE', '5560000000', '5560000000', 'gleif_registered_as', 'register_verified', 'r')"
    )
    esef_rows = (
        "INSERT INTO corpscout.esef_document_company_information (source_document_id, package_sha256, lei, period_end, fiscal_year, extraction_status, company_description, description_language, model_provider, model_name, prompt_version, source_run_id, extracted_at, resolved_at) VALUES "
        "('doc-1', 'p1', '5493001kjtiigc8y1r12', '2024-12-31', 2024, 'ok', 'Old filing text', 'en', 'p', 'm', 'v', 'r', '', toDateTime64('2026-08-01 00:00:00', 3)), "
        "('doc-2', 'p2', '5493001kjtiigc8y1r12', '2025-12-31', 2025, 'ok', 'New filing text', '', 'p', 'm', 'v', 'r', '', toDateTime64('2026-09-01 00:00:00', 3)), "
        # doc-3 carries a different lei that never appears in the map, so it never joins into
        # the Swedish view -- the same exclusion the old country_iso2 = 'FI' predicate gave.
        "('doc-3', 'p3', 'X', '2025-12-31', 2025, 'ok', 'Finnish', 'en', 'p', 'm', 'v', 'r', '', toDateTime64('2026-09-02 00:00:00', 3))"
    )
    script = _schema() + [esef_map_row, esef_rows, _scope(esef.esef_current_sql(), "esef"), _insert(esef.esef_select_sql(), ["5560000000"]),
                          f"SELECT lei, description, description_language, toString(observed_at) FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"]
    lines = _run(script, join_use_nulls=0)
    assert lines == ["5560000000", "5493001KJTIIGC8Y1R12\tNew filing text\ten\t2026-09-01 00:00:00.000"]


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_wikidata_links_by_orgnr_and_by_lei(join_use_nulls: int) -> None:
    rows = [
        SCB_ROW,
        "INSERT INTO corpscout.se_scb_companies (company_id, company_id_raw, legal_name, source_run_id, source_record_id, source_payload_hash, observed_at) VALUES ('5561111111', '5561111111', 'Lei AB', 'r', 'rec-2', 'h', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))",
        "INSERT INTO corpscout.wikidata_companies (wikidata_id, wikidata_url, name, name_normalized, official_name, company_description, inception_date, source_system, source_run_id, source_record_id, source_payload_hash, retrieved_at, resolved_at) VALUES "
        "('Q1', 'u', 'SCB', 'scb', 'SCB Aktiebolag', 'A Swedish firm.', toDate('1970-01-01'), 's', 'r', 'Q1', 'h', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), toDateTime64('2026-09-01 00:00:00', 3, 'UTC')), "
        "('Q2', 'u', 'Lei', 'lei', NULL, NULL, toDate('1999-12-31'), 's', 'r', 'Q2', 'h', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'), toDateTime64('2026-09-02 00:00:00', 3, 'UTC'))",
        "INSERT INTO corpscout.wikidata_company_identifiers (wikidata_id, identifier_type, wikidata_property_id, identifier_value, is_primary, source_system, source_run_id, source_record_id, source_payload_hash, retrieved_at, resolved_at) VALUES "
        "('Q1', 'se_orgnr', 'P', '556000-0000', 1, 's', 'r', 'x', 'h', now64(3), now64(3)), "
        "('Q2', 'lei', 'P', '5493001kjtiigc8y1r12', 1, 's', 'r', 'y', 'h', now64(3), now64(3))",
        "INSERT INTO corpscout.company_identifier (issuer_scheme, issuer_id, country_code, company_id, match_method, match_confidence, registration_authority_id, registered_as_raw, company_id_normalized, entity_status, registration_status, is_current, successor_issuer_id, first_seen_date, last_seen_date, source_run_id, resolved_at) VALUES "
        "('lei', '5493001KJTIIGC8Y1R12', 'SE', '5561111111', 'm', 'c', 'RA', '', '5561111111', 'ACTIVE', 'ISSUED', 1, '', today(), today(), 'r', now64(3))",
    ]
    script = _schema() + rows + [
        _scope(wikidata.wikidata_current_sql(), "wikidata"),
        _insert(wikidata.wikidata_select_sql(), ["5560000000", "5561111111"]),
        f"SELECT company_id, wikidata_id, legal_name, toString(incorporation_date), description, description_language, source_record_uid FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL ORDER BY company_id",
    ]
    lines = _run(script, join_use_nulls=join_use_nulls)
    assert lines[:2] == ["5560000000", "5561111111"]
    assert lines[2] == "5560000000\tQ1\tSCB Aktiebolag\t\\N\tA Swedish firm.\ten\twikidata:Q1"
    assert lines[3] == "5561111111\tQ2\t\\N\t1999-12-31\t\\N\t\\N\twikidata:Q2"


def test_ratsit_takes_the_newest_report_and_maps_status() -> None:
    rows = (
        "INSERT INTO corpscout.se_ratsit_company (company_id, result_sha256, normalizer_version, schema_version, parser_version, requested_url, source_url, result_bucket, result_object_key, name, organization_number, legal_form, status, business_description, normalized_at) VALUES "
        f"('5560000000', repeat('a', 64), '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', 'k', 'Old Name AB', '556000-0000', 'Aktiebolag', 'Aktiv', 'Gammal text', toDateTime64('2026-08-01 00:00:00', 6, 'UTC')), "
        f"('5560000000', repeat('b', 64), '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', 'k', 'New Name AB', '556000-0000', 'Aktiebolag', 'Konkurs inledd 2026-04-21', 'Ny text', toDateTime64('2026-09-01 00:00:00', 6, 'UTC')), "
        f"('5560000000', repeat('c', 64), 'ratsit-normalizer-v1', 1, 'p', 'u', 'u', 'b', 'k', 'Stale AB', '556000-0000', NULL, NULL, NULL, toDateTime64('2026-09-05 00:00:00', 6, 'UTC'))"
    )
    script = _schema() + [rows,
        _scope(ratsit.ratsit_current_sql(), "ratsit", normalizer_version=RATSIT_NORMALIZER_VERSION),
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        f"SELECT legal_name, legal_form_code, status, description, description_language, description_sv, source_record_uid, toString(observed_at) FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL",
    ]
    lines = _run(script, join_use_nulls=0)
    assert lines == ["5560000000", f"New Name AB\t\\N\tinactive\tNy text\tsv\tNy text\tratsit:{'b' * 64}\t2026-09-01 00:00:00.000"]


RATSIT_ROWS = (
    "INSERT INTO corpscout.se_ratsit_company (company_id, result_sha256, normalizer_version, schema_version, parser_version, requested_url, source_url, result_bucket, result_object_key, name, organization_number, legal_form, status, business_description, normalized_at) VALUES "
    f"('5560000000', repeat('a', 64), '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', 'k', 'Old Name AB', '556000-0000', 'Aktiebolag', 'Aktiv', 'Gammal text', toDateTime64('2026-08-01 00:00:00', 6, 'UTC')), "
    f"('5560000000', repeat('b', 64), '{RATSIT_NORMALIZER_VERSION}', 1, 'p', 'u', 'u', 'b', 'k', 'New Name AB', '556000-0000', 'Aktiebolag', 'Aktiv', 'Ny text', toDateTime64('2026-09-01 00:00:00', 6, 'UTC'))"
)
RATSIT_TRANSLATION_ROW = (
    "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
    "('corpscout.se_ratsit_company', 'business_description', cityHash64('Ny text'), 'sv', 'en', 'New text', 'p', 'm', 1)"
)


@pytest.mark.parametrize("join_use_nulls", [0, 1], ids=["join_use_nulls_off", "join_use_nulls_on"])
def test_ratsit_with_a_translation_writes_the_english_text(join_use_nulls: int) -> None:
    script = _schema() + [
        RATSIT_ROWS,
        RATSIT_TRANSLATION_ROW,
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        f"SELECT legal_name, description, description_language, description_sv, toString(observed_at) FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL",
    ]
    # version 1 is 1970: the report's own stamp is the later one and stays.
    assert _run(script, join_use_nulls=join_use_nulls) == ["New Name AB\tNew text\ten\tNy text\t2026-09-01 00:00:00.000"]


def test_a_later_translation_re_selects_ratsit_and_flips_the_language() -> None:
    late_translation = (
        "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
        "('corpscout.se_ratsit_company', 'business_description', cityHash64('Ny text'), 'sv', 'en', 'New text', 'p', 'm', "
        "toUnixTimestamp(toDateTime('2026-09-10 00:00:00', 'UTC')))"
    )
    scope = _bind(changed_scope_sql(current_sql=ratsit.ratsit_current_sql()), source="ratsit", normalizer_version=RATSIT_NORMALIZER_VERSION)
    script = _schema() + [
        RATSIT_ROWS,
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        _labelled(scope, "converged"),
        late_translation,
        _labelled(scope, "translated"),
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        # Two rows now differ only in observed_at; read the newer one without FINAL, whose
        # ReplacingMergeTree version (suggested_at) can tie inside one clickhouse-local run.
        f"SELECT description, description_language, toString(observed_at) FROM {tables.QUALIFIED_SUGGESTION_TABLE} ORDER BY observed_at DESC LIMIT 1",
    ]
    assert _run(script, join_use_nulls=0) == ["translated\t5560000000", "New text\ten\t2026-09-10 00:00:00.000"]


def test_a_translation_of_an_older_ratsit_report_does_not_keep_re_selecting() -> None:
    """current_sql stamps the newest report only. Translating the OLD report's text later
    than the newest report must not re-select forever, because the SELECT never writes
    that stamp."""
    old_text_translation = (
        "INSERT INTO corpscout.text_translations (source_table, source_column, source_text_hash, source_lang, target_lang, translated_text, provider, model, version) VALUES "
        "('corpscout.se_ratsit_company', 'business_description', cityHash64('Gammal text'), 'sv', 'en', 'Old text', 'p', 'm', "
        "toUnixTimestamp(toDateTime('2026-09-10 00:00:00', 'UTC')))"
    )
    scope = _bind(changed_scope_sql(current_sql=ratsit.ratsit_current_sql()), source="ratsit", normalizer_version=RATSIT_NORMALIZER_VERSION)
    script = _schema() + [
        RATSIT_ROWS,
        _insert(ratsit.ratsit_select_sql(), ["5560000000"], normalizer_version=RATSIT_NORMALIZER_VERSION),
        old_text_translation,
        _labelled(scope, "stale-text"),
    ]
    assert _run(script, join_use_nulls=0) == []


def test_llm_scope_selects_two_text_sources_newer_than_the_llm_row() -> None:
    def suggestion(company_id, source, description, observed):
        return (f"INSERT INTO {tables.QUALIFIED_SUGGESTION_TABLE} (company_id, source, source_record_uid, observed_at, description, suggested_at, source_run_id, extractor_version) VALUES "
                f"('{company_id}', '{source}', 'u', toDateTime64('{observed}', 3, 'UTC'), {description}, toDateTime64('{observed}', 3, 'UTC'), 'r', 'v')")
    script = _schema() + [
        suggestion("5560000000", "esef", "'a'", "2026-09-01 00:00:00"),
        suggestion("5560000000", "wikidata", "'b'", "2026-09-01 00:00:00"),
        suggestion("5561111111", "esef", "'only'", "2026-09-01 00:00:00"),
        suggestion("5562222222", "esef", "'a'", "2026-09-01 00:00:00"),
        suggestion("5562222222", "bolagsverket", "'b'", "2026-09-01 00:00:00"),
        suggestion("5562222222", "llm", "'merged'", "2026-09-02 00:00:00"),
        suggestion("5563333333", "esef", "'a'", "2026-09-03 00:00:00"),
        suggestion("5563333333", "ratsit", "'b'", "2026-09-01 00:00:00"),
        suggestion("5563333333", "llm", "'stale'", "2026-09-02 00:00:00"),
        # One source text plus a reviewer decision: the reviewer is not source text to
        # merge, so this company stays below the two-source gate.
        suggestion("5564444444", "esef", "'only'", "2026-09-01 00:00:00"),
        suggestion("5564444444", "reviewer", "'a human wrote this'", "2026-09-03 00:00:00"),
        _bind(llm_scope_sql()) + "\nORDER BY company_id",
    ]
    assert _run(script, join_use_nulls=0) == ["5560000000", "5563333333"]
