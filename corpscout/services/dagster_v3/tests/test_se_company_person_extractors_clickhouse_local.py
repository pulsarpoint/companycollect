"""The person extractors' SQL on a real ClickHouse (spec 2026-09-09 sections 3.1 and 6).

Claims a fake client cannot settle:
1. Each page select really produces the sixteen suggestion columns, in a UNION ALL whose
   live and tombstone branches agree on every column type.
2. `data` is a JSON object on every row -- the table's CONSTRAINT valid_data (Code: 469) is
   the judge, and the tombstone branch's literal '{}' has to pass it too.
3. suggestion_id equals sha256(company_id, source, slot, suggested_at) for every row, so
   the WITH-bound stamp really is the instant the id hashed.
4. The state-hash scope selects a company before anything is suggested, selects nothing
   after the page is written (it converges), selects it again when the rebuilt source drops
   one of its rows, and converges again once the tombstone is written -- under
   join_use_nulls 0 and 1, which the scope's two aggregations exist to be immune to.
5. A company the register flags has_company = 0 has every remaining slot tombstoned even
   though the signatory table still holds its rows (spec section 6), and converges too.
6. A company outside se_company_basic_info never reaches the suggestion table.
7. The normalize hand-off (`changed_rows_sql()` + `normalized_row()`) reads what these
   extractors wrote and gives the expected parse statuses, the tombstone included.
8. The Ratsit branch really picks the newest report per company (an older scan's people must
   not leak), drops a company outside the basic-info universe, skips the nameless rows, gives
   the two-row token a role-qualified slot and the URL-less row an `idx:` slot, and keeps a
   slot stable across a re-scan while tombstoning the one the new report dropped.
"""

import subprocess
from pathlib import Path

import pytest

from dagster_v3.defs.esef_filings import tables as esef_tables
from dagster_v3.defs.esef_filings.country_views import build_se_esef_view_sql
from dagster_v3.defs.se_company.basic_info.extract import insert_page_sql
from dagster_v3.defs.se_company.person import bolagsverket, esef, ratsit, tables, wikidata
from dagster_v3.defs.se_company.person.normalize import (
    RAW_ROW_COLUMNS,
    changed_rows_sql,
    normalized_row,
)
from dagster_v3.defs.se_company.person.normalize_se import NORMALIZER_VERSION
from dagster_v3.defs.se_company.person.suggestions import PERSON_SELECT_COLUMNS, PERSON_TARGET
from dagster_v3.defs.sweden_ratsit.normalization import RATSIT_NORMALIZER_VERSION
from tests.clickhouse_local import clickhouse_local_command, render

ESEF_PEOPLE_VIEW = next(
    view for view in esef_tables.SE_ESEF_VIEWS if view.table == "esef_document_people"
)
LEI = "1FOLRR5RWTWWI397R131"
DOCUMENT_ID = f"{LEI}-2023-12-31-ESEF-SE-0"
ESEF_DATA = (
    '{"organization":"Exempel AB","status":"current","confidence":"0.95",'
    '"evidence_ids":"E0006","model_provider":"glm","model_name":"z-ai\\\\/glm",'
    '"prompt_version":"esef-people-v1"}'
)

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
# (file, the exact CREATE TABLE header line to keep). The trailing newline anchors the whole
# name: `corpscout.se_company_person_` would also be a prefix of five dropped tables, and
# `corpscout.esef_document_people` is a prefix of esef_document_people_legacy.
WANTED_CREATES = (
    ("000396_corpscout_se_company_person_entity.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_suggestion\n"),
    ("000396_corpscout_se_company_person_entity.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_person_normalized\n"),
    ("000377_corpscout_se_company_basic_info.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_company_basic_info\n"),
    ("000374_corpscout_se_bolagsverket_companies.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.se_bolagsverket_companies\n"),
    ("000395_corpscout_esef_country_agnostic_products.up.sql", "CREATE TABLE IF NOT EXISTS corpscout.esef_document_people\n"),
)

COMPANY_BV = "5561552760"
COMPANY_WD = "5560125220"
COMPANY_OUTSIDE = "5569999999"
# Ratsit (spec 2026-09-11 section 4.5). Two companies: the first exercises the slot rules and
# the re-scan, the second proves an older report cannot leak.
COMPANY_RATSIT_A = "5565550001"
COMPANY_RATSIT_B = "5565550002"
# Scanned by Ratsit, absent from se_company_basic_info: the universe join must drop it.
COMPANY_RATSIT_OUTSIDE = "5565550003"
RATSIT_URL_A = "https://www.ratsit.se/19800101-Anna_Ek/abc123"
RATSIT_URL_B = "https://www.ratsit.se/19751212-Cecilia_Nord/xyz789"
RATSIT_URL_OUTSIDE = "https://www.ratsit.se/19700707-Nils_Utanfor/out999"
RATSIT_REPORT_A1 = "a" * 64        # the first scan of company A
RATSIT_REPORT_A2 = "d" * 64        # its re-scan, one person short
RATSIT_REPORT_B_OLD = "b" * 64     # company B's superseded scan
RATSIT_REPORT_B = "c" * 64         # company B's current report
RATSIT_REPORT_OUTSIDE = "e" * 64   # the out-of-universe company's report
RATSIT_OUTSIDE_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_RATSIT_OUTSIDE}'"
)
# toJSONString escapes '/' as '\/' and the TSV dump doubles the backslash, exactly as
# WIKIDATA_DATA below records it.
RATSIT_DATA_VD = (
    '{"age":"45","identity_available":"true",'
    '"profile_url":"https:\\\\/\\\\/www.ratsit.se\\\\/19800101-Anna_Ek\\\\/abc123",'
    '"display_name_raw":"Anna Ek, 45 år","ratsit_person_id":"abc123","external":"false"}'
)
# No age, no URL: mapFilter drops the three empty keys instead of rendering them null.
RATSIT_DATA_BO = '{"identity_available":"true","display_name_raw":"Bo Ek","external":"true"}'
RATSIT_ROWS_SQL = (
    f"SELECT {', '.join(PERSON_SELECT_COLUMNS)}, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL WHERE source = 'ratsit' "
    "ORDER BY company_id, slot"
)
RATSIT_COMPANY_COLUMNS = (
    "company_id, result_sha256, normalizer_version, schema_version, parser_version, "
    "requested_url, source_url, result_bucket, result_object_key, name, organization_number, "
    "source_date_modified, industry_code_count, summary_count, responsible_people_count, "
    "establishment_count, financial_report_count, financial_period_count, "
    "people_at_address_count, normalized_at"
)
STATEMENT_KEY = "st1"
BOARD_DATA = '{"signatory_kind":"board_signature","statement_key":"st1","person_seq":"1"}'
CERT_DATA = '{"signatory_kind":"certification","statement_key":"st1","person_seq":"1"}'

RAW_ROW_CHECK_COLUMNS = (*PERSON_SELECT_COLUMNS, "suggested_at")
RAW_ROWS_SQL = (
    f"SELECT {', '.join(PERSON_SELECT_COLUMNS)}, toString(suggested_at) "
    f"FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL ORDER BY company_id, source, slot"
)
# The same hash formula as PERSON_TRAILING_SELECT_SQL: '\\n' in the Python source is a
# literal backslash-n in the SQL text, which ClickHouse's string literal turns into a real
# newline when concat() builds the hashed string.
IDENTITY_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    "WHERE suggestion_id != lower(hex(SHA256(concat(company_id, '\\n', toString(source), '\\n', "
    "slot, '\\n', toString(suggested_at)))))"
)
DATA_CHECK_SQL = (
    f"SELECT countIf(JSONType(data) != 'Object') FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL"
)
OUTSIDE_CHECK_SQL = (
    f"SELECT count() FROM {tables.QUALIFIED_SUGGESTION_TABLE} FINAL "
    f"WHERE company_id = '{COMPANY_OUTSIDE}'"
)


def _statements(path: Path, keep) -> list[str]:
    out: list[str] = []
    for raw in path.read_text(encoding="utf-8").split(";"):
        statement = "\n".join(
            line for line in raw.splitlines() if not line.strip().startswith("--")
        ).strip()
        if statement and keep(statement):
            out.append(statement)
    return out


def _schema() -> list[str]:
    schema = ["CREATE DATABASE IF NOT EXISTS corpscout"]
    for name, header in WANTED_CREATES:
        found = _statements(MIGRATIONS_DIR / name, lambda s, h=header: s.startswith(h.rstrip("\n")) and h in s + "\n")
        assert len(found) == 1, (name, header, len(found))
        schema += found
    for fixture in ("se_basic_info_source_tables.sql", "se_company_person_source_tables.sql"):
        schema += [s.strip() for s in (FIXTURES_DIR / fixture).read_text(encoding="utf-8").split(";") if s.strip()]
    schema.append(build_se_esef_view_sql(ESEF_PEOPLE_VIEW).rstrip(";"))
    return schema


def _insert(select_sql: str, ids: list[str], *, extractor_version: str, **params: str) -> str:
    return render(
        insert_page_sql(select_sql=select_sql, target=PERSON_TARGET),
        {
            "company_ids": ids, "source_run_id": "run-1",
            "extractor_version": extractor_version, **params,
        },
    )


def _scope(scope_sql: str, source: str, **params: str) -> str:
    """`params` carries a source's own select parameters (Ratsit's normalizer_version);
    `render` asserts no `%(name)s` survives, so a forgotten one fails loudly right here."""
    return render(scope_sql, {"source": source, **params}) + "\nORDER BY company_id"


def _ordered(sql: str, order_by: str) -> str:
    return f"SELECT * FROM ({sql}) AS ordered ORDER BY {order_by}"


def _sections(lines: list[str]) -> dict[str, list[list[str]]]:
    result: dict[str, list[list[str]]] = {}
    current = ""
    for line in lines:
        if line.startswith("@@"):
            current = line[2:]
            result[current] = []
        else:
            result[current].append(line.split("\t"))
    return result


def _as_row(fields: list[str]) -> tuple[str | None, ...]:
    assert len(fields) == len(RAW_ROW_COLUMNS), fields
    return tuple(None if field == "\\N" else field for field in fields)


SIGNATORY_COLUMNS = (
    "company_id, fiscal_year, statement_key, signatory_kind, person_seq, "
    "first_name, last_name, role_original, role_kind, resolved_at"
)
BOARD_ROW = (
    f"('{COMPANY_BV}', 2024, '{STATEMENT_KEY}', 'board_signature', 1, 'Anna', 'Svensson', "
    "'Styrelseledamot', 'board_member', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)
CERT_ROW = (
    f"('{COMPANY_BV}', 2024, '{STATEMENT_KEY}', 'certification', 1, 'Anna', 'Svensson', "
    "'Styrelseledamot', 'board_member', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)
OUTSIDE_ROW = (
    f"('{COMPANY_OUTSIDE}', 2024, 'st9', 'board_signature', 1, 'Nils', 'Utanfor', "
    "'', 'unknown', toDateTime64('2026-09-01 00:00:00', 3, 'UTC'))"
)


def _signatory_insert(rows: tuple[str, ...]) -> str:
    return (
        f"INSERT INTO corpscout.se_financial_report_signatories ({SIGNATORY_COLUMNS}) VALUES "
        + ", ".join(rows)
    )


def _register_row(company_id: str, observed_at: str, has_company: int) -> str:
    """One se_bolagsverket_companies row. The table is ReplacingMergeTree(observed_at) ordered
    by company_id, so a later row with has_company = 0 is the register's own tombstone."""
    return (
        "INSERT INTO corpscout.se_bolagsverket_companies (company_id, observed_at, has_company) "
        f"VALUES ('{company_id}', toDateTime64('{observed_at}', 3, 'UTC'), {has_company})"
    )


def _ratsit_report(company_id: str, sha: str, normalized_at: str, modified: str | None) -> str:
    """One se_ratsit_company row. The table is ReplacingMergeTree(normalized_at) ordered by
    (company_id, result_sha256, normalizer_version) and is never pruned, so a re-scanned
    company keeps both rows and only the newest normalized_at is the current report."""
    source_date = "NULL" if modified is None else f"toDate32('{modified}')"
    orgnr = company_id[-10:]
    return (
        f"INSERT INTO corpscout.se_ratsit_company ({RATSIT_COMPANY_COLUMNS}) VALUES "
        f"('{company_id}', '{sha}', '{RATSIT_NORMALIZER_VERSION}', 1, 'ratsit-parser-v1', "
        f"'https://www.ratsit.se/{orgnr}', 'https://www.ratsit.se/{orgnr}', 'bucket', "
        f"'sweden_ratsit/pilot/company_id={company_id}/report.json', 'Exempel AB', '{orgnr}', "
        f"{source_date}, 0, 0, 0, 0, 0, 0, 0, toDateTime64('{normalized_at}', 6, 'UTC'))"
    )


def _ratsit_person(
    company_id: str,
    sha: str,
    index: int,
    *,
    name: str | None,
    role: str,
    url: str | None,
    age: int | None,
    display_raw: str | None,
    normalized_at: str,
) -> str:
    """One se_ratsit_responsible_people row. `name=None` is the GDPR-limited shape the
    normalizer writes for a role-only entry: no name, no age, no URL, identity_available
    false (migration 000346's CONSTRAINT se_ratsit_responsible_identity)."""

    def text(value: str | None) -> str:
        return "NULL" if value is None else "'" + value.replace("'", "''") + "'"

    return (
        "INSERT INTO corpscout.se_ratsit_responsible_people (company_id, result_sha256, "
        "normalizer_version, person_index, display_name, display_name_raw, name, age, "
        "identity_available, role, profile_url, normalized_at) VALUES "
        f"('{company_id}', '{sha}', '{RATSIT_NORMALIZER_VERSION}', {index}, {text(name)}, "
        f"{text(display_raw)}, {text(name)}, {'NULL' if age is None else age}, "
        f"{0 if name is None else 1}, '{role}', {text(url)}, "
        f"toDateTime64('{normalized_at}', 6, 'UTC'))"
    )


def _script_statements() -> list[str]:
    ratsit_scope = _scope(
        ratsit.ratsit_changed_scope_sql(), "ratsit",
        normalizer_version=RATSIT_NORMALIZER_VERSION,
    )
    ratsit_insert = _insert(
        ratsit.ratsit_select_sql(),
        [COMPANY_RATSIT_A, COMPANY_RATSIT_B, COMPANY_RATSIT_OUTSIDE],
        extractor_version=ratsit.RATSIT_PERSON_EXTRACTOR_VERSION,
        normalizer_version=RATSIT_NORMALIZER_VERSION,
    )
    bv_scope = _scope(bolagsverket.bolagsverket_changed_scope_sql(), "bolagsverket")
    bv_insert = _insert(
        bolagsverket.bolagsverket_select_sql(), [COMPANY_BV],
        extractor_version=bolagsverket.BOLAGSVERKET_PERSON_EXTRACTOR_VERSION,
    )
    esef_scope = _scope(esef.esef_changed_scope_sql(), "esef")
    esef_insert = _insert(
        esef.esef_select_sql(), [COMPANY_BV],
        extractor_version=esef.ESEF_PERSON_EXTRACTOR_VERSION,
    )
    wd_scope = _scope(wikidata.wikidata_changed_scope_sql(), "wikidata")
    wd_insert = _insert(
        wikidata.wikidata_select_sql(), [COMPANY_WD],
        extractor_version=wikidata.WIKIDATA_PERSON_EXTRACTOR_VERSION,
    )
    changed_rows = _ordered(
        render(
            changed_rows_sql(),
            {"company_ids": [COMPANY_BV, COMPANY_WD], "normalizer_version": NORMALIZER_VERSION},
        ),
        "company_id, source, slot",
    )
    return [
        *_schema(),
        # The universe: COMPANY_OUTSIDE and COMPANY_RATSIT_OUTSIDE deliberately have no
        # basic-info row.
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_BV}')",
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_WD}')",
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_RATSIT_A}')",
        f"INSERT INTO corpscout.se_company_basic_info (company_id) VALUES ('{COMPANY_RATSIT_B}')",
        "INSERT INTO corpscout.esef_entity_registry_map (lei, country_iso2, registry_id_raw, "
        f"registry_id, match_source, link_status) VALUES ('{LEI}', 'SE', '{COMPANY_BV}', "
        f"'{COMPANY_BV}', 'gleif', 'register_verified')",
        "INSERT INTO corpscout.esef_document_people (candidate_uid, source_record_uid, "
        "source_document_id, lei, fiscal_year, name, role, role_category, organization, status, "
        "effective_from, effective_to, confidence, evidence_ids, model_provider, model_name, "
        "prompt_version, source_run_id, extracted_at) VALUES "
        f"('{'c' * 64}', '{'r' * 64}', '{DOCUMENT_ID}', '{LEI}', 2023, 'Öberg, Håkan', "
        "'Verkställande direktör', 'chief_executive', 'Exempel AB', 'current', "
        "toDate32('2021-07-16'), NULL, 0.95, ['E0006'], 'glm', 'z-ai/glm', 'esef-people-v1', "
        "'run-0', toDateTime64('2026-09-02 00:00:00', 3, 'UTC'))",
        "SELECT '@@esef_scope_1'",
        esef_scope,
        esef_insert,
        "SELECT '@@esef_scope_2'",
        esef_scope,
        "INSERT INTO corpscout.wikidata_company_identifiers (wikidata_id, identifier_type, "
        "wikidata_property_id, identifier_value, is_primary, source_system, source_run_id, "
        "source_record_id, source_payload_hash, retrieved_at, resolved_at) VALUES "
        f"('Q9', 'se_orgnr', 'P6460', '556012-5220', 1, 'wikidata', 'run-0', 'i1', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC'))",
        "INSERT INTO corpscout.wikidata_persons (person_wikidata_id, name, name_normalized, "
        "description, birth_year, image_url, wikidata_url, source_system, source_run_id, "
        "source_record_id, source_payload_hash, retrieved_at, resolved_at) VALUES "
        "('Q404522', 'Jens Fischer', 'jens fischer', 'Swedish cinematographer', 1946, NULL, "
        f"'http://www.wikidata.org/entity/Q404522', 'wikidata', 'run-0', 'p1', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC')), "
        "('_blank1', 'http://www.wikidata.org/.well-known/genid/abc', 'genid', NULL, NULL, NULL, "
        f"NULL, 'wikidata', 'run-0', 'p2', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC'))",
        "INSERT INTO corpscout.wikidata_company_people (company_wikidata_id, person_wikidata_id, "
        "role_property, role_label, start_date, end_date, is_current, source_system, source_run_id, "
        "source_record_id, source_payload_hash, retrieved_at, resolved_at) VALUES "
        "('Q9', 'Q404522', 'P169', 'chief executive officer', toDate('2021-07-16'), NULL, 1, "
        f"'wikidata', 'run-0', 'Q9:P169:Q404522', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC')), "
        "('Q9', '_blank1', 'P112', 'founder', NULL, NULL, 1, "
        f"'wikidata', 'run-0', 'Q9:P112:_blank1', '{'0' * 64}', "
        "toDateTime64('2026-09-03 00:00:00', 3, 'UTC'), toDateTime64('2026-09-03 00:00:00', 3, 'UTC'))",
        "SELECT '@@wd_scope_1'",
        wd_scope,
        wd_insert,
        "SELECT '@@wd_scope_2'",
        wd_scope,
        _register_row(COMPANY_BV, "2026-09-01 00:00:00", 1),
        _signatory_insert((BOARD_ROW, CERT_ROW, OUTSIDE_ROW)),
        "SELECT '@@bv_scope_1'",
        bv_scope,
        bv_insert,
        "SELECT '@@raw_rows_1'",
        RAW_ROWS_SQL,
        "SELECT '@@identity_check'",
        IDENTITY_CHECK_SQL,
        "SELECT '@@data_check'",
        DATA_CHECK_SQL,
        "SELECT '@@outside_check'",
        OUTSIDE_CHECK_SQL,
        "SELECT '@@bv_scope_2'",
        bv_scope,
        # The source is rebuilt whole (stage + EXCHANGE TABLES on prod) and this time the
        # certification signature line is gone.
        "SELECT sleep(0.01) FORMAT Null",
        "TRUNCATE TABLE corpscout.se_financial_report_signatories",
        _signatory_insert((BOARD_ROW, OUTSIDE_ROW)),
        "SELECT '@@bv_scope_3'",
        bv_scope,
        bv_insert,
        "SELECT '@@raw_rows_2'",
        RAW_ROWS_SQL,
        "SELECT '@@bv_scope_4'",
        bv_scope,
        "SELECT '@@data_check_2'",
        DATA_CHECK_SQL,
        "SELECT '@@changed_rows'",
        changed_rows,
        # The register deregisters the company (spec section 6). The signatory table still
        # holds its board signature line -- reports are never deleted -- so only the
        # has_company flag can retire the slot.
        "SELECT sleep(0.01) FORMAT Null",
        _register_row(COMPANY_BV, "2026-09-02 00:00:00", 0),
        "SELECT '@@bv_scope_5'",
        bv_scope,
        bv_insert,
        "SELECT '@@raw_rows_3'",
        RAW_ROWS_SQL,
        "SELECT '@@bv_scope_6'",
        bv_scope,
        "SELECT '@@data_check_3'",
        DATA_CHECK_SQL,
        # --- Ratsit (spec 2026-09-11 section 4.5) ----------------------------------
        # Company A's first scan: a VD with a dated URL, a Delgivningsbar person sharing
        # that token, a nameless GDPR row and a named row with no URL at all.
        _ratsit_report(COMPANY_RATSIT_A, RATSIT_REPORT_A1, "2026-09-09 00:00:00", "2026-08-30"),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A1, 0, name="Anna Ek", role="VD",
            url=RATSIT_URL_A, age=45, display_raw="Anna Ek, 45 år",
            normalized_at="2026-09-09 00:00:00",
        ),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A1, 1, name="Anna Ek",
            role="Delgivningsbar person", url=RATSIT_URL_A, age=45,
            display_raw="Anna Ek, 45 år", normalized_at="2026-09-09 00:00:00",
        ),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A1, 2, name=None, role="Extern firmatecknare",
            url=None, age=None, display_raw=None, normalized_at="2026-09-09 00:00:00",
        ),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A1, 3, name="Bo Ek", role="Extern VD",
            url=None, age=None, display_raw="Bo Ek", normalized_at="2026-09-09 00:00:00",
        ),
        # Company B: a superseded scan that must not leak, then the current report (no
        # source_date_modified, so the role year comes from the scan's own stamp).
        _ratsit_report(COMPANY_RATSIT_B, RATSIT_REPORT_B_OLD, "2026-08-01 00:00:00", "2024-05-05"),
        _ratsit_person(
            COMPANY_RATSIT_B, RATSIT_REPORT_B_OLD, 0, name="Old Vd", role="VD",
            url="https://www.ratsit.se/19600101-Old_Vd/old111", age=None,
            display_raw="Old Vd", normalized_at="2026-08-01 00:00:00",
        ),
        _ratsit_report(COMPANY_RATSIT_B, RATSIT_REPORT_B, "2026-09-09 00:00:00", None),
        _ratsit_person(
            COMPANY_RATSIT_B, RATSIT_REPORT_B, 0, name="Cecilia Nord", role="Prokurist",
            url=RATSIT_URL_B, age=51, display_raw="Cecilia Nord, 51 år",
            normalized_at="2026-09-09 00:00:00",
        ),
        # Scanned like the others, but the entity has never heard of it.
        _ratsit_report(
            COMPANY_RATSIT_OUTSIDE, RATSIT_REPORT_OUTSIDE, "2026-09-09 00:00:00", "2026-08-30"
        ),
        _ratsit_person(
            COMPANY_RATSIT_OUTSIDE, RATSIT_REPORT_OUTSIDE, 0, name="Nils Utanfor", role="VD",
            url=RATSIT_URL_OUTSIDE, age=55, display_raw="Nils Utanfor, 55 år",
            normalized_at="2026-09-09 00:00:00",
        ),
        "SELECT '@@ratsit_scope_1'",
        ratsit_scope,
        ratsit_insert,
        "SELECT '@@ratsit_rows_1'",
        RATSIT_ROWS_SQL,
        "SELECT '@@ratsit_scope_2'",
        ratsit_scope,
        "SELECT '@@ratsit_outside_check'",
        RATSIT_OUTSIDE_CHECK_SQL,
        # The re-scan: Bo Ek is gone and both Anna rows keep their slots under a new hash.
        "SELECT sleep(0.01) FORMAT Null",
        _ratsit_report(COMPANY_RATSIT_A, RATSIT_REPORT_A2, "2026-09-10 00:00:00", "2026-09-09"),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A2, 0, name="Anna Ek", role="VD",
            url=RATSIT_URL_A, age=46, display_raw="Anna Ek, 46 år",
            normalized_at="2026-09-10 00:00:00",
        ),
        _ratsit_person(
            COMPANY_RATSIT_A, RATSIT_REPORT_A2, 1, name="Anna Ek",
            role="Delgivningsbar person", url=RATSIT_URL_A, age=46,
            display_raw="Anna Ek, 46 år", normalized_at="2026-09-10 00:00:00",
        ),
        "SELECT '@@ratsit_scope_3'",
        ratsit_scope,
        ratsit_insert,
        "SELECT '@@ratsit_rows_2'",
        RATSIT_ROWS_SQL,
        "SELECT '@@ratsit_scope_4'",
        ratsit_scope,
        "SELECT '@@data_check_4'",
        DATA_CHECK_SQL,
    ]


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    script = f"SET join_use_nulls = {request.param};\n" + ";\n".join(_script_statements()) + ";\n"
    try:
        completed = subprocess.run(
            clickhouse_local_command(), input=script, capture_output=True, text=True, timeout=900
        )
    except (OSError, subprocess.TimeoutExpired) as exc:  # pragma: no cover - env
        pytest.skip(f"clickhouse-local is unusable here: {exc}")
    assert completed.returncode == 0, completed.stderr or completed.stdout
    return _sections([line for line in completed.stdout.splitlines() if line.strip()])


def _rows(sections: dict[str, list[list[str]]], name: str) -> list[dict[str, str]]:
    return [dict(zip(RAW_ROW_CHECK_COLUMNS, fields, strict=True)) for fields in sections[name]]


def test_the_scope_selects_the_company_then_converges(sections) -> None:
    assert sections["bv_scope_1"] == [[COMPANY_BV]]
    assert sections["bv_scope_2"] == []


def test_both_signature_lines_land_as_their_own_slot(sections) -> None:
    rows = [r for r in _rows(sections, "raw_rows_1") if r["source"] == "bolagsverket"]
    assert len(rows) == 2
    assert {row["data"] for row in rows} == {BOARD_DATA, CERT_DATA}
    assert len({row["slot"] for row in rows}) == 2
    for row in rows:
        assert row["source"] == "bolagsverket"
        assert row["first_name"] == "Anna" and row["last_name"] == "Svensson"
        assert row["full_name"] == "\\N"
        assert row["role_original"] == "Styrelseledamot"
        assert row["role_key"] == "board_member"
        assert row["fiscal_year"] == "2024"
        assert row["document_ref"] == STATEMENT_KEY
        assert row["birth_year"] == row["wikidata_id"] == "\\N"
        assert row["role_from"] == row["role_to"] == "\\N"
        # slot = report record uid + signatory uid, both 64 hex characters.
        record_uid, signatory_uid = row["slot"].split(":")
        assert len(record_uid) == len(signatory_uid) == 64
        assert row["source_record_id"] == record_uid


def test_suggestion_id_matches_the_stamp_hash_and_data_is_always_an_object(sections) -> None:
    assert sections["identity_check"] == [["0"]]
    assert sections["data_check"] == [["0"]]
    assert sections["data_check_2"] == [["0"]]


def test_a_company_outside_the_basic_info_universe_is_never_written(sections) -> None:
    assert sections["outside_check"] == [["0"]]


def test_a_vanished_signature_line_is_tombstoned_and_the_scope_reconverges(sections) -> None:
    assert sections["bv_scope_3"] == [[COMPANY_BV]]
    before = {
        row["slot"]: row for row in _rows(sections, "raw_rows_1") if row["source"] == "bolagsverket"
    }
    rows = [r for r in _rows(sections, "raw_rows_2") if r["source"] == "bolagsverket"]
    assert len(rows) == 2
    tombstones = [row for row in rows if row["data"] == "{}"]
    assert len(tombstones) == 1
    [tombstone] = tombstones
    for column in ("full_name", "first_name", "last_name", "birth_year", "wikidata_id",
                   "role_original", "role_key", "fiscal_year", "role_from", "role_to",
                   "document_ref"):
        assert tombstone[column] == "\\N", column
    assert tombstone["source_record_id"] == ""
    assert before[tombstone["slot"]]["data"] == CERT_DATA
    assert tombstone["suggested_at"] > before[tombstone["slot"]]["suggested_at"]
    [survivor] = [row for row in rows if row["data"] != "{}"]
    assert survivor["data"] == BOARD_DATA
    assert survivor["suggested_at"] > before[survivor["slot"]]["suggested_at"]
    assert sections["bv_scope_4"] == []


def test_a_deregistered_company_has_its_remaining_slots_tombstoned(sections) -> None:
    """Spec section 6: tombstones on has_company = 0. The register flips, the company's
    signature lines leave the live branch, and the slot still live is retired -- while the slot
    already tombstoned is left exactly as it was, which is what makes the scope converge."""
    assert sections["bv_scope_5"] == [[COMPANY_BV]]
    before = {
        row["slot"]: row for row in _rows(sections, "raw_rows_2") if row["source"] == "bolagsverket"
    }
    board_slot = next(slot for slot, row in before.items() if row["data"] != "{}")
    cert_slot = next(slot for slot, row in before.items() if row["data"] == "{}")
    rows = {
        row["slot"]: row for row in _rows(sections, "raw_rows_3") if row["source"] == "bolagsverket"
    }
    assert set(rows) == {board_slot, cert_slot}
    for slot, row in rows.items():
        assert row["data"] == "{}", slot
        assert row["first_name"] == row["last_name"] == row["role_key"] == "\\N", slot
        assert row["source_record_id"] == "", slot
    assert rows[board_slot]["suggested_at"] > before[board_slot]["suggested_at"]
    # Already a tombstone, so the page did not rewrite it.
    assert rows[cert_slot]["suggested_at"] == before[cert_slot]["suggested_at"]
    assert sections["bv_scope_6"] == []
    assert sections["data_check_3"] == [["0"]]


def test_the_esef_row_keeps_the_delivered_full_name_dates_and_extras(sections) -> None:
    assert sections["esef_scope_1"] == [[COMPANY_BV]]
    assert sections["esef_scope_2"] == []
    [row] = [r for r in _rows(sections, "raw_rows_1") if r["source"] == "esef"]
    assert row["slot"] == f"{DOCUMENT_ID}:{'c' * 64}"
    assert row["source_record_id"] == "r" * 64
    assert row["full_name"] == "Öberg, Håkan"
    assert row["first_name"] == row["last_name"] == "\\N"
    assert row["role_original"] == "Verkställande direktör"
    assert row["role_key"] == "chief_executive"
    assert row["fiscal_year"] == "2023"
    assert row["role_from"] == "2021-07-16" and row["role_to"] == "\\N"
    assert row["document_ref"] == DOCUMENT_ID
    assert row["data"] == ESEF_DATA


WIKIDATA_DATA = (
    '{"description":"Swedish cinematographer","image_url":"",'
    '"wikidata_url":"http:\\\\/\\\\/www.wikidata.org\\\\/entity\\\\/Q404522",'
    '"name_normalized":"jens fischer","is_current":"1"}'
)


def test_the_wikidata_row_carries_the_qid_birth_year_span_and_description(sections) -> None:
    assert sections["wd_scope_1"] == [[COMPANY_WD]]
    assert sections["wd_scope_2"] == []
    rows = [r for r in _rows(sections, "raw_rows_1") if r["source"] == "wikidata"]
    # The blank-node statement is not a person and never lands.
    assert len(rows) == 1
    [row] = rows
    assert row["company_id"] == COMPANY_WD
    assert row["slot"] == "Q9:P169:Q404522"
    assert row["full_name"] == "Jens Fischer"
    assert row["birth_year"] == "1946"
    assert row["wikidata_id"] == "Q404522"
    assert row["role_original"] == "chief executive officer"
    assert row["role_key"] == "P169"
    assert row["fiscal_year"] == "\\N"
    assert row["role_from"] == "2021-07-16" and row["role_to"] == "\\N"
    assert row["document_ref"] == "\\N"
    assert row["data"] == WIKIDATA_DATA


def test_the_normalize_hand_off_gives_the_expected_parse_statuses(sections) -> None:
    rows = [normalized_row(_as_row(fields), None) for fields in sections["changed_rows"]]
    status = tables.NORMALIZED_COLUMNS.index("parse_status")
    notes = tables.NORMALIZED_COLUMNS.index("parse_notes")
    code = tables.NORMALIZED_COLUMNS.index("role_code")
    name = tables.NORMALIZED_COLUMNS.index("display_name")
    by_status = sorted((row[status], row[name], row[code], tuple(row[notes])) for row in rows)
    assert by_status == [
        ("no_person", "", None, ("empty name",)),
        ("ok", "Anna Svensson", "board_member", ()),
        # normalize_se_person records the comma form of spec rule 4.1 ("Last, First") as a
        # parse note -- verified directly against normalize_se_person(RawPerson(source="esef",
        # full_name="Öberg, Håkan", ...)), which returns parse_notes=("comma form",).
        ("ok", "Håkan Öberg", "chief_executive_officer", ("comma form",)),
        ("ok", "Jens Fischer", "chief_executive_officer", ()),
    ]


def test_the_ratsit_rows_are_the_current_reports_named_people(sections) -> None:
    # Two companies, not three: COMPANY_RATSIT_OUTSIDE is outside the basic-info universe.
    assert sections["ratsit_scope_1"] == [[COMPANY_RATSIT_A], [COMPANY_RATSIT_B]]
    rows = {
        row["slot"]: row
        for row in _rows(sections, "ratsit_rows_1")
        if row["company_id"] == COMPANY_RATSIT_A
    }
    # person_index 2 is the nameless GDPR row: a role, no identity, never a person.
    assert set(rows) == {"abc123:vd", "abc123:delgivningsbar person", "idx:3"}
    vd = rows["abc123:vd"]
    assert vd["full_name"] == "Anna Ek"
    assert vd["first_name"] == vd["last_name"] == "\\N"
    assert vd["birth_year"] == "1980"                    # from the URL's 19800101
    assert vd["role_original"] == "VD"
    assert vd["role_key"] == "\\N"                       # Ratsit has no machine code
    assert vd["fiscal_year"] == "2026"                   # source_date_modified 2026-08-30
    assert vd["wikidata_id"] == vd["role_from"] == vd["role_to"] == "\\N"
    assert vd["document_ref"] == "\\N"
    assert vd["source_record_id"] == f"ratsit:{RATSIT_REPORT_A1}:0"
    assert vd["data"] == RATSIT_DATA_VD
    # The same token twice in one report: both slots carry the lowercased role.
    assert rows["abc123:delgivningsbar person"]["role_original"] == "Delgivningsbar person"
    assert rows["abc123:delgivningsbar person"]["birth_year"] == "1980"
    # Named, but no URL: no token, so the slot is the person index, the birth year is NULL
    # and `Extern VD` sets data.external.
    bo = rows["idx:3"]
    assert bo["full_name"] == "Bo Ek" and bo["birth_year"] == "\\N"
    assert bo["data"] == RATSIT_DATA_BO
    assert sections["ratsit_scope_2"] == []              # the scan converges in one pass


def test_a_ratsit_company_outside_the_basic_info_universe_is_never_written(sections) -> None:
    """The universe join of `report`: COMPANY_RATSIT_OUTSIDE has a current report and a named
    VD, and the page select was handed its id, but the entity has no basic-info row for it --
    so it produces no live row, no suggestion, and no state-hash scope hit either (it is on
    neither side of the comparison)."""
    assert sections["ratsit_outside_check"] == [["0"]]
    assert [COMPANY_RATSIT_OUTSIDE] not in sections["ratsit_scope_1"]
    assert not [
        r for r in _rows(sections, "ratsit_rows_1")
        if r["company_id"] == COMPANY_RATSIT_OUTSIDE
    ]


def test_only_the_newest_ratsit_report_reaches_the_suggestion_table(sections) -> None:
    """Company B carries two scans. `Old Vd` belongs to the superseded one and the people
    rows join the report's own (company_id, result_sha256, normalizer_version), so it never
    appears -- the report row itself is never deleted."""
    rows = [r for r in _rows(sections, "ratsit_rows_1") if r["company_id"] == COMPANY_RATSIT_B]
    assert len(rows) == 1
    [row] = rows
    assert row["slot"] == "xyz789"                       # a token seen once keeps it bare
    assert row["full_name"] == "Cecilia Nord"
    assert row["role_original"] == "Prokurist"
    assert row["source_record_id"] == f"ratsit:{RATSIT_REPORT_B}:0"
    # No source_date_modified on the current report: the role year is the scan's stamp.
    assert row["fiscal_year"] == "2026"


def test_a_ratsit_slot_survives_a_rescan_and_a_dropped_person_is_tombstoned(sections) -> None:
    """The token is Ratsit's own person id, so a re-scan rewrites the person's row in place
    (new source_record_id, same slot) while the slot the new report no longer delivers gets
    the shared per-slot tombstone."""
    assert sections["ratsit_scope_3"] == [[COMPANY_RATSIT_A]]
    before = {
        r["slot"]: r
        for r in _rows(sections, "ratsit_rows_1")
        if r["company_id"] == COMPANY_RATSIT_A
    }
    rows = {
        r["slot"]: r
        for r in _rows(sections, "ratsit_rows_2")
        if r["company_id"] == COMPANY_RATSIT_A
    }
    assert set(rows) == set(before)
    survivor = rows["abc123:vd"]
    assert survivor["full_name"] == "Anna Ek"
    assert survivor["source_record_id"] == f"ratsit:{RATSIT_REPORT_A2}:0"
    assert survivor["suggested_at"] > before["abc123:vd"]["suggested_at"]
    tombstone = rows["idx:3"]
    assert tombstone["data"] == "{}"
    assert tombstone["source_record_id"] == ""
    for column in ("full_name", "first_name", "last_name", "birth_year", "wikidata_id",
                   "role_original", "role_key", "fiscal_year", "role_from", "role_to",
                   "document_ref"):
        assert tombstone[column] == "\\N", column
    assert sections["ratsit_scope_4"] == []
    assert sections["data_check_4"] == [["0"]]
