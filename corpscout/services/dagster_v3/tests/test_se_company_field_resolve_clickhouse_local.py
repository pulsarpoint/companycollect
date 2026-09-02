"""Executes the field-registry resolve path end to end in a disposable clickhouse-local:
the changed-company scan for every reason, every field's resolve statement, the wide
projection, and the parity check -- against the migrations' DDL, with the parameters
bound by ClickHouse's own ``SET param_<name>`` (the server-side path the backoffice
takes), twice (join_use_nulls 0 and 1; every LEFT JOIN miss is read through ifNull).

One company, Svenska Handelsbanken (5020077862), with candidates from every source the
info registry names and one reviewer decision; a second company, BETA, with a legal name
from wikidata only, which the scan must never select (spec 8.3: no register name, no
publication). The expected wide row is hand-written below; the array provenance columns
are compared as sets (plan 2 decides their order), everything else column by column.

The resolve statements executed here are rendered by fields.sql -- the same text plan 1's
export writes into se_company_field_registry (this script inserts those rows too, so the
scan's registry-version comparison runs against real rows). The asset reads them back
from that table; the FakeClient tests in test_se_company_field_resolve.py cover that.
"""

import ast
import hashlib
import json
import re
import subprocess
import uuid
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from dagster_v3.defs.se_company.fields.parity import (
    PARITY_COLUMNS,
    build_parity_snapshot_sql,
    build_parity_sql,
    build_rows_per_field_source_sql,
)
from dagster_v3.defs.se_company.fields.policies import policy_for
from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, field_by_name, field_names
from dagster_v3.defs.se_company.fields.resolve import SELECTION_COLUMNS, build_changed_companies_sql
from dagster_v3.defs.se_company.fields.sql import render_projection_sql, render_resolve_sql
from dagster_v3.defs.se_company.info import INSERT_COLUMNS as OLD_INSERT_COLUMNS
from tests.se_company_ddl import declared_columns
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal

pytestmark = pytest.mark.integration

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "migrations"
BASE_MIGRATIONS = (
    "000150_corpscout_se_translations.up.sql",  # se_code_labels, the legal-form dictionary
    "000297_corpscout_se_company_info.up.sql",
    "000299_corpscout_se_company_info_sole_traders.up.sql",
    "000300_corpscout_se_company_info_scb_english.up.sql",
    "000301_corpscout_se_company_info_description_sv.up.sql",
    "000304_corpscout_se_company_info_llm_enhanced.up.sql",
    "000305_corpscout_se_code_labels_swedish.up.sql",
    "000306_corpscout_se_company_info_legal_form_label.up.sql",
    "000365_corpscout_se_company_info_esef_enrichment.up.sql",
    "000371_corpscout_se_company_info_field_value.up.sql",
)
# Plan 1's migrations (the three field tables, the eight wide columns, the widened
# decision CHECKs) are numbered after 000372: picked up by number so this file needs no
# edit when they land. Statements aimed at other tables -- and 000377's MATERIALIZED
# VIEW -- are filtered out by _schema_statements.
LATER_MIGRATIONS = tuple(sorted(
    path.name for path in MIGRATIONS_DIR.glob("[0-9]*.up.sql") if path.name > "000372"))
NEEDED_TABLES = frozenset({
    "se_code_labels", "se_company_info_scb", "se_company_info_esef", "se_company_info_wikidata",
    "se_company_info", "se_company_info_field_value", "se_company_info_enrichment_observation",
    "se_company_field_registry", "se_company_field_candidate", "se_company_field",
})
_TABLE_RE = re.compile(r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE)

HB = "5020077862"  # Svenska Handelsbanken AB
BETA = "5560125220"  # a legal name from wikidata only: never a register name, never published
LEI = "NHBDILHZTYCNBV5UYZ31"
WIKIDATA_ID = "Q1155005"
SUGGESTION_ID = uuid.UUID(int=7)
DECISION_ID = uuid.UUID(int=1)
DECISION_ID_2 = uuid.UUID(int=2)
RUN_1, RUN_2, RUN_3 = "resolve-run-1", "resolve-run-2", "resolve-run-3"
T_REGISTER = datetime(2026, 8, 1, tzinfo=UTC)
T_ESEF = datetime(2025, 4, 2, tzinfo=UTC)
T_FINANCIAL = datetime(2024, 12, 31, tzinfo=UTC)
T_DOMAIN = datetime(2026, 8, 10, tzinfo=UTC)
T_LLM = datetime(2026, 8, 15, tzinfo=UTC)
T_EXTRACT = datetime(2026, 8, 20, 12, tzinfo=UTC)
T_DECISION = datetime(2026, 8, 21, 9, tzinfo=UTC)
T_OLD_ROW = datetime(2026, 8, 25, tzinfo=UTC)  # the pre-cutover publisher's row: after the decision
T_RESOLVE_1 = datetime(2026, 9, 2, 10, tzinfo=UTC)
T_EXTRACT_2 = datetime(2026, 9, 3, 8, tzinfo=UTC)
T_RESOLVE_2 = datetime(2026, 9, 3, 10, tzinfo=UTC)
T_DECISION_2 = datetime(2026, 9, 4, 9, tzinfo=UTC)
T_RESOLVE_3 = datetime(2026, 9, 4, 10, tzinfo=UTC)
T_REGISTRY = datetime(2026, 8, 1, tzinfo=UTC)
T_REGISTRY_BUMP = datetime(2026, 9, 5, tzinfo=UTC)
NO_CUTOFF = "2099-12-31 23:59:59"
PAST_CUTOFF = "2000-01-01 00:00:00"
LLM_EN = "Handelsbanken is a Swedish bank offering retail and corporate banking across the Nordics."
LLM_SV = "Handelsbanken aer en svensk bank med privat- och foeretagsbank i Norden."
DECISION_SV = "Svenska Handelsbanken aer en svensk fullservicebank."
DECISION_EN_2 = "Handelsbanken is a Swedish full-service bank."
WIKIDATA_EN_2 = "Swedish bank and financial services group"
BV_UID, SCB_UID, WD_UID = f"bv:{HB}", f"scb:{HB}", f"wikidata:{WIKIDATA_ID}"
ESEF_UID, DOMAIN_UID, FIN_UID = "esef:doc-hb-2023", "domain:handelsbanken.com:fp-1", f"bv-fin:{HB}:2024"
# se_code_labels rows for BOTH code systems the two registers use (000306): the registry
# ranks scb first for legal_form_code, so the published code is the SCB juridisk-form
# number and the projection's label join has to find it. Labels as the curated dictionary
# (sweden_company/translation.py) spells them, transliterated like every other Swedish
# string in this file.
LEGAL_FORM_LABELS = (
    ("AB-ORGFO", "Limited company (aktiebolag)", "Aktiebolag"),
    ("49", "Other limited company (oevriga aktiebolag)", "Oevriga aktiebolag"),
)
SCB_LEGAL_FORM = LEGAL_FORM_LABELS[1]


def _json(**members: object) -> str:
    return json.dumps(members, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


# (field, source, source_record_uid, value, value_json, observed_at) -- spec 5.1 / 4.2.
HB_CANDIDATES = (
    ("legal_name", "bolagsverket", BV_UID, "Svenska Handelsbanken AB", _json(compare_key="svenska handelsbanken ab"), T_REGISTER),
    ("legal_name", "scb", SCB_UID, "Svenska Handelsbanken AB", _json(compare_key="svenska handelsbanken ab"), T_REGISTER),
    ("legal_name", "wikidata", WD_UID, "Svenska Handelsbanken", _json(compare_key="svenska handelsbanken"), T_REGISTER),
    ("legal_form_code", "bolagsverket", BV_UID, "AB-ORGFO", _json(compare_key="ab-orgfo"), T_REGISTER),
    ("legal_form_code", "scb", SCB_UID, SCB_LEGAL_FORM[0], _json(compare_key="49"), T_REGISTER),
    ("status", "bolagsverket", BV_UID, "active", _json(compare_key="active"), T_REGISTER),
    ("status", "scb", SCB_UID, "active", _json(compare_key="active"), T_REGISTER),
    ("incorporation_date", "bolagsverket", BV_UID, "1955-06-14", _json(compare_key="1955-06-14"), T_REGISTER),
    ("incorporation_date", "scb", SCB_UID, "1955-06-14", _json(compare_key="1955-06-14"), T_REGISTER),
    ("description", "llm", str(SUGGESTION_ID), LLM_EN, _json(compare_key=LLM_EN.lower(), language="en"), T_LLM),
    ("description", "esef", ESEF_UID, "Handelsbanken is a Swedish credit institution.",
     _json(compare_key="handelsbanken is a swedish credit institution.", language="en"), T_ESEF),
    ("description", "wikidata", WD_UID, "Swedish bank", _json(compare_key="swedish bank", language="en"), T_REGISTER),
    ("description", "scb", SCB_UID, "Banking.", _json(compare_key="banking.", language="en"), T_REGISTER),
    ("description_sv", "llm", str(SUGGESTION_ID), LLM_SV, _json(compare_key=LLM_SV.lower(), language="sv"), T_LLM),
    ("description_sv", "scb", SCB_UID, "Bankverksamhet.", _json(compare_key="bankverksamhet.", language="sv"), T_REGISTER),
    # code_set / revision: the members plan 2's scb extractor puts beside the compare_key.
    ("primary_sni_code", "scb", SCB_UID, "64190", _json(compare_key="64190", code_set="SNI"), T_REGISTER),
    ("primary_nace_code", "scb", SCB_UID, "64.19", _json(compare_key="64.19", revision="NACE_REV_2"), T_REGISTER),
    ("industry_label_en", "scb", SCB_UID, "Other monetary intermediation", _json(compare_key="other monetary intermediation"), T_REGISTER),
    ("website", "domains", DOMAIN_UID, "https://www.handelsbanken.com", _json(compare_key="handelsbanken.com"), T_DOMAIN),
    ("website", "wikidata", WD_UID, "https://www.handelsbanken.se", _json(compare_key="handelsbanken.se"), T_REGISTER),
    ("employee_count", "bolagsverket", FIN_UID, "11000",
     _json(compare_key="11000", count=11000, as_of="2024-12-31", period="FY2024"), T_FINANCIAL),
    ("employee_count", "wikidata", WD_UID, "12000",
     _json(compare_key="12000", count=12000, as_of="2023-12-31", period="2023"), T_REGISTER),
    # Plan 2's shape (candidates/common.latest_revenue_json_sql / revenue_value_sql): the
    # display value is "<currency> <amount> FY<year>" and the amounts are JSON NUMBERS in
    # ClickHouse's own Decimal text -- the projection reads them with
    # toDecimal128OrNull(JSONExtractRaw(...)), which is NULL for a quoted number.
    ("latest_revenue", "bolagsverket", FIN_UID, "SEK 58000000000 FY2024",
     _json(compare_key="sek:58000000000:2024", amount=58000000000, currency="SEK",
           amount_usd=5500000000, fiscal_year=2024, period_end="2024-12-31"), T_FINANCIAL),
)
BETA_CANDIDATES = (
    ("legal_name", "wikidata", "wikidata:Q2", "Beta AB", _json(compare_key="beta ab"), T_REGISTER),
)
# Which source wins each field under the registry's precedence (spec 4.2); description_sv
# is decided by the reviewer and has no candidate winner. INFO_REGISTRY ranks scb FIRST
# for the four identity fields (registry.py: the SCB artifact is what the pilot
# published), so scb wins them here and the published legal form is its numeric code.
WINNERS = {
    "legal_name": "scb", "legal_form_code": "scb", "status": "scb",
    "incorporation_date": "scb", "description": "llm", "primary_sni_code": "scb",
    "primary_nace_code": "scb", "industry_label_en": "scb", "website": "domains",
    "employee_count": "bolagsverket", "latest_revenue": "bolagsverket",
}
WINNING_ROWS = tuple(row for row in HB_CANDIDATES if WINNERS.get(row[0]) == row[1])


def _evidence_hash(row: tuple) -> str:
    """The candidate table's MATERIALIZED evidence_hash (spec 5.1), recomputed in Python."""
    field, source, uid, value, value_json, _ = row
    return hashlib.sha256("\n".join((field, source, uid, value, value_json)).encode()).hexdigest()


EXPECTED_UIDS = {row[2] for row in WINNING_ROWS}
EXPECTED_HASHES = {_evidence_hash(row) for row in WINNING_ROWS}
WIDE_COLUMNS = tuple(c for c in declared_columns("se_company_info") if c != "evidence_set_hash")
EXPECTED_WIDE = {
    "company_id": HB, "legal_name": "Svenska Handelsbanken AB", "legal_form_code": SCB_LEGAL_FORM[0],
    "legal_form_label_en": SCB_LEGAL_FORM[1], "legal_form_label_sv": SCB_LEGAL_FORM[2],
    "status": "active", "incorporation_date": "1955-06-14",
    "description": LLM_EN, "description_sv": DECISION_SV, "description_language": "en", "llm_enhanced": "true",
    "description_source_count": "4", "primary_nace_code": "64.19", "primary_sni_code": "64190",
    "wikidata_id": WIKIDATA_ID, "lei": LEI,
    "suggestion_id": str(SUGGESTION_ID), "model_provider": "deepseek", "model_name": "deepseek-v4-flash",
    "prompt_version": "se-company-info-description-v3", "source_run_id": RUN_1,
    "resolved_at": "2026-09-02 10:00:00.000",
    "industry_label_en": "Other monetary intermediation", "website": "https://www.handelsbanken.com",
    "employee_count": "11000", "employee_count_as_of": "2024-12-31",
    "latest_revenue_currency": "SEK", "latest_revenue_fiscal_year": "2024",
}
EXPECTED_DECIMALS = {"latest_revenue_amount": Decimal("58000000000.00"), "latest_revenue_amount_usd": Decimal("5500000000.00")}
EXPECTED_SETS = {
    "description_sources": {"llm", "esef", "wikidata", "scb"},
    "description_source_record_uids": {str(SUGGESTION_ID), ESEF_UID, WD_UID, SCB_UID},
    "correction_ids": {str(DECISION_ID)},
    "source_record_uids": EXPECTED_UIDS,
    "evidence_hashes": EXPECTED_HASHES,
}
SCAN_LABELS = ("scan_never_published", "scan_settled_1", "scan_new_candidates", "scan_settled_2",
               "scan_decision_pending", "scan_settled_3", "scan_resolve_all", "scan_resolve_all_past_cutoff",
               "scan_version_changed")


def _schema_statements(migrations: tuple[str, ...]) -> list[str]:
    """CREATE/ALTER TABLE statements for NEEDED_TABLES only, in migration order."""
    statements: list[str] = []
    for name in migrations:
        text = (MIGRATIONS_DIR / name).read_text(encoding="utf-8")
        for raw in text.split(";"):
            statement = "\n".join(line for line in raw.splitlines() if not line.strip().startswith("--")).strip()
            if not statement:
                continue
            if statement.upper().startswith("CREATE DATABASE"):
                statements.append(statement)
                continue
            match = _TABLE_RE.match(statement)
            if match and match.group(1) in NEEDED_TABLES:
                statements.append(statement)
    return statements


def _string_array(values) -> str:
    return "[" + ", ".join(_literal(str(v)) for v in values) + "]"


def _param_text(value: object) -> str:
    """The text ClickHouse parses for a query parameter of the declared type."""
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    if isinstance(value, list | tuple):
        return "[" + ",".join("'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'" for v in value) + "]"
    return str(value)


def _param_literal(text: str) -> str:
    """``text`` as the string literal of a ``SET param_<name>``, backslash-escaped exactly
    ONCE: ClickHouse unescapes the literal and only then parses the result as the
    parameter's declared type, so the array quotes _param_text wrote must survive that one
    unescape (``_literal``'s doubled quotes on top of them would be a Code: 62)."""
    return "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _bound(sql: str, **values: object) -> str:
    """``sql`` preceded by one ``SET param_<name>`` per value: ClickHouse's own server-side
    binding, the path the backoffice's clickhouse-js query_params takes."""
    sets = "".join(f"SET param_{name} = {_param_literal(_param_text(value))};\n" for name, value in values.items())
    return sets + sql + ";\n"


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query} FORMAT TSV;\n"


def _marked_bound(label: str, sql: str, **values: object) -> str:
    return f"SELECT '@@{label}';\n" + _bound(sql + " FORMAT TSV", **values)


def _candidates_sql(company: str, rows: tuple, extracted_at: datetime) -> str:
    values = ",\n".join(
        f"('{company}', '{field}', '{source}', {_literal(uid)}, {_literal(value)}, {_literal(value_json)}, "
        f"{_literal(observed_at)}, {_literal(extracted_at)}, 'v1', 'extract-1')"
        for field, source, uid, value, value_json, observed_at in rows)
    return ("INSERT INTO corpscout.se_company_field_candidate (company_id, field, source, source_record_uid, value, "
            f"value_json, observed_at, extracted_at, extractor_version, source_run_id) VALUES\n{values};\n")


def _decision_sql(value_id: uuid.UUID, field: str, value: str, created_at: datetime) -> str:
    return ("INSERT INTO corpscout.se_company_info_field_value (value_id, company_id, field, value, source, source_ref, "
            f"source_at, decided_by, note, created_at) VALUES ('{value_id}', '{HB}', '{field}', {_literal(value)}, "
            f"'reviewer', '', NULL, 'backoffice', 'harness', {_literal(created_at)});\n")


def _registry_row(spec, *, policy_version: str, stamp: datetime) -> str:
    policy = policy_for(spec)
    return (f"('{INFO_REGISTRY.datatype}', '{INFO_REGISTRY.country}', '{spec.name}', '{spec.value_type}', "
            f"'{spec.display_group}', {_literal(spec.structured)}, {_literal(spec.python_only)}, "
            f"{_string_array(spec.sources)}, '{policy.name}', {_literal(policy_version)}, "
            f"{_literal(render_resolve_sql(INFO_REGISTRY, spec))}, '{INFO_REGISTRY.version}', {_literal(stamp)})")


REGISTRY_INSERT = ("INSERT INTO corpscout.se_company_field_registry (datatype, country, field, value_type, display_group, "
                   "structured, python_only, sources, policy_name, policy_version, resolve_sql, registry_version, version) VALUES\n")


def _registry_rows_sql() -> str:
    """What plan 1's export asset writes: one row per field plus the projection row."""
    rows = [_registry_row(spec, policy_version=policy_for(spec).version, stamp=T_REGISTRY) for spec in INFO_REGISTRY.fields]
    rows.append(f"('info', 'SE', '*', 'projection', '', 0, 0, [], '', '', "
                f"{_literal(render_projection_sql(INFO_REGISTRY))}, '{INFO_REGISTRY.version}', {_literal(T_REGISTRY)})")
    return REGISTRY_INSERT + ",\n".join(rows) + ";\n"


def _registry_bump_sql() -> str:
    """A newer export of legal_name under a bumped policy version: what a policy edit does."""
    spec = field_by_name(INFO_REGISTRY, "legal_name")
    return REGISTRY_INSERT + _registry_row(spec, policy_version=f"{policy_for(spec).name}-v2", stamp=T_REGISTRY_BUMP) + ";\n"


def _resolve_pass(*, run_id: str, resolved_at: datetime) -> str:
    """What one batch of the asset does for [HB]: every field's statement in registry
    order, then the projection -- the same text the registry rows above carry."""
    parts = [_bound(render_resolve_sql(INFO_REGISTRY, spec), field=spec.name, company_ids=[HB],
                    source_run_id=run_id, resolved_at=resolved_at)
             for spec in INFO_REGISTRY.fields]
    parts.append(_bound(render_projection_sql(INFO_REGISTRY), company_ids=[HB]))
    return "".join(parts)


def _scan(label: str, *, resolve_all: int = 0, resolve_all_before: str = NO_CUTOFF) -> str:
    return _marked_bound(label, build_changed_companies_sql(INFO_REGISTRY), company_ids=[], all_companies=1,
                         resolve_all=resolve_all, resolve_all_before=resolve_all_before, after_company_id="",
                         page_size=10)


def _old_row_values() -> str:
    """The pre-cutover publisher's row for HB, in the OLD insert order: every value the
    parity check must find equal, except primary_sni_code -- 64191 is the one deliberate
    mismatch, so the check is seen counting, not just passing."""
    return (f"'{HB}', 'Svenska Handelsbanken AB', '{SCB_LEGAL_FORM[0]}', {_literal(SCB_LEGAL_FORM[1])}, "
            f"{_literal(SCB_LEGAL_FORM[2])}, "
            f"'active', '1955-06-14', {_literal(LLM_EN)}, {_literal(DECISION_SV)}, 'en', true, "
            f"['esef', 'wikidata', 'scb'], ['{ESEF_UID}', '{WD_UID}', '{SCB_UID}'], 3, "
            f"'64.19', '64191', '{WIKIDATA_ID}', '{LEI}', ['{SCB_UID}'], ['{'a' * 64}'], ['{DECISION_ID}'], "
            f"'{SUGGESTION_ID}', 'deepseek', 'deepseek-v4-flash', 'se-company-info-description-v3', 'old-run', "
            f"{_literal(T_OLD_ROW)}")


WIDE_ROW_SQL = ("SELECT " + ", ".join(f"ifNull(toString({c}), '') AS {c}" for c in WIDE_COLUMNS)
                + f" FROM corpscout.se_company_info FINAL WHERE company_id = '{HB}'")
RESOLVED_ROWS_SQL = ("SELECT field, source, ifNull(toString(decision_id), ''), value, toString(candidate_count), "
                     "toString(arraySort(agreeing_sources)), policy_version, registry_version, source_run_id "
                     f"FROM corpscout.se_company_field FINAL WHERE company_id = '{HB}' ORDER BY field")

_CODE_LABEL_ROWS = ",\n".join(
    f"('legal_form', '{code}', {_literal(label_en)}, {_literal(label_sv)}, toDateTime('2026-08-01 00:00:00'))"
    for code, label_en, label_sv in LEGAL_FORM_LABELS)

FIXTURE = f"""
INSERT INTO corpscout.se_code_labels (code_type, code, label_en, label_sv, version)
VALUES
{_CODE_LABEL_ROWS};

INSERT INTO corpscout.se_company_info_wikidata
    (company_id, source_record_uid, observed_at, source_run_id, wikidata_id, wikidata_url, name)
VALUES ('{HB}', '{WD_UID}', {_literal(T_REGISTER)}, 'fixture', '{WIKIDATA_ID}',
        'https://www.wikidata.org/wiki/{WIKIDATA_ID}', 'Handelsbanken');

INSERT INTO corpscout.se_company_info_esef
    (company_id, source_record_uid, observed_at, source_run_id, source_document_id, lei, entity_name, fiscal_year,
     company_description, description_language, description_confidence, products_and_services_json, business_segments_json)
VALUES ('{HB}', '{ESEF_UID}', {_literal(T_ESEF)}, 'fixture', 'doc-hb-2023', '{LEI}', 'Svenska Handelsbanken AB', 2023,
        'Handelsbanken is a Swedish credit institution.', 'en', 0.9, '[]', '[]');

INSERT INTO corpscout.se_company_info_enrichment_observation
    (suggestion_id, company_id, input_hash, suggestion, raw_response, model_provider, model_name, prompt_version,
     prompt_tokens, completion_tokens, source_run_id, created_at)
VALUES ('{SUGGESTION_ID}', '{HB}', '{'f' * 64}',
        {_literal(_json(description=LLM_EN, description_sv=LLM_SV, language="en", rationale="merged"))},
        '', 'deepseek', 'deepseek-v4-flash', 'se-company-info-description-v3', 450, 450, 'llm-run', {_literal(T_LLM)});
""".strip() + "\n"


def _script(*, join_use_nulls: int) -> str:
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;\n")
    parts.append(";\n".join(_schema_statements((*BASE_MIGRATIONS, *LATER_MIGRATIONS))) + ";\n")
    parts.append(FIXTURE)
    parts.append(_candidates_sql(HB, HB_CANDIDATES, T_EXTRACT))
    parts.append(_candidates_sql(BETA, BETA_CANDIDATES, T_EXTRACT))
    parts.append(_decision_sql(DECISION_ID, "description_sv", DECISION_SV, T_DECISION))
    parts.append(_registry_rows_sql())

    # Nothing published: HB is selected for being new (and, by construction, for its
    # candidates and its decision being newer than the epoch); BETA never is.
    parts.append(_scan("scan_never_published"))

    # The old publisher's row and the parity snapshot taken from it, before the rebuild.
    parts.append(f"INSERT INTO corpscout.se_company_info ({', '.join(OLD_INSERT_COLUMNS)}) VALUES ({_old_row_values()});\n")
    parts.append(build_parity_snapshot_sql() + ";\n")

    # Pass 1: every field, then the projection -- the rebuild.
    parts.append(_resolve_pass(run_id=RUN_1, resolved_at=T_RESOLVE_1))
    parts.append(_marked("wide_row_1", WIDE_ROW_SQL))
    parts.append(_marked("resolved_rows_1", RESOLVED_ROWS_SQL))
    parts.append(_marked("parity_1", build_parity_sql()))
    parts.append(_marked("rows_per_field_source_1", build_rows_per_field_source_sql()))
    parts.append(_scan("scan_settled_1"))

    # A re-extracted candidate (newer extracted_at, new text) re-selects HB; a pass settles it.
    parts.append(_candidates_sql(HB, (("description", "wikidata", WD_UID, WIKIDATA_EN_2,
                                       _json(compare_key=WIKIDATA_EN_2.lower(), language="en"), T_EXTRACT_2),), T_EXTRACT_2))
    parts.append(_scan("scan_new_candidates"))
    parts.append(_resolve_pass(run_id=RUN_2, resolved_at=T_RESOLVE_2))
    parts.append(_scan("scan_settled_2"))

    # A decision after publication re-selects HB; the next pass publishes it and settles.
    parts.append(_decision_sql(DECISION_ID_2, "description", DECISION_EN_2, T_DECISION_2))
    parts.append(_scan("scan_decision_pending"))
    parts.append(_resolve_pass(run_id=RUN_3, resolved_at=T_RESOLVE_3))
    parts.append(_scan("scan_settled_3"))
    parts.append(_marked("wide_row_3", WIDE_ROW_SQL))

    # resolve_all re-selects a settled company -- unless its resolved_at is past the cutoff.
    parts.append(_scan("scan_resolve_all", resolve_all=1))
    parts.append(_scan("scan_resolve_all_past_cutoff", resolve_all=1, resolve_all_before=PAST_CUTOFF))

    # A policy version bump in the registry export re-selects every company resolved under the old one.
    parts.append(_registry_bump_sql())
    parts.append(_scan("scan_version_changed"))
    return "".join(parts)


# TSV escapes what a String value may not carry raw. An Array column is written by the
# array serializer (['a','b'], quotes raw), but toString() of one is a String, so its
# quotes arrive as \' -- both are read back through _unescape below.
_TSV_ESCAPES = {"\\": "\\", "'": "'", '"': '"', "b": "\b", "f": "\f", "n": "\n", "r": "\r", "t": "\t", "0": "\0"}


def _unescape(value: str) -> str:
    """One TSV field as ClickHouse escaped it."""
    out: list[str] = []
    index = 0
    while index < len(value):
        if value[index] == "\\" and index + 1 < len(value):
            following = value[index + 1]
            out.append(_TSV_ESCAPES.get(following, following))
            index += 2
            continue
        out.append(value[index])
        index += 1
    return "".join(out)


@pytest.fixture(scope="module", params=(0, 1), ids=("join_use_nulls_off", "join_use_nulls_on"))
def sections(request: pytest.FixtureRequest) -> dict[str, list[list[str]]]:
    command = _clickhouse_local_command()
    try:
        completed = subprocess.run(command, input=_script(join_use_nulls=request.param),
                                   capture_output=True, text=True, timeout=900)
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
            result[current].append([_unescape(value) for value in line.split("\t")])
    return result


def _flags(row: list[str]) -> tuple[bool, ...]:
    """The reason flags of one scan row, in SELECTION_COLUMNS order (UInt8 or Bool text)."""
    assert len(row) == len(SELECTION_COLUMNS)
    return tuple(value in ("1", "true") for value in row[1:])


def _scan_rows(sections: dict[str, list[list[str]]], label: str) -> dict[str, tuple[bool, ...]]:
    return {row[0]: _flags(row) for row in sections[label]}


def test_the_scan_selects_only_companies_with_a_register_name_and_names_the_reason(sections) -> None:
    """Reasons in SELECTION_REASONS order: never_published, new_candidates, decision_pending,
    version_changed. They overlap for a never-published company (its epoch resolved_at is
    older than everything). BETA has only a wikidata legal name and is never selected."""
    assert _scan_rows(sections, "scan_never_published") == {HB: (True, True, True, False)}
    assert _scan_rows(sections, "scan_settled_1") == {}
    assert _scan_rows(sections, "scan_new_candidates") == {HB: (False, True, False, False)}
    assert _scan_rows(sections, "scan_settled_2") == {}
    assert _scan_rows(sections, "scan_decision_pending") == {HB: (False, False, True, False)}
    assert _scan_rows(sections, "scan_settled_3") == {}
    for label in SCAN_LABELS:
        assert BETA not in _scan_rows(sections, label), label


def test_resolve_all_honours_its_cutoff(sections) -> None:
    assert _scan_rows(sections, "scan_resolve_all") == {HB: (False, False, False, False)}
    assert _scan_rows(sections, "scan_resolve_all_past_cutoff") == {}


def test_a_registry_policy_bump_re_selects_every_resolved_company(sections) -> None:
    assert _scan_rows(sections, "scan_version_changed") == {HB: (False, False, False, True)}


def test_the_resolved_rows_carry_winner_decision_agreement_and_versions(sections) -> None:
    rows = {row[0]: row for row in sections["resolved_rows_1"]}
    assert set(rows) == set(field_names(INFO_REGISTRY))
    for name, source in WINNERS.items():
        field, resolved_source, decision_id, value, *_ = rows[name]
        assert (resolved_source, decision_id) == (source, ""), name
        assert value == next(row[3] for row in WINNING_ROWS if row[0] == name), name
    # Rank order: scb beats bolagsverket and wikidata; the two registers agree on the name.
    assert rows["legal_name"][4:6] == ["3", "['bolagsverket','scb']"]
    assert rows["description"][4:6] == ["4", "['llm']"]
    assert rows["website"][4] == "2"
    # The decision beats the llm winner; its row names the decision and the reviewer.
    assert rows["description_sv"][1:4] == ["reviewer", str(DECISION_ID), DECISION_SV]
    # Every row is stamped with what resolved it.
    for name, row in rows.items():
        assert row[6] == policy_for(field_by_name(INFO_REGISTRY, name)).version, name
        assert row[7] == INFO_REGISTRY.version and row[8] == RUN_1, name


def test_the_wide_row_equals_the_expected_handelsbanken_row(sections) -> None:
    [values] = sections["wide_row_1"]
    row = dict(zip(WIDE_COLUMNS, values, strict=True))
    for column, expected in EXPECTED_WIDE.items():
        assert row[column] == expected, column
    for column, expected in EXPECTED_DECIMALS.items():
        assert Decimal(row[column]) == expected, column
    for column, expected in EXPECTED_SETS.items():
        assert set(ast.literal_eval(row[column])) - {""} == expected, column
    # Every deployed column has an expectation above: a new column cannot slip in unasserted.
    assert set(WIDE_COLUMNS) == set(EXPECTED_WIDE) | set(EXPECTED_DECIMALS) | set(EXPECTED_SETS)


def test_a_decision_on_the_description_replaces_the_model_text_and_its_provenance(sections) -> None:
    [values] = sections["wide_row_3"]
    row = dict(zip(WIDE_COLUMNS, values, strict=True))
    assert row["description"] == DECISION_EN_2 and row["description_sv"] == DECISION_SV
    assert row["llm_enhanced"] == "false" and row["suggestion_id"] == ""
    assert row["description_language"] == ""  # a decided description carries no value_json (fields/sql.py reads JSONExtractString(value_json, 'language'))
    # Not from the llm source, so the model columns are fields/sql.py's deterministic constants.
    assert row["model_provider"] == "deterministic" and row["model_name"] == "se-company-info-rules"
    assert row["prompt_version"] == "se-company-info-rules-v1"
    assert set(ast.literal_eval(row["correction_ids"])) == {str(DECISION_ID), str(DECISION_ID_2)}
    assert row["source_run_id"] == RUN_3 and row["resolved_at"] == "2026-09-04 10:00:00.000"
    # The re-extracted wikidata text is a candidate (counted), never the published one.
    assert row["description_source_count"] == "4"


def test_the_parity_check_reports_per_column_mismatches(sections) -> None:
    [values] = sections["parity_1"]
    named = dict(zip(PARITY_COLUMNS, values, strict=True))
    counts = {name: int(named[name]) for name in PARITY_COLUMNS if not name.endswith("_samples")}
    expected = dict.fromkeys(counts, 0)
    expected.update({"companies_compared": 1, "primary_sni_code": 1})
    assert counts == expected
    assert ast.literal_eval(named["primary_sni_code_samples"]) == [HB]
    assert all(ast.literal_eval(named[name]) == [] for name in PARITY_COLUMNS
               if name.endswith("_samples") and name != "primary_sni_code_samples")
    per_source = [(field, source, int(rows)) for field, source, rows in sections["rows_per_field_source_1"]]
    assert per_source == sorted([*((field, source, 1) for field, source in WINNERS.items()), ("description_sv", "reviewer", 1)])
