"""The registry's generated SQL, pinned as text here and executed against real DDL in the
clickhouse-local harness further down (spec 7.4, 8.3)."""

import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from dagster_v3.defs.se_company.fields.registry import INFO_REGISTRY, field_by_name, field_names
from dagster_v3.defs.se_company.fields.sql import (
    array_literal,
    rank_sql,
    render_projection_select_sql,
    render_projection_sql,
    render_resolve_sql,
    sql_string,
)
from dagster_v3.defs.se_company.fields.tables import SE_COMPANY_INFO_COLUMNS
from tests.se_company_ddl import MIGRATIONS_DIR, projection_aliases
from tests.test_se_company_person_clickhouse_local import _clickhouse_local_command, _literal

LEGAL_NAME_RESOLVE_SQL = """INSERT INTO corpscout.se_company_field
    (company_id, field, value, value_json, source, source_record_uid, observed_at, decision_id, policy_name, policy_version, candidate_count, agreeing_sources, registry_version, source_run_id, resolved_at)
WITH
    candidates AS (
        SELECT c.company_id AS company_id, c.source AS source, c.source_record_uid AS source_record_uid,
               c.value AS value, c.value_json AS value_json, c.observed_at AS observed_at
        FROM corpscout.se_company_field_candidate AS c FINAL
        WHERE c.field = {field:String} AND c.company_id IN {company_ids:Array(String)}
    ),
    eligible AS (
        SELECT c.company_id AS company_id, c.source AS source, c.source_record_uid AS source_record_uid,
               c.value AS value, c.value_json AS value_json, c.observed_at AS observed_at,
               indexOf(['scb', 'bolagsverket', 'wikidata'], c.source) AS rank,
               if(JSONHas(c.value_json, 'compare_key'), JSONExtractString(c.value_json, 'compare_key'), lowerUTF8(trim(c.value))) AS compare_key
        FROM candidates AS c
        WHERE has(['scb', 'bolagsverket', 'wikidata'], c.source) AND (c.value IS NOT NULL AND trim(c.value) != '')
    ),
    agreement AS (
        SELECT company_id, compare_key, arraySort(groupUniqArray(source)) AS agreeing_sources
        FROM eligible GROUP BY company_id, compare_key
    ),
    counted AS (
        SELECT company_id, toUInt16(count()) AS candidate_count FROM eligible GROUP BY company_id
    ),
    winner AS (
        SELECT c.* FROM eligible AS c
        ORDER BY c.company_id, rank ASC, c.observed_at DESC, c.source_record_uid DESC
        LIMIT 1 BY c.company_id
    ),
    decision AS (
        SELECT company_id,
               argMax((value, source, source_ref, source_at, created_at, value_id),
                      (created_at, toString(value_id))) AS latest
        FROM corpscout.se_company_info_field_value
        WHERE field = {field:String} AND company_id IN {company_ids:Array(String)}
        GROUP BY company_id
    ),
    live AS (
        SELECT company_id, assumeNotNull(tupleElement(latest, 1)) AS value,
               tupleElement(latest, 2) AS source, tupleElement(latest, 3) AS source_ref,
               ifNull(tupleElement(latest, 4), tupleElement(latest, 5)) AS observed_at,
               tupleElement(latest, 6) AS decision_id
        FROM decision WHERE tupleElement(latest, 1) IS NOT NULL
    ),
    decided AS (
        SELECT c.company_id AS company_id, c.value AS value, c.value_json AS value_json,
               c.source AS source, c.source_record_uid AS source_record_uid,
               c.observed_at AS observed_at, c.decision_id AS decision_id,
               if(JSONHas(c.value_json, 'compare_key'), JSONExtractString(c.value_json, 'compare_key'), lowerUTF8(trim(c.value))) AS compare_key
        FROM (
            SELECT l.company_id AS company_id, l.value AS value, ifNull(k.value_json, '') AS value_json,
                   l.source AS source, l.source_ref AS source_record_uid,
                   l.observed_at AS observed_at, l.decision_id AS decision_id
            FROM live AS l
            LEFT JOIN candidates AS k
                ON k.company_id = l.company_id AND k.source = l.source AND k.source_record_uid = l.source_ref
        ) AS c
    )
SELECT
    d.company_id AS company_id, {field:String} AS field, d.value AS value, d.value_json AS value_json,
    d.source AS source, d.source_record_uid AS source_record_uid, d.observed_at AS observed_at,
    toNullable(d.decision_id) AS decision_id,
    'source_precedence' AS policy_name, 'source_precedence-v1' AS policy_version,
    ifNull(n.candidate_count, toUInt16(0)) AS candidate_count, a.agreeing_sources AS agreeing_sources,
    'se-info-v1' AS registry_version, {source_run_id:String} AS source_run_id,
    {resolved_at:DateTime64(3)} AS resolved_at
FROM decided AS d
LEFT JOIN counted AS n ON n.company_id = d.company_id
LEFT JOIN agreement AS a ON a.company_id = d.company_id AND a.compare_key = d.compare_key
UNION ALL
SELECT
    w.company_id AS company_id, {field:String} AS field, w.value AS value, w.value_json AS value_json,
    w.source AS source, w.source_record_uid AS source_record_uid, w.observed_at AS observed_at,
    CAST(NULL AS Nullable(UUID)) AS decision_id,
    'source_precedence' AS policy_name, 'source_precedence-v1' AS policy_version,
    ifNull(n.candidate_count, toUInt16(0)) AS candidate_count, a.agreeing_sources AS agreeing_sources,
    'se-info-v1' AS registry_version, {source_run_id:String} AS source_run_id,
    {resolved_at:DateTime64(3)} AS resolved_at
FROM winner AS w
LEFT JOIN counted AS n ON n.company_id = w.company_id
LEFT JOIN agreement AS a ON a.company_id = w.company_id AND a.compare_key = w.compare_key
WHERE w.company_id NOT IN (SELECT company_id FROM decided)"""


def test_literals() -> None:
    assert sql_string("it's") == "'it\\'s'"
    assert array_literal(("scb", "bolagsverket")) == "['scb', 'bolagsverket']"
    assert array_literal(()) == "[]"
    assert rank_sql(field_by_name(INFO_REGISTRY, "website")) == "indexOf(['domains', 'wikidata'], c.source)"


def test_the_legal_name_resolve_statement_is_pinned_as_text() -> None:
    assert render_resolve_sql(INFO_REGISTRY, field_by_name(INFO_REGISTRY, "legal_name")) == LEGAL_NAME_RESOLVE_SQL


def test_every_field_renders_its_own_precedence_and_the_same_parameters() -> None:
    for field in INFO_REGISTRY.fields:
        sql = render_resolve_sql(INFO_REGISTRY, field)
        assert f"indexOf({array_literal(field.sources)}, c.source) AS rank" in sql
        assert f"WHERE has({array_literal(field.sources)}, c.source) AND (" in sql
        for parameter in ("{field:String}", "{company_ids:Array(String)}", "{source_run_id:String}",
                          "{resolved_at:DateTime64(3)}"):
            assert parameter in sql
        assert "%(" not in sql  # server-side parameters only, never driver-side ones
        assert "'se-info-v1' AS registry_version" in sql
        # The release path: the decision CTE aggregates a tuple, never the bare Nullable value.
        assert "argMax(value," not in sql


def test_the_projection_inserts_every_wide_column_in_ddl_order() -> None:
    select = render_projection_select_sql(INFO_REGISTRY)
    assert projection_aliases(select) == list(SE_COMPANY_INFO_COLUMNS)
    statement = render_projection_sql(INFO_REGISTRY)
    assert statement.startswith(
        "INSERT INTO corpscout.se_company_info\n    (" + ", ".join(SE_COMPANY_INFO_COLUMNS) + ")\nWITH\n"
    )
    assert statement.endswith(select)
    assert statement.count("{company_ids:Array(String)}") == 7 and "{field:String}" not in statement


def test_the_projection_reads_every_registry_field_by_name() -> None:
    """The pivot is hand-written per wide column, so a field added to the registry must be
    wired into it (or deliberately left out of the wide row) -- this makes that a decision
    rather than an omission. json fields are read through their value_json members."""
    select = render_projection_select_sql(INFO_REGISTRY)
    for name in field_names(INFO_REGISTRY):
        assert f"field = '{name}'" in select, name
    assert f"has({array_literal(field_names(INFO_REGISTRY))}, f.field)" in select
    for member in ("'count'", "'as_of'", "'amount'", "'currency'", "'amount_usd'", "'fiscal_year'", "'language'"):
        assert member in select
    # spec 8.3: the SCB/Bolagsverket legal-name gate, the label lookup, the kept id columns.
    assert "WHERE field = 'legal_name' AND source IN ('scb', 'bolagsverket')" in select
    assert "FROM corpscout.se_code_labels WHERE code_type = 'legal_form'" in select
    assert "FROM corpscout.se_company_info_wikidata" in select and "FROM corpscout.se_company_info_esef" in select
    assert "FROM corpscout.se_company_info_enrichment_observation" in select


# --------------------------------------------------------------------------------------
# clickhouse-local harness: the statements above, executed against the migrations' DDL
# with seeded candidates, decisions, labels, artifacts and one observation. Runs twice,
# with and without join_use_nulls, like the info harness: every LEFT JOIN miss must read
# the same under both.

# Every migration that creates or alters one of NEEDED_TABLES, in ledger order. GRANT
# statements and statements aimed at other tables are filtered out per statement.
NEEDED_MIGRATIONS = (
    "000150_corpscout_se_translations.up.sql",
    "000297_corpscout_se_company_info.up.sql",
    "000299_corpscout_se_company_info_sole_traders.up.sql",
    "000301_corpscout_se_company_info_description_sv.up.sql",
    "000304_corpscout_se_company_info_llm_enhanced.up.sql",
    "000305_corpscout_se_code_labels_swedish.up.sql",
    "000306_corpscout_se_company_info_legal_form_label.up.sql",
    "000365_corpscout_se_company_info_esef_enrichment.up.sql",
    "000371_corpscout_se_company_info_field_value.up.sql",
    "000373_corpscout_se_company_field_tables.up.sql",
    "000374_corpscout_se_company_info_field_columns.up.sql",
    "000375_corpscout_se_company_info_field_value_registry_checks.up.sql",
)
NEEDED_TABLES = frozenset({
    "se_code_labels", "se_company_info", "se_company_info_esef", "se_company_info_wikidata",
    "se_company_info_enrichment_observation", "se_company_info_field_value",
    "se_company_field_registry", "se_company_field_candidate", "se_company_field",
})
_TABLE_RE = re.compile(r"^(?:CREATE TABLE(?: IF NOT EXISTS)?|ALTER TABLE)\s+corpscout\.(\w+)", re.IGNORECASE)
_PARAMETER_RE = re.compile(r"\{([a-z_]+):[A-Za-z0-9(), ]+\}")


def _schema_statements(migrations: tuple[str, ...]) -> list[str]:
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


def render_named(sql: str, parameters: dict[str, Any]) -> str:
    """Inline the server-side ``{name:Type}`` placeholders for the CLI. clickhouse-local
    takes --param_name flags, but one script here runs a statement with several parameter
    sets, so the literals are substituted in the text instead."""

    def literal(match: re.Match[str]) -> str:
        value = parameters[match.group(1)]
        if isinstance(value, tuple | list):
            return "[" + ", ".join(_literal(item) for item in value) + "]"
        return _literal(value)

    rendered = _PARAMETER_RE.sub(literal, sql)
    assert not _PARAMETER_RE.search(rendered), rendered
    return rendered


ALPHA = "5565200028"    # the full row: every field, two decisions, all artifacts
BETA = "5560125220"     # release: an older decision then a NULL row; two scb records
EPSILON = "5567654321"  # precedence over recency; an unlisted source; an llm description
GAMMA = "5569999991"    # legal name from wikidata only: resolved, never published
NOBODY = "5560000010"   # in the company set, no candidates, no decisions
COMPANIES = (ALPHA, BETA, EPSILON, GAMMA, NOBODY)
RUN_ID = "fixture-run-1"
T_RESOLVED = datetime(2026, 9, 2, 12, 0, tzinfo=UTC)
T_RESOLVED_2 = datetime(2026, 9, 2, 12, 5, tzinfo=UTC)
D_ALPHA_NAME = "11111111-1111-1111-1111-111111111111"
D_ALPHA_EMPLOYEES = "22222222-2222-2222-2222-222222222222"
D_BETA_OLD = "33333333-3333-3333-3333-333333333333"
D_BETA_RELEASE = "44444444-4444-4444-4444-444444444444"
SUGGESTION = "55555555-5555-5555-5555-555555555555"
D_EPSILON_NAME = "66666666-6666-6666-6666-666666666666"

FIXTURE = f"""
INSERT INTO corpscout.se_company_field_candidate
    (company_id, field, source, source_record_uid, value, value_json, observed_at, extracted_at, extractor_version, source_run_id)
VALUES
    ('{ALPHA}', 'legal_name', 'scb', 'scb-a', 'Alpha AB', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'legal_name', 'wikidata', 'wikidata:Q1', 'Alpha', '', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'legal_name', 'bolagsverket', 'bv-a', 'Alpha Aktiebolag', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'legal_form_code', 'bolagsverket', 'bv-a', 'AB-ORGFO', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'status', 'bolagsverket', 'bv-a', 'active', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'incorporation_date', 'bolagsverket', 'bv-a', '2001-02-03', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'description', 'esef', 'doc-1', 'Alpha builds payment software.', '{{"language":"en"}}', '2025-04-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'description', 'wikidata', 'wikidata:Q1', 'alpha builds payment software.', '{{"language":"en"}}', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'description', 'scb', 'scb-a', 'IT consultants.', '{{"language":"en"}}', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'description_sv', 'scb', 'scb-a', 'IT-konsulter.', '{{"language":"sv"}}', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'primary_sni_code', 'scb', 'scb-a', '62010', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'primary_nace_code', 'scb', 'scb-a', '6201', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'industry_label_en', 'scb', 'scb-a', 'Computer programming activities', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'website', 'domains', 'fp-1', 'https://alpha.example', '', '2026-08-03 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'website', 'wikidata', 'wikidata:Q1', 'http://alpha.example/', '', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'employee_count', 'esef', 'doc-1', '1 200', '{{"compare_key":"1200","count":1200,"as_of":"2024-12-31","period":"FY2024"}}', '2024-12-31 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'employee_count', 'wikidata', 'wikidata:Q1', '1150', '{{"compare_key":"1150","count":1150}}', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{ALPHA}', 'latest_revenue', 'esef', 'doc-1', 'SEK 123,456,789.50', '{{"compare_key":"SEK:2024:123456789.50","amount":123456789.5,"currency":"SEK","amount_usd":11500000.25,"fiscal_year":2024,"period_end":"2024-12-31"}}', '2024-12-31 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{BETA}', 'legal_name', 'scb', 'scb-b1', 'Beta AB', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{BETA}', 'legal_name', 'scb', 'scb-b2', 'Beta Holding AB', '', '2026-08-05 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'legal_name', 'bolagsverket', 'bv-e', 'Epsilon AB', '', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'legal_name', 'scb', 'scb-e', 'Epsilon Aktiebolag', '', '2026-07-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'legal_name', 'ratsit', 'ratsit-e', 'Epsilon (ratsit)', '', '2026-08-30 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'description', 'llm', '{SUGGESTION}', 'Epsilon makes widgets.', '{{"language":"en"}}', '2026-08-10 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{EPSILON}', 'description', 'scb', 'scb-e', 'Widget maker.', '{{"language":"en"}}', '2026-08-01 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{GAMMA}', 'legal_name', 'wikidata', 'wikidata:Q9', 'Gamma', '', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}'),
    ('{GAMMA}', 'status', 'ratsit', 'ratsit-g', 'active', '', '2026-08-02 00:00:00', '2026-08-20 00:00:00', 'v1', '{RUN_ID}');

INSERT INTO corpscout.se_company_info_field_value
    (value_id, company_id, field, value, source, source_ref, source_at, decided_by, note, created_at)
VALUES
    ('{D_ALPHA_NAME}', '{ALPHA}', 'legal_name', 'Alpha Group AB', 'reviewer', '', NULL, 'backoffice', '', '2026-08-25 10:00:00'),
    ('{D_ALPHA_EMPLOYEES}', '{ALPHA}', 'employee_count', '1150', 'wikidata', 'wikidata:Q1', '2026-08-02 00:00:00', 'backoffice', '', '2026-08-25 10:00:00'),
    ('{D_BETA_OLD}', '{BETA}', 'legal_name', 'Beta Corp', 'reviewer', '', NULL, 'backoffice', '', '2026-08-10 10:00:00'),
    ('{D_BETA_RELEASE}', '{BETA}', 'legal_name', NULL, 'reviewer', '', NULL, 'backoffice', '', '2026-08-11 10:00:00');

INSERT INTO corpscout.se_code_labels (code_type, code, label_en, label_sv, version)
VALUES ('legal_form', 'AB-ORGFO', 'Limited company (aktiebolag)', 'Aktiebolag', toDateTime('2026-08-02 00:00:00'));

INSERT INTO corpscout.se_company_info_wikidata
    (company_id, source_record_uid, observed_at, source_run_id, wikidata_id, wikidata_url, name)
VALUES ('{ALPHA}', 'wikidata:Q1', '2026-08-02 00:00:00', '{RUN_ID}', 'Q1', 'https://www.wikidata.org/wiki/Q1', 'Alpha');

INSERT INTO corpscout.se_company_info_esef
    (company_id, source_record_uid, observed_at, source_run_id, source_document_id, lei, fiscal_year,
     company_description, description_language, description_confidence)
VALUES ('{ALPHA}', 'doc-1', '2025-04-02 00:00:00', '{RUN_ID}', 'doc-1', '5493001KJTIIGC8Y1R12', 2024,
        'Alpha builds payment software.', 'en', 0.9);

INSERT INTO corpscout.se_company_info_enrichment_observation
    (suggestion_id, company_id, input_hash, suggestion, raw_response, model_provider, model_name, prompt_version,
     prompt_tokens, completion_tokens, source_run_id, created_at)
VALUES ('{SUGGESTION}', '{EPSILON}', repeat('0', 64), '{{}}', '', 'deepseek', 'deepseek-v4-flash',
        'se-company-info-description-v3', 1, 1, '{RUN_ID}', '2026-08-10 00:00:00');
""".strip()

# The backoffice sequence: a new decision, the one-field resolve for that company alone,
# then the projection for that company alone.
EPSILON_DECISION_SQL = f"""
INSERT INTO corpscout.se_company_info_field_value
    (value_id, company_id, field, value, source, source_ref, source_at, decided_by, note, created_at)
VALUES ('{D_EPSILON_NAME}', '{EPSILON}', 'legal_name', 'Epsilon Group AB', 'reviewer', '', NULL, 'backoffice', '', '2026-08-26 10:00:00');
""".strip()

RESOLVED_COLUMNS = (
    "company_id", "field", "value", "source", "source_record_uid", "observed_at", "decision_id",
    "candidate_count", "agreeing_sources", "value_json", "policy_name", "policy_version",
    "registry_version", "source_run_id", "resolved_at",
)
RESOLVED_SQL = (
    "SELECT company_id, field, value, source, source_record_uid, toString(observed_at), "
    "ifNull(toString(decision_id), ''), candidate_count, agreeing_sources, value_json, policy_name, "
    "policy_version, registry_version, source_run_id, toString(resolved_at) "
    "FROM corpscout.se_company_field FINAL ORDER BY company_id, field"
)
# Arrays are selected bare (TSV renders them as ['a','b']); Nullables through ifNull(toString()).
WIDE_COLUMNS = (
    "company_id", "legal_name", "legal_form_code", "legal_form_label_en", "legal_form_label_sv", "status",
    "incorporation_date", "description", "description_sv", "description_language", "llm_enhanced",
    "description_sources", "description_source_record_uids", "description_source_count",
    "primary_nace_code", "primary_sni_code", "industry_label_en", "website", "employee_count",
    "employee_count_as_of", "latest_revenue_amount", "latest_revenue_currency", "latest_revenue_amount_usd",
    "latest_revenue_fiscal_year", "wikidata_id", "lei", "source_record_uids", "evidence_hash_count",
    "correction_ids", "suggestion_id", "model_provider", "model_name", "prompt_version", "source_run_id",
    "resolved_at",
)
WIDE_SQL = (
    "SELECT company_id, legal_name, ifNull(legal_form_code, ''), legal_form_label_en, legal_form_label_sv, status, "
    "ifNull(toString(incorporation_date), ''), ifNull(description, ''), ifNull(description_sv, ''), "
    "description_language, toUInt8(llm_enhanced), description_sources, description_source_record_uids, "
    "description_source_count, primary_nace_code, primary_sni_code, industry_label_en, ifNull(website, ''), "
    "ifNull(toString(employee_count), ''), ifNull(toString(employee_count_as_of), ''), "
    "ifNull(toString(toFloat64(latest_revenue_amount)), ''), latest_revenue_currency, "
    "ifNull(toString(toFloat64(latest_revenue_amount_usd)), ''), ifNull(toString(latest_revenue_fiscal_year), ''), "
    "ifNull(wikidata_id, ''), ifNull(lei, ''), source_record_uids, length(evidence_hashes), correction_ids, "
    "ifNull(toString(suggestion_id), ''), model_provider, model_name, prompt_version, source_run_id, "
    "toString(resolved_at) FROM corpscout.se_company_info FINAL ORDER BY company_id"
)


def _marked(label: str, query: str) -> str:
    return f"SELECT '@@{label}';\n{query} FORMAT TSV;\n"


def _script(*, join_use_nulls: int) -> str:
    parts: list[str] = []
    if join_use_nulls:
        parts.append("SET join_use_nulls = 1;")
    parts.append(";\n".join(_schema_statements(NEEDED_MIGRATIONS)) + ";")
    parts.append(FIXTURE)
    params = {"company_ids": COMPANIES, "source_run_id": RUN_ID, "resolved_at": T_RESOLVED}
    for field in INFO_REGISTRY.fields:
        parts.append(render_named(render_resolve_sql(INFO_REGISTRY, field), {**params, "field": field.name}) + ";")
    parts.append(_marked("resolved", RESOLVED_SQL))
    parts.append(render_named(render_projection_sql(INFO_REGISTRY), {"company_ids": COMPANIES}) + ";")
    parts.append(_marked("wide", WIDE_SQL))
    parts.append(_marked(
        "wide_columns",
        "SELECT name FROM system.columns WHERE database = 'corpscout' AND table = 'se_company_info' ORDER BY position",
    ))
    parts.append(EPSILON_DECISION_SQL)
    legal_name = field_by_name(INFO_REGISTRY, "legal_name")
    parts.append(render_named(render_resolve_sql(INFO_REGISTRY, legal_name), {
        "company_ids": (EPSILON,), "source_run_id": "backoffice-1", "resolved_at": T_RESOLVED_2,
        "field": "legal_name"}) + ";")
    parts.append(_marked("resolved_after_decision", RESOLVED_SQL))
    parts.append(render_named(render_projection_sql(INFO_REGISTRY), {"company_ids": (EPSILON,)}) + ";")
    parts.append(_marked("wide_after_decision", WIDE_SQL))
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


def _resolved(rows: list[list[str]]) -> dict[tuple[str, str], dict[str, str]]:
    return {(row[0], row[1]): dict(zip(RESOLVED_COLUMNS, row, strict=True)) for row in rows}


def _wide(rows: list[list[str]]) -> dict[str, dict[str, str]]:
    return {row[0]: dict(zip(WIDE_COLUMNS, row, strict=True)) for row in rows}


@pytest.mark.integration
def test_a_decision_beats_the_winner(sections: dict[str, list[list[str]]]) -> None:
    row = _resolved(sections["resolved"])[(ALPHA, "legal_name")]
    assert (row["value"], row["source"], row["source_record_uid"]) == ("Alpha Group AB", "reviewer", "")
    assert row["decision_id"] == D_ALPHA_NAME
    assert row["observed_at"] == "2026-08-25 10:00:00.000"  # source_at NULL -> the decision's created_at
    # The three eligible candidates are still counted; a typed value agrees with none of them.
    assert (row["candidate_count"], row["agreeing_sources"], row["value_json"]) == ("3", "[]", "")


@pytest.mark.integration
def test_a_released_decision_falls_back_to_the_winner(sections: dict[str, list[list[str]]]) -> None:
    """BETA's newest decision row is a release (value NULL) written after a value row. A
    bare argMax(value, ...) would skip the NULL row and resurrect 'Beta Corp'; the tuple
    aggregation keeps it, so the winner is used -- and among BETA's two scb records the
    newer observation wins (same-source recency)."""
    row = _resolved(sections["resolved"])[(BETA, "legal_name")]
    assert (row["value"], row["source"], row["source_record_uid"]) == ("Beta Holding AB", "scb", "scb-b2")
    assert row["decision_id"] == "" and row["observed_at"] == "2026-08-05 00:00:00.000"
    assert (row["candidate_count"], row["agreeing_sources"]) == ("2", "['scb']")


@pytest.mark.integration
def test_precedence_beats_recency_and_unlisted_sources_are_ignored(sections: dict[str, list[list[str]]]) -> None:
    row = _resolved(sections["resolved"])[(EPSILON, "legal_name")]
    # scb is rank 1 although bolagsverket observed a month later; ratsit is not in the tuple.
    assert (row["value"], row["source"], row["source_record_uid"]) == ("Epsilon Aktiebolag", "scb", "scb-e")
    assert (row["candidate_count"], row["agreeing_sources"]) == ("2", "['scb']")
    # GAMMA's only status candidate is from ratsit, which status does not list: no row.
    assert (GAMMA, "status") not in _resolved(sections["resolved"])


@pytest.mark.integration
def test_agreement_is_counted_on_the_compare_key(sections: dict[str, list[list[str]]]) -> None:
    resolved = _resolved(sections["resolved"])
    description = resolved[(ALPHA, "description")]
    # llm is absent, so esef (rank 2) wins; wikidata's text differs only in case -> agrees.
    assert (description["value"], description["source"]) == ("Alpha builds payment software.", "esef")
    assert (description["candidate_count"], description["agreeing_sources"]) == ("3", "['esef','wikidata']")
    assert description["value_json"] == '{"language":"en"}'
    employees = resolved[(ALPHA, "employee_count")]
    # A decision that copied the wikidata candidate carries that candidate's value_json, and
    # its compare_key ('1150') agrees with wikidata alone.
    assert (employees["value"], employees["source"], employees["decision_id"]) == ("1150", "wikidata", D_ALPHA_EMPLOYEES)
    assert employees["value_json"] == '{"compare_key":"1150","count":1150}'
    assert (employees["candidate_count"], employees["agreeing_sources"]) == ("2", "['wikidata']")


@pytest.mark.integration
def test_no_row_when_nothing_and_every_row_carries_provenance(sections: dict[str, list[list[str]]]) -> None:
    resolved = _resolved(sections["resolved"])
    assert not [key for key in resolved if key[0] == NOBODY]
    assert len(resolved) == 16  # ALPHA 12 fields + BETA 1 + EPSILON 2 + GAMMA 1
    for row in resolved.values():
        assert (row["policy_name"], row["policy_version"], row["registry_version"]) == (
            "source_precedence", "source_precedence-v1", "se-info-v1")
        assert (row["source_run_id"], row["resolved_at"]) == (RUN_ID, "2026-09-02 12:00:00.000")


@pytest.mark.integration
def test_the_projection_builds_the_expected_wide_rows(sections: dict[str, list[list[str]]]) -> None:
    wide = _wide(sections["wide"])
    assert set(wide) == {ALPHA, BETA, EPSILON}  # GAMMA: no scb/bolagsverket legal name; NOBODY: nothing
    assert wide[ALPHA] == {
        "company_id": ALPHA, "legal_name": "Alpha Group AB", "legal_form_code": "AB-ORGFO",
        "legal_form_label_en": "Limited company (aktiebolag)", "legal_form_label_sv": "Aktiebolag",
        "status": "active", "incorporation_date": "2001-02-03",
        "description": "Alpha builds payment software.", "description_sv": "IT-konsulter.",
        "description_language": "en", "llm_enhanced": "0",
        "description_sources": "['esef','scb','wikidata']",
        "description_source_record_uids": "['doc-1','scb-a','wikidata:Q1']", "description_source_count": "3",
        "primary_nace_code": "6201", "primary_sni_code": "62010",
        "industry_label_en": "Computer programming activities", "website": "https://alpha.example",
        "employee_count": "1150", "employee_count_as_of": "",  # the decided wikidata value has no as_of
        "latest_revenue_amount": "123456789.5", "latest_revenue_currency": "SEK",
        "latest_revenue_amount_usd": "11500000.25", "latest_revenue_fiscal_year": "2024",
        "wikidata_id": "Q1", "lei": "5493001KJTIIGC8Y1R12",
        "source_record_uids": "['bv-a','doc-1','fp-1','scb-a','wikidata:Q1']",  # unique, no reviewer ''
        "evidence_hash_count": "11",  # one per winning candidate row: bv-a x3, doc-1 x2, fp-1, scb-a x4, Q1
        "correction_ids": f"['{D_ALPHA_NAME}','{D_ALPHA_EMPLOYEES}']",
        "suggestion_id": "", "model_provider": "deterministic", "model_name": "se-company-info-rules",
        "prompt_version": "se-company-info-rules-v1", "source_run_id": RUN_ID,
        "resolved_at": "2026-09-02 12:00:00.000",
    }
    epsilon = wide[EPSILON]
    assert (epsilon["legal_name"], epsilon["description"], epsilon["llm_enhanced"]) == ("Epsilon Aktiebolag", "Epsilon makes widgets.", "1")
    assert (epsilon["suggestion_id"], epsilon["model_provider"], epsilon["model_name"], epsilon["prompt_version"]) == (
        SUGGESTION, "deepseek", "deepseek-v4-flash", "se-company-info-description-v3")
    assert (epsilon["description_sources"], epsilon["description_source_count"]) == ("['llm','scb']", "2")
    assert (epsilon["source_record_uids"], epsilon["evidence_hash_count"]) == (f"['{SUGGESTION}','scb-e']", "2")
    beta = wide[BETA]
    assert (beta["legal_name"], beta["legal_form_code"], beta["status"], beta["description"]) == ("Beta Holding AB", "", "", "")
    assert (beta["description_sources"], beta["description_source_count"], beta["correction_ids"]) == ("[]", "0", "[]")
    assert (beta["employee_count"], beta["latest_revenue_amount"], beta["website"]) == ("", "", "")
    assert (beta["source_record_uids"], beta["evidence_hash_count"]) == ("['scb-b2']", "1")


@pytest.mark.integration
def test_the_deployed_wide_columns_are_what_the_ddl_replay_says(sections: dict[str, list[list[str]]]) -> None:
    from tests.se_company_ddl import declared_columns

    assert [row[0] for row in sections["wide_columns"]] == declared_columns("se_company_info")


@pytest.mark.integration
def test_a_decision_re_resolves_one_field_and_re_pivots_one_company(sections: dict[str, list[list[str]]]) -> None:
    """Spec 9's sequence, executed: after a decision the backoffice runs the field's
    statement for that company, then the projection. Only that field's row moves
    (ReplacingMergeTree(resolved_at), newer version wins); the wide row picks it up with
    the decision id, the new run id and the new resolved_at."""
    resolved = _resolved(sections["resolved_after_decision"])
    assert (resolved[(EPSILON, "legal_name")]["value"], resolved[(EPSILON, "legal_name")]["decision_id"]) == (
        "Epsilon Group AB", D_EPSILON_NAME)
    assert (resolved[(EPSILON, "legal_name")]["source_run_id"], resolved[(EPSILON, "legal_name")]["resolved_at"]) == (
        "backoffice-1", "2026-09-02 12:05:00.000")
    assert (resolved[(EPSILON, "description")]["source_run_id"], resolved[(EPSILON, "description")]["resolved_at"]) == (
        RUN_ID, "2026-09-02 12:00:00.000")
    assert len(resolved) == 16  # one version per (company, field) under FINAL
    wide = _wide(sections["wide_after_decision"])
    epsilon = wide[EPSILON]
    assert (epsilon["legal_name"], epsilon["correction_ids"]) == ("Epsilon Group AB", f"['{D_EPSILON_NAME}']")
    assert (epsilon["source_run_id"], epsilon["resolved_at"]) == ("backoffice-1", "2026-09-02 12:05:00.000")
    assert epsilon["source_record_uids"] == f"['{SUGGESTION}']"  # scb-e no longer wins anything
    assert wide[ALPHA]["resolved_at"] == "2026-09-02 12:00:00.000"  # untouched by EPSILON's re-pivot
