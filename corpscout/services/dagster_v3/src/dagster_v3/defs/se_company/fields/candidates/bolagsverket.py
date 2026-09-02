"""Bolagsverket-side candidates for the SE info registry.

Reads: the bolagsverket row of se_company_registry_current (a plain MergeTree snapshot --
never FINAL) for the legal facts, with the scb row's derived_status beside it so the
status candidate can say whether the two registers disagree (value_json.conflict, the same
rule as se_companies.status_conflict); se_financials_bolagsverket_current (a view over the
annual accounts) for employee_count and latest_revenue, each from the newest fiscal year
that carries it. The change scan reads the metrics TABLE's resolved_at because the view
exposes no stamp.
"""

from functools import partial

import dagster as dg

from dagster_v3.defs.se_company.common import DATABASE
from dagster_v3.defs.se_company.fields.candidates.common import (
    FINANCIAL_MEMBERS_SQL,
    CandidateExtractor,
    changed_companies_scope_sql,
    candidate_rows_from_result,
    clean_text_sql,
    compare_key_text_sql,
    define_candidate_asset,
    financial_view_ctes_sql,
    json_object_sql,
    json_string_sql,
)

SOURCE = "bolagsverket"
EXTRACTOR_VERSION = "bolagsverket-candidates-v1"
REGISTRY_TABLE = "se_company_registry_current"
FINANCIALS_VIEW = "se_financials_bolagsverket_current"
FINANCIALS_TABLE = "se_bolagsverket_financial_metrics"


def build_scope_sql() -> str:
    return changed_companies_scope_sql(source=SOURCE, changes_sql=f"""    SELECT company_id, observed_at AS changed_at FROM {DATABASE}.{REGISTRY_TABLE}
    WHERE source = '{SOURCE}' AND has_company = 1
    UNION ALL
    SELECT company_id, resolved_at AS changed_at FROM {DATABASE}.{FINANCIALS_TABLE}""")


def _member(field: str, *, value: str, compare_key: str, extra: dict[str, str] | None = None) -> str:
    members = {"compare_key": json_string_sql(compare_key), **(extra or {})}
    return (f"SELECT company_id, '{field}', source_record_uid, observed_at, {value},\n"
            f"    {json_object_sql(members)}\nFROM registry WHERE {value} != ''")


def build_candidates_sql() -> str:
    conflict = "if(scb_status != '' AND scb_status != status, 'true', 'false')"
    return f"""WITH scb AS (
    SELECT company_id, trim(ifNull(toString(derived_status), '')) AS scb_status
    FROM {DATABASE}.{REGISTRY_TABLE}
    WHERE source = 'scb' AND has_company = 1 AND company_id IN %(company_ids)s
),
registry AS (
    SELECT bv.company_id AS company_id, bv.source_record_uid AS source_record_uid, bv.observed_at AS observed_at,
        {clean_text_sql('bv.legal_name')} AS legal_name,
        {clean_text_sql('toString(bv.legal_form_code)')} AS legal_form_code,
        trim(ifNull(toString(bv.derived_status), '')) AS status,
        ifNull(toString(bv.incorporation_date), '') AS incorporation_date,
        ifNull(scb.scb_status, '') AS scb_status
    FROM {DATABASE}.{REGISTRY_TABLE} AS bv
    LEFT JOIN scb ON scb.company_id = bv.company_id
    WHERE bv.source = '{SOURCE}' AND bv.has_company = 1 AND bv.company_id IN %(company_ids)s
),
{financial_view_ctes_sql(FINANCIALS_VIEW)}
{_member('legal_name', value='legal_name', compare_key=compare_key_text_sql('legal_name'))}
UNION ALL
{_member('legal_form_code', value='legal_form_code', compare_key='lowerUTF8(legal_form_code)')}
UNION ALL
{_member('status', value='status', compare_key='lowerUTF8(status)', extra={'conflict': conflict})}
UNION ALL
{_member('incorporation_date', value='incorporation_date', compare_key='incorporation_date')}
UNION ALL
{FINANCIAL_MEMBERS_SQL}"""


rows_from_result = partial(candidate_rows_from_result, source=SOURCE, extractor_version=EXTRACTOR_VERSION)

EXTRACTOR = CandidateExtractor(
    source=SOURCE, extractor_version=EXTRACTOR_VERSION,
    source_tables=(REGISTRY_TABLE, FINANCIALS_VIEW, FINANCIALS_TABLE),
    build_scope_sql=build_scope_sql, build_candidates_sql=build_candidates_sql,
)

se_company_field_candidates_bolagsverket = define_candidate_asset(
    EXTRACTOR,
    deps=("sweden_company_profile_history_clickhouse", "se_bolagsverket_financial_metrics_clickhouse"),
    description=(
        "Bolagsverket-side field candidates for Swedish companies: the register's own legal "
        "facts (status flagged when SCB disagrees) and employee count / latest revenue from the "
        "annual accounts. Preview by default; execute: true appends new evidence."
    ),
)

defs = dg.Definitions(assets=[se_company_field_candidates_bolagsverket])
