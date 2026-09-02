"""ESEF candidates for the SE info registry.

Reads: se_company_info_esef (the artifact; the newest filing per company by fiscal year,
the pick info_rules makes) for the description, and se_financials_esef_current (a view; no
FINAL) for employee_count and latest_revenue from the newest period carrying each. The
change scan reads esef_financial_metrics.resolved_at through the same LEI -> company_id
link the view uses, because the view exposes no stamp of its own.
"""

from functools import partial

import dagster as dg

from dagster_v3.defs.se_company.common import DATABASE
from dagster_v3.defs.se_company.fields.candidates.common import (
    FINANCIAL_MEMBERS_SQL,
    CandidateExtractor,
    changed_companies_scope_sql,
    candidate_rows_from_result,
    compare_key_text_sql,
    define_candidate_asset,
    financial_view_ctes_sql,
    json_object_sql,
    json_string_sql,
)

SOURCE = "esef"
EXTRACTOR_VERSION = "esef-candidates-v1"
ARTIFACT_TABLE = "se_company_info_esef"
FINANCIALS_VIEW = "se_financials_esef_current"
FINANCIALS_TABLE = "esef_financial_metrics"
IDENTIFIERS_TABLE = "company_identifier"


def build_scope_sql() -> str:
    return changed_companies_scope_sql(source=SOURCE, changes_sql=f"""    SELECT company_id, observed_at AS changed_at FROM {DATABASE}.{ARTIFACT_TABLE}
    UNION ALL
    SELECT identifiers.company_id AS company_id, toDateTime64(metrics.resolved_at, 3, 'UTC') AS changed_at
    FROM {DATABASE}.{FINANCIALS_TABLE} AS metrics
    INNER JOIN {DATABASE}.{IDENTIFIERS_TABLE} AS identifiers
        ON identifiers.issuer_scheme = 'lei' AND identifiers.issuer_id = upperUTF8(trimBoth(metrics.lei))
    WHERE identifiers.country_code = 'SE' AND identifiers.is_current = 1""")


def build_candidates_sql() -> str:
    description_json = json_object_sql({
        "compare_key": json_string_sql(compare_key_text_sql("description")),
        "language": json_string_sql("if(language = '', 'en', language)"),
    })
    return f"""WITH artifact AS (
    SELECT company_id, source_record_uid, observed_at, trim(company_description) AS description,
        toString(description_language) AS language
    FROM {DATABASE}.{ARTIFACT_TABLE} FINAL
    WHERE company_id IN %(company_ids)s AND trim(company_description) != ''
    ORDER BY fiscal_year DESC, observed_at DESC, source_record_uid DESC
    LIMIT 1 BY company_id
),
{financial_view_ctes_sql(FINANCIALS_VIEW)}
SELECT company_id, 'description', source_record_uid, observed_at, description,
    {description_json}
FROM artifact
UNION ALL
{FINANCIAL_MEMBERS_SQL}"""


rows_from_result = partial(candidate_rows_from_result, source=SOURCE, extractor_version=EXTRACTOR_VERSION)

EXTRACTOR = CandidateExtractor(
    source=SOURCE, extractor_version=EXTRACTOR_VERSION,
    source_tables=(ARTIFACT_TABLE, FINANCIALS_VIEW, FINANCIALS_TABLE, IDENTIFIERS_TABLE),
    build_scope_sql=build_scope_sql, build_candidates_sql=build_candidates_sql,
)

se_company_field_candidates_esef = define_candidate_asset(
    EXTRACTOR,
    deps=("se_company_info_esef_clickhouse", "esef_financial_metrics_clickhouse", "company_identifier_clickhouse"),
    description=(
        "ESEF field candidates for Swedish issuers: the newest filing's company description "
        "and employee count / latest revenue from the ESEF financial view. Preview by default; "
        "execute: true appends new evidence."
    ),
)

defs = dg.Definitions(assets=[se_company_field_candidates_esef])
