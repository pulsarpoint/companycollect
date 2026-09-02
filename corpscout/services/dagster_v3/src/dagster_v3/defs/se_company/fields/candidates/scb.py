"""SCB-side candidates for the SE info registry.

Reads: se_company_info_scb (the artifact, newest version) for the legal facts and the
activity description in both languages -- the very columns the old publisher copied into
se_company_info, so the cutover parity check holds by construction; se_industries (newest
primary row) for the SNI/NACE codes; nace_categories for the class label.
Emits: legal_name, legal_form_code, status, incorporation_date, description, description_sv,
primary_sni_code, primary_nace_code, industry_label_en.

source_record_uid is the artifact's uid for the six artifact fields and the industry row's
own uid for the three industry fields; observed_at is the artifact version stamp and the
industry bulk stamp respectively. A company changes for the scan when either carries a
stamp newer than the source's last extracted_at. se_company_registry_current is deliberately
not read: its scb row differs from the artifact (which is se_companies' merged view), and
the registry decided identity ranks scb first precisely because the artifact is what was
published so far.
"""

from functools import partial

import dagster as dg

from dagster_v3.defs.se_company.common import DATABASE
from dagster_v3.defs.se_company.fields.candidates.common import (
    CandidateExtractor,
    changed_companies_scope_sql,
    candidate_rows_from_result,
    clean_text_sql,
    compare_key_text_sql,
    define_candidate_asset,
    json_object_sql,
    json_string_sql,
    nace_digits_sql,
    nace_labels_cte_sql,
)

SOURCE = "scb"
EXTRACTOR_VERSION = "scb-candidates-v1"
ARTIFACT_TABLE = "se_company_info_scb"
INDUSTRIES_TABLE = "se_industries"
NACE_TABLE = "nace_categories"
# SNI 2007 five-digit codes are NACE Rev. 2 classes plus a national digit; the backoffice
# labels published codes from this version too.
NACE_VERSION = "NACE_REV_2"


def build_scope_sql() -> str:
    return changed_companies_scope_sql(source=SOURCE, changes_sql=f"""    SELECT company_id, observed_at AS changed_at FROM {DATABASE}.{ARTIFACT_TABLE}
    UNION ALL
    SELECT company_id, updated_from_raw_at AS changed_at FROM {DATABASE}.{INDUSTRIES_TABLE}""")


def _member(field: str, *, value: str, compare_key: str, source: str, extra: dict[str, str] | None = None) -> str:
    """One UNION member: CANDIDATE_SELECT_COLUMNS for ``field`` from CTE ``source``."""
    members = {"compare_key": json_string_sql(compare_key), **(extra or {})}
    return (f"SELECT company_id, '{field}', source_record_uid, observed_at, {value},\n"
            f"    {json_object_sql(members)}\nFROM {source} WHERE {value} != ''")


def build_candidates_sql() -> str:
    return f"""WITH artifact AS (
    SELECT company_id, source_record_uid, observed_at,
        {clean_text_sql(f'{ARTIFACT_TABLE}.legal_name')} AS legal_name_clean,
        {clean_text_sql(f'{ARTIFACT_TABLE}.legal_name_raw')} AS legal_name_raw_clean,
        if(legal_name_clean != '', legal_name_clean, legal_name_raw_clean) AS legal_name,
        {clean_text_sql('legal_form_code')} AS legal_form_code,
        trim(toString(status)) AS status,
        if(incorporation_date = toDate32('1900-01-01'), '', ifNull(toString(incorporation_date), '')) AS incorporation_date,
        {clean_text_sql('activity_description')} AS description_sv,
        {clean_text_sql('activity_description_en')} AS description_en,
        if(description_en != '', description_en, description_sv) AS description,
        if(description_en != '', 'en', 'sv') AS language
    FROM {DATABASE}.{ARTIFACT_TABLE} FINAL
    WHERE company_id IN %(company_ids)s
    ORDER BY observed_at DESC, source_record_uid DESC
    LIMIT 1 BY company_id
),
industry AS (
    SELECT company_id,
        argMax({INDUSTRIES_TABLE}.sni_code, ({INDUSTRIES_TABLE}.updated_from_raw_at, {INDUSTRIES_TABLE}.sni_code)) AS sni_code,
        argMax({INDUSTRIES_TABLE}.nace_rev2_class_code, ({INDUSTRIES_TABLE}.updated_from_raw_at, {INDUSTRIES_TABLE}.sni_code)) AS nace_code,
        argMax({INDUSTRIES_TABLE}.source_record_uid, ({INDUSTRIES_TABLE}.updated_from_raw_at, {INDUSTRIES_TABLE}.sni_code)) AS source_record_uid,
        max(updated_from_raw_at) AS observed_at
    FROM {DATABASE}.{INDUSTRIES_TABLE} FINAL
    WHERE is_primary = 1 AND company_id IN %(company_ids)s
    GROUP BY company_id
),
labels AS (
    {nace_labels_cte_sql()}
),
industry_labelled AS (
    SELECT industry.company_id AS company_id, industry.source_record_uid AS source_record_uid,
        industry.observed_at AS observed_at, trim(industry.sni_code) AS sni_code,
        {nace_digits_sql('trim(industry.nace_code)')} AS nace_code,
        {clean_text_sql('labels.label_en')} AS label_en
    FROM industry
    LEFT JOIN labels ON labels.classification_version = '{NACE_VERSION}' AND labels.normalized_code = substring(trim(industry.sni_code), 1, 4)
)
{_member('legal_name', value='legal_name', compare_key=compare_key_text_sql('legal_name'), source='artifact')}
UNION ALL
{_member('legal_form_code', value='legal_form_code', compare_key='lowerUTF8(legal_form_code)', source='artifact')}
UNION ALL
{_member('status', value='status', compare_key='lowerUTF8(status)', source='artifact')}
UNION ALL
{_member('incorporation_date', value='incorporation_date', compare_key='incorporation_date', source='artifact')}
UNION ALL
{_member('description', value='description', compare_key=compare_key_text_sql('description'), source='artifact', extra={'language': json_string_sql('language')})}
UNION ALL
{_member('description_sv', value='description_sv', compare_key=compare_key_text_sql('description_sv'), source='artifact', extra={'language': json_string_sql("'sv'")})}
UNION ALL
{_member('primary_sni_code', value='sni_code', compare_key='sni_code', source='industry_labelled', extra={'code_set': json_string_sql("'SNI'")})}
UNION ALL
{_member('primary_nace_code', value='nace_code', compare_key='nace_code', source='industry_labelled', extra={'revision': json_string_sql(f"'{NACE_VERSION}'")})}
UNION ALL
{_member('industry_label_en', value='label_en', compare_key=compare_key_text_sql('label_en'), source='industry_labelled')}"""


rows_from_result = partial(candidate_rows_from_result, source=SOURCE, extractor_version=EXTRACTOR_VERSION)

EXTRACTOR = CandidateExtractor(
    source=SOURCE, extractor_version=EXTRACTOR_VERSION,
    source_tables=(ARTIFACT_TABLE, INDUSTRIES_TABLE, NACE_TABLE),
    build_scope_sql=build_scope_sql, build_candidates_sql=build_candidates_sql,
)

se_company_field_candidates_scb = define_candidate_asset(
    EXTRACTOR,
    deps=("se_company_info_scb_clickhouse", "sweden_company_industries_clickhouse", "nace_categories_clickhouse"),
    description=(
        "SCB-side field candidates for Swedish companies: the legal facts and the description "
        "in both languages from the SCB artifact, primary SNI/NACE and the NACE class label from "
        "the register's industry rows. Preview by default; execute: true appends new evidence."
    ),
)

defs = dg.Definitions(assets=[se_company_field_candidates_scb])
