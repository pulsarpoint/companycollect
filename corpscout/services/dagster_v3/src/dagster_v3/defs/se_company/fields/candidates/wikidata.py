"""Wikidata candidates for the SE info registry.

Reads: se_company_info_wikidata (the artifact, one row per linked entity) for description,
official name, inception date, industry label and employee count; wikidata_companies for
the employee count's point in time (the artifact does not carry it); wikidata_company_websites
for the entity's official website (primary candidate first). Every candidate keeps the
artifact's uid wikidata:<QID> -- the website row is a facet of that entity, not a record
of its own -- while the website's observed_at is the website row's resolved_at.
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
    json_nullable_string_sql,
    json_object_sql,
    json_string_sql,
)

SOURCE = "wikidata"
EXTRACTOR_VERSION = "wikidata-candidates-v1"
ARTIFACT_TABLE = "se_company_info_wikidata"
ENTITIES_TABLE = "wikidata_companies"
WEBSITES_TABLE = "wikidata_company_websites"


def build_scope_sql() -> str:
    artifact = f"(SELECT company_id, wikidata_id FROM {DATABASE}.{ARTIFACT_TABLE}) AS artifact"
    return changed_companies_scope_sql(source=SOURCE, changes_sql=f"""    SELECT company_id, observed_at AS changed_at FROM {DATABASE}.{ARTIFACT_TABLE}
    UNION ALL
    SELECT artifact.company_id AS company_id, websites.resolved_at AS changed_at
    FROM {DATABASE}.{WEBSITES_TABLE} AS websites
    INNER JOIN {artifact} ON artifact.wikidata_id = websites.wikidata_id
    UNION ALL
    SELECT artifact.company_id AS company_id, entities.resolved_at AS changed_at
    FROM {DATABASE}.{ENTITIES_TABLE} AS entities
    INNER JOIN {artifact} ON artifact.wikidata_id = entities.wikidata_id""")


def _member(field: str, *, value: str, compare_key: str, extra: dict[str, str] | None = None) -> str:
    members = {"compare_key": json_string_sql(compare_key), **(extra or {})}
    return (f"SELECT company_id, '{field}', source_record_uid, observed_at, {value},\n"
            f"    {json_object_sql(members)}\nFROM artifact WHERE {value} != ''")


def build_candidates_sql() -> str:
    # employee_count_json_sql (the shared helper) wraps as_of/period through json_string_sql,
    # which on ClickHouse 26.5 returns SQL NULL -- not the JSON token "null" -- for a NULL
    # argument, collapsing the whole concat() into NULL. period here is always unknown (the
    # artifact carries no period for this count), so it is the literal 'null' token, never
    # routed through toJSONString; as_of goes through json_nullable_string_sql, which spells
    # that token out itself, over a nullIf that maps both a genuine gap in wikidata_companies
    # and a LEFT JOIN miss ('' under join_use_nulls = 0, NULL under 1) to NULL.
    employee_count_expr = "assumeNotNull(artifact.employee_count)"
    employee_json = json_object_sql({
        "compare_key": json_string_sql(f"toString({employee_count_expr})"),
        "count": f"toString({employee_count_expr})",
        "as_of": json_nullable_string_sql("nullIf(entities.employee_as_of, '')"),
        "period": "'null'",
    })
    website_json = json_object_sql({
        "compare_key": json_string_sql("lowerUTF8(websites.root_domain)"),
        "root_domain": json_string_sql("websites.root_domain"),
    })
    return f"""WITH artifact AS (
    SELECT company_id, source_record_uid, observed_at, wikidata_id,
        {clean_text_sql('company_description')} AS description,
        {clean_text_sql('official_name')} AS legal_name,
        if(inception_date = toDate('1970-01-01'), '', ifNull(toString(inception_date), '')) AS incorporation_date,
        {clean_text_sql('industry_label')} AS industry_label,
        employee_count
    FROM {DATABASE}.{ARTIFACT_TABLE} FINAL
    WHERE company_id IN %(company_ids)s
),
entities AS (
    SELECT wikidata_id, ifNull(toString(employee_count_point_in_time), '') AS employee_as_of
    FROM {DATABASE}.{ENTITIES_TABLE} FINAL
    WHERE wikidata_id IN (SELECT wikidata_id FROM artifact)
),
websites AS (
    SELECT wikidata_id, website_url, root_domain, resolved_at
    FROM {DATABASE}.{WEBSITES_TABLE} FINAL
    WHERE wikidata_id IN (SELECT wikidata_id FROM artifact) AND trim(website_url) != ''
    ORDER BY is_primary_candidate DESC, website_normalized_url ASC
    LIMIT 1 BY wikidata_id
)
{_member('description', value='description', compare_key=compare_key_text_sql('description'), extra={'language': json_string_sql("'en'")})}
UNION ALL
{_member('legal_name', value='legal_name', compare_key=compare_key_text_sql('legal_name'))}
UNION ALL
{_member('incorporation_date', value='incorporation_date', compare_key='incorporation_date')}
UNION ALL
{_member('industry_label_en', value='industry_label', compare_key=compare_key_text_sql('industry_label'))}
UNION ALL
SELECT artifact.company_id, 'employee_count', artifact.source_record_uid, artifact.observed_at,
    toString(assumeNotNull(artifact.employee_count)),
    {employee_json}
FROM artifact
LEFT JOIN entities ON entities.wikidata_id = artifact.wikidata_id
WHERE artifact.employee_count IS NOT NULL
UNION ALL
SELECT artifact.company_id, 'website', artifact.source_record_uid, websites.resolved_at, websites.website_url,
    {website_json}
FROM artifact
INNER JOIN websites ON websites.wikidata_id = artifact.wikidata_id"""


rows_from_result = partial(candidate_rows_from_result, source=SOURCE, extractor_version=EXTRACTOR_VERSION)

EXTRACTOR = CandidateExtractor(
    source=SOURCE, extractor_version=EXTRACTOR_VERSION,
    source_tables=(ARTIFACT_TABLE, ENTITIES_TABLE, WEBSITES_TABLE),
    build_scope_sql=build_scope_sql, build_candidates_sql=build_candidates_sql,
)

se_company_field_candidates_wikidata = define_candidate_asset(
    EXTRACTOR,
    deps=("se_company_info_wikidata_clickhouse", "wikidata_companies_clickhouse", "wikidata_company_websites_clickhouse"),
    description=(
        "Wikidata field candidates for Swedish companies: description, official name, inception, "
        "industry label, employee count with its point in time, and the official website. "
        "Preview by default; execute: true appends new evidence."
    ),
)

defs = dg.Definitions(assets=[se_company_field_candidates_wikidata])
