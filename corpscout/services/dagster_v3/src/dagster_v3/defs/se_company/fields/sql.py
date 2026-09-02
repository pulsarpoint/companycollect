"""Renders the registry into the ClickHouse statements both runners execute verbatim.

``render_resolve_sql`` -- one INSERT per field into corpscout.se_company_field (spec 7.4):
the live decision from se_company_info_field_value wins when its value is not NULL,
otherwise the policy's winner among the eligible candidates; no row when neither.
``render_projection_sql`` -- the wide corpscout.se_company_info row re-pivoted from the
resolved rows (spec 8.3), inserting in SE_COMPANY_INFO_COLUMNS order.

Parameters are ClickHouse server-side ``{name:Type}`` placeholders -- ``field``,
``company_ids``, ``source_run_id`` and ``resolved_at`` for resolve, ``company_ids`` alone
for the projection. clickhouse-driver renders them server-side only when the Client is
built with settings={'server_side_params': True} (0.2.7+); the backoffice passes them as
query_params. Never mix in driver-side ``%(name)s`` placeholders.

Verified on ClickHouse 26.5 (tests/test_se_company_field_sql.py harness):
- ``argMax(arg, key)`` SKIPS rows whose ``arg`` is NULL, so the decision CTE aggregates a
  tuple -- a release row (value IS NULL) must be the latest, not be skipped in favour of
  the older value it released.
- A ``WITH`` clause is visible in both branches of a UNION ALL; an alias may precede FINAL.
"""

from dagster_v3.defs.se_company.fields.policies import policy_for
from dagster_v3.defs.se_company.fields.registry import DatatypeRegistry, FieldSpec, field_names
from dagster_v3.defs.se_company.fields.tables import (
    SE_COMPANY_FIELD,
    SE_COMPANY_FIELD_CANDIDATE,
    SE_COMPANY_FIELD_COLUMNS,
    SE_COMPANY_INFO,
    SE_COMPANY_INFO_COLUMNS,
    SE_COMPANY_INFO_FIELD_VALUE,
)


def sql_string(value: str) -> str:
    """``value`` as a ClickHouse string literal (backslash-escaped quotes)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def array_literal(values: tuple[str, ...]) -> str:
    """``values`` as an inline ClickHouse Array(String) literal, e.g. ['scb', 'esef']."""
    return "[" + ", ".join(sql_string(value) for value in values) + "]"


def rank_sql(field: FieldSpec) -> str:
    """1-based position of the candidate's source in the field's precedence tuple (0 = not listed)."""
    return f"indexOf({array_literal(field.sources)}, c.source)"


# The candidate columns every CTE carries, aliased explicitly so a renamed DDL column
# fails here rather than binding by position.
CANDIDATE_PROJECTION = (
    "c.company_id AS company_id, c.source AS source, c.source_record_uid AS source_record_uid,\n"
    "               c.value AS value, c.value_json AS value_json, c.observed_at AS observed_at"
)


def render_resolve_sql(registry: DatatypeRegistry, field: FieldSpec) -> str:
    policy = policy_for(field)
    sources = array_literal(field.sources)
    columns = ", ".join(SE_COMPANY_FIELD_COLUMNS)
    return f"""INSERT INTO {SE_COMPANY_FIELD}
    ({columns})
WITH
    candidates AS (
        SELECT {CANDIDATE_PROJECTION}
        FROM {SE_COMPANY_FIELD_CANDIDATE} AS c FINAL
        WHERE c.field = {{field:String}} AND c.company_id IN {{company_ids:Array(String)}}
    ),
    eligible AS (
        SELECT {CANDIDATE_PROJECTION},
               {rank_sql(field)} AS rank,
               {policy.compare_key_sql(field)} AS compare_key
        FROM candidates AS c
        WHERE has({sources}, c.source) AND ({policy.candidate_filter_sql(field)})
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
        ORDER BY c.company_id, {policy.winner_order_sql(field)}
        LIMIT 1 BY c.company_id
    ),
    decision AS (
        SELECT company_id,
               argMax((value, source, source_ref, source_at, created_at, value_id),
                      (created_at, toString(value_id))) AS latest
        FROM {SE_COMPANY_INFO_FIELD_VALUE}
        WHERE field = {{field:String}} AND company_id IN {{company_ids:Array(String)}}
        GROUP BY company_id
    ),
    live AS (
        SELECT company_id, assumeNotNull(tupleElement(latest, 1)) AS value,
               tupleElement(latest, 2) AS source, tupleElement(latest, 3) AS source_ref,
               ifNull(tupleElement(latest, 4), tupleElement(latest, 5)) AS observed_at,
               tupleElement(latest, 6) AS decision_id
        FROM decision WHERE tupleElement(latest, 1) IS NOT NULL AND trim(assumeNotNull(tupleElement(latest, 1))) != ''
    ),
    decided AS (
        SELECT c.company_id AS company_id, c.value AS value, c.value_json AS value_json,
               c.source AS source, c.source_record_uid AS source_record_uid,
               c.observed_at AS observed_at, c.decision_id AS decision_id,
               {policy.compare_key_sql(field)} AS compare_key
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
    d.company_id AS company_id, {{field:String}} AS field, d.value AS value, d.value_json AS value_json,
    d.source AS source, d.source_record_uid AS source_record_uid, d.observed_at AS observed_at,
    toNullable(d.decision_id) AS decision_id,
    {sql_string(policy.name)} AS policy_name, {sql_string(policy.version)} AS policy_version,
    ifNull(n.candidate_count, toUInt16(0)) AS candidate_count, a.agreeing_sources AS agreeing_sources,
    {sql_string(registry.version)} AS registry_version, {{source_run_id:String}} AS source_run_id,
    {{resolved_at:DateTime64(3, 'UTC')}} AS resolved_at
FROM decided AS d
LEFT JOIN counted AS n ON n.company_id = d.company_id
LEFT JOIN agreement AS a ON a.company_id = d.company_id AND a.compare_key = d.compare_key
UNION ALL
SELECT
    w.company_id AS company_id, {{field:String}} AS field, w.value AS value, w.value_json AS value_json,
    w.source AS source, w.source_record_uid AS source_record_uid, w.observed_at AS observed_at,
    CAST(NULL AS Nullable(UUID)) AS decision_id,
    {sql_string(policy.name)} AS policy_name, {sql_string(policy.version)} AS policy_version,
    ifNull(n.candidate_count, toUInt16(0)) AS candidate_count, a.agreeing_sources AS agreeing_sources,
    {sql_string(registry.version)} AS registry_version, {{source_run_id:String}} AS source_run_id,
    {{resolved_at:DateTime64(3, 'UTC')}} AS resolved_at
FROM winner AS w
LEFT JOIN counted AS n ON n.company_id = w.company_id
LEFT JOIN agreement AS a ON a.company_id = w.company_id AND a.compare_key = w.compare_key
WHERE w.company_id NOT IN (SELECT company_id FROM decided)"""


def render_projection_select_sql(registry: DatatypeRegistry) -> str:
    """The wide row per company from its resolved rows (spec 8.3), as a bare SELECT so a
    caller can stage it (publish_with_stage) or insert it directly (render_projection_sql).

    Column sources: registry fields by name (json members extracted from value_json);
    legal-form labels from se_code_labels on the resolved code; wikidata_id / lei from the
    artifacts as today; llm provenance from the observation when the description came from
    the llm source, the pilot's deterministic constants otherwise; correction_ids = every
    decision id applied; source_record_uids / evidence_hashes = the winning candidates'.
    A company is published only with a legal_name candidate from scb or bolagsverket and
    at least one winning candidate uid (the table's has_evidence CHECK).
    """
    fields = array_literal(field_names(registry))
    return f"""WITH
    resolved AS (
        SELECT f.company_id AS company_id, f.field AS field, f.value AS value, f.value_json AS value_json,
               f.source AS source, f.source_record_uid AS source_record_uid, f.decision_id AS decision_id,
               f.source_run_id AS source_run_id, f.resolved_at AS resolved_at,
               ifNull(c.evidence_hash, '') AS evidence_hash
        FROM {SE_COMPANY_FIELD} AS f FINAL
        LEFT JOIN (
            SELECT company_id, field, source, source_record_uid, toString(evidence_hash) AS evidence_hash
            FROM {SE_COMPANY_FIELD_CANDIDATE} FINAL
            WHERE company_id IN {{company_ids:Array(String)}}
        ) AS c ON c.company_id = f.company_id AND c.field = f.field AND c.source = f.source
              AND c.source_record_uid = f.source_record_uid
        WHERE f.company_id IN {{company_ids:Array(String)}} AND has({fields}, f.field)
    ),
    pivot AS (
        SELECT company_id,
               anyIf(value, field = 'legal_name') AS legal_name,
               anyIf(value, field = 'legal_form_code') AS legal_form_code,
               anyIf(value, field = 'status') AS status,
               anyIf(value, field = 'incorporation_date') AS incorporation_date_text,
               anyIf(value, field = 'description') AS description,
               anyIf(JSONExtractString(value_json, 'language'), field = 'description') AS description_language,
               anyIf(source, field = 'description') AS description_source,
               anyIf(source_record_uid, field = 'description') AS description_source_record_uid,
               anyIf(value, field = 'description_sv') AS description_sv,
               anyIf(value, field = 'primary_nace_code') AS primary_nace_code,
               anyIf(value, field = 'primary_sni_code') AS primary_sni_code,
               anyIf(value, field = 'industry_label_en') AS industry_label_en,
               anyIf(value, field = 'website') AS website,
               anyIf(JSONExtract(value_json, 'count', 'Nullable(UInt64)'), field = 'employee_count') AS employee_count,
               anyIf(toDate32OrNull(JSONExtractString(value_json, 'as_of')), field = 'employee_count') AS employee_count_as_of,
               anyIf(toDecimal128OrNull(JSONExtractRaw(value_json, 'amount'), 2), field = 'latest_revenue') AS latest_revenue_amount,
               anyIf(JSONExtractString(value_json, 'currency'), field = 'latest_revenue') AS latest_revenue_currency,
               anyIf(toDecimal128OrNull(JSONExtractRaw(value_json, 'amount_usd'), 2), field = 'latest_revenue') AS latest_revenue_amount_usd,
               anyIf(JSONExtract(value_json, 'fiscal_year', 'Nullable(UInt16)'), field = 'latest_revenue') AS latest_revenue_fiscal_year,
               arraySort(groupUniqArrayIf(source_record_uid, source_record_uid != '')) AS source_record_uids,
               arraySort(groupUniqArrayIf(evidence_hash, evidence_hash != '')) AS evidence_hashes,
               arraySort(groupArrayIf(assumeNotNull(decision_id), decision_id IS NOT NULL)) AS correction_ids,
               max(resolved_at) AS last_resolved_at,
               argMax(source_run_id, resolved_at) AS last_source_run_id
        FROM resolved
        GROUP BY company_id
    ),
    description_candidates AS (
        SELECT company_id,
               arrayMap(t -> tupleElement(t, 1), arraySort(groupArray((source, source_record_uid)))) AS description_sources,
               arrayMap(t -> tupleElement(t, 2), arraySort(groupArray((source, source_record_uid)))) AS description_source_record_uids,
               toUInt8(count()) AS description_source_count
        FROM {SE_COMPANY_FIELD_CANDIDATE} FINAL
        WHERE field = 'description' AND company_id IN {{company_ids:Array(String)}}
        GROUP BY company_id
    ),
    publishable AS (
        SELECT DISTINCT company_id FROM {SE_COMPANY_FIELD_CANDIDATE} FINAL
        WHERE field = 'legal_name' AND source IN ('scb', 'bolagsverket')
          AND company_id IN {{company_ids:Array(String)}}
    ),
    legal_form_labels AS (
        SELECT code, argMax(label_en, version) AS label_en, argMax(label_sv, version) AS label_sv
        FROM corpscout.se_code_labels WHERE code_type = 'legal_form' GROUP BY code
    ),
    wikidata_ids AS (
        SELECT company_id, argMax(wikidata_id, observed_at) AS wikidata_id
        FROM corpscout.se_company_info_wikidata
        WHERE company_id IN {{company_ids:Array(String)}} GROUP BY company_id
    ),
    leis AS (
        SELECT company_id, argMax(lei, observed_at) AS lei
        FROM corpscout.se_company_info_esef
        WHERE company_id IN {{company_ids:Array(String)}} GROUP BY company_id
    ),
    observations AS (
        SELECT toString(suggestion_id) AS suggestion_ref,
               argMax(model_provider, created_at) AS model_provider,
               argMax(model_name, created_at) AS model_name,
               argMax(prompt_version, created_at) AS prompt_version
        FROM corpscout.se_company_info_enrichment_observation
        WHERE company_id IN {{company_ids:Array(String)}} GROUP BY suggestion_id
    )
SELECT
    p.company_id AS company_id,
    p.legal_name AS legal_name,
    nullIf(p.legal_form_code, '') AS legal_form_code,
    ifNull(lf.label_en, '') AS legal_form_label_en,
    ifNull(lf.label_sv, '') AS legal_form_label_sv,
    p.status AS status,
    toDate32OrNull(p.incorporation_date_text) AS incorporation_date,
    nullIf(p.description, '') AS description,
    nullIf(p.description_sv, '') AS description_sv,
    p.description_language AS description_language,
    p.description_source = 'llm' AS llm_enhanced,
    dc.description_sources AS description_sources,
    dc.description_source_record_uids AS description_source_record_uids,
    ifNull(dc.description_source_count, toUInt8(0)) AS description_source_count,
    p.primary_nace_code AS primary_nace_code,
    p.primary_sni_code AS primary_sni_code,
    p.industry_label_en AS industry_label_en,
    nullIf(p.website, '') AS website,
    p.employee_count AS employee_count,
    p.employee_count_as_of AS employee_count_as_of,
    p.latest_revenue_amount AS latest_revenue_amount,
    p.latest_revenue_currency AS latest_revenue_currency,
    p.latest_revenue_amount_usd AS latest_revenue_amount_usd,
    p.latest_revenue_fiscal_year AS latest_revenue_fiscal_year,
    nullIf(ifNull(w.wikidata_id, ''), '') AS wikidata_id,
    nullIf(ifNull(e.lei, ''), '') AS lei,
    p.source_record_uids AS source_record_uids,
    p.evidence_hashes AS evidence_hashes,
    p.correction_ids AS correction_ids,
    if(p.description_source = 'llm', toUUIDOrNull(p.description_source_record_uid), NULL) AS suggestion_id,
    if(p.description_source = 'llm', ifNull(o.model_provider, ''), 'deterministic') AS model_provider,
    if(p.description_source = 'llm', ifNull(o.model_name, ''), 'se-company-info-rules') AS model_name,
    if(p.description_source = 'llm', ifNull(o.prompt_version, ''), 'se-company-info-rules-v1') AS prompt_version,
    p.last_source_run_id AS source_run_id,
    p.last_resolved_at AS resolved_at
FROM pivot AS p
INNER JOIN publishable AS pub ON pub.company_id = p.company_id
LEFT JOIN legal_form_labels AS lf ON lf.code = p.legal_form_code
LEFT JOIN description_candidates AS dc ON dc.company_id = p.company_id
LEFT JOIN wikidata_ids AS w ON w.company_id = p.company_id
LEFT JOIN leis AS e ON e.company_id = p.company_id
LEFT JOIN observations AS o ON o.suggestion_ref = p.description_source_record_uid
WHERE p.legal_name != '' AND notEmpty(p.source_record_uids)"""


def render_projection_sql(registry: DatatypeRegistry) -> str:
    """The projection as one INSERT -- what the registry's field = '*' row carries."""
    columns = ", ".join(SE_COMPANY_INFO_COLUMNS)
    return f"INSERT INTO {SE_COMPANY_INFO}\n    ({columns})\n{render_projection_select_sql(registry)}"
