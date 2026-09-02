"""The registry's generated SQL, pinned as text here and executed against real DDL in the
clickhouse-local harness further down (spec 7.4, 8.3)."""

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
from tests.se_company_ddl import projection_aliases

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
    assert "argMax(value," not in LEGAL_NAME_RESOLVE_SQL


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
