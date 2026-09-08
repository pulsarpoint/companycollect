# Retire the se_companies spine (basic-info slice 5)

Owner-run, on the companycollect ClickHouse, AFTER the slice-5 dagster_v3 deploy, the
dbt rebuilds (company_domain_suggestions staging, company_serving publish) and the
backoffice merge are live and smoke-tested.

## Gates

```sql
-- 1. No view or materialized view reads the spine (stg_se_company_match_features is
--    rebuilt by dbt from se_company_basic_info before this runs). The match is on a
--    FROM or JOIN, so a provenance string literal cannot hold the gate.
SELECT count() = 0 AS no_readers FROM system.tables
WHERE database = 'corpscout' AND engine IN ('View', 'MaterializedView')
  AND match(create_table_query, '(FROM|JOIN)\\s+(corpscout\\.)?se_companies([^_a-z]|$)');

-- 2. Row counts recorded.
SELECT count() FROM corpscout.se_companies;
SELECT count() FROM corpscout.text_translations
WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description';
```

Dagster gates: the code location has no `sweden_company_companies_clickhouse` asset, and
`sweden_company_translation_load`'s field names `corpscout.se_bolagsverket_companies`.

## Drops

```sql
DROP TABLE IF EXISTS corpscout.se_companies;
ALTER TABLE corpscout.text_translations
    DELETE WHERE source_table = 'corpscout.se_companies' AND source_column = 'activity_description';
```

The DELETE is a lightweight delete over 2,176,663 rows; watch it with
`SELECT * FROM system.mutations WHERE table = 'text_translations' AND NOT is_done`.
