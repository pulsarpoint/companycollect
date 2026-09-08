# Retire the se_company_info tables (basic-info slice 4)

Owner-run, on the companycollect ClickHouse, AFTER migration 000391 is applied and the
dagster_v3 deploy without the old publisher is live. Never drops
`se_company_info_enrichment_observation` (the basic-info LLM extractor's paid-call cache).

## Gates (every one must hold before the DROPs)

```sql
-- 1. The serving view no longer names the old table.
SELECT countIf(create_table_query LIKE '%se_company_info%') = 0 AS view_rebased
FROM system.tables WHERE database = 'corpscout' AND name = 'se_companies_serving';

-- 2. No view or materialized view references any of the five tables.
SELECT count() = 0 AS no_readers FROM system.tables
WHERE database = 'corpscout' AND engine IN ('View', 'MaterializedView')
  AND (create_table_query LIKE '%se_company_info_scb%'
       OR create_table_query LIKE '%se_company_info_esef%'
       OR create_table_query LIKE '%se_company_info_wikidata%'
       OR create_table_query LIKE '%se_company_info_field_value%'
       OR create_table_query LIKE '%corpscout.se_company_info %'
       OR create_table_query LIKE '%corpscout.se_company_info\n%');

-- 3. Row counts recorded before the drop (paste into the retirement note).
SELECT table, sum(rows) AS rows FROM system.parts WHERE database = 'corpscout' AND active
  AND table IN ('se_company_info', 'se_company_info_scb', 'se_company_info_esef',
                'se_company_info_wikidata', 'se_company_info_field_value')
GROUP BY table ORDER BY table;
```

Dagster gates, from the webserver: `se_company_info_weekly` and
`se_company_info_field_value_sensor` are absent or STOPPED, and the asset
`se_company_info_clickhouse` is not in the code location.

## Drops

The retired render goes FIRST: it is the one remaining reader of `se_company_info` (gate 2
lists it until it is gone), and it exists only for a rollback of 000391. Drop it once the
new view has served for a full refresh; if a rollback is still on the table, stop here.

```sql
DROP VIEW IF EXISTS corpscout.se_companies_serving_retired;
DROP TABLE IF EXISTS corpscout.se_company_info_field_value;
DROP TABLE IF EXISTS corpscout.se_company_info_wikidata;
DROP TABLE IF EXISTS corpscout.se_company_info_esef;
DROP TABLE IF EXISTS corpscout.se_company_info_scb;
DROP TABLE IF EXISTS corpscout.se_company_info;
```

Applied 2026-09-08: rows recorded before the drop were se_company_info 3,779,090,
se_company_info_scb 3,749,662, se_company_info_esef 675, se_company_info_wikidata 3,119,
se_company_info_field_value 2.
