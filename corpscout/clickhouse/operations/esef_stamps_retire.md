# Retire the ESEF stamp columns (spec 2026-09-09, slice 1)

Owner-run on the companycollect ClickHouse AFTER 000395 is applied, dagster_v3 is deployed,
the map is rebuilt, the three projections have refilled their new tables, and the
company_serving publish is green.

**This is a point of no return for 000395.** Its down migration renames
`esef_document_people_legacy` / `esef_document_business_items_legacy` /
`esef_document_group_relationships_legacy` back onto the live table names; once the drops
below run, those `_legacy` tables are gone and 000395 can no longer be reverted with
`migrate down` -- only forward, by a new migration.

## Gates

```sql
SELECT count() FROM corpscout.esef_document_people;                 -- > 0 (refilled)
SELECT count() FROM corpscout.esef_document_business_items;         -- > 0
SELECT count() FROM corpscout.esef_document_group_relationships;    -- > 0
SELECT name FROM system.tables WHERE database = 'corpscout' AND engine IN ('View','MaterializedView')
  AND match(create_table_query, '(country_iso2|country_code|company_id)') AND name LIKE 'esef%';  -- none
```

## Drops

```sql
ALTER TABLE corpscout.esef_disclosures DROP COLUMN IF EXISTS country_iso2, DROP COLUMN IF EXISTS company_id;
ALTER TABLE corpscout.esef_document_concept_labels DROP COLUMN IF EXISTS country_iso2, DROP COLUMN IF EXISTS company_id;
ALTER TABLE corpscout.esef_document_contact_candidates DROP COLUMN IF EXISTS country_iso2, DROP COLUMN IF EXISTS company_id;
ALTER TABLE corpscout.esef_document_company_information DROP COLUMN IF EXISTS country_iso2, DROP COLUMN IF EXISTS company_id;
DROP TABLE IF EXISTS corpscout.esef_document_people_legacy;
DROP TABLE IF EXISTS corpscout.esef_document_business_items_legacy;
DROP TABLE IF EXISTS corpscout.esef_document_group_relationships_legacy;
```
