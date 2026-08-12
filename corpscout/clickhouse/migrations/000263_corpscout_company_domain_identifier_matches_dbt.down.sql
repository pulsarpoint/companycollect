ALTER TABLE corpscout.company_domain_dbt_discovery_runs
    DROP COLUMN IF EXISTS unmatched_company_count;

ALTER TABLE corpscout.company_domain_dbt_discovery_runs
    DROP COLUMN IF EXISTS directory_only_company_count;

ALTER TABLE corpscout.company_domain_dbt_discovery_runs
    DROP COLUMN IF EXISTS ambiguous_company_count;

ALTER TABLE corpscout.company_domain_dbt_discovery_runs
    DROP COLUMN IF EXISTS matched_company_count;

DROP TABLE IF EXISTS corpscout.company_domain_identifier_matches_dbt;
