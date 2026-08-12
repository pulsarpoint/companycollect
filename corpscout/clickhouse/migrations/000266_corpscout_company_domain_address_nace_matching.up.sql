CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.company_domain_suggestions_dbt
    ADD COLUMN IF NOT EXISTS address_score Float32 DEFAULT 0
    AFTER people_score;

CREATE OR REPLACE VIEW corpscout.company_domain_suggestions_active AS
SELECT suggestions.*
FROM corpscout.company_domain_suggestions_dbt AS suggestions
INNER JOIN
(
    SELECT
        country_iso2,
        argMax(
            discovery_run_id,
            tuple(completed_at, discovery_run_id)
        ) AS discovery_run_id
    FROM corpscout.company_domain_dbt_discovery_runs FINAL
    GROUP BY country_iso2
) AS active_run USING (country_iso2, discovery_run_id);
