CREATE DATABASE IF NOT EXISTS corpscout;

-- What a country's procurement sources do and do not cover.
--
-- This is the one part of the government-contract signal that is materialized
-- rather than derived. Coverage carries editorial prose -- "Doffin, Norway's
-- national procurement register, is not ingested, so contracts below the EU
-- publication thresholds are absent entirely" -- which no query over the rows
-- can produce. The contracts themselves are views over the source tables, in a
-- later migration.
--
-- Partitioned by country so each country's asset replaces only its own row.
CREATE TABLE IF NOT EXISTS corpscout.company_signal_coverage
(
    country_code LowCardinality(String),
    signal_name LowCardinality(String),
    coverage_status LowCardinality(String),
    coverage_from Nullable(Date),
    coverage_to Nullable(Date),
    source_slugs Array(String),
    source_updated_at Nullable(DateTime64(3, 'UTC')),
    resolved_at DateTime64(3, 'UTC'),
    caveat String
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, signal_name);
