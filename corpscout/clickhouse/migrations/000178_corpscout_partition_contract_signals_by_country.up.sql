CREATE DATABASE IF NOT EXISTS corpscout;

-- Partition the government-contract signal tables by country so each country's
-- asset can REPLACE PARTITION its own rows without touching another's. A
-- partition key cannot be ALTERed, so the tables are recreated rather than
-- modified. They are fully derived and rebuild in seconds, so no data is
-- copied -- the per-country assets repopulate them.
--
-- Country stays first in ORDER BY as well: the partition prunes, the sort key
-- still serves the per-company lookups the detail page makes.
DROP TABLE IF EXISTS corpscout.company_government_contract_evidence;

CREATE TABLE IF NOT EXISTS corpscout.company_government_contract_evidence
(
    country_code LowCardinality(String),
    company_id String,
    evidence_id String,
    source_slugs Array(String),
    source_references Array(String),
    publication_date Nullable(Date),
    buyer_name String,
    title String,
    agreement_type LowCardinality(String),
    source_updated_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id, evidence_id);

DROP TABLE IF EXISTS corpscout.company_government_contract_summary;

CREATE TABLE IF NOT EXISTS corpscout.company_government_contract_summary
(
    country_code LowCardinality(String),
    company_id String,
    public_award_count UInt32,
    public_award_last_date Nullable(Date),
    source_slugs Array(String),
    source_updated_at DateTime64(3, 'UTC'),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY country_code
ORDER BY (country_code, company_id);

DROP TABLE IF EXISTS corpscout.company_signal_coverage;

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
