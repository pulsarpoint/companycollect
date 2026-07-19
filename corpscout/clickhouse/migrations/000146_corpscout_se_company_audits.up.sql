CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.se_company_audits
(
    company_id String,
    fiscal_year Int32,
    statement_key String,
    audit_firm String,
    opinion_kind LowCardinality(String), -- 'standard' | 'modified' | 'unknown' (firm known, no pateckning fact)
    opinion_date Nullable(Date32),
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (company_id, fiscal_year, statement_key);
