CREATE DATABASE IF NOT EXISTS corpscout;

ALTER TABLE corpscout.se_financial_metrics
    ADD COLUMN IF NOT EXISTS observation_kind LowCardinality(String)
        DEFAULT 'reported' AFTER fiscal_year,
    ADD COLUMN IF NOT EXISTS source_fiscal_year Nullable(UInt16)
        DEFAULT fiscal_year AFTER observation_kind;

DROP TABLE IF EXISTS corpscout.se_financial_history;
