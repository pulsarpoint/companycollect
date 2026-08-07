DROP TABLE IF EXISTS corpscout.se_bolagsverket_financial_observations;

ALTER TABLE corpscout.se_financial_facts
    DROP COLUMN IF EXISTS context_period_end;

ALTER TABLE corpscout.se_financial_facts
    DROP COLUMN IF EXISTS context_period_start;
