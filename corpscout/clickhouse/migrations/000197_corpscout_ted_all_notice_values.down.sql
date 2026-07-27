DROP TABLE IF EXISTS corpscout.ted_notice_lots;

ALTER TABLE corpscout.ted_notice_winners
    DROP COLUMN IF EXISTS subcontracting_currency,
    DROP COLUMN IF EXISTS subcontracting_amount_usd,
    DROP COLUMN IF EXISTS subcontracting_amount_original;

ALTER TABLE corpscout.ted_notices
    DROP COLUMN IF EXISTS framework_total_approximate_currency,
    DROP COLUMN IF EXISTS framework_total_approximate_amount_usd,
    DROP COLUMN IF EXISTS framework_total_approximate_amount_original,
    DROP COLUMN IF EXISTS framework_total_maximum_currency,
    DROP COLUMN IF EXISTS framework_total_maximum_amount_usd,
    DROP COLUMN IF EXISTS framework_total_maximum_amount_original,
    DROP COLUMN IF EXISTS framework_maximum_currency,
    DROP COLUMN IF EXISTS framework_maximum_amount_usd,
    DROP COLUMN IF EXISTS framework_maximum_amount_original,
    DROP COLUMN IF EXISTS estimated_value_currency,
    DROP COLUMN IF EXISTS estimated_value_amount_usd,
    DROP COLUMN IF EXISTS estimated_value_amount_original;
