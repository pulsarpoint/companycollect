ALTER TABLE corpscout.ted_notice_lots
    DROP COLUMN IF EXISTS cpv_additional_codes,
    DROP COLUMN IF EXISTS cpv_code;

ALTER TABLE corpscout.ted_notices
    DROP COLUMN IF EXISTS cpv_additional_codes,
    DROP COLUMN IF EXISTS cpv_code;
