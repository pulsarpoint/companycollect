CREATE DATABASE IF NOT EXISTS corpscout;

-- Store the CPV classification TED publishes.
--
-- TED is the register that DEFINES the Common Procurement Vocabulary, and the
-- ted_procurement design doc committed to keeping it ("Contacts/industry
-- (§8b/8c): none in source, CPV kept verbatim"). No column was ever added, so
-- cpv_code read 0% for every TED-fed country while the national registers
-- reported 99-100% (Doffin 100%, Hilma 100%, UHM 99.9%). The classification was
-- in every notice XML the whole time, unparsed.
--
-- Measured over 288 notices across 8 countries and 3 months, using the exact
-- paths the parser now uses: 99.7% carry a procedure-level main code, 99.8% of
-- lots carry their own, every value is exactly 8 digits, and listName is always
-- "cpv" (never another vocabulary).
--
-- Two grains, because a multi-lot notice routinely splits across unrelated
-- categories -- one sampled notice had 13 lots with 13 distinct main codes. A
-- notice-level code alone would report that procurement as one category.
--
-- Main (BT-262) and additional (BT-263) stay in separate columns rather than one
-- array: they are different claims. Main says what this mainly buys, additional
-- says what else it touches, and summing spend across both would count the same
-- award once per category it mentions. Up to 19 additional codes were observed
-- on a single procedure.
--
-- Adding columns is enough on its own -- the publish path replaces whole country
-- slices, so the values appear as each month is re-parsed from the XML already
-- mirrored to S3 (126,735 notices, 8 countries, 31 months, 4.23 GB, zero
-- re-downloads).

ALTER TABLE corpscout.ted_notices
    ADD COLUMN IF NOT EXISTS cpv_code String AFTER notice_title,
    ADD COLUMN IF NOT EXISTS cpv_additional_codes Array(String) AFTER cpv_code;

ALTER TABLE corpscout.ted_notice_lots
    ADD COLUMN IF NOT EXISTS cpv_code String AFTER lot_title,
    ADD COLUMN IF NOT EXISTS cpv_additional_codes Array(String) AFTER cpv_code;
