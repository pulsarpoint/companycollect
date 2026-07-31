CREATE DATABASE IF NOT EXISTS corpscout;

-- GLEIF's ISIN-to-LEI mapping: instrument to issuer, worldwide.
--
-- The nearest thing to a global register of which company issued which
-- security. GLEIF publishes it daily with ANNA, the body coordinating the
-- national numbering agencies that assign ISINs, and it is free: 9.1M pairs
-- covering Germany, the US, the UK, Switzerland, the Nordics and beyond.
--
-- Why it matters here: the existing instrument_issuer comes from ESMA FIRDS,
-- which records only EU/EEA admissions. This carries the same relationship
-- without that boundary, so the LEI branch of company_traded_symbols stops
-- being a European feature.
--
-- What it does NOT solve: it maps ISIN to LEI, so a market without LEI
-- adoption is absent regardless of how large it is. Measured on the
-- 2026-07-31 file, it holds zero Brazilian ISINs against 4.1M German ones.
-- Brazil needs its national bridge (br_b3_instruments) either way.
CREATE TABLE IF NOT EXISTS corpscout.gleif_isin_lei
(
    isin String,
    lei String,
    source_url String,
    source_file_name String,
    source_run_id String,
    retrieved_at DateTime
)
ENGINE = MergeTree
ORDER BY (isin, lei);
