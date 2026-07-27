CREATE DATABASE IF NOT EXISTS corpscout;

-- What each procurement register IS, so the UI stops carrying it as constants.
--
-- Grain is one row per SOURCE, not per (country, source). TED is one register
-- serving three countries, and a per-country grain would store its licence and
-- operator three times and invite them to drift. Countries are an array, so a
-- country page filters and a source page does not have to.
--
-- This is not coverage. How much of a register we hold, over what span, with
-- what caveat, is per (country, source) and lives in company_signal_coverage.
-- This table answers the other half: who publishes this, under what licence,
-- what does it include, and where does someone wanting to BID go. That last
-- column is the one nothing else answers -- everything built so far describes
-- what was awarded, which is no use to a supplier looking for work.

CREATE TABLE IF NOT EXISTS corpscout.procurement_registers
(
    source_slug LowCardinality(String),
    register_name String,
    operator String,
    -- Every country whose contracts view reads this register.
    country_codes Array(String),
    homepage_url String,
    api_or_download_url String,
    documentation_url String,
    licence String,
    -- The register's own scope. "Below-threshold contracts are absent" is a
    -- fact about TED. "We have not loaded 2019" is a fact about us, and it
    -- belongs in company_signal_coverage.
    coverage_description String,
    open_tenders_url String,
    -- The natural grain of this source's own tables, which is what a source
    -- page lists -- deliberately not the canonical 26-column projection the
    -- country views expose.
    grain_description String,
    source_tables Array(String),
    -- How to look up one record, so the raw-record panel needs no hardcoded map.
    notice_table String,
    notice_key_column String,
    notes String,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY source_slug;
