CREATE DATABASE IF NOT EXISTS corpscout;

-- One row per contract, so a contracts page reads rows instead of building
-- an aggregation over every winner row in the country.
--
-- Migrations 000234 and 000236 precomputed the register JOIN, and every
-- contracts page but one got fast on it. Brazil did not: the source was never
-- the problem, the query SHAPE is. The list builds a two-level aggregation --
-- GROUP BY (contract_ref, source_slug) under a GROUP BY contract_ref, about a
-- dozen argMax/any/max states each -- and all of it completes before LIMIT 50
-- applies. Measured 2026-08-01, that is 31s and 13.2 GiB for one page, which
-- the app abandons as a 500.
--
-- The aggregation collapses nothing for Brazil:
--
--   BR   4,605,018 contracts from 4,605,018 winner rows   1:1
--   NO      26,124 contracts from     55,493 winner rows   2:1
--
-- So it builds 4.6M groups holding a dozen aggregate states over wide strings
-- in order to emit 4.6M rows. Norway genuinely halves and was never in
-- trouble. Doing that work once a day writes the answer here, and the page
-- filters and sorts an ordered read of it.
--
-- Every filter the page offers is contract-level -- agreement type, CPV
-- prefix, amount range, publication date range -- which is what makes one row
-- per contract a legal shape to filter on at all.
--
-- contract_date is a String because the page's aggregation produces
-- coalesce(toString(argMax(source_date, priority)), '') and sorts on it as
-- text, blanks last. That is kept exactly, so the rollup and the old query
-- order identically. publication_date is carried separately as a real Date so
-- the from/to filters compare dates rather than strings.
CREATE TABLE IF NOT EXISTS corpscout.company_contract_rollup
(
    country_code LowCardinality(String),
    -- The contract's identity across sources, as the list links to it.
    contract_ref String,
    contract_date String,
    buyer_name String,
    title String,
    agreement_type String,
    cpv_code String,
    winner_name String,
    winner_registered_id String,
    winner_match_status String,
    supplier_count UInt32,
    amount_original Nullable(Float64),
    currency String,
    amount_usd Nullable(Float64),
    source_url String,
    publication_date Nullable(Date),
    resolved_at DateTime
)
ENGINE = MergeTree
ORDER BY (country_code, contract_date, contract_ref);
