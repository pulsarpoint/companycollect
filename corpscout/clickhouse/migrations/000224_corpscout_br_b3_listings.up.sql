CREATE DATABASE IF NOT EXISTS corpscout;

-- B3's listed-issuer register: the Brazilian bridge from a company to a ticker.
--
-- The EU chain does not reach Brazil, in two independent places. Identity:
-- there is no LEI mandate, so GLEIF holds 4,259 Brazilian LEIs against 119,035
-- Swedish ones and registered_as is sparse -- CNPJ is the real key. Instruments:
-- ESMA FIRDS records only what is admitted on EU/EEA venues, so a Petrobras
-- share on B3 is simply absent (31 of 470 Brazilian ISINs appear there, the
-- cross-listed handful).
--
-- B3 publishes what closes both gaps in one record: CNPJ, the trading code root
-- and the CVM code together. That makes Brazil register-verified in the same
-- sense Sweden is -- every one of its 2,510 CNPJs resolves against br_companies
-- -- rather than matched on a name.
--
-- Kept in full, including issuers with no trading code and the ETFs and BDRs
-- B3 lists with cnpj '0'. Registration is a real fact, and `market` plus
-- `listing_date` are what separate a traded company from one that merely
-- registered a debenture: 1,976 of 2,512 carry B3's never-listed sentinel.
CREATE TABLE IF NOT EXISTS corpscout.br_b3_listings
(
    cvm_code String,
    -- 14 digits, unpunctuated. '' for the ETFs and BDRs B3 reports as '0'.
    cnpj String,
    -- The first 8, which is what br_companies is keyed on and what a company
    -- page URL carries.
    cnpj_basico String,
    -- 4 letters: PETR for PETR4. B3's own field is issuingCompany.
    ticker_root String,
    company_name String,
    trading_name String,
    -- NM, N1, N2, MB, DR1..DR3 -- and blank for the majority, which are
    -- registered issuers rather than listed companies.
    market LowCardinality(String),
    segment String,
    listing_date Nullable(Date),
    status LowCardinality(String),
    source_url String,
    source_run_id String,
    retrieved_at DateTime
)
ENGINE = MergeTree
ORDER BY (cnpj_basico, ticker_root, cvm_code);
