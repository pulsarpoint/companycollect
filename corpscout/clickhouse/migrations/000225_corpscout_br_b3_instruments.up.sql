CREATE DATABASE IF NOT EXISTS corpscout;

-- B3's instrument register: which ISIN a Brazilian trading code carries.
--
-- br_b3_listings gives a company its trading-code ROOT (PETR), which was enough
-- to reach EODHD by prefix -- substring(ticker,1,4) -- but a prefix is a guess
-- dressed as a join. B3 publishes the authoritative pairs per company, so PETR3
-- is known to be BRPETRACNOR9 rather than inferred from four letters.
--
-- That matters beyond tidiness: a root maps to every code beginning with it,
-- including ones belonging to a different instrument class, and it cannot
-- distinguish an ordinary share from a unit or a fractional-lot code.
--
-- Keyed on (cnpj_basico, ticker) because that is what both consumers need: the
-- company to reach its instruments, and an instrument to name its ISIN.
CREATE TABLE IF NOT EXISTS corpscout.br_b3_instruments
(
    cvm_code String,
    cnpj String,
    cnpj_basico String,
    -- The full trading code: PETR3, PETR4, VALE3.
    ticker String,
    -- 12 characters, BR-prefixed. '' when B3 lists a code without one.
    isin String,
    -- The company's own root, kept so a code can be traced back to its issuer
    -- even when the ISIN is absent.
    ticker_root String,
    source_url String,
    source_run_id String,
    retrieved_at DateTime
)
ENGINE = MergeTree
ORDER BY (cnpj_basico, ticker);
