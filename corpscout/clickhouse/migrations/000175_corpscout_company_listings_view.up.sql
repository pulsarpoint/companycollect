CREATE DATABASE IF NOT EXISTS corpscout;

-- Joins the three identity layers into the consumer-facing listing view.
-- Every hop is many-to-one -- an ISIN resolves to at most one issuer, and an
-- issuer to at most one company per country -- so this join never fans out and
-- the view can stay a view rather than a materialized rollup.
--
-- Note the two distinct currencies of is_current. The exposed column is the
-- instrument's trading status from instrument_venues. The WHERE clause filters
-- on company_identifier.is_current, which means the issuer to company link is
-- the live one rather than a superseded entity succession.
--
-- Counting companies must always use count(DISTINCT company_id). Issuers carry
-- roughly ninety instruments each, so a row count overstates by two orders of
-- magnitude.
CREATE VIEW IF NOT EXISTS corpscout.company_listings AS
SELECT
    c.country_code            AS country_code,
    c.company_id              AS company_id,
    c.issuer_scheme           AS issuer_scheme,
    c.issuer_id               AS issuer_id,
    v.isin                    AS isin,
    v.mic                     AS mic,
    v.operating_mic           AS operating_mic,
    v.ticker                  AS ticker,
    v.cfi_code                AS cfi_code,
    v.cfi_category            AS cfi_category,
    v.instrument_name         AS instrument_name,
    v.instrument_type         AS instrument_type,
    v.trading_currency        AS trading_currency,
    v.trading_status          AS trading_status,
    v.is_current              AS is_current,
    v.admission_date          AS admission_date,
    v.first_trade_date        AS first_trade_date,
    v.termination_date        AS termination_date,
    v.venue_source            AS venue_source,
    v.evidence_tier           AS evidence_tier,
    c.match_method            AS identity_match_method,
    c.match_confidence        AS identity_confidence,
    i.mapping_source          AS issuer_mapping_source
FROM corpscout.instrument_venues AS v
INNER JOIN corpscout.instrument_issuer AS i
    ON i.isin = v.isin
INNER JOIN corpscout.company_identifier AS c
    ON c.issuer_scheme = i.issuer_scheme
   AND c.issuer_id = i.issuer_id
WHERE c.is_current = 1;
