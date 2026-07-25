CREATE DATABASE IF NOT EXISTS corpscout;

-- Which company an issuer identifier belongs to, per country.
-- issuer_scheme and issuer_id lead the sort key because the join always arrives
-- from corpscout.instrument_issuer holding that pair. Country filtering is
-- secondary.
--
-- A row exists only when company_id was found in that country's register. The
-- register is the ground truth: an issuer whose registered_as does not resolve
-- produces no row rather than an unverified guess.
--
-- One company may hold several issuer identifiers over time through entity
-- succession, so is_current distinguishes the live link from superseded ones.
--
-- For a national-identifier scheme the row is self-referential, with issuer_id
-- equal to company_id. That is the cost of one uniform join path, and it
-- disappears for schemes where the issuer identifier is not the company id.
CREATE TABLE IF NOT EXISTS corpscout.company_identifier
(
    issuer_scheme                LowCardinality(String),
    issuer_id                    String,
    country_code                 LowCardinality(String),
    company_id                   String,
    match_method                 LowCardinality(String),
    match_confidence             LowCardinality(String),
    registration_authority_id    LowCardinality(String),
    registered_as_raw            String,
    company_id_normalized        String,
    entity_status                LowCardinality(String),
    registration_status          LowCardinality(String),
    is_current                   UInt8,
    successor_issuer_id          String,
    first_seen_date              Date,
    last_seen_date               Date,
    source_run_id                String,
    resolved_at                  DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY (issuer_scheme, issuer_id, country_code, company_id);
