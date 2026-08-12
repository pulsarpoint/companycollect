CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.lv_company_addresses
(
    country_iso2 LowCardinality(String),
    source_slug LowCardinality(String),
    source_run_id String,
    source_url String,
    regcode String,
    address String,
    postal_code String,
    address_id String,
    region_code LowCardinality(String),
    city_code LowCardinality(String),
    atvk_code LowCardinality(String),
    vzd_address_text Nullable(String),
    vzd_address_postal_code Nullable(String),
    vzd_address_status LowCardinality(Nullable(String)),
    address_city_name Nullable(String),
    address_municipality_name Nullable(String),
    address_latitude Nullable(Float64),
    address_longitude Nullable(Float64),
    has_address UInt8,
    address_fingerprint FixedString(64),
    observation_fingerprint FixedString(64),
    observed_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(observed_at)
PARTITION BY toYear(observed_at)
ORDER BY (regcode, observed_at, observation_fingerprint);

CREATE OR REPLACE VIEW corpscout.lv_company_addresses_current AS
SELECT
    regcode,
    latest.1 AS country_iso2,
    latest.2 AS source_slug,
    latest.3 AS source_run_id,
    latest.4 AS source_url,
    latest.5 AS address,
    latest.6 AS postal_code,
    latest.7 AS address_id,
    latest.8 AS region_code,
    latest.9 AS city_code,
    latest.10 AS atvk_code,
    latest.11 AS vzd_address_text,
    latest.12 AS vzd_address_postal_code,
    latest.13 AS vzd_address_status,
    latest.14 AS address_city_name,
    latest.15 AS address_municipality_name,
    latest.16 AS address_latitude,
    latest.17 AS address_longitude,
    latest.18 AS has_address,
    latest.19 AS address_fingerprint,
    latest.20 AS observation_fingerprint,
    latest.21 AS observed_at,
    toUInt8(1) AS has_observation
FROM
(
    SELECT
        regcode,
        argMax(
            tuple(
                country_iso2,
                source_slug,
                source_run_id,
                source_url,
                address,
                postal_code,
                address_id,
                region_code,
                city_code,
                atvk_code,
                vzd_address_text,
                vzd_address_postal_code,
                vzd_address_status,
                address_city_name,
                address_municipality_name,
                address_latitude,
                address_longitude,
                has_address,
                address_fingerprint,
                observation_fingerprint,
                observed_at
            ),
            tuple(observed_at, source_run_id)
        ) AS latest
    FROM corpscout.lv_company_addresses
    GROUP BY regcode
);

-- The fallback to the legacy lv_companies columns keeps reads valid between
-- applying this additive migration and the first address-history materialization.
CREATE OR REPLACE VIEW corpscout.lv_companies_current AS
SELECT
    c.* EXCEPT (
        address,
        postal_code,
        address_id,
        region_code,
        city_code,
        atvk_code,
        vzd_address_text,
        vzd_address_postal_code,
        vzd_address_status,
        address_city_name,
        address_municipality_name,
        address_latitude,
        address_longitude
    ),
    if(a.has_observation = 1, a.address, c.address) AS address,
    if(a.has_observation = 1, a.postal_code, c.postal_code) AS postal_code,
    if(a.has_observation = 1, a.address_id, c.address_id) AS address_id,
    if(a.has_observation = 1, a.region_code, c.region_code) AS region_code,
    if(a.has_observation = 1, a.city_code, c.city_code) AS city_code,
    if(a.has_observation = 1, a.atvk_code, c.atvk_code) AS atvk_code,
    if(a.has_observation = 1, a.vzd_address_text, c.vzd_address_text) AS vzd_address_text,
    if(
        a.has_observation = 1,
        a.vzd_address_postal_code,
        c.vzd_address_postal_code
    ) AS vzd_address_postal_code,
    if(a.has_observation = 1, a.vzd_address_status, c.vzd_address_status) AS vzd_address_status,
    if(a.has_observation = 1, a.address_city_name, c.address_city_name) AS address_city_name,
    if(
        a.has_observation = 1,
        a.address_municipality_name,
        c.address_municipality_name
    ) AS address_municipality_name,
    if(a.has_observation = 1, a.address_latitude, c.address_latitude) AS address_latitude,
    if(a.has_observation = 1, a.address_longitude, c.address_longitude) AS address_longitude
FROM corpscout.lv_companies AS c
LEFT JOIN corpscout.lv_company_addresses_current AS a ON a.regcode = c.regcode;

CREATE OR REPLACE VIEW corpscout.lv_companies_translated AS
SELECT
    c.*,
    ifNull(act.translated_text, '') AS activity_text_en
FROM corpscout.lv_companies_current AS c
LEFT JOIN (
    SELECT source_text_hash, argMax(translated_text, version) AS translated_text
    FROM corpscout.text_translations
    WHERE source_table = 'corpscout.lv_companies'
      AND source_column = 'activity_text_original'
      AND source_lang = 'lv'
      AND target_lang = 'en'
    GROUP BY source_text_hash
) AS act ON act.source_text_hash = cityHash64(ifNull(c.activity_text_original, ''));
