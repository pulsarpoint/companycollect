{{ config(materialized='table', order_by=['company_id', 'address_type', 'source', 'address_key']) }}

SELECT
    company_id,
    address_fingerprint AS address_key,
    address_type,
    source,
    ifNull(raw_address, '') AS raw_address,
    arrayStringConcat(arrayFilter(value -> value != '', [
        ifNull(care_of, ''), ifNull(street_address, ''),
        trim(concat(ifNull(postal_code, ''), ' ', ifNull(post_town, ''))),
        if(ifNull(country_code, '') IN ('', '{{ var("country_code") }}'), '', country_code)
    ]), ', ') AS display_address,
    normalized_address,
    ifNull(street_address, '') AS street_address,
    ifNull(care_of, '') AS care_of,
    ifNull(postal_code, '') AS postal_code,
    ifNull(post_town, '') AS post_town,
    if(ifNull(country_code, '') = '', '{{ var("country_code") }}', upperUTF8(country_code)) AS resolved_country_code,
    toUInt8(CAST(ifNull(country_code, ''), 'String') NOT IN ('', '{{ var("country_code") }}')) AS is_foreign,
    CAST(NULL, 'Nullable(Float64)') AS latitude,
    CAST(NULL, 'Nullable(Float64)') AS longitude,
    'pending' AS geocode_status,
    '' AS geocode_provider,
    '' AS geocode_precision,
    CAST(NULL, 'Nullable(DateTime64(3, \'UTC\'))') AS geocoded_at,
    source_record_uid,
    now64(3, 'UTC') AS resolved_at
FROM {{ source('corpscout', 'se_company_addresses_current') }}
WHERE has_address = 1 AND has_observation = 1
